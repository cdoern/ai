// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Single request-body processor for the Anthropic create-message operation.
//!
//! Operation identity comes from the request head through the Anthropic
//! Messages registry, so the body is never inspected to decide whether this
//! filter applies. Neither body shape nor `anthropic-version` can override that
//! match: a Chat Completions-shaped body on `/v1/messages` is still the
//! create-message operation, and an Anthropic-shaped body on another path is
//! not.
//!
//! A matched body is deserialized exactly once, and that one parsed value
//! produces every downstream fact: the routing metadata, the promoted headers
//! and filter results, and [`AnthropicMessagesState`]. The forwarded bytes are
//! left untouched; a later filter that transforms them does so explicitly.
//!
//! This filter owns that parse outright. The classifier and validator it
//! replaced each read the same body independently, and the classifier inferred
//! the protocol from body shape with an `anthropic-version` tiebreak — a
//! heuristic the registry makes unnecessary.
//!
//! # YAML
//!
//! ```yaml
//! filter: anthropic_messages_request
//! ```
//!
//! # Full YAML
//!
//! ```yaml
//! filter: anthropic_messages_request
//! on_invalid: reject
//! max_body_bytes: 1048576
//! headers:
//!   format: x-praxis-ai-format
//!   model: x-praxis-ai-model
//!   stream: x-praxis-ai-stream
//! ```

mod config;

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::needless_raw_strings,
    clippy::needless_raw_string_hashes,
    reason = "tests"
)]
mod tests;

use async_trait::async_trait;
use bytes::Bytes;
use praxis_filter::{
    BodyAccess, BodyMode, ErrorResponseFormatterHandle, FilterAction, FilterError, HttpFilter, HttpFilterContext,
    builtins::http::payload_processing::OnInvalidBehavior, parse_filter_config,
};
use tracing::{debug, trace};

use self::config::{AnthropicMessagesRequestConfig, build_config};
use crate::{
    anthropic::{
        routes::{AnthropicMessagesOperation, match_route},
        wire,
    },
    classifier::{AiRequestFormat, ClassifiedRequest, classify_object},
    promotion::is_promotable_value,
};

/// Filter name as configured in a pipeline.
const FILTER_NAME: &str = "anthropic_messages_request";

/// Canonical state for one Anthropic create-message request.
///
/// Stored in request extensions so translation, web search, and guardrail
/// consumers read one parse rather than each deserializing the body again.
#[derive(Clone, Debug)]
pub struct AnthropicMessagesState {
    /// The parsed create-message body.
    ///
    /// Held so message-extracting consumers do not re-parse the forwarded
    /// bytes. The forwarded bytes themselves are never mutated here.
    pub request_body: serde_json::Value,

    /// Extracted `model`, when present.
    pub model: Option<String>,

    /// Extracted `stream`, when present.
    pub stream: Option<bool>,

    /// Extracted `max_tokens`, when present.
    pub max_tokens: Option<u64>,

    /// Whether `tools` is a non-empty array.
    pub has_tools: bool,
}

/// Processes an Anthropic create-message body once and publishes its facts.
///
/// The registry decides which operation this filter owns, so only
/// `POST /v1/messages` is processed. Every other Anthropic Messages operation —
/// token counting, the batch family — is released untouched, as is traffic on
/// any other path.
///
/// Rejects a missing, malformed, or non-object body with an
/// Anthropic-compatible client error. `on_invalid` governs a body that parses
/// but classifies as another protocol.
pub struct AnthropicMessagesRequestFilter {
    /// Parsed and validated configuration.
    config: AnthropicMessagesRequestConfig,
}

impl AnthropicMessagesRequestFilter {
    /// Apply `on_invalid` to a body that fails the create-message envelope rules.
    ///
    /// The two filters this replaced disagreed here, and which answer applied
    /// depended on whether a chain happened to include the validator: the
    /// validator refused a malformed body outright, while the classifier with
    /// `on_invalid: continue` forwarded it and let the backend decide. One
    /// filter cannot do both implicitly, so `on_invalid` selects it — `reject`
    /// for a gateway that refuses malformed payloads, `continue` for a chain
    /// that forwards them unchanged.
    fn handle_bad_envelope(&self, ctx: &mut HttpFilterContext<'_>, reason: &BadEnvelope) -> FilterAction {
        match self.config.on_invalid {
            OnInvalidBehavior::Reject | OnInvalidBehavior::Error => {
                debug!(reason = reason.message(), "rejecting create-message envelope");
                FilterAction::Reject(wire::invalid_request_rejection(reason.message()))
            },
            OnInvalidBehavior::Continue => {
                // The head still identified Anthropic traffic, so the error
                // shape is installed even though the payload is forwarded.
                trace!(reason = reason.message(), "forwarding malformed create-message body");
                install_error_formatter(ctx);
                FilterAction::Release
            },
        }
    }

    /// Create a filter from parsed YAML config.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the YAML config is invalid.
    pub fn from_config(config: &serde_yaml::Value) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: AnthropicMessagesRequestConfig = parse_filter_config(FILTER_NAME, config)?;
        let validated = build_config(cfg)?;
        Ok(Box::new(Self { config: validated }))
    }
}

#[async_trait]
impl HttpFilter for AnthropicMessagesRequestFilter {
    fn name(&self) -> &'static str {
        "anthropic_messages_request"
    }

    fn request_body_access(&self) -> BodyAccess {
        BodyAccess::ReadOnly
    }

    fn request_body_mode(&self) -> BodyMode {
        BodyMode::StreamBuffer {
            max_bytes: Some(self.config.max_body_bytes),
        }
    }

    async fn on_request(&self, _ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        Ok(FilterAction::Continue)
    }

    async fn on_request_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<FilterAction, FilterError> {
        if !end_of_stream {
            return Ok(FilterAction::Continue);
        }

        if !is_create_message(ctx) {
            trace!(
                method = %ctx.request.method,
                path = ctx.request.uri.path(),
                "not the Anthropic create-message operation"
            );
            return Ok(FilterAction::Release);
        }

        let parsed = match parse_envelope(body) {
            Ok(value) => value,
            Err(reason) => return Ok(self.handle_bad_envelope(ctx, &reason)),
        };

        // One parse feeds classification and state alike.
        //
        // The head already decided this is the Anthropic create-message
        // operation, so the endpoint is the authority on protocol: the format
        // is Anthropic Messages whatever the payload's field mix resembles. The
        // classifier this replaced had to guess from body shape and break ties
        // on `anthropic-version`, which labelled an ordinary Anthropic body —
        // `messages` with no Responses or Conversations markers — as Chat
        // Completions.
        let mut classified = classify_object(parsed.as_object().unwrap_or(&EMPTY_OBJECT));
        classified.format = AiRequestFormat::AnthropicMessages;

        debug!(model = ?classified.model, "processed anthropic create-message body");

        install_error_formatter(ctx);
        write_metadata(ctx, &classified);
        promote_headers(ctx, &classified, &self.config);
        promote_filter_results(ctx, &classified)?;
        insert_state(ctx, parsed, &classified);

        Ok(FilterAction::Release)
    }
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

/// Whether the request head resolves to the Anthropic create-message operation.
///
/// The registry is consulted directly rather than through the published
/// [`AiOperationMatch`] extension, because a head-phase extension is not visible
/// during `StreamBuffer` pre-read. Both read the same registry, so the operation
/// identity is identical either way.
///
/// [`AiOperationMatch`]: crate::operation_classifier::AiOperationMatch
fn is_create_message(ctx: &HttpFilterContext<'_>) -> bool {
    match_route(ctx.request.method.as_str(), ctx.request.uri.path())
        .is_some_and(|route| route.spec.operation == AnthropicMessagesOperation::CreateMessage)
}

/// Install the Anthropic error response formatter.
///
/// The head already identified this as Anthropic traffic, so the formatter is
/// installed for every matched create-message request rather than depending on
/// what the body turned out to contain.
fn install_error_formatter(ctx: &mut HttpFilterContext<'_>) {
    let formatter =
        crate::anthropic::error_response_formatter::AnthropicErrorFormatter::from_request_headers(&ctx.request.headers);
    ctx.extensions.insert(ErrorResponseFormatterHandle::new(formatter));
}

/// An empty object, so classifying a forwarded malformed body has a
/// well-defined input without allocating per request.
static EMPTY_OBJECT: std::sync::LazyLock<serde_json::Map<String, serde_json::Value>> =
    std::sync::LazyLock::new(serde_json::Map::new);

/// Why a create-message envelope could not be accepted.
enum BadEnvelope {
    /// No body, or an empty one.
    Missing,
    /// Present but not parseable as JSON.
    NotJson,
    /// Valid JSON that is not a top-level object.
    NotObject,
}

impl BadEnvelope {
    /// Client-facing reason text.
    const fn message(&self) -> &'static str {
        match *self {
            Self::Missing => "request body is required",
            Self::NotJson => "request body is not valid JSON",
            Self::NotObject => "request body is not a JSON object",
        }
    }
}

/// Parse the create-message envelope, naming the first rule it breaks.
fn parse_envelope(body: &Option<Bytes>) -> Result<serde_json::Value, BadEnvelope> {
    let bytes = body
        .as_deref()
        .filter(|chunk| !chunk.is_empty())
        .ok_or(BadEnvelope::Missing)?;
    let parsed = serde_json::from_slice::<serde_json::Value>(bytes).map_err(|error| {
        debug!(%error, "create-message body is not valid JSON");
        BadEnvelope::NotJson
    })?;
    if parsed.is_object() {
        Ok(parsed)
    } else {
        Err(BadEnvelope::NotObject)
    }
}

/// Write durable routing metadata.
fn write_metadata(ctx: &mut HttpFilterContext<'_>, classified: &ClassifiedRequest) {
    ctx.set_metadata("anthropic_messages_request.format", classified.format.as_str());

    if let Some(model) = &classified.model
        && is_promotable_value(model)
    {
        ctx.set_metadata("anthropic_messages_request.model", model.clone());
    }
    if let Some(stream) = classified.stream {
        ctx.set_metadata(
            "anthropic_messages_request.stream",
            if stream { "true" } else { "false" },
        );
    }
    if let Some(max_tokens) = classified.max_tokens {
        ctx.set_metadata("anthropic_messages_request.max_tokens", max_tokens.to_string());
    }
    if classified.has_tools {
        ctx.set_metadata("anthropic_messages_request.has_tools", "true");
    }
}

/// Promote the configured routing headers.
fn promote_headers(
    ctx: &mut HttpFilterContext<'_>,
    classified: &ClassifiedRequest,
    config: &AnthropicMessagesRequestConfig,
) {
    if let Some(name) = &config.headers.format
        && let Ok(value) = http::HeaderValue::from_str(classified.format.as_str())
        && let Ok(header) = http::HeaderName::try_from(name.as_str())
    {
        ctx.request_headers_to_set.push((header, value));
    }
    if let Some(name) = &config.headers.model
        && let Some(model) = &classified.model
        && is_promotable_value(model)
        && let Ok(value) = http::HeaderValue::from_str(model)
        && let Ok(header) = http::HeaderName::try_from(name.as_str())
    {
        ctx.request_headers_to_set.push((header, value));
    }
    if let Some(name) = &config.headers.stream
        && let Some(stream) = classified.stream
        && let Ok(value) = http::HeaderValue::from_str(if stream { "true" } else { "false" })
        && let Ok(header) = http::HeaderName::try_from(name.as_str())
    {
        ctx.request_headers_to_set.push((header, value));
    }
}

/// Publish filter results for `on_result` branch conditions.
///
/// # Errors
///
/// Returns [`FilterError`] when a filter result cannot be published.
fn promote_filter_results(ctx: &mut HttpFilterContext<'_>, classified: &ClassifiedRequest) -> Result<(), FilterError> {
    let results = ctx.filter_results.entry(FILTER_NAME).or_default();

    results.set("format", classified.format.as_str())?;

    if let Some(model) = &classified.model
        && is_promotable_value(model)
    {
        results.set("model", model.clone())?;
    }
    if let Some(stream) = classified.stream {
        results.set("stream", if stream { "true" } else { "false" })?;
    }
    if let Some(max_tokens) = classified.max_tokens {
        results.set("max_tokens", max_tokens.to_string())?;
    }
    if classified.has_tools {
        results.set("has_tools", "true")?;
    }

    Ok(())
}

/// Insert the canonical state derived from the one parse.
fn insert_state(ctx: &mut HttpFilterContext<'_>, parsed: serde_json::Value, classified: &ClassifiedRequest) {
    ctx.extensions.insert(AnthropicMessagesState {
        request_body: parsed,
        model: classified.model.clone(),
        stream: classified.stream,
        max_tokens: classified.max_tokens,
        has_tools: classified.has_tools,
    });
}

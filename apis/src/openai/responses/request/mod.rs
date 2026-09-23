// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Single request-body processor for the Responses create operation.
//!
//! Operation identity comes from the request head through the Responses
//! registry, so the body is never inspected to decide whether this filter
//! applies. A matched create request is then deserialized exactly once, and
//! that one parsed value produces every downstream fact: the classification
//! metadata, the promoted headers and filter results, the proxy-owned
//! identifiers, and [`ResponsesState`].
//!
//! Create requests with `background=true` are rejected, because Praxis does not
//! implement the asynchronous Responses lifecycle.
//!
//! This replaces the pair of `openai_responses_format` and
//! `openai_responses_validate` for create requests. Those two each parsed the
//! same body independently, so routing facts, proxy-owned defaults, and state
//! could be derived from different parses of one request.
//!
//! Metadata and filter results keep the `openai_responses_format` namespace.
//! Twelve downstream filters read those keys, and renaming them is a separate
//! change rather than a side effect of consolidating the parse.
//!
//! # YAML
//!
//! ```yaml
//! filter: openai_responses_request
//! on_invalid: reject
//! headers:
//!   format: x-praxis-ai-format
//!   model: x-praxis-ai-model
//!   stream: x-praxis-ai-stream
//! ```

#[cfg(test)]
mod tests;

use async_trait::async_trait;
use bytes::Bytes;
use praxis_filter::{
    FilterAction, FilterError, HttpFilter, HttpFilterContext,
    body::{BodyAccess, BodyMode, MAX_JSON_BODY_BYTES},
    parse_filter_config,
};
use tracing::{debug, trace};

use super::{
    config::{ResponsesFormatConfig, build_config},
    error::{responses_error_rejection, responses_error_rejection_with_code},
    extract_conversation_id,
    routes::{self as responses_routes, ResponsesOperation},
    state::ResponsesState,
};
use crate::{
    classifier::{AiRequestFormat, ClassifiedRequest, classify_object},
    operation::Transport,
};

/// Filter name as configured in a pipeline.
const FILTER_NAME: &str = "openai_responses_request";

/// Processes the Responses create request body once and initializes state.
///
/// Replaces the `openai_responses_format` and `openai_responses_validate` pair
/// for create requests. Configuration is unchanged from
/// `openai_responses_format`, so a chain that ran both swaps them for this one
/// filter and keeps the same `on_invalid` and `headers` settings.
///
/// The operation is recognized from the request head, so only `POST
/// /v1/responses` is processed. Every other request — including Conversations
/// API traffic and the `WebSocket` handshake at the same path — is released
/// untouched, and `on_invalid` governs only bodies that fail to parse.
///
/// Rejects `background=true` with a 400, matching `openai_responses_format`,
/// because Praxis does not implement the asynchronous Responses lifecycle.
///
/// Promotes `openai_responses_format.*` metadata and filter results, and
/// generates `responses.response_id` (`resp_` + 32 hex chars, CSPRNG),
/// `responses.conversation_id`, `responses.store`, `responses.background`, and
/// `responses.stream`.
pub struct OpenaiResponsesRequestFilter {
    /// Classification and promotion configuration.
    config: ResponsesFormatConfig,
}

impl OpenaiResponsesRequestFilter {
    /// Create the filter from parsed YAML configuration.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] when configuration is invalid.
    pub fn from_config(config: &serde_yaml::Value) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: ResponsesFormatConfig = parse_filter_config(FILTER_NAME, config)?;
        let validated = build_config(FILTER_NAME, cfg)?;
        Ok(Box::new(Self { config: validated }))
    }
}

#[async_trait]
impl HttpFilter for OpenaiResponsesRequestFilter {
    fn name(&self) -> &'static str {
        "openai_responses_request"
    }

    fn request_body_access(&self) -> BodyAccess {
        BodyAccess::ReadOnly
    }

    fn request_body_mode(&self) -> BodyMode {
        BodyMode::StreamBuffer {
            max_bytes: Some(MAX_JSON_BODY_BYTES),
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

        if !is_create_response(ctx) {
            trace!(
                method = %ctx.request.method,
                path = ctx.request.uri.path(),
                "not the Responses create operation"
            );
            return Ok(FilterAction::Release);
        }

        let parsed = match parse_request_body(body, &self.config) {
            Ok(value) => value,
            Err(action) => return Ok(action),
        };

        let Some(obj) = parsed.as_object() else {
            debug!("rejecting create request whose body is not a JSON object");
            return Ok(reject_invalid("request body must be a JSON object"));
        };

        // The one parse feeds classification, promotion, and state alike.
        let mut classified = classify_object(obj);

        // The matched operation is authoritative. A valid create body may omit
        // every discriminator the body heuristics look for — `{"model":"gpt-5"}`
        // is a legitimate create request — and would otherwise be published as
        // `unknown`, which makes downstream Responses filters skip it and lets
        // `background: true` past its rejection. Only unknowns are upgraded, so
        // a body positively identified as another format keeps that identity.
        if classified.format == AiRequestFormat::UnknownJson {
            classified.format = AiRequestFormat::Responses;
        }

        if let Some(action) = super::handle_unsupported_background(&classified) {
            return Ok(action);
        }

        if let Some(action) = reject_conflicting_history_selectors(&parsed) {
            return Ok(action);
        }

        publish_request_facts(ctx, &classified, parsed, &self.config)?;

        Ok(FilterAction::Release)
    }
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

/// Publish everything the one parse produced.
///
/// Kept out of `on_request_body` so the filter entry point stays a readable
/// sequence of guards.
///
/// # Errors
///
/// Returns [`FilterError`] when a filter result cannot be published.
fn publish_request_facts(
    ctx: &mut HttpFilterContext<'_>,
    classified: &ClassifiedRequest,
    parsed: serde_json::Value,
    config: &ResponsesFormatConfig,
) -> Result<(), FilterError> {
    let response_id = format!("resp_{}", ctx.id_generator.generate(ctx.time_source));
    let conversation_id = resolve_conversation_id(ctx, &parsed);
    let mode = super::compute_mode(classified);

    super::install_error_formatter(ctx, classified.format);
    super::write_metadata(ctx, classified, mode);
    super::promote_headers(ctx, classified, config, mode);
    super::promote_filter_results(ctx, classified, mode)?;

    enrich_context(ctx, classified, &response_id, &conversation_id);
    insert_responses_state(ctx, parsed, &response_id);

    debug!(
        response_id = %response_id,
        conversation_id = %conversation_id,
        mode = ?mode,
        "create request processed and state initialized"
    );

    Ok(())
}

/// Whether this request is the Responses create operation.
///
/// Resolved from the request head through the shared registry — the same source
/// of truth the `openai_operation` classifier uses — so no body heuristic
/// decides whether this filter applies, and the filter works whether or not the
/// classifier is present in the chain.
fn is_create_response(ctx: &HttpFilterContext<'_>) -> bool {
    responses_routes::match_route(ctx.request.method.as_str(), ctx.request.uri.path(), Transport::Http)
        .is_some_and(|route| route.spec.operation == ResponsesOperation::CreateResponse)
}

/// Parse the create request body as JSON.
///
/// A body that cannot be parsed is not a classification failure the backend can
/// resolve, so `on_invalid` governs whether it is rejected here or forwarded.
fn parse_request_body(body: &Option<Bytes>, config: &ResponsesFormatConfig) -> Result<serde_json::Value, FilterAction> {
    let Some(chunk) = body.as_deref() else {
        debug!("rejecting create request with missing body");
        return Err(reject_invalid("request body is required"));
    };

    match serde_json::from_slice(chunk) {
        Ok(value) => Ok(value),
        Err(error) => {
            debug!(error = %error, "failed to parse create request body");
            Err(super::handle_invalid_format(AiRequestFormat::InvalidJson, config).unwrap_or(FilterAction::Release))
        },
    }
}

/// Reject requests that select both supported sources of conversation history.
fn reject_conflicting_history_selectors(body: &serde_json::Value) -> Option<FilterAction> {
    let conflicts = body.get("previous_response_id").is_some_and(|value| !value.is_null())
        && body.get("conversation").is_some_and(|value| !value.is_null());
    conflicts.then(|| {
        FilterAction::Reject(responses_error_rejection_with_code(
            400,
            "invalid_request_error",
            "mutually_exclusive_parameters",
            "Mutually exclusive parameters. Ensure you are only providing one of: 'previous_response_id' or 'conversation'.",
        ))
    })
}

/// Build a 400 rejection with a Responses API error body.
fn reject_invalid(message: &str) -> FilterAction {
    FilterAction::Reject(responses_error_rejection(400, "invalid_request_error", message))
}

/// Extract or generate a conversation ID for the request.
fn resolve_conversation_id(ctx: &HttpFilterContext<'_>, body: &serde_json::Value) -> String {
    if let Some(id) = extract_conversation_id(body) {
        trace!(conversation_id = %id, "conversation ID extracted from request");
        id
    } else {
        let id = format!("conv_{}", ctx.id_generator.generate(ctx.time_source));
        trace!(conversation_id = %id, "conversation ID generated");
        id
    }
}

/// Publish validated request facts for downstream filters.
///
/// Reads the parsed classification directly rather than round-tripping through
/// classifier metadata, so these values cannot disagree with the body they came
/// from. Spec defaults apply: `store` defaults to true, the others to false.
fn enrich_context(
    ctx: &mut HttpFilterContext<'_>,
    classified: &ClassifiedRequest,
    response_id: &str,
    conversation_id: &str,
) {
    ctx.set_metadata("responses.response_id", response_id);
    ctx.set_metadata("responses.conversation_id", conversation_id);

    let store = classified.store.unwrap_or(true);
    let background = classified.background.unwrap_or(false);
    let stream = classified.stream.unwrap_or(false);

    ctx.set_metadata("responses.store", if store { "true" } else { "false" });
    ctx.set_metadata("responses.background", if background { "true" } else { "false" });
    ctx.set_metadata("responses.stream", if stream { "true" } else { "false" });

    trace!(store, background, stream, "request facts published");
}

/// Initialize canonical request state, including metadata that must survive IRR steps.
fn insert_responses_state(ctx: &mut HttpFilterContext<'_>, parsed: serde_json::Value, response_id: &str) {
    let mut state = ResponsesState::from_request_body(parsed);
    state.response_id = Some(response_id.to_owned());
    ctx.extensions.insert(state);
}

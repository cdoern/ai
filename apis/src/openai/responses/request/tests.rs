// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Unit tests for the consolidated Responses create request processor.

#![expect(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use bytes::Bytes;
use praxis_filter::{
    FilterAction, HttpFilter, HttpFilterContext, Request,
    body::{BodyAccess, BodyMode},
};
use serde_json::json;

use super::*;
use crate::{
    openai::responses::state::ResponsesState,
    test_utils::{make_filter_context, make_request},
};

/// Build the filter from YAML, defaulting to an empty mapping.
fn filter(yaml: &str) -> Box<dyn HttpFilter> {
    let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
    OpenaiResponsesRequestFilter::from_config(&value).unwrap()
}

/// Build a filter with default configuration.
fn default_filter() -> Box<dyn HttpFilter> {
    filter("{}")
}

/// Build a `POST /v1/responses` request.
fn create_request() -> Request {
    make_request(http::Method::POST, "/v1/responses")
}

/// Drive one body through the filter and return the action.
async fn run(filter: &dyn HttpFilter, request: &Request, body: &serde_json::Value) -> FilterAction {
    let mut ctx = make_filter_context(request);
    let mut bytes = Some(Bytes::from(serde_json::to_vec(body).unwrap()));
    filter.on_request_body(&mut ctx, &mut bytes, true).await.unwrap()
}

/// Drive one streaming create request and return its context.
async fn run_streaming_create<'a>(filter: &dyn HttpFilter, request: &'a Request) -> HttpFilterContext<'a> {
    let mut ctx = make_filter_context(request);
    let mut body = Some(Bytes::from(
        serde_json::to_vec(&json!({"model": "gpt-4.1", "input": "hi", "stream": true})).unwrap(),
    ));

    let action = filter.on_request_body(&mut ctx, &mut body, true).await.unwrap();
    assert!(matches!(action, FilterAction::Release));
    ctx
}

#[tokio::test]
async fn a_create_request_publishes_classification_metadata() {
    let filter = default_filter();
    let request = create_request();
    let ctx = run_streaming_create(filter.as_ref(), &request).await;

    // Published under the namespace downstream filters already read.
    assert_eq!(
        ctx.filter_metadata
            .get("openai_responses_format.format")
            .map(String::as_str),
        Some("openai_responses")
    );
    assert_eq!(
        ctx.filter_metadata
            .get("openai_responses_format.stream")
            .map(String::as_str),
        Some("true")
    );
}

#[tokio::test]
async fn a_create_request_publishes_validated_facts_and_state_from_one_parse() {
    let filter = default_filter();
    let request = create_request();
    let ctx = run_streaming_create(filter.as_ref(), &request).await;

    // Derived from the same parse rather than round-tripped through metadata.
    assert_eq!(
        ctx.filter_metadata.get("responses.stream").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        ctx.filter_metadata.get("responses.store").map(String::as_str),
        Some("true"),
        "store defaults to true per the OpenAI specification"
    );
    assert!(
        ctx.filter_metadata
            .get("responses.response_id")
            .is_some_and(|id| id.starts_with("resp_")),
        "a proxy-owned response ID is generated"
    );

    let state = ctx.extensions.get::<ResponsesState>().expect("state initialized");
    assert!(state.response_id.as_ref().is_some_and(|id| id.starts_with("resp_")));
}

/// Classification once moved `model` out of the parsed value, which a shared
/// parse would forward upstream as an empty string.
#[tokio::test]
async fn classification_leaves_the_body_intact_for_state() {
    let filter = default_filter();
    let request = create_request();
    let mut ctx = make_filter_context(&request);
    let mut body = Some(Bytes::from(
        serde_json::to_vec(&json!({"model": "gpt-4.1", "input": "hi"})).unwrap(),
    ));

    drop(filter.on_request_body(&mut ctx, &mut body, true).await.unwrap());

    let state = ctx.extensions.get::<ResponsesState>().expect("state initialized");
    assert_eq!(
        state.request_body.get("model").and_then(serde_json::Value::as_str),
        Some("gpt-4.1"),
        "state must retain the model the client sent"
    );
}

#[tokio::test]
async fn a_non_create_responses_operation_is_left_alone() {
    let filter = default_filter();
    let request = make_request(http::Method::GET, "/v1/responses/resp_123");
    let mut ctx = make_filter_context(&request);
    let mut body = None;

    let action = filter.on_request_body(&mut ctx, &mut body, true).await.unwrap();

    assert!(matches!(action, FilterAction::Release));
    assert!(
        ctx.extensions.get::<ResponsesState>().is_none(),
        "a bodyless operation must not initialize create state"
    );
    assert!(
        !ctx.filter_metadata.contains_key("openai_responses_format.format"),
        "identity comes from the request head, so nothing is published here"
    );
}

#[tokio::test]
async fn a_non_responses_path_is_left_alone() {
    let filter = default_filter();
    let request = make_request(http::Method::POST, "/v1/chat/completions");
    let mut ctx = make_filter_context(&request);
    let mut body = Some(Bytes::from(
        serde_json::to_vec(&json!({"model": "gpt-4.1", "messages": []})).unwrap(),
    ));

    let action = filter.on_request_body(&mut ctx, &mut body, true).await.unwrap();

    assert!(matches!(action, FilterAction::Release));
    assert!(
        ctx.extensions.get::<ResponsesState>().is_none(),
        "a Chat Completions body must not enter Responses processing"
    );
}

#[tokio::test]
async fn conflicting_history_selectors_are_rejected() {
    let filter = default_filter();
    let request = create_request();
    let action = run(
        filter.as_ref(),
        &request,
        &json!({"model": "m", "input": "hi", "previous_response_id": "resp_1", "conversation": "conv_1"}),
    )
    .await;

    assert!(
        matches!(action, FilterAction::Reject(_)),
        "previous_response_id and conversation are mutually exclusive"
    );
}

#[tokio::test]
async fn background_mode_is_rejected_before_upstream_contact() {
    let filter = default_filter();
    let request = create_request();
    let action = run(
        filter.as_ref(),
        &request,
        &json!({"model": "gpt-4.1", "input": "hi", "background": true}),
    )
    .await;

    assert!(
        matches!(action, FilterAction::Reject(_)),
        "Praxis does not implement the asynchronous Responses lifecycle"
    );
}

#[tokio::test]
async fn a_missing_body_is_rejected() {
    let filter = default_filter();
    let request = create_request();
    let mut ctx = make_filter_context(&request);
    let mut body = None;

    let action = filter.on_request_body(&mut ctx, &mut body, true).await.unwrap();

    assert!(matches!(action, FilterAction::Reject(_)), "create requires a body");
}

#[tokio::test]
async fn a_non_object_body_is_rejected() {
    let filter = default_filter();
    let request = create_request();
    let action = run(filter.as_ref(), &request, &json!("just a string")).await;

    assert!(
        matches!(action, FilterAction::Reject(_)),
        "a create body must be a JSON object"
    );
}

#[tokio::test]
async fn store_and_background_defaults_follow_the_specification() {
    let filter = default_filter();
    let request = create_request();
    let mut ctx = make_filter_context(&request);
    let mut body = Some(Bytes::from(
        serde_json::to_vec(&json!({"model": "gpt-4.1", "input": "hi"})).unwrap(),
    ));

    drop(filter.on_request_body(&mut ctx, &mut body, true).await.unwrap());

    assert_eq!(
        ctx.filter_metadata.get("responses.store").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        ctx.filter_metadata.get("responses.background").map(String::as_str),
        Some("false")
    );
    assert_eq!(
        ctx.filter_metadata.get("responses.stream").map(String::as_str),
        Some("false")
    );
}

#[test]
fn the_filter_declares_bounded_buffering() {
    let filter = default_filter();
    assert_eq!(filter.request_body_access(), BodyAccess::ReadOnly);
    assert!(matches!(
        filter.request_body_mode(),
        BodyMode::StreamBuffer { max_bytes: Some(_) }
    ));
}

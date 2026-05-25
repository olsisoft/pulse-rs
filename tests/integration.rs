//! Offline integration tests for the Pulse Rust client.
//!
//! Every test uses [`wiremock`] to spin up a real HTTP server on a random
//! port and return canned responses. The point is to pin the wire format
//! the client speaks against the Pulse OpenAPI spec, not to exercise a
//! real Pulse server.

use futures_util::{SinkExt, StreamExt};
use pulse_client::{
    derive_ws_url, iq_and, iq_leaf, iq_or, IQQueryOptions, IQScanOptions, MlPredictOptions,
    ModelUpload, PulseClient, PulseError,
};
use serde_json::json;
use wiremock::matchers::{
    body_string_contains, header, header_exists, method, path, query_param, query_param_is_missing,
};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn start_server() -> MockServer {
    MockServer::start().await
}

fn new_client(server: &MockServer, token: Option<&str>) -> PulseClient {
    let mut builder = PulseClient::builder().base_url(server.uri());
    if let Some(t) = token {
        builder = builder.token(t);
    }
    builder.build().expect("builder")
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_is_mutable() {
    let server = start_server().await;
    let client = new_client(&server, None);
    assert!(client.token().is_none());
    client.set_token("abc");
    assert_eq!(client.token().as_deref(), Some("abc"));
    client.clear_token();
    assert!(client.token().is_none());
}

#[tokio::test]
async fn missing_base_url_fails() {
    let err = PulseClient::builder().build().unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

#[tokio::test]
async fn empty_base_url_fails() {
    let err = PulseClient::builder().base_url("").build().unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

#[tokio::test]
async fn base_url_trailing_slashes_stripped() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "version": "2.6.0" })))
        .mount(&server)
        .await;

    let trailing = format!("{}//", server.uri());
    let client = PulseClient::builder().base_url(trailing).build().unwrap();
    let v = client.version().await.unwrap();
    assert_eq!(v["version"], "2.6.0");
}

// ---------------------------------------------------------------------------
// Version (public)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn version_is_public_no_token() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/version"))
        .and(query_param_is_missing("token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "version": "2.6.0", "edition": "desktop" })),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, None);
    assert!(client.token().is_none());
    let info = client.version().await.unwrap();
    assert_eq!(info["edition"], "desktop");
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn login_caches_token() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/auth/login"))
        .and(body_string_contains(r#""alice""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "token": "new.jwt.token",
            "refreshToken": "refresh.token",
            "activeOrg": { "id": "org1", "name": "Acme" }
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, None);
    let response = client.auth().login("alice", "secret").await.unwrap();
    assert_eq!(client.token().as_deref(), Some("new.jwt.token"));
    assert_eq!(response["refreshToken"], "refresh.token");
}

#[tokio::test]
async fn login_failure_raises_auth_error_no_token_cached() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/auth/login"))
        .respond_with(
            ResponseTemplate::new(401).set_body_json(json!({ "error": "Invalid credentials" })),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, None);
    let err = client.auth().login("alice", "wrong").await.unwrap_err();
    assert!(err.is_auth_error());
    assert!(format!("{err}").contains("Invalid credentials"));
    assert!(client.token().is_none());
}

#[tokio::test]
async fn refresh_caches_new_token() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/auth/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "token": "refreshed.jwt" })))
        .mount(&server)
        .await;

    let client = new_client(&server, None);
    client.auth().refresh("rtok").await.unwrap();
    assert_eq!(client.token().as_deref(), Some("refreshed.jwt"));
}

#[tokio::test]
async fn organizations_unwraps_envelope() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/auth/organizations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "organizations": [{ "id": "o1", "name": "Acme" }]
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let orgs = client.auth().organizations().await.unwrap();
    assert_eq!(orgs.len(), 1);
    assert_eq!(orgs[0]["id"], "o1");
}

#[tokio::test]
async fn switch_org_caches_new_token() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/auth/switch-org"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "token": "switched.jwt" })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    client.auth().switch_org("org2").await.unwrap();
    assert_eq!(client.token().as_deref(), Some("switched.jwt"));
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipelines_list_unwraps_envelope() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "pipelines": [
                { "id": "p1", "name": "demo" },
                { "id": "p2", "name": "fraud" }
            ]
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let pipelines = client.pipelines().list().await.unwrap();
    assert_eq!(pipelines.len(), 2);
    assert_eq!(pipelines[0]["id"], "p1");
}

#[tokio::test]
async fn pipelines_list_returns_empty_on_missing_envelope() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let pipelines = client.pipelines().list().await.unwrap();
    assert!(pipelines.is_empty());
}

#[tokio::test]
async fn pipelines_get_returns_one() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines/p1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "id": "p1", "name": "demo" })),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client.pipelines().get("p1").await.unwrap();
    assert_eq!(result["id"], "p1");
}

#[tokio::test]
async fn pipelines_get_missing_raises_not_found() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines/nope"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({ "error": "not found" })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client.pipelines().get("nope").await.unwrap_err();
    assert!(err.is_not_found());
    assert_eq!(err.status_code(), Some(404));
}

#[tokio::test]
async fn pipelines_create_returns_created() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/pulse/pipelines"))
        .and(body_string_contains(r#""name":"new""#))
        .respond_with(
            ResponseTemplate::new(201).set_body_json(json!({ "id": "p3", "name": "new" })),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .pipelines()
        .create(&json!({
            "name": "new",
            "nodes": [{ "id": "n1", "type": "source" }]
        }))
        .await
        .unwrap();
    assert_eq!(result["id"], "p3");
}

#[tokio::test]
async fn pipelines_create_validation_raises_validation_error() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/pulse/pipelines"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "Missing required field: nodes"
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client
        .pipelines()
        .create(&json!({ "name": "bad" }))
        .await
        .unwrap_err();
    assert!(err.is_validation_error());
}

#[tokio::test]
async fn pipelines_delete_204_returns_ok() {
    let server = start_server().await;
    Mock::given(method("DELETE"))
        .and(path("/api/pulse/pipelines/p1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    client.pipelines().delete("p1").await.unwrap();
}

#[tokio::test]
async fn path_params_are_url_encoded() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines/foo%2Fbar"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "foo/bar" })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client.pipelines().get("foo/bar").await.unwrap();
    assert_eq!(result["id"], "foo/bar");
}

// ---------------------------------------------------------------------------
// Agents + Templates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn agents_list_unwraps_envelope() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/agents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agents": [
                { "id": "a1", "name": "fraud-detector", "engineType": "streaming" }
            ]
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let agents = client.agents().list().await.unwrap();
    assert_eq!(agents[0]["engineType"], "streaming");
}

#[tokio::test]
async fn agents_get_returns_one() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/agents/a1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "id": "a1", "name": "fraud-detector" })),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client.agents().get("a1").await.unwrap();
    assert_eq!(result["id"], "a1");
}

// ---- B-115 Phase 1: agents.update + agents.delete ----

#[tokio::test]
async fn agents_update_puts_full_config_and_returns_fresh_snapshot() {
    let server = start_server().await;
    Mock::given(method("PUT"))
        .and(path("/api/pulse/agents/a1"))
        .and(body_string_contains(r#""name":"fraud-detector-v2""#))
        .and(body_string_contains(r#""engineType":"rule-based""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "a1",
            "name": "fraud-detector-v2",
            "engineType": "rule-based",
            "status": "running",
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let new_config = json!({
        "name": "fraud-detector-v2",
        "engineType": "rule-based",
        "config": {
            "rules": [{"if": "amount > 5000", "then": "block"}]
        }
    });
    let result = client.agents().update("a1", &new_config).await.unwrap();
    assert_eq!(result["name"], "fraud-detector-v2");

    let received = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(body["engineType"], "rule-based");
    assert_eq!(body["config"]["rules"][0]["if"], "amount > 5000");
}

#[tokio::test]
async fn agents_update_raises_validation_on_self_loop_400() {
    let server = start_server().await;
    Mock::given(method("PUT"))
        .and(path("/api/pulse/agents/a1"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "Agent would self-loop: outputTopic == inputTopic",
            "unsafeFields": ["outputTopic"]
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let bad = json!({"name": "x", "inputTopic": "t", "outputTopic": "t"});
    let err = client.agents().update("a1", &bad).await.unwrap_err();
    match err {
        PulseError::Validation { body, .. } => {
            let msg = body.unwrap()["error"].as_str().unwrap().to_string();
            assert!(msg.contains("self-loop"), "got: {msg}");
        }
        other => panic!("expected Validation, got {other:?}"),
    }
}

#[tokio::test]
async fn agents_update_raises_not_found_on_missing_agent() {
    let server = start_server().await;
    Mock::given(method("PUT"))
        .and(path("/api/pulse/agents/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": "Agent not found: missing"
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client
        .agents()
        .update("missing", &json!({"name": "x"}))
        .await
        .unwrap_err();
    assert!(err.is_not_found());
}

#[tokio::test]
async fn agents_update_url_encodes_agent_id() {
    let server = start_server().await;
    Mock::given(method("PUT"))
        .and(path("/api/pulse/agents/tenant%2Fagent"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"id": "tenant/agent", "name": "x"})),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .agents()
        .update("tenant/agent", &json!({"name": "x"}))
        .await
        .unwrap();
    assert_eq!(result["id"], "tenant/agent");
}

#[tokio::test]
async fn agents_delete_returns_ok_on_204() {
    let server = start_server().await;
    Mock::given(method("DELETE"))
        .and(path("/api/pulse/agents/a1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    client.agents().delete("a1").await.unwrap();
}

#[tokio::test]
async fn agents_delete_raises_not_found() {
    let server = start_server().await;
    Mock::given(method("DELETE"))
        .and(path("/api/pulse/agents/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "Agent not found"})))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client.agents().delete("missing").await.unwrap_err();
    assert!(err.is_not_found());
}

#[tokio::test]
async fn agents_update_without_token_raises_no_token_synchronously() {
    let server = start_server().await;
    // No Mock — verify the no-token check fires before any HTTP call.
    let client = new_client(&server, None);
    let err = client
        .agents()
        .update("a1", &json!({"name": "x"}))
        .await
        .unwrap_err();
    assert!(matches!(err, PulseError::NoToken { .. }));
}

#[tokio::test]
async fn agents_delete_without_token_raises_no_token_synchronously() {
    let server = start_server().await;
    let client = new_client(&server, None);
    let err = client.agents().delete("a1").await.unwrap_err();
    assert!(matches!(err, PulseError::NoToken { .. }));
}

#[tokio::test]
async fn templates_list_unwraps_envelope() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/templates"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "templates": [{ "id": "fraud-detection", "name": "Fraud Detection" }]
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let templates = client.templates().list().await.unwrap();
    assert_eq!(templates[0]["id"], "fraud-detection");
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_token_raises_no_token_error_before_any_http_call() {
    let server = start_server().await;
    // NB: no Mock::given(...) registered — wiremock returns 404 for unmatched
    // requests, but if we behaved correctly the client never reaches the wire.
    let client = new_client(&server, None);
    let err = client.pipelines().list().await.unwrap_err();
    assert!(err.is_auth_error());
    assert!(matches!(err, PulseError::NoToken { .. }));
}

#[tokio::test]
async fn rate_limit_parses_retry_after_from_body() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines"))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({
            "error": "Rate limit exceeded",
            "errorCode": "RATE_LIMITED",
            "retryAfterSeconds": 60,
            "limit": 120,
            "remaining": 0
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client.pipelines().list().await.unwrap_err();
    match err {
        PulseError::RateLimit {
            retry_after_seconds,
            ..
        } => assert_eq!(retry_after_seconds, Some(60)),
        other => panic!("expected RateLimit, got {other:?}"),
    }
}

#[tokio::test]
async fn rate_limit_falls_back_to_retry_after_header() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "30")
                .set_body_string("Too Many Requests"),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client.pipelines().list().await.unwrap_err();
    match err {
        PulseError::RateLimit {
            retry_after_seconds,
            ..
        } => assert_eq!(retry_after_seconds, Some(30)),
        other => panic!("expected RateLimit, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_5xx_raises_generic_api_error() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "error": "Internal", "errorClass": "NPE"
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client.pipelines().list().await.unwrap_err();
    assert_eq!(err.status_code(), Some(500));
    assert!(!err.is_auth_error());
    assert!(!err.is_not_found());
    assert!(!err.is_validation_error());
    assert!(!err.is_rate_limited());
    assert!(matches!(err, PulseError::Api { .. }));
}

#[tokio::test]
async fn bearer_token_attached_to_outbound_request() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines"))
        .and(header("authorization", "Bearer fake.jwt.token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pipelines": [] })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt.token"));
    let pipelines = client.pipelines().list().await.unwrap();
    assert!(pipelines.is_empty());
}

#[tokio::test]
async fn user_agent_header_is_set() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/pipelines"))
        .and(header_exists("user-agent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pipelines": [] })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    client.pipelines().list().await.unwrap();
    // Stricter check: the actual UA value
    let requests = server.received_requests().await.unwrap();
    let ua = requests
        .last()
        .unwrap()
        .headers
        .get("user-agent")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ua.contains("pulse-client-rust"), "got UA: {ua}");
}

// ---------------------------------------------------------------------------
// events().stream() — B-098 Phase 7 SSE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn events_stream_yields_parsed_events() {
    let server = start_server().await;
    let sse_body = concat!(
        "data: {\"type\":\"fraud_signal\",\"payload\":{\"customerId\":\"c1\"}}\n\n",
        "data: {\"type\":\"heartbeat\"}\n\n",
    );
    Mock::given(method("GET"))
        .and(path("/api/pulse/events/stream"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_body),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let mut stream = client.events().stream().await.unwrap();

    let mut collected = Vec::new();
    while let Some(event) = stream.next().await {
        collected.push(event.unwrap());
    }
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0]["type"], "fraud_signal");
    assert_eq!(collected[1]["type"], "heartbeat");
}

#[tokio::test]
async fn events_stream_skips_comments() {
    let server = start_server().await;
    let sse_body = concat!(
        ": keep-alive\n\n",
        "data: {\"type\":\"a\"}\n\n",
        ": another\n\n",
        "data: {\"type\":\"b\"}\n\n",
    );
    Mock::given(method("GET"))
        .and(path("/api/pulse/events/stream"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_body),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let mut stream = client.events().stream().await.unwrap();
    let mut types: Vec<String> = Vec::new();
    while let Some(event) = stream.next().await {
        if let Some(s) = event.unwrap()["type"].as_str() {
            types.push(s.to_string());
        }
    }
    assert_eq!(types, vec!["a".to_string(), "b".to_string()]);
}

#[tokio::test]
async fn events_stream_falls_back_to_raw_for_non_json() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/events/stream"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string("data: not-json-here\n\n"),
        )
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let mut stream = client.events().stream().await.unwrap();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event["data"], "not-json-here");
}

#[tokio::test]
async fn events_stream_no_token_returns_no_token_error_synchronously() {
    let server = start_server().await;
    // No Mock — if the client reached the wire, wiremock 404s and we'd
    // misdiagnose. The no-token check fires before any network call.
    let client = new_client(&server, None);
    let err = client.events().stream().await.unwrap_err();
    assert!(matches!(err, PulseError::NoToken { .. }));
}

#[tokio::test]
async fn events_stream_returns_auth_error_on_401() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/events/stream"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({ "error": "expired" })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("expired.jwt"));
    let err = client.events().stream().await.unwrap_err();
    assert!(err.is_auth_error());
}

// ---------------------------------------------------------------------------
// B-106 Interactive Queries
// ---------------------------------------------------------------------------
//
// Mirrors the test coverage of pulse-py / pulse-js / pulse-java / pulse-go:
// happy path + URL encoding + null value + 404 key-not-found vs
// agent-not-queryable + 400 invalid filter + auth gating.

#[tokio::test]
async fn iq_summary_returns_state_metadata() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/fraud-detector/state"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "fraud-detector",
            "queryable": true,
            "backend": "rocksdb",
            "hotSize": 1500,
            "hotBytes": 32768,
            "coldSize": 50000,
            "coldBytes": 4_194_304_i64,
            "lastCheckpointId": 42,
            "totalSize": 51500,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let summary = client.iq().summary("fraud-detector").await.unwrap();
    assert_eq!(summary["queryable"], true);
    assert_eq!(summary["backend"], "rocksdb");
    assert_eq!(summary["totalSize"], 51500);
}

#[tokio::test]
async fn iq_summary_handles_non_queryable_agent() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/rule-agent/state"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "rule-agent",
            "queryable": false,
            "backend": "none",
            "hotSize": 0,
            "hotBytes": 0,
            "coldSize": 0,
            "coldBytes": 0,
            "lastCheckpointId": -1,
            "totalSize": 0,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let summary = client.iq().summary("rule-agent").await.unwrap();
    assert_eq!(summary["queryable"], false);
    assert_eq!(summary["lastCheckpointId"], -1);
}

#[tokio::test]
async fn iq_summary_url_encodes_agent_id_with_slash() {
    // The handler verifies the wire path is percent-encoded by matching the
    // exact escaped path. wiremock's `path` matcher works against the URL's
    // raw path (pre-decode), so `tenant/agent` → `tenant%2Fagent` here.
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/tenant%2Fagent/state"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "tenant/agent",
            "queryable": true,
            "backend": "rocksdb",
            "hotSize": 0,
            "hotBytes": 0,
            "coldSize": 0,
            "coldBytes": 0,
            "lastCheckpointId": 0,
            "totalSize": 0,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client.iq().summary("tenant/agent").await.unwrap();
    assert_eq!(result["agentId"], "tenant/agent");
}

#[tokio::test]
async fn iq_get_returns_value_at_key() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path(
            "/api/pulse/iq/agents/fraud-detector/state/value/customer-42",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "fraud-detector",
            "key": "customer-42",
            "value": { "tx_count_60s": 7, "total_amount_60s": 12500 },
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .iq()
        .get("fraud-detector", "customer-42")
        .await
        .unwrap();
    assert_eq!(result["key"], "customer-42");
    assert_eq!(result["value"]["tx_count_60s"], 7);
}

#[tokio::test]
async fn iq_get_url_encodes_key_with_colon_and_slash() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path(
            "/api/pulse/iq/agents/sessions/state/value/user%3A123%2Forders",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "sessions",
            "key": "user:123/orders",
            "value": ["o1", "o2", "o3"],
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .iq()
        .get("sessions", "user:123/orders")
        .await
        .unwrap();
    let values = result["value"].as_array().unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0], "o1");
}

#[tokio::test]
async fn iq_get_returns_null_value_when_present_with_null() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/value/k1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "a1",
            "key": "k1",
            "value": null,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client.iq().get("a1", "k1").await.unwrap();
    let obj = result.as_object().expect("object");
    assert!(
        obj.contains_key("value"),
        "'value' key must be present even when null"
    );
    assert!(result["value"].is_null());
}

#[tokio::test]
async fn iq_get_404_key_not_found_raises_with_key_body() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/value/missing-key"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": "Key not found",
            "agentId": "a1",
            "key": "missing-key",
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client.iq().get("a1", "missing-key").await.unwrap_err();
    match err {
        PulseError::NotFound { body, .. } => {
            let b = body.expect("body");
            assert_eq!(b["error"], "Key not found");
            assert_eq!(b["key"], "missing-key");
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn iq_get_404_agent_not_queryable_raises_with_reason() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/value/k1"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": "Agent has no queryable state",
            "agentId": "a1",
            "reason": "non-streaming or stopped",
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client.iq().get("a1", "k1").await.unwrap_err();
    match err {
        PulseError::NotFound { body, .. } => {
            assert_eq!(body.unwrap()["reason"], "non-streaming or stopped");
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// B-113 time-travel IQ — get_as_of / diff / replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn iq_get_as_of_sends_as_of_param_and_returns_resolved_value() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/user-sessions/state/value/u42"))
        .and(query_param("as_of", "-1h"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "user-sessions",
            "key": "u42",
            "value": { "cart_value": 0 },
            "asOf": 1_716_000_000_000_i64,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .iq()
        .get_as_of("user-sessions", "u42", "-1h")
        .await
        .unwrap();
    assert_eq!(result["key"], "u42");
    assert_eq!(result["value"]["cart_value"], 0);
    assert_eq!(result["asOf"], 1_716_000_000_000_i64);
}

#[tokio::test]
async fn iq_get_without_as_of_omits_the_param() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/value/k1"))
        .and(query_param_is_missing("as_of"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "a1",
            "key": "k1",
            "value": 42,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client.iq().get("a1", "k1").await.unwrap();
    assert_eq!(result["value"], 42);
}

#[tokio::test]
async fn iq_diff_sends_from_to_and_returns_changes() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/user-sessions/state/diff/u42"))
        .and(query_param("from", "-1h"))
        .and(query_param("to", "now"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "user-sessions",
            "key": "u42",
            "fromTs": 1_716_000_000_000_i64,
            "toTs": 1_716_003_600_000_i64,
            "changes": {
                "cart_value": { "delta": 70.0, "from": 0, "to": 70 },
            },
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .iq()
        .diff("user-sessions", "u42", "-1h", "now")
        .await
        .unwrap();
    assert_eq!(result["fromTs"], 1_716_000_000_000_i64);
    assert_eq!(result["toTs"], 1_716_003_600_000_i64);
    assert_eq!(result["changes"]["cart_value"]["delta"], 70.0);
    assert_eq!(result["changes"]["cart_value"]["to"], 70);
}

#[tokio::test]
async fn iq_diff_url_encodes_key_with_slash() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path(
            "/api/pulse/iq/agents/sessions/state/diff/user%3A123%2Forders",
        ))
        .and(query_param("from", "-30m"))
        .and(query_param("to", "now"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "sessions",
            "key": "user:123/orders",
            "changes": {},
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .iq()
        .diff("sessions", "user:123/orders", "-30m", "now")
        .await
        .unwrap();
    assert!(result["changes"].as_object().unwrap().is_empty());
}

#[tokio::test]
async fn events_replay_unwraps_changes_array() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path(
            "/api/pulse/iq/agents/user-sessions/state/replay/u42",
        ))
        .and(query_param("from", "-1h"))
        .and(query_param("to", "now"))
        .and(query_param("limit", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "user-sessions",
            "key": "u42",
            "changes": [
                { "timestamp": 1_716_000_000_000_i64, "changeType": "PUT", "value": { "cart_value": 30 }, "eventId": "e1" },
                { "timestamp": 1_716_000_500_000_i64, "changeType": "PUT", "value": { "cart_value": 70 }, "eventId": "e2" },
            ],
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let changes = client
        .events()
        .replay("user-sessions", "u42", "-1h", "now", 100)
        .await
        .unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0]["changeType"], "PUT");
    assert_eq!(changes[1]["value"]["cart_value"], 70);
    assert_eq!(changes[1]["eventId"], "e2");
}

#[tokio::test]
async fn events_replay_returns_empty_when_changes_absent() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/replay/k1"))
        .and(query_param("limit", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "a1",
            "key": "k1",
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let changes = client
        .events()
        .replay("a1", "k1", "-1h", "now", 50)
        .await
        .unwrap();
    assert!(changes.is_empty());
}

#[tokio::test]
async fn iq_scan_returns_entries_with_default_limit() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/scan"))
        .and(query_param("limit", "100"))
        .and(query_param_is_missing("start"))
        .and(query_param_is_missing("end"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "a1",
            "entries": [
                { "key": "k1", "value": 1 },
                { "key": "k2", "value": 2 },
            ],
            "count": 2,
            "truncated": false,
            "limitApplied": 100,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client.iq().scan("a1", IQScanOptions::new()).await.unwrap();
    let entries = result["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn iq_scan_passes_through_range_params() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/scan"))
        .and(query_param("limit", "50"))
        .and(query_param("start", "alice"))
        .and(query_param("end", "bob"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "a1",
            "entries": [],
            "count": 0,
            "truncated": false,
            "limitApplied": 50,
            "start": "alice",
            "end": "bob",
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let opts = IQScanOptions::new().start("alice").end("bob").limit(50);
    let result = client.iq().scan("a1", opts).await.unwrap();
    assert_eq!(result["count"], 0);
}

#[tokio::test]
async fn iq_scan_404_agent_not_queryable_raises() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/scan"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": "Agent has no queryable state",
            "agentId": "a1",
            "reason": "non-streaming or stopped",
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let err = client
        .iq()
        .scan("a1", IQScanOptions::new())
        .await
        .unwrap_err();
    assert!(err.is_not_found());
}

#[tokio::test]
async fn iq_list_keys_returns_keys_array() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/iq/agents/a1/state/keys"))
        .and(query_param("limit", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "a1",
            "keys": ["alpha", "beta", "gamma"],
            "count": 3,
            "truncated": false,
            "limitApplied": 100,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .iq()
        .list_keys("a1", IQScanOptions::new())
        .await
        .unwrap();
    let keys = result["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], "alpha");
}

#[tokio::test]
async fn iq_query_flat_with_filter_sends_filter_in_body() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/pulse/iq/agents/fraud-detector/state/query"))
        .and(body_string_contains(r#""field":"tx_count_60s""#))
        .and(body_string_contains(r#""op":"gt""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "fraud-detector",
            "entries": [
                { "key": "c1", "value": { "tx_count_60s": 8 } },
            ],
            "count": 1,
            "totalScanned": 1500,
            "matchedCount": 1,
            "truncated": false,
            "limitApplied": 100,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let opts = IQQueryOptions::new().filter(iq_leaf("tx_count_60s", "gt", 5));
    let result = client.iq().query("fraud-detector", opts).await.unwrap();
    assert_eq!(result["count"], 1);

    let received = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(body["filter"]["field"], "tx_count_60s");
    assert_eq!(body["filter"]["op"], "gt");
    assert_eq!(body["filter"]["value"], 5);
}

#[tokio::test]
async fn iq_query_grouped_returns_groups_and_sends_group_by() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/pulse/iq/agents/users/state/query"))
        .and(body_string_contains(r#""groupBy":"plan""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "users",
            "groups": [
                { "groupKey": "free", "count": 8420 },
                { "groupKey": "pro", "count": 312 },
            ],
            "groupCount": 2,
            "totalScanned": 8732,
            "matchedCount": 8732,
            "truncated": false,
            "limitApplied": 100,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .iq()
        .query("users", IQQueryOptions::new().group_by("plan"))
        .await
        .unwrap();
    assert_eq!(result["groupCount"], 2);
    assert_eq!(result["groups"][0]["groupKey"], "free");
}

#[tokio::test]
async fn iq_query_empty_options_sends_no_body() {
    let server = start_server().await;
    // Custom matcher: assert Content-Length is 0 / absent. The handler in
    // build_query_body() returns an empty object → client switches to None
    // payload → reqwest omits the Content-Type + body entirely.
    struct NoBody;
    impl wiremock::Match for NoBody {
        fn matches(&self, request: &Request) -> bool {
            request.body.is_empty()
        }
    }

    Mock::given(method("POST"))
        .and(path("/api/pulse/iq/agents/a1/state/query"))
        .and(NoBody)
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "agentId": "a1",
            "entries": [],
            "count": 0,
            "totalScanned": 0,
            "matchedCount": 0,
            "truncated": false,
            "limitApplied": 100,
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let result = client
        .iq()
        .query("a1", IQQueryOptions::new())
        .await
        .unwrap();
    assert_eq!(result["count"], 0);
}

#[tokio::test]
async fn iq_query_400_invalid_filter_raises_validation() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/pulse/iq/agents/a1/state/query"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "filter cannot mix discriminators (field/and/or/not) at the same level"
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    // Build a knowingly-bad filter (mixes field + and at same level) — the
    // SDK can't catch this client-side because the spec rule is structural;
    // the server enforces it.
    let bad = json!({
        "field": "a",
        "and": [iq_leaf("b", "eq", 1)],
    });
    let err = client
        .iq()
        .query("a1", IQQueryOptions::new().filter(bad))
        .await
        .unwrap_err();
    match err {
        PulseError::Validation { body, .. } => {
            let msg = body.unwrap()["error"].as_str().unwrap().to_string();
            assert!(msg.contains("discriminator"), "got: {msg}");
        }
        other => panic!("expected Validation, got {other:?}"),
    }
}

#[tokio::test]
async fn iq_summary_without_token_raises_no_token_synchronously() {
    let server = start_server().await;
    // No Mock — if the client reached the wire we'd get a 404 and
    // misdiagnose. The no-token check fires before any network call.
    let client = new_client(&server, None);
    let err = client.iq().summary("a1").await.unwrap_err();
    assert!(matches!(err, PulseError::NoToken { .. }));
}

#[tokio::test]
async fn iq_filter_helpers_build_correct_shape() {
    // Pure unit-style assertion — no HTTP. Verifies that the four free
    // helpers compose into the recursive IQFilterExpression shape the
    // server's parser expects.
    let leaf = iq_leaf("plan", "eq", "pro");
    assert_eq!(leaf["field"], "plan");
    assert_eq!(leaf["op"], "eq");
    assert_eq!(leaf["value"], "pro");

    let and = iq_and(vec![iq_leaf("a", "eq", 1), iq_leaf("b", "gt", 2)]);
    assert_eq!(and["and"].as_array().unwrap().len(), 2);

    let or = iq_or(vec![iq_leaf("a", "eq", 1)]);
    assert_eq!(or["or"].as_array().unwrap().len(), 1);

    let not = pulse_client::iq_not(iq_leaf("a", "eq", 1));
    assert_eq!(not["not"]["field"], "a");
}

// ---------------------------------------------------------------------------
// B-107 Streams DSL
// ---------------------------------------------------------------------------
//
// Mirrors pulse-py / pulse-js / pulse-java / pulse-go coverage: per-operator
// shape + constructor validation + iot-template round-trip + client.streams()
// HTTP integration.

use pulse_client::{
    aggs, windows, BranchSpec, BroadcastJoinOptions, CdcJoinOptions, CepOptions,
    EnrichAsyncOptions, ExtractOptions, MapLlmOptions, MapOptions, McpCallOptions, StreamBuilder,
    WindowOptions, WindowSpec,
};
use std::collections::BTreeMap;

// ----- Window factories ---------------------------------------------------

#[test]
fn windows_tumbling_emits_expected_string() {
    assert_eq!(windows::tumbling("60s").spec(), "tumbling(60s)");
}

#[test]
fn windows_sliding_emits_expected_string() {
    assert_eq!(windows::sliding("10m", "1m").spec(), "sliding(10m,1m)");
}

#[test]
fn windows_session_emits_expected_string() {
    assert_eq!(windows::session("30s").spec(), "session(30s)");
}

#[test]
fn windows_global_emits_expected_string() {
    assert_eq!(windows::global().spec(), "global");
}

#[test]
fn windows_count_emits_expected_string() {
    assert_eq!(windows::count(100).spec(), "count(100)");
}

#[test]
fn windows_count_sliding_emits_expected_string() {
    assert_eq!(
        windows::count_sliding(100, 10).spec(),
        "count_sliding(100,10)"
    );
}

#[test]
#[should_panic(expected = "size")]
fn windows_tumbling_rejects_blank() {
    windows::tumbling("");
}

#[test]
#[should_panic(expected = "slide")]
fn windows_sliding_rejects_blank_slide() {
    windows::sliding("10m", "");
}

#[test]
#[should_panic(expected = "size")]
fn windows_sliding_rejects_blank_size() {
    windows::sliding("   ", "1m");
}

#[test]
#[should_panic(expected = "positive")]
fn windows_count_rejects_zero() {
    windows::count(0);
}

#[test]
#[should_panic(expected = "positive")]
fn windows_count_sliding_rejects_zero_slide() {
    windows::count_sliding(100, 0);
}

#[test]
fn window_spec_display_returns_spec() {
    assert_eq!(format!("{}", windows::tumbling("60s")), "tumbling(60s)");
}

#[test]
#[should_panic(expected = "non-empty")]
fn window_spec_new_rejects_blank() {
    let _ = WindowSpec::new("");
}

// ----- Aggregator factories -----------------------------------------------

#[test]
fn aggs_all_emit_expected_strings() {
    assert_eq!(aggs::count(), "count()");
    assert_eq!(aggs::sum("amount"), "sum(amount)");
    assert_eq!(aggs::avg("price"), "avg(price)");
    assert_eq!(aggs::min("latency"), "min(latency)");
    assert_eq!(aggs::max("latency"), "max(latency)");
    assert_eq!(aggs::collect_list("sku"), "collect_list(sku)");
    assert_eq!(aggs::distinct_count("user_id"), "distinct_count(user_id)");
}

#[test]
#[should_panic(expected = "field")]
fn aggs_sum_rejects_blank() {
    aggs::sum("");
}

#[test]
#[should_panic(expected = "field")]
fn aggs_avg_rejects_blank() {
    aggs::avg("   ");
}

// ----- StreamBuilder per-operator shape -----------------------------------

fn ops_of(b: &StreamBuilder) -> Vec<serde_json::Map<String, serde_json::Value>> {
    b.operators().to_vec()
}

#[test]
fn stream_filter_emits_validator_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .filter("amount > 1000");
    let ops = ops_of(&b);
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0]["type"], "filter");
    assert_eq!(ops[0]["condition"], "amount > 1000");
}

#[test]
#[should_panic(expected = "condition")]
fn stream_filter_rejects_blank() {
    let _ = StreamBuilder::anonymous().from_topic("in").filter("");
}

#[test]
fn stream_map_with_fields_only() {
    let mut fields = BTreeMap::new();
    fields.insert("alert".into(), "concat(id, '!')".into());
    let b = StreamBuilder::anonymous().from_topic("in").map(MapOptions {
        fields: Some(fields),
        target_type: None,
    });
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "map");
    assert_eq!(ops[0]["fields"]["alert"], "concat(id, '!')");
}

#[test]
fn stream_map_with_target_type_only() {
    let b = StreamBuilder::anonymous().from_topic("in").map(MapOptions {
        fields: None,
        target_type: Some("alert".into()),
    });
    assert_eq!(ops_of(&b)[0]["targetType"], "alert");
}

#[test]
#[should_panic(expected = "does nothing")]
fn stream_map_rejects_empty() {
    let _ = StreamBuilder::anonymous()
        .from_topic("in")
        .map(MapOptions::default());
}

#[test]
fn stream_flat_map_emits_validator_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .flat_map("items");
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "flatMap");
    assert_eq!(ops[0]["splitField"], "items");
}

#[test]
#[should_panic(expected = "split_field")]
fn stream_flat_map_rejects_blank() {
    let _ = StreamBuilder::anonymous().from_topic("in").flat_map("");
}

#[test]
fn stream_key_by_emits_validator_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .key_by("deviceId");
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "keyBy");
    assert_eq!(ops[0]["field"], "deviceId");
}

#[test]
fn stream_window_with_aggregations() {
    let mut aggs_map = BTreeMap::new();
    aggs_map.insert("avgTemp".into(), aggs::avg("temperature"));
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .window_with_aggs(windows::tumbling("60s"), aggs_map);
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "window");
    assert_eq!(ops[0]["spec"], "tumbling(60s)");
    assert_eq!(ops[0]["aggregations"]["avgTemp"], "avg(temperature)");
}

#[test]
fn stream_window_from_str_accepts_raw_string() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .window_from_str("sliding(10m,1m)", WindowOptions::default());
    assert_eq!(ops_of(&b)[0]["spec"], "sliding(10m,1m)");
}

#[test]
fn stream_window_with_output_topic_and_trigger() {
    let b = StreamBuilder::anonymous().from_topic("in").window_full(
        windows::tumbling("60s"),
        WindowOptions {
            output_topic: Some("late-data".into()),
            trigger: Some(json!({"kind": "earlyFire", "afterEvents": 10})),
            ..Default::default()
        },
    );
    let ops = ops_of(&b);
    assert_eq!(ops[0]["outputTopic"], "late-data");
    assert_eq!(ops[0]["trigger"]["kind"], "earlyFire");
}

#[test]
fn stream_branch_emits_validator_shape() {
    let b = StreamBuilder::anonymous().from_topic("in").branch(vec![
        BranchSpec::new("tier == 'gold'", "vip-events"),
        BranchSpec::new("tier == 'silver'", "std-events"),
    ]);
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "branch");
    let branches = ops[0]["branches"].as_array().unwrap();
    assert_eq!(branches.len(), 2);
    assert_eq!(branches[0]["condition"], "tier == 'gold'");
    assert_eq!(branches[0]["topic"], "vip-events");
}

#[test]
#[should_panic(expected = "at least one")]
fn stream_branch_rejects_empty() {
    let _ = StreamBuilder::anonymous().from_topic("in").branch(vec![]);
}

#[test]
#[should_panic(expected = "condition")]
fn stream_branch_rejects_blank_condition() {
    let _ = StreamBuilder::anonymous()
        .from_topic("in")
        .branch(vec![BranchSpec::new("", "x")]);
}

#[test]
fn stream_enrich_emits_validator_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .enrich("customers", "customerId");
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "enrich");
    assert_eq!(ops[0]["lookupTopic"], "customers");
    assert_eq!(ops[0]["keyField"], "customerId");
}

#[test]
fn stream_enrich_async_full_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .enrich_async(EnrichAsyncOptions {
            url: "https://x.example/lookup/{id}".into(),
            parallelism: Some(8),
            queue_size: Some(128),
            timeout_ms: Some(5000),
            max_retries: Some(3),
            retry_backoff_ms: Some(200),
            ordering: Some("PRESERVE_INPUT".into()),
            on_failure: Some("EMIT_ERROR".into()),
        });
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "enrichAsync");
    assert_eq!(ops[0]["url"], "https://x.example/lookup/{id}");
    assert_eq!(ops[0]["parallelism"], 8);
    assert_eq!(ops[0]["queueSize"], 128);
    assert_eq!(ops[0]["ordering"], "PRESERVE_INPUT");
    assert_eq!(ops[0]["onFailure"], "EMIT_ERROR");
}

#[test]
#[should_panic(expected = "ordering")]
fn stream_enrich_async_rejects_bad_ordering() {
    let _ = StreamBuilder::anonymous()
        .from_topic("in")
        .enrich_async(EnrichAsyncOptions {
            url: "https://x".into(),
            ordering: Some("SHUFFLED".into()),
            ..Default::default()
        });
}

#[test]
#[should_panic(expected = "on_failure")]
fn stream_enrich_async_rejects_bad_on_failure() {
    let _ = StreamBuilder::anonymous()
        .from_topic("in")
        .enrich_async(EnrichAsyncOptions {
            url: "https://x".into(),
            on_failure: Some("EXPLODE".into()),
            ..Default::default()
        });
}

#[test]
fn stream_cep_emits_validator_shape() {
    let seq = vec![
        json!({"name": "add", "match": "type == 'ADD_TO_CART'", "within": "10m"}),
        json!({"name": "view", "match": "type == 'VIEW_CART'", "follow": "followedBy"}),
    ];
    let b = StreamBuilder::anonymous().from_topic("in").cep(
        seq,
        CepOptions {
            within: Some("20m".into()),
            name: Some("cart-flow".into()),
        },
    );
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "cep");
    assert_eq!(ops[0]["within"], "20m");
    assert_eq!(ops[0]["name"], "cart-flow");
    assert_eq!(ops[0]["sequence"].as_array().unwrap().len(), 2);
}

#[test]
#[should_panic(expected = "non-empty sequence")]
fn stream_cep_rejects_empty_sequence() {
    let _ = StreamBuilder::anonymous()
        .from_topic("in")
        .cep(vec![], CepOptions::default());
}

// ----- B-109 map_llm / extract / mcp_call -----

#[test]
fn stream_map_llm_full_shape() {
    let b = StreamBuilder::anonymous().from_topic("in").map_llm(
        "Summarise: {body}",
        MapLlmOptions {
            output_field: "summary".into(),
            model: Some("gemma3:7b".into()),
            temperature: Some(0.0),
            max_tokens: Some(64),
            parallelism: Some(8),
            ordering: Some("UNORDERED".into()),
            on_failure: Some("PASS_THROUGH".into()),
            max_calls_per_sec: Some(50),
        },
    );
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "mapLlm");
    assert_eq!(ops[0]["prompt"], "Summarise: {body}");
    assert_eq!(ops[0]["outputField"], "summary");
    assert_eq!(ops[0]["model"], "gemma3:7b");
    assert_eq!(ops[0]["ordering"], "UNORDERED");
    assert_eq!(ops[0]["onFailure"], "PASS_THROUGH");
    assert_eq!(ops[0]["maxCallsPerSec"], 50);
}

#[test]
#[should_panic(expected = "output_field")]
fn stream_map_llm_rejects_blank_output_field() {
    let _ = StreamBuilder::anonymous().from_topic("in").map_llm(
        "p",
        MapLlmOptions {
            output_field: "".into(),
            ..Default::default()
        },
    );
}

#[test]
fn stream_extract_full_shape() {
    let mut schema = BTreeMap::new();
    schema.insert("intent".into(), "string".into());
    schema.insert("urgency".into(), "int".into());
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .extract(ExtractOptions {
            instruction: "Extract intent and urgency".into(),
            schema,
            model: Some("gemma3:7b".into()),
            temperature: Some(0.0),
            ..Default::default()
        });
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "extract");
    assert_eq!(ops[0]["instruction"], "Extract intent and urgency");
    assert_eq!(ops[0]["schema"]["intent"], "string");
    assert_eq!(ops[0]["schema"]["urgency"], "int");
}

#[test]
#[should_panic(expected = "non-empty schema")]
fn stream_extract_rejects_empty_schema() {
    let _ = StreamBuilder::anonymous()
        .from_topic("in")
        .extract(ExtractOptions {
            instruction: "x".into(),
            ..Default::default()
        });
}

#[test]
fn stream_mcp_call_full_shape() {
    let mut args = BTreeMap::new();
    args.insert("customer_id".into(), json!("{customerId}"));
    let b = StreamBuilder::anonymous().from_topic("in").mcp_call(
        "crm.lookup_customer",
        McpCallOptions {
            args: Some(args),
            output_field: Some("customer".into()),
            parallelism: Some(4),
            ordering: Some("UNORDERED".into()),
            on_failure: Some("EMIT_ERROR".into()),
        },
    );
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "mcpCall");
    assert_eq!(ops[0]["tool"], "crm.lookup_customer");
    assert_eq!(ops[0]["args"]["customer_id"], "{customerId}");
    assert_eq!(ops[0]["outputField"], "customer");
    assert_eq!(ops[0]["onFailure"], "EMIT_ERROR");
}

#[test]
fn stream_mcp_call_minimal_fire_and_forget() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .mcp_call("pagerduty.create_incident", McpCallOptions::default());
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "mcpCall");
    assert_eq!(ops[0]["tool"], "pagerduty.create_incident");
    assert!(!ops[0].contains_key("args"));
}

#[test]
#[should_panic(expected = "tool")]
fn stream_mcp_call_rejects_blank_tool() {
    let _ = StreamBuilder::anonymous()
        .from_topic("in")
        .mcp_call("", McpCallOptions::default());
}

#[test]
fn stream_broadcast_join_full_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .broadcast_join(BroadcastJoinOptions {
            join_key_field: "userId".into(),
            streaming_topic: Some("users-table".into()),
            name: Some("users-join".into()),
            max_bytes: Some(10_000_000),
            refresh_mode: Some("cdc".into()),
            interval_millis: Some(30_000),
        });
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "broadcastJoin");
    assert_eq!(ops[0]["joinKeyField"], "userId");
    assert_eq!(ops[0]["refreshMode"], "cdc");
    assert_eq!(ops[0]["maxBytes"], 10_000_000_i64);
}

#[test]
#[should_panic(expected = "refresh_mode")]
fn stream_broadcast_join_rejects_bad_refresh_mode() {
    let _ = StreamBuilder::anonymous()
        .from_topic("in")
        .broadcast_join(BroadcastJoinOptions {
            join_key_field: "k".into(),
            refresh_mode: Some("random".into()),
            ..Default::default()
        });
}

#[test]
fn stream_cdc_join_full_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .cdc_join(CdcJoinOptions {
            source: "postgres://orders".into(),
            join_key: Some("orderId".into()),
            table: Some("orders".into()),
            state_backend: Some("rocksdb".into()),
        });
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "cdcJoin");
    assert_eq!(ops[0]["source"], "postgres://orders");
    assert_eq!(ops[0]["joinKey"], "orderId");
}

#[test]
fn stream_cdc_join_minimal_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("in")
        .cdc_join(CdcJoinOptions {
            source: "postgres://orders".into(),
            ..Default::default()
        });
    let ops = ops_of(&b);
    assert_eq!(ops[0]["type"], "cdcJoin");
    assert!(!ops[0].contains_key("joinKey"));
}

// ----- Full pipeline build ------------------------------------------------

#[test]
fn stream_minimal_pipeline_builds() {
    let out = StreamBuilder::new("p1")
        .from_topic("in")
        .filter("x > 0")
        .build()
        .unwrap();
    assert_eq!(out["name"], "p1");
    let nodes = out["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0]["type"], "source");
    assert_eq!(nodes[0]["config"]["engine"], "kafka");
    assert_eq!(nodes[1]["type"], "agent");
    assert_eq!(nodes[1]["config"]["engine"], "streaming");
}

#[test]
fn stream_name_via_named() {
    let out = StreamBuilder::anonymous()
        .named("p2")
        .from_topic("in")
        .filter("x > 0")
        .build()
        .unwrap();
    assert_eq!(out["name"], "p2");
}

#[test]
fn stream_build_with_name_overrides_constructor() {
    let out = StreamBuilder::new("ignored")
        .from_topic("in")
        .filter("x > 0")
        .build_with_name("actual")
        .unwrap();
    assert_eq!(out["name"], "actual");
}

#[test]
fn stream_description_propagates() {
    let out = StreamBuilder::new("p3")
        .described_as("desc")
        .from_topic("in")
        .filter("x > 0")
        .build()
        .unwrap();
    assert_eq!(out["description"], "desc");
}

#[test]
fn stream_agent_label_setter() {
    let out = StreamBuilder::new("p4")
        .with_agent_label("Per-Device Average")
        .from_topic("in")
        .filter("x > 0")
        .build()
        .unwrap();
    assert_eq!(out["nodes"][1]["label"], "Per-Device Average");
}

#[test]
fn stream_emits_sink_when_to_topic_with_channel() {
    let out = StreamBuilder::new("p5")
        .from_topic("in")
        .filter("x > 0")
        .to_topic_with_channel("out", "email")
        .build()
        .unwrap();
    let nodes = out["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[2]["type"], "sink");
    assert_eq!(nodes[2]["config"]["channel"], "email");
    assert_eq!(nodes[2]["config"]["inputTopic"], "out");
}

#[test]
fn stream_skips_sink_when_to_topic_only() {
    let out = StreamBuilder::new("p6")
        .from_topic("in")
        .filter("x > 0")
        .to_topic("out")
        .build()
        .unwrap();
    let nodes = out["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[1]["config"]["outputTopic"], "out");
}

#[test]
fn stream_to_state_clears_output_and_sink() {
    let out = StreamBuilder::new("p7")
        .from_topic("in")
        .filter("x > 0")
        .to_topic_with_channel("out", "email")
        .to_state()
        .build()
        .unwrap();
    let nodes = out["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    assert!(!nodes[1]["config"]
        .as_object()
        .unwrap()
        .contains_key("outputTopic"));
}

#[test]
fn stream_source_engine_and_label() {
    let out = StreamBuilder::new("p8")
        .from_topic_with_engine("in", "mqtt")
        .with_source_label("Sensor readings")
        .with_source_config("qos", json!(1))
        .filter("x > 0")
        .build()
        .unwrap();
    assert_eq!(out["nodes"][0]["label"], "Sensor readings");
    assert_eq!(out["nodes"][0]["config"]["engine"], "mqtt");
    assert_eq!(out["nodes"][0]["config"]["qos"], 1);
}

#[test]
fn stream_build_rejects_missing_name() {
    let err = StreamBuilder::anonymous()
        .from_topic("in")
        .filter("x > 0")
        .build()
        .unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

#[test]
fn stream_build_rejects_missing_source() {
    let err = StreamBuilder::new("p").filter("x > 0").build().unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

#[test]
fn stream_build_rejects_empty_operator_chain() {
    let err = StreamBuilder::new("p")
        .from_topic("in")
        .build()
        .unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

#[test]
#[should_panic(expected = "name")]
fn stream_new_rejects_blank_name() {
    let _ = StreamBuilder::new("   ");
}

#[test]
fn stream_chain_ordering_preserved() {
    let mut aggs_map = BTreeMap::new();
    aggs_map.insert("cnt".into(), aggs::count());
    let mut fields = BTreeMap::new();
    fields.insert("out".into(), "cnt".into());
    let out = StreamBuilder::new("p9")
        .from_topic("in")
        .filter("a > 0")
        .key_by("k")
        .window_with_aggs(windows::tumbling("60s"), aggs_map)
        .filter("cnt > 5")
        .map(MapOptions {
            fields: Some(fields),
            target_type: None,
        })
        .build()
        .unwrap();
    let ops = out["nodes"][1]["config"]["operators"].as_array().unwrap();
    let types: Vec<&str> = ops.iter().map(|op| op["type"].as_str().unwrap()).collect();
    assert_eq!(types, vec!["filter", "keyBy", "window", "filter", "map"]);
}

#[test]
fn stream_iot_template_round_trip() {
    let mut aggs_map = BTreeMap::new();
    aggs_map.insert("avgTemp".into(), aggs::avg("temperature"));
    let out = StreamBuilder::new("iot-temperature-aggregator")
        .with_agent_label("Per-Device Average")
        .from_topic_with_engine("sensor-readings", "mqtt")
        .with_source_label("Sensor readings")
        .key_by("deviceId")
        .window_with_aggs(windows::tumbling("60s"), aggs_map)
        .filter("avgTemp > 75")
        .to_topic_with_channel("sensor-minute-averages", "email")
        .with_sink_label("Heat Alert")
        .build()
        .unwrap();

    let nodes = out["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3);
    let types: Vec<&str> = nodes.iter().map(|n| n["type"].as_str().unwrap()).collect();
    assert_eq!(types, vec!["source", "agent", "sink"]);

    assert_eq!(nodes[0]["label"], "Sensor readings");
    assert_eq!(nodes[0]["config"]["engine"], "mqtt");
    assert_eq!(nodes[0]["config"]["inputTopic"], "sensor-readings");

    assert_eq!(nodes[1]["label"], "Per-Device Average");
    assert_eq!(nodes[1]["config"]["engine"], "streaming");
    assert_eq!(nodes[1]["config"]["inputTopic"], "sensor-readings");
    assert_eq!(nodes[1]["config"]["outputTopic"], "sensor-minute-averages");
    let ops = nodes[1]["config"]["operators"].as_array().unwrap();
    assert_eq!(ops.len(), 3);
    assert_eq!(ops[0]["type"], "keyBy");
    assert_eq!(ops[0]["field"], "deviceId");
    assert_eq!(ops[1]["type"], "window");
    assert_eq!(ops[1]["spec"], "tumbling(60s)");
    assert_eq!(ops[1]["aggregations"]["avgTemp"], "avg(temperature)");
    assert_eq!(ops[2]["type"], "filter");
    assert_eq!(ops[2]["condition"], "avgTemp > 75");

    assert_eq!(nodes[2]["label"], "Heat Alert");
    assert_eq!(nodes[2]["config"]["channel"], "email");
    assert_eq!(nodes[2]["config"]["inputTopic"], "sensor-minute-averages");
}

// ----- client.streams() — compile + deploy --------------------------------

#[tokio::test]
async fn streams_compile_returns_value_without_http_call() {
    let server = start_server().await;
    // No Mock — if compile() reached the wire, wiremock 404s and we'd notice
    // when calling deploy. compile() should never touch the network.
    let client = new_client(&server, Some("fake.jwt"));
    let b = StreamBuilder::new("p").from_topic("in").filter("x > 0");
    let out = client.streams().compile(&b).unwrap();
    assert_eq!(out["name"], "p");
}

#[tokio::test]
async fn streams_deploy_posts_built_definition_to_pipelines_endpoint() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/pulse/pipelines"))
        .and(body_string_contains(r#""name":"fraud-detector""#))
        .and(body_string_contains(r#""type":"window""#))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "p-42",
            "name": "fraud-detector",
            "status": "running",
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let mut aggs_map = BTreeMap::new();
    aggs_map.insert("cnt".into(), aggs::count());
    let b = StreamBuilder::new("fraud-detector")
        .from_topic("payments")
        .filter("amount > 1000")
        .key_by("customer_id")
        .window_with_aggs(windows::tumbling("60s"), aggs_map)
        .filter("cnt > 5")
        .to_topic("fraud-alerts");

    let result = client.streams().deploy(&b).await.unwrap();
    assert_eq!(result["id"], "p-42");

    // Verify the wire body was the DSL-compiled definition
    let received = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(body["name"], "fraud-detector");
    let nodes = body["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2); // no sink (no SinkChannel set)
    let ops = nodes[1]["config"]["operators"].as_array().unwrap();
    assert_eq!(ops[2]["type"], "window");
}

#[tokio::test]
async fn streams_deploy_with_name_override_propagates_to_wire_body() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/pulse/pipelines"))
        .and(body_string_contains(r#""name":"renamed""#))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "p", "name": "renamed",
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let b = StreamBuilder::new("original")
        .from_topic("in")
        .filter("x > 0");
    client
        .streams()
        .deploy_with_name(&b, "renamed")
        .await
        .unwrap();
}

#[tokio::test]
async fn streams_deploy_without_token_raises_no_token_synchronously() {
    let server = start_server().await;
    // No Mock — verify the no-token check fires before any HTTP call.
    let client = new_client(&server, None);
    let b = StreamBuilder::new("p").from_topic("in").filter("x > 0");
    let err = client.streams().deploy(&b).await.unwrap_err();
    assert!(matches!(err, PulseError::NoToken { .. }));
}

// ---------------------------------------------------------------------------
// B-112 — ml_predict operator shape (StreamBuilder)
// ---------------------------------------------------------------------------

#[test]
fn ml_predict_minimal_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("tx")
        .ml_predict(MlPredictOptions {
            model: "fraud".into(),
            input_fields: vec!["amount".into(), "country".into()],
            output_field: "prediction".into(),
            ..Default::default()
        });
    let ops = ops_of(&b);
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op["type"], "mlPredict");
    assert_eq!(op["model"], "fraud");
    assert_eq!(op["inputFields"], json!(["amount", "country"]));
    assert_eq!(op["outputField"], "prediction");
    assert!(!op.contains_key("parallelism"));
    assert!(!op.contains_key("ordering"));
    assert!(!op.contains_key("onFailure"));
}

#[test]
fn ml_predict_full_shape() {
    let b = StreamBuilder::anonymous()
        .from_topic("tx")
        .ml_predict(MlPredictOptions {
            model: "fraud".into(),
            input_fields: vec!["amount".into()],
            output_field: "p".into(),
            parallelism: Some(8),
            ordering: Some("UNORDERED".into()),
            on_failure: Some("PASS_THROUGH".into()),
        });
    let op = &ops_of(&b)[0];
    assert_eq!(op["parallelism"], 8);
    assert_eq!(op["ordering"], "UNORDERED");
    assert_eq!(op["onFailure"], "PASS_THROUGH");
}

#[test]
#[should_panic(expected = "model")]
fn ml_predict_blank_model_panics() {
    let _ = StreamBuilder::anonymous()
        .from_topic("tx")
        .ml_predict(MlPredictOptions {
            model: "".into(),
            input_fields: vec!["a".into()],
            output_field: "p".into(),
            ..Default::default()
        });
}

#[test]
#[should_panic(expected = "input_fields")]
fn ml_predict_empty_input_fields_panics() {
    let _ = StreamBuilder::anonymous()
        .from_topic("tx")
        .ml_predict(MlPredictOptions {
            model: "m".into(),
            input_fields: vec![],
            output_field: "p".into(),
            ..Default::default()
        });
}

#[test]
#[should_panic(expected = "input_fields")]
fn ml_predict_blank_input_field_panics() {
    let _ = StreamBuilder::anonymous()
        .from_topic("tx")
        .ml_predict(MlPredictOptions {
            model: "m".into(),
            input_fields: vec!["ok".into(), "  ".into()],
            output_field: "p".into(),
            ..Default::default()
        });
}

#[test]
#[should_panic(expected = "ordering")]
fn ml_predict_bad_ordering_panics() {
    let _ = StreamBuilder::anonymous()
        .from_topic("tx")
        .ml_predict(MlPredictOptions {
            model: "m".into(),
            input_fields: vec!["a".into()],
            output_field: "p".into(),
            ordering: Some("SOMETIMES".into()),
            ..Default::default()
        });
}

// ---------------------------------------------------------------------------
// B-112 — ModelsResource (client.models())
// ---------------------------------------------------------------------------

#[tokio::test]
async fn models_list_unwraps_envelope() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/ml-models"))
        .and(header_exists("authorization"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "models": [{ "name": "fraud", "runtime": "onnx" }]
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let models = client.models().list().await.unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["name"], "fraud");
}

#[tokio::test]
async fn models_get_by_name() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/ml-models/fraud"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "name": "fraud", "runtime": "onnx", "version": 3
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let m = client.models().get("fraud").await.unwrap();
    assert_eq!(m["version"], 3);
}

#[tokio::test]
async fn models_delete() {
    let server = start_server().await;
    Mock::given(method("DELETE"))
        .and(path("/api/pulse/ml-models/fraud"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    client.models().delete("fraud").await.unwrap();
}

#[tokio::test]
async fn models_upload_sends_multipart_form() {
    let server = start_server().await;
    Mock::given(method("POST"))
        .and(path("/api/pulse/ml-models"))
        .and(header_exists("authorization"))
        // multipart boundary → content-type starts with multipart/form-data
        .and(wiremock::matchers::header_regex(
            "content-type",
            "multipart/form-data",
        ))
        // the text parts + file part name land in the body
        .and(body_string_contains("name=\"name\""))
        .and(body_string_contains("fraud-classifier"))
        .and(body_string_contains("name=\"runtime\""))
        .and(body_string_contains("onnx"))
        .and(body_string_contains("name=\"model\""))
        .and(body_string_contains("name=\"inputSchema\""))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "name": "fraud-classifier", "runtime": "onnx", "sha256": "deadbeef"
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));
    let mut input = BTreeMap::new();
    input.insert("amount".to_string(), "float".to_string());
    let meta = client
        .models()
        .upload(
            ModelUpload::from_bytes("fraud-classifier", b"\x08\x01onnx-bytes".to_vec())
                .input_schema(input),
        )
        .await
        .unwrap();
    assert_eq!(meta["sha256"], "deadbeef");
}

#[tokio::test]
async fn models_upload_blank_name_is_invalid_config() {
    let server = start_server().await;
    let client = new_client(&server, Some("fake.jwt"));
    let err = client
        .models()
        .upload(ModelUpload::from_bytes("  ", b"x".to_vec()))
        .await
        .unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

#[tokio::test]
async fn models_upload_requires_exactly_one_source() {
    let server = start_server().await;
    let client = new_client(&server, Some("fake.jwt"));
    // Neither path nor data → InvalidConfig (constructed by hand).
    let upload = ModelUpload {
        name: "m".into(),
        ..Default::default()
    };
    let err = client.models().upload(upload).await.unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

#[tokio::test]
async fn models_upload_empty_bytes_is_invalid_config() {
    let server = start_server().await;
    let client = new_client(&server, Some("fake.jwt"));
    let err = client
        .models()
        .upload(ModelUpload::from_bytes("m", Vec::new()))
        .await
        .unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

// ---------------------------------------------------------------------------
// B-114 — Duplex URL derivation (mirrors pulse-py derive_ws_url)
// ---------------------------------------------------------------------------

#[test]
fn duplex_url_http_bumps_port_and_carries_token() {
    let url = derive_ws_url("http://localhost:9090", "fraud", Some("ey.jwt"));
    assert_eq!(
        url,
        "ws://localhost:9091/api/pulse/agents/fraud/duplex?token=ey.jwt"
    );
}

#[test]
fn duplex_url_https_is_wss() {
    let url = derive_ws_url("https://h:443", "a", None);
    assert_eq!(url, "wss://h:444/api/pulse/agents/a/duplex");
}

// ---------------------------------------------------------------------------
// B-114 — Duplex WebSocket round-trip against a real tokio-tungstenite server
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duplex_round_trip() {
    use tokio_tungstenite::tungstenite::Message;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Server: send `connected`, then for each `send` echo an `ack` + `output`.
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        ws.send(Message::text(
            json!({ "type": "connected", "agentId": "fraud" }).to_string(),
        ))
        .await
        .unwrap();

        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Text(text) = msg {
                let v: serde_json::Value = serde_json::from_str(&text).unwrap();
                if v["type"] == "send" {
                    let cid = v["correlationId"].clone();
                    let amount = v["payload"]["amount"].clone();
                    ws.send(Message::text(
                        json!({ "type": "ack", "correlationId": cid }).to_string(),
                    ))
                    .await
                    .unwrap();
                    ws.send(Message::text(
                        json!({
                            "type": "output",
                            "correlationId": cid,
                            "event": { "payload": { "decision": "DENY", "echo": amount } }
                        })
                        .to_string(),
                    ))
                    .await
                    .unwrap();
                }
            } else if let Message::Close(_) = msg {
                break;
            }
        }
    });

    // The client derives ws://host:port+1 from base_url, so bind base_url to
    // port-1 and point duplex_at directly at the real server URL instead.
    let client = PulseClient::builder()
        .base_url("http://127.0.0.1:1")
        .build()
        .unwrap();
    let ws_url = format!("ws://{addr}/api/pulse/agents/fraud/duplex");
    let mut ch = client.duplex_at(ws_url).await.unwrap();

    let cid = ch
        .send(&json!({ "amount": 5000 }), Some("tx-1"))
        .await
        .unwrap();
    assert_eq!(cid, "tx-1");

    let out = ch.recv().await.unwrap();
    assert_eq!(out.correlation_id.as_deref(), Some("tx-1"));
    assert_eq!(out.event["payload"]["decision"], "DENY");
    assert_eq!(out.event["payload"]["echo"], 5000);

    // A generated correlation id round-trips too.
    let cid2 = ch.send(&json!({ "amount": 1 }), None).await.unwrap();
    let out2 = ch.recv().await.unwrap();
    assert_eq!(out2.correlation_id.as_deref(), Some(cid2.as_str()));

    ch.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn duplex_server_error_frame_on_open_is_validation_error() {
    use tokio_tungstenite::tungstenite::Message;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        ws.send(Message::text(
            json!({ "type": "error", "error": "unknown agent" }).to_string(),
        ))
        .await
        .unwrap();
        let _ = ws.close(None).await;
    });

    let client = PulseClient::builder()
        .base_url("http://127.0.0.1:1")
        .build()
        .unwrap();
    let ws_url = format!("ws://{addr}/api/pulse/agents/nope/duplex");
    let err = client.duplex_at(ws_url).await.unwrap_err();
    assert!(matches!(err, PulseError::Validation { .. }));
    server.await.unwrap();
}

#[tokio::test]
async fn duplex_blank_agent_id_is_invalid_config() {
    let client = PulseClient::builder()
        .base_url("http://localhost:9090")
        .build()
        .unwrap();
    let err = client.duplex("   ").await.unwrap_err();
    assert!(matches!(err, PulseError::InvalidConfig(_)));
}

// ---------------------------------------------------------------------------
// Connectors catalogue (B-093 follow-up)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn connectors_list_and_helpers() {
    let server = start_server().await;
    Mock::given(method("GET"))
        .and(path("/api/pulse/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "sinks": [{ "subType": "segment", "displayName": "Segment" }],
            "sources": [{ "subType": "posthog-source", "displayName": "PostHog Source (poll)" }]
        })))
        .mount(&server)
        .await;

    let client = new_client(&server, Some("fake.jwt"));

    let catalog = client.connectors().list().await.unwrap();
    assert_eq!(catalog["sinks"][0]["subType"], "segment");

    let sinks = client.connectors().sinks().await.unwrap();
    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0]["subType"], "segment");

    let sources = client.connectors().sources().await.unwrap();
    assert_eq!(sources[0]["subType"], "posthog-source");
}

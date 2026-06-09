//! B-170 / GA-gate #312 — opt-in retry policy (mirrors the Python reference).
//!
//! Each test drives a real request through the client against a wiremock server
//! whose responses are sequenced per call, and asserts the attempt count.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pulse_client::{PulseClient, PulseError, RetryPolicy};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Responds with `responses[i]` for the i-th request (clamped to the last), and
/// records the total number of requests in `hits`.
struct Seq {
    hits: Arc<AtomicUsize>,
    responses: Vec<(u16, serde_json::Value)>,
}

impl Respond for Seq {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let i = self
            .hits
            .fetch_add(1, Ordering::SeqCst)
            .min(self.responses.len() - 1);
        let (status, body) = &self.responses[i];
        ResponseTemplate::new(*status).set_body_json(body.clone())
    }
}

fn fast_client(server: &MockServer, retries: u32, non_idem: bool) -> PulseClient {
    PulseClient::builder()
        .base_url(server.uri())
        .token("t")
        .retry(RetryPolicy {
            max_retries: retries,
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            on_status: vec![502, 503, 504],
            retry_non_idempotent: non_idem,
        })
        .build()
        .unwrap()
}

async fn mount(
    server: &MockServer,
    m: &str,
    p: &str,
    responses: Vec<(u16, serde_json::Value)>,
) -> Arc<AtomicUsize> {
    let hits = Arc::new(AtomicUsize::new(0));
    Mock::given(method(m))
        .and(path(p))
        .respond_with(Seq {
            hits: hits.clone(),
            responses,
        })
        .mount(server)
        .await;
    hits
}

#[tokio::test]
async fn off_by_default() {
    let server = MockServer::start().await;
    let hits = mount(
        &server,
        "GET",
        "/api/pulse/version",
        vec![(503, json!({})), (200, json!({"ok": true}))],
    )
    .await;
    let c = PulseClient::builder()
        .base_url(server.uri())
        .build()
        .unwrap(); // no .retry(...)
    let err = c.version().await.unwrap_err();
    assert!(
        matches!(err, PulseError::Api { status: 503, .. }),
        "got {err:?}"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "retries off → exactly one attempt"
    );
}

#[tokio::test]
async fn idempotent_get_retried_on_5xx_then_succeeds() {
    let server = MockServer::start().await;
    let hits = mount(
        &server,
        "GET",
        "/api/pulse/version",
        vec![(503, json!({})), (502, json!({})), (200, json!({"v": 1}))],
    )
    .await;
    let c = fast_client(&server, 2, false);
    assert_eq!(c.version().await.unwrap(), json!({"v": 1}));
    assert_eq!(hits.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn exhausts_then_returns_error() {
    let server = MockServer::start().await;
    let hits = mount(&server, "GET", "/api/pulse/version", vec![(503, json!({}))]).await;
    let c = fast_client(&server, 2, false);
    let err = c.version().await.unwrap_err();
    assert!(
        matches!(err, PulseError::Api { status: 503, .. }),
        "got {err:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 3); // initial + 2 retries
}

#[tokio::test]
async fn rate_limit_retried_for_post_honouring_retry_after() {
    let server = MockServer::start().await;
    let hits = mount(
        &server,
        "POST",
        "/api/pulse/pipelines",
        vec![
            (429, json!({"retryAfterSeconds": 0})),
            (201, json!({"id": "p1"})),
        ],
    )
    .await;
    let c = fast_client(&server, 1, false);
    assert_eq!(
        c.pipelines().create(&json!({"name": "x"})).await.unwrap(),
        json!({"id": "p1"})
    );
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn post_5xx_not_retried_by_default() {
    let server = MockServer::start().await;
    let hits = mount(
        &server,
        "POST",
        "/api/pulse/pipelines",
        vec![(503, json!({})), (201, json!({"id": "p1"}))],
    )
    .await;
    let c = fast_client(&server, 3, false);
    let err = c
        .pipelines()
        .create(&json!({"name": "x"}))
        .await
        .unwrap_err();
    assert!(
        matches!(err, PulseError::Api { status: 503, .. }),
        "got {err:?}"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "POST 5xx is not retried by default"
    );
}

#[tokio::test]
async fn post_5xx_retried_when_opted_in() {
    let server = MockServer::start().await;
    let hits = mount(
        &server,
        "POST",
        "/api/pulse/pipelines",
        vec![(503, json!({})), (201, json!({"id": "p1"}))],
    )
    .await;
    let c = fast_client(&server, 2, true); // retry_non_idempotent
    assert_eq!(
        c.pipelines().create(&json!({"name": "x"})).await.unwrap(),
        json!({"id": "p1"})
    );
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn terminal_404_not_retried() {
    let server = MockServer::start().await;
    let hits = mount(
        &server,
        "GET",
        "/api/pulse/pipelines/nope",
        vec![(404, json!({"error": "nope"}))],
    )
    .await;
    let c = fast_client(&server, 3, false);
    let err = c.pipelines().get("nope").await.unwrap_err();
    assert!(matches!(err, PulseError::NotFound { .. }), "got {err:?}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

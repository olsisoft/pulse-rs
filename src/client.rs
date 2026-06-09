//! The `PulseClient` and its [`PulseClientBuilder`].

use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use reqwest::Method;
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Value;

use crate::duplex::{derive_ws_url, DuplexChannel};
use crate::error::PulseError;
use crate::events::EventsResource;
use crate::iq::IQResource;
use crate::resources::{
    AgentsResource, AuthResource, ConnectorsResource, ModelsResource, PipelinesResource,
    TemplatesResource, UsersResource, WasmResource,
};
use crate::streams::StreamsResource;

const USER_AGENT: &str = "pulse-client-rust/2.6.0";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Async HTTP client for the Pulse REST API.
///
/// # Example
///
/// ```no_run
/// use pulse_client::PulseClient;
///
/// # async fn run() -> Result<(), pulse_client::PulseError> {
/// let client = PulseClient::builder()
///     .base_url("http://localhost:9090")
///     .build()?;
///
/// client.auth().login("alice", "secret").await?;
///
/// for pipeline in client.pipelines().list().await? {
///     println!("{}", pipeline["name"]);
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Thread safety
///
/// `PulseClient` is `Clone` and cheap to clone — the underlying reqwest client
/// pools connections, and the token sits behind an `Arc<RwLock>`. Share a
/// single instance across tasks.
#[derive(Clone)]
pub struct PulseClient {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) base_url: String,
    pub(crate) http: reqwest::Client,
    pub(crate) token: RwLock<Option<String>>,
    pub(crate) retry: RetryPolicy,
}

/// Opt-in automatic-retry policy. The default ([`RetryPolicy::default`]) has
/// `max_retries == 0`, i.e. retries are **off** — the client makes exactly one
/// attempt per request. Enable with [`PulseClientBuilder::retry`].
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retries after the first attempt. `0` = off (default).
    pub max_retries: u32,
    /// Base backoff; the per-attempt ceiling is `backoff * 2^attempt`.
    pub backoff: Duration,
    /// Per-attempt backoff cap.
    pub max_backoff: Duration,
    /// Retryable 5xx statuses (default `502, 503, 504`).
    pub on_status: Vec<u16>,
    /// When `true`, also retries non-idempotent methods (POST/PATCH) on
    /// 5xx/transport. Default `false` → only GET/HEAD/PUT/DELETE are retried on
    /// those, so a POST create is never silently duplicated.
    pub retry_non_idempotent: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0, // off
            backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(10),
            on_status: vec![502, 503, 504],
            retry_non_idempotent: false,
        }
    }
}

impl RetryPolicy {
    /// A policy that retries up to `max_retries` times with otherwise-default
    /// backoff (200ms base, 10s cap) and the default retryable statuses.
    pub fn with_max_retries(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Self::default()
        }
    }
}

fn is_idempotent(method: &Method) -> bool {
    *method == Method::GET
        || *method == Method::HEAD
        || *method == Method::PUT
        || *method == Method::DELETE
        || *method == Method::OPTIONS
}

/// Full-jitter exponential backoff: a uniform delay in `[0, min(max_backoff,
/// backoff * 2^attempt)]`. Uses sub-second wall-clock nanos as entropy so no
/// `rand` dependency is added.
fn backoff_delay(policy: &RetryPolicy, attempt: u32) -> Duration {
    let base_ms = policy.backoff.as_millis() as u64;
    let factor = 1u64 << attempt.min(20); // cap the shift to avoid overflow
    let ceiling_ms = base_ms
        .saturating_mul(factor)
        .min(policy.max_backoff.as_millis() as u64);
    if ceiling_ms == 0 {
        return Duration::ZERO;
    }
    let entropy = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    Duration::from_millis(entropy % (ceiling_ms + 1))
}

impl PulseClient {
    pub fn builder() -> PulseClientBuilder {
        PulseClientBuilder::default()
    }

    /// Returns the current bearer token, or `None` if none is set.
    pub fn token(&self) -> Option<String> {
        self.inner.token.read().ok().and_then(|guard| guard.clone())
    }

    /// Updates the bearer token used by subsequent authenticated requests.
    /// Safe to call from multiple tasks concurrently.
    pub fn set_token<S: Into<String>>(&self, token: S) {
        if let Ok(mut guard) = self.inner.token.write() {
            *guard = Some(token.into());
        }
    }

    /// Clears the bearer token, effectively logging out the client.
    pub fn clear_token(&self) {
        if let Ok(mut guard) = self.inner.token.write() {
            *guard = None;
        }
    }

    // ------------------------------------------------------------------
    // Resource accessors
    // ------------------------------------------------------------------
    pub fn auth(&self) -> AuthResource<'_> {
        AuthResource { client: self }
    }

    pub fn pipelines(&self) -> PipelinesResource<'_> {
        PipelinesResource { client: self }
    }

    pub fn agents(&self) -> AgentsResource<'_> {
        AgentsResource { client: self }
    }

    pub fn templates(&self) -> TemplatesResource<'_> {
        TemplatesResource { client: self }
    }

    pub fn users(&self) -> UsersResource<'_> {
        UsersResource { client: self }
    }

    /// `client.models()` — B-112 embedded ML model registry (upload / list /
    /// get / delete ONNX models scored by the streaming `ml_predict` operator).
    pub fn models(&self) -> ModelsResource<'_> {
        ModelsResource { client: self }
    }

    /// `client.wasm()` — B-110 sandboxed WASM module registry (upload / list /
    /// get / delete WebAssembly modules run by the streaming `wasm` operator).
    pub fn wasm(&self) -> WasmResource<'_> {
        WasmResource { client: self }
    }

    /// `client.connectors()` — the connector catalogue (B-093 family + every
    /// native / bridged connector); use a `subType` as a pipeline node `type`.
    pub fn connectors(&self) -> ConnectorsResource<'_> {
        ConnectorsResource { client: self }
    }

    pub fn events(&self) -> EventsResource<'_> {
        EventsResource { client: self }
    }

    pub fn iq(&self) -> IQResource<'_> {
        IQResource { client: self }
    }

    /// `client.streams()` — B-107 Kafka-Streams-like declarative DSL.
    pub fn streams(&self) -> StreamsResource<'_> {
        StreamsResource { client: self }
    }

    /// B-114 — open a bidirectional duplex channel to an agent.
    ///
    /// Streams events IN and receives the agent's correlated outputs OUT on a
    /// single WebSocket — the synchronous-decision path (fraud, pricing, A/B
    /// assignment). The endpoint runs on the Pulse WebSocket port (REST port
    /// + 1); the URL is derived from this client's `base_url` + token.
    ///
    /// ```no_run
    /// # use pulse_client::PulseClient;
    /// # use serde_json::json;
    /// # async fn run(client: &PulseClient) -> Result<(), pulse_client::PulseError> {
    /// let mut ch = client.duplex("fraud-detector").await?;
    /// let cid = ch.send(&json!({ "amount": 5000 }), Some("tx-1")).await?;
    /// let output = ch.recv().await?;
    /// assert_eq!(output.correlation_id, Some(cid));
    /// ch.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`PulseError::InvalidConfig`] if `agent_id` is blank.
    /// - [`PulseError::Duplex`] on a WebSocket handshake / transport failure.
    /// - [`PulseError::Validation`] if the server rejects the agent with an
    ///   `error` frame on open.
    pub async fn duplex(&self, agent_id: &str) -> Result<DuplexChannel, PulseError> {
        if agent_id.trim().is_empty() {
            return Err(PulseError::InvalidConfig(
                "agent_id must be a non-empty string".to_string(),
            ));
        }
        let token = self.token();
        let url = derive_ws_url(&self.inner.base_url, agent_id, token.as_deref());
        DuplexChannel::connect(url).await
    }

    /// Open a duplex channel at an explicit WebSocket URL, bypassing the
    /// REST-port-+-1 derivation. Useful when the WebSocket endpoint sits
    /// behind a separate gateway / hostname.
    pub async fn duplex_at(&self, ws_url: impl Into<String>) -> Result<DuplexChannel, PulseError> {
        DuplexChannel::connect(ws_url.into()).await
    }

    /// `GET /api/pulse/version` — public, no JWT required. Returns the
    /// Pulse server's build + version metadata.
    pub async fn version(&self) -> Result<Value, PulseError> {
        self.request(Method::GET, "/api/pulse/version", None::<&()>, false)
            .await
    }

    // ------------------------------------------------------------------
    // Internal: request execution + error translation
    // ------------------------------------------------------------------
    /// Runs [`request_once`](Self::request_once) under the opt-in retry policy.
    /// With retries off (the default) this is exactly one attempt.
    pub(crate) async fn request<B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        authenticated: bool,
    ) -> Result<Value, PulseError> {
        let mut attempt: u32 = 0;
        loop {
            let result = self
                .request_once(method.clone(), path, body, authenticated)
                .await;
            let err = match &result {
                Ok(_) => return result,
                Err(e) => e,
            };
            if attempt >= self.inner.retry.max_retries {
                return result;
            }
            match self.retry_delay(&method, err, attempt) {
                Some(delay) => {
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                None => return result,
            }
        }
    }

    /// Returns `Some(delay)` when `err` is retryable for `method` at this
    /// attempt, else `None`. 429 → any method (honour Retry-After); `on_status`
    /// 5xx + transport → idempotent methods only (unless `retry_non_idempotent`).
    fn retry_delay(&self, method: &Method, err: &PulseError, attempt: u32) -> Option<Duration> {
        let policy = &self.inner.retry;
        if let PulseError::RateLimit {
            retry_after_seconds,
            ..
        } = err
        {
            // 429: rejected, never processed → safe to retry any method.
            return Some(
                retry_after_seconds
                    .map(|s| Duration::from_secs(s as u64))
                    .unwrap_or_else(|| backoff_delay(policy, attempt)),
            );
        }
        if !is_idempotent(method) && !policy.retry_non_idempotent {
            return None;
        }
        match err {
            PulseError::Api { status, .. } if policy.on_status.contains(status) => {
                Some(backoff_delay(policy, attempt))
            }
            PulseError::Transport(_) => Some(backoff_delay(policy, attempt)),
            _ => None,
        }
    }

    async fn request_once<B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        authenticated: bool,
    ) -> Result<Value, PulseError> {
        let url = format!("{}{path}", self.inner.base_url);
        let mut req = self.inner.http.request(method, url);

        if authenticated {
            match self.token() {
                Some(token) if !token.is_empty() => {
                    req = req.bearer_auth(token);
                }
                _ => {
                    return Err(PulseError::NoToken {
                        path: path.to_string(),
                    });
                }
            }
        }

        if let Some(payload) = body {
            req = req.json(payload);
        }

        let response = req.send().await?;
        let status = response.status();

        if status == StatusCode::NO_CONTENT {
            return Ok(Value::Object(Default::default()));
        }

        if status.is_success() {
            // Read body; empty body → empty object so callers can `.get()`
            let bytes = response.bytes().await?;
            if bytes.is_empty() {
                return Ok(Value::Object(Default::default()));
            }
            return Ok(serde_json::from_slice(&bytes)?);
        }

        // Non-success — translate to a typed error
        let retry_after_header = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u32>().ok());

        let bytes = response.bytes().await?;
        let parsed_body: Option<Value> = if bytes.is_empty() {
            None
        } else {
            match serde_json::from_slice::<Value>(&bytes) {
                Ok(v) => Some(v),
                Err(_) => {
                    let raw = String::from_utf8_lossy(&bytes);
                    let trimmed = if raw.len() > 200 { &raw[..200] } else { &raw };
                    Some(serde_json::json!({ "error": trimmed }))
                }
            }
        };

        Err(translate_error(
            status,
            path,
            parsed_body,
            retry_after_header,
        ))
    }

    /// B-112 — issue a `multipart/form-data` POST (the ML model-upload path).
    ///
    /// Shares the auth + error-translation logic of [`request`](Self::request)
    /// but sends a pre-built [`reqwest::multipart::Form`] instead of a JSON
    /// body. Always authenticated.
    pub(crate) async fn request_multipart(
        &self,
        path: &str,
        form: reqwest::multipart::Form,
    ) -> Result<Value, PulseError> {
        let url = format!("{}{path}", self.inner.base_url);
        let token = match self.token() {
            Some(token) if !token.is_empty() => token,
            _ => {
                return Err(PulseError::NoToken {
                    path: path.to_string(),
                });
            }
        };

        let response = self
            .inner
            .http
            .request(Method::POST, url)
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await?;
        let status = response.status();

        if status == StatusCode::NO_CONTENT {
            return Ok(Value::Object(Default::default()));
        }
        if status.is_success() {
            let bytes = response.bytes().await?;
            if bytes.is_empty() {
                return Ok(Value::Object(Default::default()));
            }
            return Ok(serde_json::from_slice(&bytes)?);
        }

        let retry_after_header = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u32>().ok());
        let bytes = response.bytes().await?;
        let parsed_body: Option<Value> = if bytes.is_empty() {
            None
        } else {
            match serde_json::from_slice::<Value>(&bytes) {
                Ok(v) => Some(v),
                Err(_) => {
                    let raw = String::from_utf8_lossy(&bytes);
                    let trimmed = if raw.len() > 200 { &raw[..200] } else { &raw };
                    Some(serde_json::json!({ "error": trimmed }))
                }
            }
        };
        Err(translate_error(
            status,
            path,
            parsed_body,
            retry_after_header,
        ))
    }
}

fn translate_error(
    status: StatusCode,
    path: &str,
    body: Option<Value>,
    retry_after_header: Option<u32>,
) -> PulseError {
    let path = path.to_string();
    match status {
        StatusCode::UNAUTHORIZED => PulseError::Auth { path, body },
        StatusCode::NOT_FOUND => PulseError::NotFound { path, body },
        StatusCode::BAD_REQUEST => PulseError::Validation { path, body },
        StatusCode::TOO_MANY_REQUESTS => {
            let retry_from_body = body
                .as_ref()
                .and_then(|v| v.get("retryAfterSeconds"))
                .and_then(|v| v.as_u64())
                .map(|n| n as u32);
            PulseError::RateLimit {
                path,
                body,
                retry_after_seconds: retry_from_body.or(retry_after_header),
            }
        }
        other => PulseError::Api {
            status: other.as_u16(),
            path,
            body,
        },
    }
}

fn strip_trailing_slash(url: &str) -> String {
    let mut s = url.to_string();
    while s.len() > 1 && s.ends_with('/') {
        s.pop();
    }
    s
}

// ----------------------------------------------------------------------
// Builder
// ----------------------------------------------------------------------

/// Fluent builder for [`PulseClient`].
#[derive(Default, Debug)]
pub struct PulseClientBuilder {
    base_url: Option<String>,
    token: Option<String>,
    timeout: Option<Duration>,
    http: Option<reqwest::Client>,
    retry: Option<RetryPolicy>,
}

impl PulseClientBuilder {
    /// Required — the Pulse server URL (e.g. `http://localhost:9090`).
    pub fn base_url<S: Into<String>>(mut self, base_url: S) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    /// Optional — pre-minted JWT to attach as `Authorization: Bearer <token>`.
    pub fn token<S: Into<String>>(mut self, token: S) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Optional — per-request timeout. Default 30 seconds.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Optional — bring-your-own [`reqwest::Client`] (shared connection pools,
    /// custom TLS / proxy / mTLS config, tracing middleware).
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Optional — enable opt-in, bounded, full-jitter exponential-backoff
    /// retries. Off by default. 429 (rate limited) is always retried for any
    /// method (honouring Retry-After); `on_status` 5xx and transport errors are
    /// retried only for idempotent methods unless `retry_non_idempotent` is set.
    /// Terminal 4xx are never retried.
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = Some(policy);
        self
    }

    pub fn build(self) -> Result<PulseClient, PulseError> {
        let base_url = self
            .base_url
            .ok_or_else(|| PulseError::InvalidConfig("base_url is required".to_string()))?;
        if base_url.is_empty() {
            return Err(PulseError::InvalidConfig(
                "base_url cannot be empty".to_string(),
            ));
        }

        let http = match self.http {
            Some(c) => c,
            None => reqwest::Client::builder()
                .timeout(self.timeout.unwrap_or(DEFAULT_TIMEOUT))
                .user_agent(USER_AGENT)
                .build()
                .map_err(PulseError::Transport)?,
        };

        Ok(PulseClient {
            inner: Arc::new(Inner {
                base_url: strip_trailing_slash(&base_url),
                http,
                token: RwLock::new(self.token),
                retry: self.retry.unwrap_or_default(),
            }),
        })
    }
}

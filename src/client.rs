//! The `PulseClient` and its [`PulseClientBuilder`].

use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use reqwest::Method;
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Value;

use crate::error::PulseError;
use crate::events::EventsResource;
use crate::iq::IQResource;
use crate::resources::{
    AgentsResource, AuthResource, PipelinesResource, TemplatesResource, UsersResource,
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

    /// `GET /api/pulse/version` — public, no JWT required. Returns the
    /// Pulse server's build + version metadata.
    pub async fn version(&self) -> Result<Value, PulseError> {
        self.request(Method::GET, "/api/pulse/version", None::<&()>, false)
            .await
    }

    // ------------------------------------------------------------------
    // Internal: request execution + error translation
    // ------------------------------------------------------------------
    pub(crate) async fn request<B: Serialize + ?Sized>(
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
            }),
        })
    }
}

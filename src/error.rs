//! Typed error hierarchy for the Pulse client.
//!
//! Every HTTP error from the Pulse server is translated into a variant of
//! [`PulseError`]. Callers match precisely with `match` or check categories
//! via the convenience methods (`is_auth_error`, `is_not_found`, etc.).

use std::fmt;

use serde_json::Value;

/// The single error type returned by every `PulseClient` method.
#[derive(Debug)]
pub enum PulseError {
    /// 401 — invalid / missing / expired JWT.
    Auth { path: String, body: Option<Value> },
    /// 404 — the resource does not exist.
    NotFound { path: String, body: Option<Value> },
    /// 400 — the request body is malformed.
    Validation { path: String, body: Option<Value> },
    /// 429 — per-user or per-IP rate limit hit. Carries the server's advised
    /// wait time, parsed from either the `retryAfterSeconds` JSON field or the
    /// `Retry-After` HTTP header. `None` means the server gave no hint.
    RateLimit {
        path: String,
        body: Option<Value>,
        retry_after_seconds: Option<u32>,
    },
    /// Any other non-2xx status code (5xx, unexpected 4xx).
    Api {
        status: u16,
        path: String,
        body: Option<Value>,
    },
    /// The HTTP transport itself failed (connection refused, timeout, DNS,
    /// TLS handshake, etc.). Wraps the underlying reqwest error.
    Transport(reqwest::Error),
    /// JSON serialisation / deserialisation failure. The wire format the
    /// server returned doesn't match what the client expected.
    Json(serde_json::Error),
    /// The caller invoked an authenticated endpoint without setting a token
    /// first. Surfaces before any network call, so it has no `body`.
    NoToken { path: String },
    /// The supplied configuration is invalid (e.g. empty `base_url`).
    InvalidConfig(String),
    /// B-114 — a duplex WebSocket channel failed (handshake, framing, or the
    /// connection dropped). Carries a human-readable description of the cause.
    Duplex(String),
}

impl PulseError {
    pub fn is_auth_error(&self) -> bool {
        matches!(self, PulseError::Auth { .. } | PulseError::NoToken { .. })
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, PulseError::NotFound { .. })
    }

    pub fn is_validation_error(&self) -> bool {
        matches!(self, PulseError::Validation { .. })
    }

    pub fn is_rate_limited(&self) -> bool {
        matches!(self, PulseError::RateLimit { .. })
    }

    /// HTTP status code, if the error carries one. `None` for transport /
    /// JSON / no-token / config errors.
    pub fn status_code(&self) -> Option<u16> {
        match self {
            PulseError::Auth { .. } | PulseError::NoToken { .. } => Some(401),
            PulseError::NotFound { .. } => Some(404),
            PulseError::Validation { .. } => Some(400),
            PulseError::RateLimit { .. } => Some(429),
            PulseError::Api { status, .. } => Some(*status),
            PulseError::Transport(_)
            | PulseError::Json(_)
            | PulseError::InvalidConfig(_)
            | PulseError::Duplex(_) => None,
        }
    }

    /// The parsed JSON error body the server returned, if any.
    pub fn body(&self) -> Option<&Value> {
        match self {
            PulseError::Auth { body, .. }
            | PulseError::NotFound { body, .. }
            | PulseError::Validation { body, .. }
            | PulseError::RateLimit { body, .. }
            | PulseError::Api { body, .. } => body.as_ref(),
            _ => None,
        }
    }

    pub fn path(&self) -> Option<&str> {
        match self {
            PulseError::Auth { path, .. }
            | PulseError::NotFound { path, .. }
            | PulseError::Validation { path, .. }
            | PulseError::RateLimit { path, .. }
            | PulseError::Api { path, .. }
            | PulseError::NoToken { path } => Some(path),
            _ => None,
        }
    }
}

impl fmt::Display for PulseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let summary = match self {
            PulseError::Auth { path, body } => format_http(401, path, body.as_ref()),
            PulseError::NotFound { path, body } => format_http(404, path, body.as_ref()),
            PulseError::Validation { path, body } => format_http(400, path, body.as_ref()),
            PulseError::RateLimit { path, body, .. } => format_http(429, path, body.as_ref()),
            PulseError::Api { status, path, body } => format_http(*status, path, body.as_ref()),
            PulseError::Transport(e) => return write!(f, "pulse: HTTP transport failure — {e}"),
            PulseError::Json(e) => return write!(f, "pulse: JSON encode/decode failure — {e}"),
            PulseError::NoToken { path } => {
                return write!(
                    f,
                    "pulse: no token set for {path} — call client.auth().login(...).await first \
                     or pass .token(...) to the builder"
                );
            }
            PulseError::InvalidConfig(msg) => return write!(f, "pulse: invalid config — {msg}"),
            PulseError::Duplex(msg) => return write!(f, "pulse: duplex channel failure — {msg}"),
        };
        write!(f, "{summary}")
    }
}

fn format_http(status: u16, path: &str, body: Option<&Value>) -> String {
    let mut msg = format!("pulse: HTTP {status} from {path}");
    if let Some(v) = body {
        if let Some(err) = v
            .get("error")
            .and_then(Value::as_str)
            .or_else(|| v.get("errorMessage").and_then(Value::as_str))
            .or_else(|| v.get("message").and_then(Value::as_str))
        {
            msg.push_str(" — ");
            msg.push_str(err);
        }
    }
    msg
}

impl std::error::Error for PulseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PulseError::Transport(e) => Some(e),
            PulseError::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for PulseError {
    fn from(e: reqwest::Error) -> Self {
        PulseError::Transport(e)
    }
}

impl From<serde_json::Error> for PulseError {
    fn from(e: serde_json::Error) -> Self {
        PulseError::Json(e)
    }
}

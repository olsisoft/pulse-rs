//! B-114 — bidirectional duplex channel for synchronous decision agents.
//!
//! Opens ONE WebSocket to `/api/pulse/agents/{id}/duplex`: events are streamed
//! IN and the agent's correlated outputs come back OUT on the same connection,
//! matched by a correlation id. Eliminates the 2-connection publish-then-poll
//! pattern for decision microservices (fraud, pricing, A/B assignment).
//!
//! The duplex endpoint runs on the Pulse WebSocket port (REST port + 1 by
//! convention); [`derive_ws_url`] derives it from the client's `base_url`.
//!
//! # Example
//!
//! ```no_run
//! use pulse_client::PulseClient;
//! use serde_json::json;
//!
//! # async fn run(client: &PulseClient) -> Result<(), pulse_client::PulseError> {
//! let mut ch = client.duplex("fraud-detector").await?;
//! let cid = ch.send(&json!({ "amount": 5000 }), Some("tx-1")).await?;
//! let output = ch.recv().await?;
//! assert_eq!(output.correlation_id.as_deref(), Some("tx-1"));
//! let _ = cid;
//! ch.close().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::error::PulseError;

/// Monotonic counter feeding generated correlation ids (combined with a
/// millisecond timestamp so ids stay unique across channels in a process).
static CORRELATION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Builds the duplex WebSocket URL from the client's REST `base_url`.
///
/// `http`→`ws` / `https`→`wss`, host unchanged, port → REST port + 1 (the
/// Pulse WebSocket server convention). The JWT, when set, rides as a `token`
/// query param (the server reads it from the upgrade request line).
///
/// Mirrors `pulse_client._duplex.derive_ws_url` in the Python SDK.
pub fn derive_ws_url(base_url: &str, agent_id: &str, token: Option<&str>) -> String {
    // Split scheme.
    let (scheme, rest) = match base_url.split_once("://") {
        Some((s, r)) => (s, r),
        None => ("http", base_url),
    };
    let ws_scheme = if scheme.eq_ignore_ascii_case("https") {
        "wss"
    } else {
        "ws"
    };

    // The authority is everything up to the first '/' (path), '?' (query) or
    // '#' (fragment) — Pulse base URLs are bare origins, but be defensive.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];

    // Strip userinfo if present (`user:pass@host:port`).
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, hp)| hp)
        .unwrap_or(authority);

    let netloc = match host_port.rsplit_once(':') {
        // host:port → bump the port by one (the WS server convention).
        Some((h, p)) if !h.is_empty() => match p.parse::<u32>() {
            Ok(port) => format!("{h}:{}", port + 1),
            // Not a numeric port (e.g. an IPv6 literal) → leave untouched.
            Err(_) => host_port.to_string(),
        },
        // No explicit port → host unchanged (cannot bump an absent port).
        _ if host_port.is_empty() => "localhost".to_string(),
        _ => host_port.to_string(),
    };

    let path = format!("/api/pulse/agents/{}/duplex", encode_segment(agent_id));
    match token {
        Some(t) if !t.is_empty() => {
            format!("{ws_scheme}://{netloc}{path}?token={}", encode_query(t))
        }
        _ => format!("{ws_scheme}://{netloc}{path}"),
    }
}

/// An agent output event received over the duplex channel.
///
/// `event` is the agent's output event (`id` / `topic` / `type` / `key` /
/// `payload`); `correlation_id` identifies the input that produced it (the id
/// returned by the matching [`DuplexChannel::send`]).
#[derive(Debug, Clone)]
pub struct DuplexOutput {
    /// The agent's output event JSON.
    pub event: Value,
    /// Correlation id matching the input that produced this output, if the
    /// server supplied one.
    pub correlation_id: Option<String>,
}

/// An open duplex session.
///
/// [`send`](Self::send) publishes an event to the agent's input topic and
/// returns its correlation id; [`recv`](Self::recv) returns the next output
/// event the agent produced. Ack / pong / connected frames are consumed
/// transparently by [`recv`](Self::recv).
pub struct DuplexChannel {
    url: String,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl DuplexChannel {
    /// Connect and complete the duplex handshake.
    ///
    /// The server sends a `connected` frame first (or `error` + close for an
    /// unknown agent / disabled duplex). An `error` first frame surfaces as
    /// [`PulseError::Validation`].
    pub(crate) async fn connect(url: String) -> Result<Self, PulseError> {
        let (mut ws, _resp) = connect_async(&url)
            .await
            .map_err(|e| PulseError::Duplex(format!("connect {url}: {e}")))?;

        // Read the first frame — `connected` (proceed) or `error` (abort).
        let first = read_json_frame(&mut ws, &url).await?;
        if first.get("type").and_then(Value::as_str) == Some("error") {
            // Best-effort close, then surface the server's error payload.
            let _ = ws.close(None).await;
            let body = first.get("error").cloned().or(Some(first));
            return Err(PulseError::Validation { path: url, body });
        }
        Ok(Self { url, ws })
    }

    /// Publish `payload` to the agent's input topic.
    ///
    /// Returns the correlation id (generated when `correlation_id` is `None`)
    /// that the matching output will carry.
    pub async fn send(
        &mut self,
        payload: &Value,
        correlation_id: Option<&str>,
    ) -> Result<String, PulseError> {
        let cid = match correlation_id {
            Some(c) if !c.is_empty() => c.to_string(),
            _ => generate_correlation_id(),
        };
        let frame = json!({
            "type": "send",
            "correlationId": cid,
            "payload": payload,
        });
        let text = serde_json::to_string(&frame)?;
        self.ws
            .send(Message::text(text))
            .await
            .map_err(|e| PulseError::Duplex(format!("send on {}: {e}", self.url)))?;
        Ok(cid)
    }

    /// Return the next agent output event.
    ///
    /// Skips `ack` / `pong` / `connected` frames transparently. An `error`
    /// frame surfaces as [`PulseError::Validation`].
    pub async fn recv(&mut self) -> Result<DuplexOutput, PulseError> {
        loop {
            let msg = read_json_frame(&mut self.ws, &self.url).await?;
            match msg.get("type").and_then(Value::as_str) {
                Some("output") => {
                    let event = match msg.get("event") {
                        Some(Value::Object(_)) => msg.get("event").cloned().unwrap_or(Value::Null),
                        Some(other) => json!({ "value": other }),
                        None => Value::Null,
                    };
                    let correlation_id = msg
                        .get("correlationId")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    return Ok(DuplexOutput {
                        event,
                        correlation_id,
                    });
                }
                Some("error") => {
                    let body = msg.get("error").cloned().or(Some(msg));
                    return Err(PulseError::Validation {
                        path: self.url.clone(),
                        body,
                    });
                }
                // ack / pong / connected / anything else → skip
                _ => continue,
            }
        }
    }

    /// Close the channel cleanly.
    pub async fn close(mut self) -> Result<(), PulseError> {
        self.ws
            .close(None)
            .await
            .map_err(|e| PulseError::Duplex(format!("close {}: {e}", self.url)))
    }
}

impl std::fmt::Debug for DuplexChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuplexChannel")
            .field("url", &self.url)
            .finish()
    }
}

/// Read frames until a JSON text/binary frame arrives, skipping ping/pong and
/// surfacing a clear error on close / transport failure.
async fn read_json_frame(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    url: &str,
) -> Result<Value, PulseError> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text).map_err(PulseError::Json);
            }
            Some(Ok(Message::Binary(bytes))) => {
                return serde_json::from_slice(&bytes).map_err(PulseError::Json);
            }
            // Ping/pong are handled by the library's auto-pong; skip explicitly.
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(frame))) => {
                return Err(PulseError::Duplex(format!(
                    "{url} closed by server: {frame:?}"
                )));
            }
            Some(Ok(Message::Frame(_))) => continue,
            Some(Err(e)) => return Err(PulseError::Duplex(format!("{url}: {e}"))),
            None => {
                return Err(PulseError::Duplex(format!(
                    "{url}: connection closed before a frame arrived"
                )))
            }
        }
    }
}

/// Generates a unique correlation id without pulling in the `uuid` crate:
/// `<millis-since-epoch>-<process-monotonic-counter>`.
fn generate_correlation_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let n = CORRELATION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("pulse-{millis:x}-{n:x}")
}

/// Percent-encode a path segment (same unreserved set as `resources::encode_path`).
fn encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Percent-encode a query-param value (same unreserved set, encodes `&`/`=`/etc).
fn encode_query(value: &str) -> String {
    encode_segment(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_http_to_ws_bumps_port() {
        let url = derive_ws_url("http://localhost:9090", "fraud", Some("ey.jwt"));
        assert_eq!(
            url,
            "ws://localhost:9091/api/pulse/agents/fraud/duplex?token=ey.jwt"
        );
    }

    #[test]
    fn derive_https_to_wss() {
        let url = derive_ws_url("https://pulse.example.com:443", "pricing", None);
        assert_eq!(
            url,
            "wss://pulse.example.com:444/api/pulse/agents/pricing/duplex"
        );
    }

    #[test]
    fn derive_default_port_when_absent() {
        // No explicit port → host unchanged (cannot bump an absent port).
        let url = derive_ws_url("http://localhost", "ab", None);
        assert_eq!(url, "ws://localhost/api/pulse/agents/ab/duplex");
    }

    #[test]
    fn derive_encodes_agent_id_and_token() {
        let url = derive_ws_url("http://h:1000", "a/b c", Some("a=b&c"));
        assert_eq!(
            url,
            "ws://h:1001/api/pulse/agents/a%2Fb%20c/duplex?token=a%3Db%26c"
        );
    }

    #[test]
    fn generated_ids_are_unique() {
        let a = generate_correlation_id();
        let b = generate_correlation_id();
        assert_ne!(a, b);
        assert!(a.starts_with("pulse-"));
    }
}

//! SSE event-stream consumer — `client.events().stream()` (B-098 Phase 7).
//!
//! Returns a [`Stream`] of parsed events, consumed via [`StreamExt::next`].
//! Cancellation is automatic: drop the stream and the underlying connection
//! closes.
//!
//! # Example
//!
//! ```no_run
//! use futures_util::StreamExt;
//! use pulse_client::PulseClient;
//!
//! # async fn run() -> Result<(), pulse_client::PulseError> {
//! let client = PulseClient::builder()
//!     .base_url("http://localhost:9090")
//!     .token("ey...")
//!     .build()?;
//!
//! let mut stream = client.events().stream().await?;
//! while let Some(event) = stream.next().await {
//!     let event = event?;
//!     println!("{}", event["type"]);
//! }
//! # Ok(())
//! # }
//! ```

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL};
use reqwest::Method;
use serde_json::Value;

use crate::client::PulseClient;
use crate::error::PulseError;

const PATH: &str = "/api/pulse/events/stream";

/// `client.events()` — accessor for the SSE event stream.
pub struct EventsResource<'c> {
    pub(crate) client: &'c PulseClient,
}

impl<'c> EventsResource<'c> {
    /// Subscribes to `GET /api/pulse/events/stream` and returns a [`Stream`]
    /// of parsed events.
    ///
    /// The future resolves once the HTTP response head is received (so auth
    /// errors surface immediately rather than on the first poll). After
    /// that, each call to [`StreamExt::next`] yields the next event as it
    /// arrives on the wire.
    pub async fn stream(self) -> Result<EventsStream, PulseError> {
        let token = self.client.token().ok_or_else(|| PulseError::NoToken {
            path: PATH.to_string(),
        })?;
        if token.is_empty() {
            return Err(PulseError::NoToken {
                path: PATH.to_string(),
            });
        }

        let url = format!("{}{PATH}", self.client.inner.base_url);
        // Note: we do NOT set a per-request `.timeout()` here — that would
        // override the client default. The Client's configured timeout
        // applies as the upper bound for the WHOLE stream. For long-running
        // subscriptions, build the client with a generous timeout (or
        // supply a custom `http_client` without one) via the builder.
        let response = self
            .client
            .inner
            .http
            .get(url)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(ACCEPT, "text/event-stream")
            .header(CACHE_CONTROL, "no-cache")
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let bytes = response.bytes().await?;
            let body = if bytes.is_empty() {
                None
            } else {
                match serde_json::from_slice::<Value>(&bytes) {
                    Ok(v) => Some(v),
                    Err(_) => {
                        let raw = String::from_utf8_lossy(&bytes);
                        Some(serde_json::json!({ "error": raw.to_string() }))
                    }
                }
            };
            return Err(match status.as_u16() {
                401 => PulseError::Auth {
                    path: PATH.to_string(),
                    body,
                },
                other => PulseError::Api {
                    status: other,
                    path: PATH.to_string(),
                    body,
                },
            });
        }

        Ok(EventsStream {
            inner: Box::pin(response.bytes_stream()),
            buffer: BytesMut::with_capacity(4096),
            data_lines: Vec::new(),
            done: false,
        })
    }

    /// `GET /api/pulse/iq/agents/{affecting_state}/state/replay/{key}?from=&to=&limit=`
    /// — B-113 state-change replay.
    ///
    /// Returns the ordered list of changes that touched a state key between
    /// two instants. `affecting_state` is the agent whose state store to
    /// inspect; `key` is the state key. `from` / `to` accept the same specs
    /// as `iq().get_as_of(...)` (`now`, `-1h`, ISO-8601, epoch millis);
    /// `limit` caps the number of changes (server default 100). Each change
    /// carries `timestamp`, `changeType` (`PUT` / `DELETE`), the resulting
    /// `value`, and `eventId` when known. The server's enclosing
    /// `{..., changes:[...]}` envelope is unwrapped — only the `changes`
    /// array is returned (empty when the response omits it).
    ///
    /// ```no_run
    /// # use pulse_client::PulseClient;
    /// # async fn run(client: &PulseClient) -> Result<(), pulse_client::PulseError> {
    /// let changes = client
    ///     .events()
    ///     .replay("user-sessions", "u42", "-1h", "now", 100)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn replay(
        self,
        affecting_state: &str,
        key: &str,
        from: &str,
        to: &str,
        limit: u32,
    ) -> Result<Vec<Value>, PulseError> {
        let path = format!(
            "/api/pulse/iq/agents/{}/state/replay/{}?from={}&to={}&limit={}",
            encode_segment(affecting_state),
            encode_segment(key),
            encode_segment(from),
            encode_segment(to),
            limit,
        );
        let result = self
            .client
            .request(Method::GET, &path, None::<&()>, true)
            .await?;
        Ok(result
            .get("changes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }
}

/// Percent-encodes a path/query segment — same aggressive semantics as the
/// IQ resource so a key like `"user:123/orders"` produces identical URL
/// bytes across the Pulse SDKs.
fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0xF) as usize] as char);
            }
        }
    }
    out
}

const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// `Stream<Item = Result<Value, PulseError>>` — yields parsed SSE events.
///
/// Created by [`EventsResource::stream`]. Drop to cancel the subscription.
pub struct EventsStream {
    inner: Pin<Box<dyn Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>,
    buffer: BytesMut,
    data_lines: Vec<String>,
    done: bool,
}

impl Stream for EventsStream {
    type Item = Result<Value, PulseError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            // First, try to extract an event from the existing buffer.
            if let Some(event) = self.try_parse_buffered_event() {
                return Poll::Ready(Some(Ok(event)));
            }

            // Need more bytes from the wire.
            match self.inner.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(PulseError::Transport(e))));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    self.buffer.extend_from_slice(&chunk);
                    // Loop back to try parsing the new bytes.
                }
            }
        }
    }
}

impl EventsStream {
    /// Walks `self.buffer` looking for the next complete event (blank-line
    /// boundary). Removes consumed bytes; returns `None` if no full event
    /// is buffered yet.
    fn try_parse_buffered_event(&mut self) -> Option<Value> {
        loop {
            let newline_pos = self.buffer.iter().position(|&b| b == b'\n')?;
            // Take the line, including the trailing \n, out of the buffer.
            let line_bytes = self.buffer.split_to(newline_pos + 1);
            // Strip trailing \n and optional \r.
            let line_len = if line_bytes.len() >= 2 && line_bytes[line_bytes.len() - 2] == b'\r' {
                line_bytes.len() - 2
            } else {
                line_bytes.len() - 1
            };
            let line = std::str::from_utf8(&line_bytes[..line_len]).unwrap_or("");

            if line.is_empty() {
                // Event boundary
                if !self.data_lines.is_empty() {
                    let payload = self.data_lines.join("\n");
                    self.data_lines.clear();
                    return Some(match serde_json::from_str::<Value>(&payload) {
                        Ok(v) => v,
                        Err(_) => serde_json::json!({ "data": payload }),
                    });
                }
                continue;
            }
            if line.starts_with(':') {
                continue; // SSE comment / keep-alive
            }
            if let Some(rest) = line.strip_prefix("data:") {
                let value = rest.strip_prefix(' ').unwrap_or(rest);
                self.data_lines.push(value.to_string());
            }
            // event:/id:/retry: consumed but not surfaced.
        }
    }
}

impl std::fmt::Debug for EventsResource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventsResource").finish()
    }
}

impl std::fmt::Debug for EventsStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventsStream")
            .field("done", &self.done)
            .field("buffered_lines", &self.data_lines.len())
            .finish()
    }
}

//! `client.iq()` — B-106 Interactive Queries.
//!
//! Query the live state of streaming agents like a database from any async
//! Rust service. The killer use case is a synchronous decision microservice
//! (fraud, rate-limit, pricing) calling [`IQResource::get`] on every
//! request and reading agent state from RAM with zero ingest-to-decision
//! lag:
//!
//! ```no_run
//! use pulse_client::PulseClient;
//!
//! # async fn run() -> Result<(), pulse_client::PulseError> {
//! let client = PulseClient::builder()
//!     .base_url("http://localhost:9090")
//!     .token("ey...")
//!     .build()?;
//!
//! let state = client.iq().get("fraud-detector", "customer-42").await?;
//! let tx_count = state["value"]["tx_count_60s"].as_u64().unwrap_or(0);
//! if tx_count > 5 {
//!     // deny payment
//! }
//! # Ok(())
//! # }
//! ```
//!
//! All methods require the `AGENT_READ` permission (Owner, Platform Admin,
//! Developer, Auditor personas by default — see B-105).
//!
//! Responses are returned as [`serde_json::Value`] so callers can paginate,
//! inspect `truncated` / `limitApplied` / `totalScanned` metadata, and read
//! fields without going through a wrapper layer. Strongly-typed structs can
//! be layered on top in user code if desired; the SDK stays close to the
//! wire.

use reqwest::Method;
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::client::PulseClient;
use crate::error::PulseError;

/// `client.iq()` — accessor for Interactive Queries.
pub struct IQResource<'c> {
    pub(crate) client: &'c PulseClient,
}

/// Optional range bounds + page size for [`IQResource::scan`] and
/// [`IQResource::list_keys`].
///
/// Default = no range, limit 100. Limit > 1000 is clamped server-side
/// (response carries `X-Pulse-Pagination-Clamped: true` header when
/// clamped — not surfaced in the parsed body).
#[derive(Debug, Clone, Default)]
pub struct IQScanOptions {
    /// Inclusive lower bound on the key range. `None` = beginning.
    pub start: Option<String>,
    /// Exclusive upper bound on the key range. `None` = end.
    pub end: Option<String>,
    /// Page size. `None` defaults to 100.
    pub limit: Option<u32>,
}

impl IQScanOptions {
    /// Returns default options (no range, limit 100).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the inclusive lower bound.
    pub fn start(mut self, start: impl Into<String>) -> Self {
        self.start = Some(start.into());
        self
    }

    /// Sets the exclusive upper bound.
    pub fn end(mut self, end: impl Into<String>) -> Self {
        self.end = Some(end.into());
        self
    }

    /// Sets the page size.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Optional inputs for [`IQResource::query`].
///
/// The `filter` is a recursive [`Value`] shaped per the IQFilterExpression
/// schema: each node MUST carry exactly ONE of `field` (leaf), `and`
/// (array of sub-expressions, all must match), `or` (array, any must
/// match), or `not` (single sub-expression). Mixing in a single node
/// returns HTTP 400.
///
/// Use the [`iq_leaf`], [`iq_and`], [`iq_or`], [`iq_not`] free functions
/// to construct filter trees ergonomically.
#[derive(Debug, Clone, Default)]
pub struct IQQueryOptions {
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<u32>,
    pub filter: Option<Value>,
    pub projection: Option<Vec<String>>,
    /// Field name to group on. `Some` switches the response shape from
    /// flat `{entries, ...}` to grouped `{groups: [{groupKey, count}], ...}`.
    /// Use `"$value"` for scalar states.
    pub group_by: Option<String>,
}

impl IQQueryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start(mut self, s: impl Into<String>) -> Self {
        self.start = Some(s.into());
        self
    }

    pub fn end(mut self, s: impl Into<String>) -> Self {
        self.end = Some(s.into());
        self
    }

    pub fn limit(mut self, n: u32) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn filter(mut self, f: Value) -> Self {
        self.filter = Some(f);
        self
    }

    pub fn projection(mut self, fields: Vec<String>) -> Self {
        self.projection = Some(fields);
        self
    }

    pub fn group_by(mut self, field: impl Into<String>) -> Self {
        self.group_by = Some(field.into());
        self
    }
}

/// Builds a leaf filter node: `{"field": ..., "op": ..., "value": ...}`.
/// Pass an empty `op` to omit it (e.g. for an `exists`-style test where
/// the field's mere presence suffices).
pub fn iq_leaf(field: &str, op: &str, value: impl Serialize) -> Value {
    let mut m = Map::new();
    m.insert("field".into(), Value::String(field.into()));
    if !op.is_empty() {
        m.insert("op".into(), Value::String(op.into()));
    }
    m.insert(
        "value".into(),
        serde_json::to_value(value).unwrap_or(Value::Null),
    );
    Value::Object(m)
}

/// Builds an AND filter combining all children.
pub fn iq_and(children: Vec<Value>) -> Value {
    json!({ "and": children })
}

/// Builds an OR filter combining all children.
pub fn iq_or(children: Vec<Value>) -> Value {
    json!({ "or": children })
}

/// Builds a NOT filter negating its child.
pub fn iq_not(child: Value) -> Value {
    json!({ "not": child })
}

impl<'c> IQResource<'c> {
    /// `GET /api/pulse/iq/agents/{id}/state` — headline state summary.
    ///
    /// Returns the IQSummary [`Value`] — always carries `agentId`,
    /// `queryable`, `backend`, `hotSize`, `hotBytes`, `coldSize`,
    /// `coldBytes`, `lastCheckpointId`, `totalSize`. When the agent has
    /// no live streaming backend: `queryable=false`, `backend="none"`,
    /// numerics 0, `lastCheckpointId=-1`.
    pub async fn summary(self, agent_id: &str) -> Result<Value, PulseError> {
        let path = format!("/api/pulse/iq/agents/{}/state", encode_segment(agent_id));
        self.client
            .request(Method::GET, &path, None::<&()>, true)
            .await
    }

    /// `GET /api/pulse/iq/agents/{id}/state/value/{key}` — point lookup.
    ///
    /// Returns the IQValue [`Value`] (`agentId`, `key`, `value` — `value`
    /// can be any JSON type including `null`).
    ///
    /// # Errors
    /// Returns [`PulseError::NotFound`] when the key is absent OR the
    /// agent is not queryable. Inspect the variant's `body` field:
    /// `error == "Key not found"` vs `error == "Agent has no queryable
    /// state"` (with `reason` field) — to distinguish.
    pub async fn get(self, agent_id: &str, key: &str) -> Result<Value, PulseError> {
        let path = format!(
            "/api/pulse/iq/agents/{}/state/value/{}",
            encode_segment(agent_id),
            encode_segment(key),
        );
        self.client
            .request(Method::GET, &path, None::<&()>, true)
            .await
    }

    /// `GET /api/pulse/iq/agents/{id}/state/scan` — paginated range scan.
    ///
    /// Inspect `truncated` to decide if more data exists; paginate by
    /// setting `opts.start` on the next call to the last returned key
    /// plus a sentinel suffix.
    pub async fn scan(self, agent_id: &str, opts: IQScanOptions) -> Result<Value, PulseError> {
        let path = format!(
            "/api/pulse/iq/agents/{}/state/scan{}",
            encode_segment(agent_id),
            scan_query(&opts),
        );
        self.client
            .request(Method::GET, &path, None::<&()>, true)
            .await
    }

    /// `GET /api/pulse/iq/agents/{id}/state/keys` — keys-only range scan.
    ///
    /// Same shape as [`scan`](Self::scan) minus the values; `keys` field
    /// is a JSON array of strings.
    pub async fn list_keys(self, agent_id: &str, opts: IQScanOptions) -> Result<Value, PulseError> {
        let path = format!(
            "/api/pulse/iq/agents/{}/state/keys{}",
            encode_segment(agent_id),
            scan_query(&opts),
        );
        self.client
            .request(Method::GET, &path, None::<&()>, true)
            .await
    }

    /// `POST /api/pulse/iq/agents/{id}/state/query` — filtered / projected
    /// / grouped query.
    ///
    /// When `opts.group_by` is set, the response shape is
    /// `{groups: [{groupKey, count}], groupCount, ...}` instead of
    /// `{entries: [...], count, ...}`.
    ///
    /// # Errors
    /// - [`PulseError::Validation`] on invalid filter syntax (HTTP 400)
    /// - [`PulseError::NotFound`] when the agent is not queryable
    pub async fn query(self, agent_id: &str, opts: IQQueryOptions) -> Result<Value, PulseError> {
        let path = format!(
            "/api/pulse/iq/agents/{}/state/query",
            encode_segment(agent_id),
        );
        let body = build_query_body(opts);
        // Empty body → send None so the server defaults to a full scan
        // (matches the in-tree handler's behaviour on missing body).
        if body.is_object() && body.as_object().is_some_and(|m| m.is_empty()) {
            self.client
                .request::<()>(Method::POST, &path, None, true)
                .await
        } else {
            self.client
                .request(Method::POST, &path, Some(&body), true)
                .await
        }
    }
}

/// Builds the `?limit=N&start=...&end=...` query suffix.
///
/// `limit` is always sent (defaulting to 100 when `opts.limit` is `None`)
/// so the server gets a deterministic value. Missing `start`/`end` are
/// omitted so the URL stays clean.
fn scan_query(opts: &IQScanOptions) -> String {
    let limit = opts.limit.unwrap_or(100);
    let mut q = format!("?limit={limit}");
    if let Some(start) = &opts.start {
        q.push_str("&start=");
        q.push_str(&encode_segment(start));
    }
    if let Some(end) = &opts.end {
        q.push_str("&end=");
        q.push_str(&encode_segment(end));
    }
    q
}

/// Flattens [`IQQueryOptions`] into the JSON body the server expects.
/// Only includes fields the caller actually set so the wire payload is
/// stable + diff-friendly.
fn build_query_body(opts: IQQueryOptions) -> Value {
    let mut m = Map::new();
    if let Some(s) = opts.start {
        m.insert("start".into(), Value::String(s));
    }
    if let Some(e) = opts.end {
        m.insert("end".into(), Value::String(e));
    }
    if let Some(l) = opts.limit {
        m.insert("limit".into(), Value::Number(l.into()));
    }
    if let Some(f) = opts.filter {
        m.insert("filter".into(), f);
    }
    if let Some(p) = opts.projection {
        m.insert(
            "projection".into(),
            Value::Array(p.into_iter().map(Value::String).collect()),
        );
    }
    if let Some(g) = opts.group_by {
        m.insert("groupBy".into(), Value::String(g));
    }
    Value::Object(m)
}

/// Percent-encodes a path segment aggressively — same semantics as
/// Python's `urllib.quote(safe='')`, Java's `URLEncoder.encode` followed
/// by `'+'`→`'%20'`, JS's `encodeURIComponent`, and Go's QueryEscape +
/// `'+'`→`'%20'`. Keeps the wire format identical across all 5 Pulse
/// SDKs so a key like `"user:123/orders"` produces the same URL bytes
/// regardless of caller language.
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

impl std::fmt::Debug for IQResource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IQResource").finish()
    }
}

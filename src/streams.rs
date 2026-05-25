//! B-107 — Kafka-Streams-like declarative DSL that compiles to a Pulse pipeline.
//!
//! The DSL is **server-side execution, client-side declaration**: the operator
//! chain is built in Rust, compiled to the JSON pipeline shape that the Pulse
//! server's `StreamingOperatorValidator` accepts, and POSTed to
//! `/api/pulse/pipelines`. Stream processing then runs on the Pulse engine
//! (3.6 M evt/s native throughput), not in the client process.
//!
//! This is the opposite of Kafka Streams (which executes in the caller's JVM).
//! The trade-off: you can't do microsecond client-side compute, but you get
//! infinite-scale stateful streaming, durable replicated state queryable via
//! B-106 IQ, and the same DSL works from any of the 5 Pulse SDKs.
//!
//! # Quick start
//!
//! ```no_run
//! use pulse_client::{aggs, windows, PulseClient, StreamBuilder};
//!
//! # async fn run() -> Result<(), pulse_client::PulseError> {
//! let client = PulseClient::builder()
//!     .base_url("http://localhost:9090")
//!     .token("ey...")
//!     .build()?;
//!
//! let mut aggregations = std::collections::BTreeMap::new();
//! aggregations.insert("avgTemp".to_string(), aggs::avg("temperature"));
//!
//! let builder = StreamBuilder::new("iot-temperature-aggregator")
//!     .from_topic_with_engine("sensor-readings", "mqtt")
//!     .key_by("deviceId")
//!     .window_with_aggs(windows::tumbling("60s"), aggregations)
//!     .filter("avgTemp > 75")
//!     .to_topic_with_channel("sensor-minute-averages", "email");
//!
//! client.streams().deploy(&builder).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Supported operators (mirror the 11 validated by the server's
//! `StreamingOperatorValidator`): `filter`, `map`, `flat_map`, `key_by`,
//! `window`, `branch`, `enrich`, `enrich_async`, `cep`, `broadcast_join`,
//! `cdc_join`.
//!
//! Conditions and field-expressions are passed as **strings** — closures /
//! lambdas are NOT supported because they can't be serialised to JSON.

use std::collections::BTreeMap;

use reqwest::Method;
use serde_json::{json, Map, Value};

use crate::client::PulseClient;
use crate::error::PulseError;

// ---------------------------------------------------------------------------
// Window specs
// ---------------------------------------------------------------------------

/// A window specification. Compiled to the string form the server expects.
///
/// Construct via the [`windows`] helpers — never instantiate directly unless
/// you've validated the raw string against `WindowEngine.parseSpec`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    spec: String,
}

impl WindowSpec {
    /// Wraps a pre-validated raw spec string. Panics on empty input.
    pub fn new(spec: impl Into<String>) -> Self {
        let spec = spec.into();
        if spec.trim().is_empty() {
            panic!("WindowSpec requires a non-empty spec string");
        }
        Self { spec }
    }

    /// The raw spec string as it will appear on the wire.
    pub fn spec(&self) -> &str {
        &self.spec
    }
}

impl std::fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.spec)
    }
}

/// Window-spec factory namespace.
///
/// Each function returns a [`WindowSpec`] compiled to the exact form the
/// server's `WindowEngine.parseSpec` accepts.
pub mod windows {
    use super::WindowSpec;

    /// Non-overlapping fixed windows: `tumbling("60s")`.
    pub fn tumbling(size: &str) -> WindowSpec {
        require_nonblank("size", size);
        WindowSpec::new(format!("tumbling({size})"))
    }

    /// Overlapping windows: `sliding("10m", "1m")` = size, slide.
    pub fn sliding(size: &str, slide: &str) -> WindowSpec {
        require_nonblank("size", size);
        require_nonblank("slide", slide);
        WindowSpec::new(format!("sliding({size},{slide})"))
    }

    /// Inactivity-bounded windows: `session("30s")`.
    pub fn session(timeout: &str) -> WindowSpec {
        require_nonblank("timeout", timeout);
        WindowSpec::new(format!("session({timeout})"))
    }

    /// Single unbounded window. Use for global aggregates.
    pub fn global() -> WindowSpec {
        WindowSpec::new("global")
    }

    /// Event-count tumbling: closes after `n` events. `count(100)`.
    pub fn count(n: u64) -> WindowSpec {
        if n == 0 {
            panic!("count window size must be positive, got 0");
        }
        WindowSpec::new(format!("count({n})"))
    }

    /// Event-count sliding: `count_sliding(100, 10)` = window, slide.
    pub fn count_sliding(size: u64, slide: u64) -> WindowSpec {
        if size == 0 || slide == 0 {
            panic!("count_sliding requires positive size and slide, got {size}, {slide}");
        }
        WindowSpec::new(format!("count_sliding({size},{slide})"))
    }

    fn require_nonblank(name: &str, value: &str) {
        if value.trim().is_empty() {
            panic!("{name} must be a non-empty string");
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregators
// ---------------------------------------------------------------------------

/// Aggregator factory namespace.
///
/// Each function returns the string template the server's `Aggregators.parse`
/// accepts inside `window.aggregations` (e.g. `"avg(temperature)"`).
pub mod aggs {
    /// Event count — no field required.
    pub fn count() -> String {
        "count()".into()
    }

    /// Sum of a numeric field: `aggs::sum("amount")`.
    pub fn sum(field: &str) -> String {
        require_nonblank("field", field);
        format!("sum({field})")
    }

    /// Average of a numeric field.
    pub fn avg(field: &str) -> String {
        require_nonblank("field", field);
        format!("avg({field})")
    }

    /// Minimum value of a numeric field.
    pub fn min(field: &str) -> String {
        require_nonblank("field", field);
        format!("min({field})")
    }

    /// Maximum value of a numeric field.
    pub fn max(field: &str) -> String {
        require_nonblank("field", field);
        format!("max({field})")
    }

    /// Collect every value of `field` into a list.
    pub fn collect_list(field: &str) -> String {
        require_nonblank("field", field);
        format!("collect_list({field})")
    }

    /// Cardinality of distinct values of `field`.
    pub fn distinct_count(field: &str) -> String {
        require_nonblank("field", field);
        format!("distinct_count({field})")
    }

    fn require_nonblank(name: &str, value: &str) {
        if value.trim().is_empty() {
            panic!("{name} must be a non-empty string");
        }
    }
}

// ---------------------------------------------------------------------------
// Option carriers
// ---------------------------------------------------------------------------

/// Options for [`StreamBuilder::map`].
#[derive(Debug, Clone, Default)]
pub struct MapOptions {
    /// Output-field-name → source-expression-string mapping.
    pub fields: Option<BTreeMap<String, String>>,
    /// Tag the output event with a `type` field.
    pub target_type: Option<String>,
}

/// Options for [`StreamBuilder::window`].
#[derive(Debug, Clone, Default)]
pub struct WindowOptions {
    /// Map of output-field → aggregator-string (use [`aggs`] for the right-hand side).
    pub aggregations: Option<BTreeMap<String, String>>,
    /// Override for where window results go.
    pub output_topic: Option<String>,
    /// Server-side trigger config (passed through opaquely).
    pub trigger: Option<Value>,
}

/// One branch of [`StreamBuilder::branch`].
#[derive(Debug, Clone)]
pub struct BranchSpec {
    pub condition: String,
    pub topic: String,
}

impl BranchSpec {
    pub fn new(condition: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            condition: condition.into(),
            topic: topic.into(),
        }
    }
}

/// Options for [`StreamBuilder::enrich_async`].
#[derive(Debug, Clone, Default)]
pub struct EnrichAsyncOptions {
    pub url: String,
    pub parallelism: Option<u32>,
    pub queue_size: Option<u32>,
    pub timeout_ms: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<u32>,
    /// Must be `"PRESERVE_INPUT"` or `"UNORDERED"`.
    pub ordering: Option<String>,
    /// Must be `"EMIT_ERROR"`, `"DROP"`, or `"PASS_THROUGH"`.
    pub on_failure: Option<String>,
}

/// Options for [`StreamBuilder::cep`].
#[derive(Debug, Clone, Default)]
pub struct CepOptions {
    pub within: Option<String>,
    pub name: Option<String>,
}

/// Options for [`StreamBuilder::broadcast_join`].
#[derive(Debug, Clone, Default)]
pub struct BroadcastJoinOptions {
    pub join_key_field: String,
    pub streaming_topic: Option<String>,
    pub name: Option<String>,
    pub max_bytes: Option<i64>,
    /// Must be `"cdc"`, `"periodic"`, or `"explicit"`.
    pub refresh_mode: Option<String>,
    pub interval_millis: Option<u32>,
}

/// Options for [`StreamBuilder::cdc_join`].
#[derive(Debug, Clone, Default)]
pub struct CdcJoinOptions {
    pub source: String,
    pub join_key: Option<String>,
    pub table: Option<String>,
    pub state_backend: Option<String>,
}

/// B-109 — options for [`StreamBuilder::map_llm`]. `output_field` is required.
#[derive(Debug, Clone, Default)]
pub struct MapLlmOptions {
    pub output_field: String,
    pub model: Option<String>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u32>,
    pub parallelism: Option<u32>,
    /// Must be `"PRESERVE_INPUT"` or `"UNORDERED"`.
    pub ordering: Option<String>,
    /// Must be `"EMIT_ERROR"`, `"DROP"`, or `"PASS_THROUGH"`.
    pub on_failure: Option<String>,
    pub max_calls_per_sec: Option<u32>,
}

/// B-109 — options for [`StreamBuilder::extract`]. `instruction` + `schema` required.
#[derive(Debug, Clone, Default)]
pub struct ExtractOptions {
    pub instruction: String,
    pub schema: BTreeMap<String, String>,
    pub model: Option<String>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u32>,
    pub on_failure: Option<String>,
}

/// B-109 Phase 2 — options for [`StreamBuilder::mcp_call`].
#[derive(Debug, Clone, Default)]
pub struct McpCallOptions {
    pub args: Option<BTreeMap<String, Value>>,
    pub output_field: Option<String>,
    pub parallelism: Option<u32>,
    pub ordering: Option<String>,
    pub on_failure: Option<String>,
}

// ---------------------------------------------------------------------------
// StreamBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for a Pulse streaming pipeline.
///
/// Chain operator methods, then call [`build`](Self::build) (or pass to
/// [`StreamsResource::deploy`]).
///
/// All operator methods take `&mut self` and return `Self` so calls chain
/// naturally. Methods that validate their inputs panic on obviously-bad
/// arguments (blank required fields, non-positive counts, unknown enum
/// values) so bugs are caught at call site, not after a 400 round-trip.
#[derive(Debug, Clone, Default)]
pub struct StreamBuilder {
    name: Option<String>,
    description: Option<String>,
    agent_label: Option<String>,
    input_topic: Option<String>,
    source_engine: Option<String>,
    source_config: Map<String, Value>,
    source_label: Option<String>,
    output_topic: Option<String>,
    sink_channel: Option<String>,
    sink_config: Map<String, Value>,
    sink_label: Option<String>,
    operators: Vec<Map<String, Value>>,
}

impl StreamBuilder {
    /// Builder with the given pipeline name preset.
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        require_nonblank("name", &name);
        Self {
            name: Some(name),
            ..Self::default()
        }
    }

    /// Builder with no preset name. Use [`named`](Self::named) or pass the
    /// name to [`build_with_name`](Self::build_with_name).
    pub fn anonymous() -> Self {
        Self::default()
    }

    // ------------------------------------------------------------------
    // Source
    // ------------------------------------------------------------------

    /// Sets the input topic. Source engine defaults to `"kafka"`.
    pub fn from_topic(mut self, topic: impl Into<String>) -> Self {
        let topic = topic.into();
        require_nonblank("topic", &topic);
        self.input_topic = Some(topic);
        self.source_engine = Some("kafka".into());
        self
    }

    /// Sets the input topic + source engine.
    pub fn from_topic_with_engine(
        mut self,
        topic: impl Into<String>,
        engine: impl Into<String>,
    ) -> Self {
        let topic = topic.into();
        let engine = engine.into();
        require_nonblank("topic", &topic);
        require_nonblank("engine", &engine);
        self.input_topic = Some(topic);
        self.source_engine = Some(engine);
        self
    }

    /// Merges extra config into the source node's `config` map.
    pub fn with_source_config(mut self, key: impl Into<String>, value: Value) -> Self {
        self.source_config.insert(key.into(), value);
        self
    }

    /// Sets the display label for the source node.
    pub fn with_source_label(mut self, label: impl Into<String>) -> Self {
        self.source_label = Some(label.into());
        self
    }

    // ------------------------------------------------------------------
    // Operators
    // ------------------------------------------------------------------

    /// Filter operator. `condition` is a CEL-like expression string.
    pub fn filter(mut self, condition: impl Into<String>) -> Self {
        let condition = condition.into();
        require_nonblank("condition", &condition);
        let mut op = Map::new();
        op.insert("type".into(), Value::String("filter".into()));
        op.insert("condition".into(), Value::String(condition));
        self.operators.push(op);
        self
    }

    /// Map operator. At least one of `options.fields` / `options.target_type` is required.
    pub fn map(mut self, options: MapOptions) -> Self {
        if options.fields.is_none() && options.target_type.is_none() {
            panic!("map operator does nothing — provide `fields` or `target_type`");
        }
        let mut op = Map::new();
        op.insert("type".into(), Value::String("map".into()));
        if let Some(fields) = options.fields {
            let mut m = Map::new();
            for (k, v) in fields {
                m.insert(k, Value::String(v));
            }
            op.insert("fields".into(), Value::Object(m));
        }
        if let Some(t) = options.target_type {
            op.insert("targetType".into(), Value::String(t));
        }
        self.operators.push(op);
        self
    }

    /// Flat-map: explode an array-valued field into one event per element.
    pub fn flat_map(mut self, split_field: impl Into<String>) -> Self {
        let split_field = split_field.into();
        require_nonblank("split_field", &split_field);
        let mut op = Map::new();
        op.insert("type".into(), Value::String("flatMap".into()));
        op.insert("splitField".into(), Value::String(split_field));
        self.operators.push(op);
        self
    }

    /// Group the stream by a top-level field value. Required before stateful ops.
    pub fn key_by(mut self, field: impl Into<String>) -> Self {
        let field = field.into();
        require_nonblank("field", &field);
        let mut op = Map::new();
        op.insert("type".into(), Value::String("keyBy".into()));
        op.insert("field".into(), Value::String(field));
        self.operators.push(op);
        self
    }

    /// Window operator with no extra options.
    pub fn window(self, spec: WindowSpec) -> Self {
        self.window_full(spec, WindowOptions::default())
    }

    /// Window operator with aggregations only.
    pub fn window_with_aggs(
        self,
        spec: WindowSpec,
        aggregations: BTreeMap<String, String>,
    ) -> Self {
        self.window_full(
            spec,
            WindowOptions {
                aggregations: Some(aggregations),
                ..Default::default()
            },
        )
    }

    /// Window operator with the full option set.
    pub fn window_full(mut self, spec: WindowSpec, options: WindowOptions) -> Self {
        let mut op = Map::new();
        op.insert("type".into(), Value::String("window".into()));
        op.insert("spec".into(), Value::String(spec.spec.clone()));
        if let Some(aggs_map) = options.aggregations {
            let mut m = Map::new();
            for (k, v) in aggs_map {
                m.insert(k, Value::String(v));
            }
            op.insert("aggregations".into(), Value::Object(m));
        }
        if let Some(out) = options.output_topic {
            op.insert("outputTopic".into(), Value::String(out));
        }
        if let Some(trig) = options.trigger {
            op.insert("trigger".into(), trig);
        }
        self.operators.push(op);
        self
    }

    /// Window operator with a raw spec string. Useful when you've already
    /// validated the spec against `WindowEngine.parseSpec`.
    pub fn window_from_str(mut self, spec: &str, options: WindowOptions) -> Self {
        require_nonblank("spec", spec);
        self = self.window_full(WindowSpec::new(spec), options);
        self
    }

    /// Branch operator: route events to different topics by condition.
    pub fn branch(mut self, branches: Vec<BranchSpec>) -> Self {
        if branches.is_empty() {
            panic!("branch operator requires at least one branch");
        }
        let mut normalised = Vec::with_capacity(branches.len());
        for (i, b) in branches.iter().enumerate() {
            if b.condition.trim().is_empty() {
                panic!("branch[{i}] requires a non-empty `condition`");
            }
            if b.topic.trim().is_empty() {
                panic!("branch[{i}] requires a non-empty `topic`");
            }
            normalised.push(json!({
                "condition": b.condition,
                "topic": b.topic,
            }));
        }
        let mut op = Map::new();
        op.insert("type".into(), Value::String("branch".into()));
        op.insert("branches".into(), Value::Array(normalised));
        self.operators.push(op);
        self
    }

    /// Synchronous enrichment: join the stream against a state-store topic.
    pub fn enrich(mut self, lookup_topic: impl Into<String>, key_field: impl Into<String>) -> Self {
        let lookup_topic = lookup_topic.into();
        let key_field = key_field.into();
        require_nonblank("lookup_topic", &lookup_topic);
        require_nonblank("key_field", &key_field);
        let mut op = Map::new();
        op.insert("type".into(), Value::String("enrich".into()));
        op.insert("lookupTopic".into(), Value::String(lookup_topic));
        op.insert("keyField".into(), Value::String(key_field));
        self.operators.push(op);
        self
    }

    /// Asynchronous HTTP enrichment. `url` supports `{field}` placeholders.
    pub fn enrich_async(mut self, options: EnrichAsyncOptions) -> Self {
        require_nonblank("url", &options.url);
        if let Some(ref o) = options.ordering {
            if o != "PRESERVE_INPUT" && o != "UNORDERED" {
                panic!("ordering must be PRESERVE_INPUT or UNORDERED, got {o:?}");
            }
        }
        if let Some(ref f) = options.on_failure {
            if f != "EMIT_ERROR" && f != "DROP" && f != "PASS_THROUGH" {
                panic!("on_failure must be EMIT_ERROR, DROP, or PASS_THROUGH, got {f:?}");
            }
        }
        let mut op = Map::new();
        op.insert("type".into(), Value::String("enrichAsync".into()));
        op.insert("url".into(), Value::String(options.url));
        if let Some(v) = options.parallelism {
            op.insert("parallelism".into(), Value::Number(v.into()));
        }
        if let Some(v) = options.queue_size {
            op.insert("queueSize".into(), Value::Number(v.into()));
        }
        if let Some(v) = options.timeout_ms {
            op.insert("timeoutMs".into(), Value::Number(v.into()));
        }
        if let Some(v) = options.max_retries {
            op.insert("maxRetries".into(), Value::Number(v.into()));
        }
        if let Some(v) = options.retry_backoff_ms {
            op.insert("retryBackoffMs".into(), Value::Number(v.into()));
        }
        if let Some(o) = options.ordering {
            op.insert("ordering".into(), Value::String(o));
        }
        if let Some(f) = options.on_failure {
            op.insert("onFailure".into(), Value::String(f));
        }
        self.operators.push(op);
        self
    }

    /// Complex Event Processing: match a sequence of conditions.
    pub fn cep(mut self, sequence: Vec<Value>, options: CepOptions) -> Self {
        if sequence.is_empty() {
            panic!("cep operator requires a non-empty sequence");
        }
        let mut op = Map::new();
        op.insert("type".into(), Value::String("cep".into()));
        op.insert("sequence".into(), Value::Array(sequence));
        if let Some(w) = options.within {
            op.insert("within".into(), Value::String(w));
        }
        if let Some(n) = options.name {
            op.insert("name".into(), Value::String(n));
        }
        self.operators.push(op);
        self
    }

    /// B-109 — enrich each event with an LLM completion. `prompt` supports
    /// `{field}` placeholders (and `{__payload__}`) substituted from the event
    /// server-side; the completion lands on the event under `output_field`.
    pub fn map_llm(mut self, prompt: impl Into<String>, options: MapLlmOptions) -> Self {
        let prompt = prompt.into();
        require_nonblank("prompt", &prompt);
        require_nonblank("output_field", &options.output_field);
        if let Some(ref o) = options.ordering {
            if o != "PRESERVE_INPUT" && o != "UNORDERED" {
                panic!("ordering must be PRESERVE_INPUT or UNORDERED, got {o:?}");
            }
        }
        check_failure(&options.on_failure);
        let mut op = Map::new();
        op.insert("type".into(), Value::String("mapLlm".into()));
        op.insert("prompt".into(), Value::String(prompt));
        op.insert("outputField".into(), Value::String(options.output_field));
        if let Some(m) = options.model {
            op.insert("model".into(), Value::String(m));
        }
        if let Some(t) = options.temperature {
            op.insert("temperature".into(), json!(t));
        }
        if let Some(n) = options.max_tokens {
            op.insert("maxTokens".into(), Value::Number(n.into()));
        }
        if let Some(n) = options.parallelism {
            op.insert("parallelism".into(), Value::Number(n.into()));
        }
        if let Some(o) = options.ordering {
            op.insert("ordering".into(), Value::String(o));
        }
        if let Some(f) = options.on_failure {
            op.insert("onFailure".into(), Value::String(f));
        }
        if let Some(n) = options.max_calls_per_sec {
            op.insert("maxCallsPerSec".into(), Value::Number(n.into()));
        }
        self.operators.push(op);
        self
    }

    /// B-109 — LLM → typed structured fields merged into the event. The LLM is
    /// asked for a JSON object keyed by `options.schema`'s fields; missing /
    /// malformed fields become null server-side.
    pub fn extract(mut self, options: ExtractOptions) -> Self {
        require_nonblank("instruction", &options.instruction);
        if options.schema.is_empty() {
            panic!("extract operator requires a non-empty schema");
        }
        check_failure(&options.on_failure);
        let mut schema = Map::new();
        for (k, v) in options.schema {
            schema.insert(k, Value::String(v));
        }
        let mut op = Map::new();
        op.insert("type".into(), Value::String("extract".into()));
        op.insert("instruction".into(), Value::String(options.instruction));
        op.insert("schema".into(), Value::Object(schema));
        if let Some(m) = options.model {
            op.insert("model".into(), Value::String(m));
        }
        if let Some(t) = options.temperature {
            op.insert("temperature".into(), json!(t));
        }
        if let Some(n) = options.max_tokens {
            op.insert("maxTokens".into(), Value::Number(n.into()));
        }
        if let Some(f) = options.on_failure {
            op.insert("onFailure".into(), Value::String(f));
        }
        self.operators.push(op);
        self
    }

    /// B-109 Phase 2 — invoke an MCP tool per event. `options.args` string
    /// values support `{field}` substitution. On success the tool output is
    /// written to `options.output_field` (omit for a fire-and-forget effect).
    pub fn mcp_call(mut self, tool: impl Into<String>, options: McpCallOptions) -> Self {
        let tool = tool.into();
        require_nonblank("tool", &tool);
        if let Some(ref o) = options.ordering {
            if o != "PRESERVE_INPUT" && o != "UNORDERED" {
                panic!("ordering must be PRESERVE_INPUT or UNORDERED, got {o:?}");
            }
        }
        check_failure(&options.on_failure);
        let mut op = Map::new();
        op.insert("type".into(), Value::String("mcpCall".into()));
        op.insert("tool".into(), Value::String(tool));
        if let Some(args) = options.args {
            let mut m = Map::new();
            for (k, v) in args {
                m.insert(k, v);
            }
            op.insert("args".into(), Value::Object(m));
        }
        if let Some(f) = options.output_field {
            op.insert("outputField".into(), Value::String(f));
        }
        if let Some(n) = options.parallelism {
            op.insert("parallelism".into(), Value::Number(n.into()));
        }
        if let Some(o) = options.ordering {
            op.insert("ordering".into(), Value::String(o));
        }
        if let Some(f) = options.on_failure {
            op.insert("onFailure".into(), Value::String(f));
        }
        self.operators.push(op);
        self
    }

    /// Broadcast join: enrich the stream against a fully-replicated table.
    pub fn broadcast_join(mut self, options: BroadcastJoinOptions) -> Self {
        require_nonblank("join_key_field", &options.join_key_field);
        if let Some(ref m) = options.refresh_mode {
            if m != "cdc" && m != "periodic" && m != "explicit" {
                panic!("refresh_mode must be cdc, periodic, or explicit, got {m:?}");
            }
        }
        let mut op = Map::new();
        op.insert("type".into(), Value::String("broadcastJoin".into()));
        op.insert("joinKeyField".into(), Value::String(options.join_key_field));
        if let Some(t) = options.streaming_topic {
            op.insert("streamingTopic".into(), Value::String(t));
        }
        if let Some(n) = options.name {
            op.insert("name".into(), Value::String(n));
        }
        if let Some(b) = options.max_bytes {
            op.insert("maxBytes".into(), Value::Number(b.into()));
        }
        if let Some(m) = options.refresh_mode {
            op.insert("refreshMode".into(), Value::String(m));
        }
        if let Some(i) = options.interval_millis {
            op.insert("intervalMillis".into(), Value::Number(i.into()));
        }
        self.operators.push(op);
        self
    }

    /// CDC join: stream-table join against a CDC-fed state table.
    pub fn cdc_join(mut self, options: CdcJoinOptions) -> Self {
        require_nonblank("source", &options.source);
        let mut op = Map::new();
        op.insert("type".into(), Value::String("cdcJoin".into()));
        op.insert("source".into(), Value::String(options.source));
        if let Some(k) = options.join_key {
            op.insert("joinKey".into(), Value::String(k));
        }
        if let Some(t) = options.table {
            op.insert("table".into(), Value::String(t));
        }
        if let Some(b) = options.state_backend {
            op.insert("stateBackend".into(), Value::String(b));
        }
        self.operators.push(op);
        self
    }

    // ------------------------------------------------------------------
    // Sink
    // ------------------------------------------------------------------

    /// Sets the output topic only. No sink node is emitted.
    pub fn to_topic(mut self, topic: impl Into<String>) -> Self {
        let topic = topic.into();
        require_nonblank("topic", &topic);
        self.output_topic = Some(topic);
        self.sink_channel = None;
        self
    }

    /// Sets the output topic + sink channel (emits a sink node).
    pub fn to_topic_with_channel(
        mut self,
        topic: impl Into<String>,
        channel: impl Into<String>,
    ) -> Self {
        let topic = topic.into();
        let channel = channel.into();
        require_nonblank("topic", &topic);
        require_nonblank("channel", &channel);
        self.output_topic = Some(topic);
        self.sink_channel = Some(channel);
        self
    }

    /// Merges extra config into the sink node's `config` map.
    pub fn with_sink_config(mut self, key: impl Into<String>, value: Value) -> Self {
        self.sink_config.insert(key.into(), value);
        self
    }

    /// Sets the display label for the sink node.
    pub fn with_sink_label(mut self, label: impl Into<String>) -> Self {
        self.sink_label = Some(label.into());
        self
    }

    /// Terminate the stream in the agent's state store (queryable via B-106 IQ).
    pub fn to_state(mut self) -> Self {
        self.output_topic = None;
        self.sink_channel = None;
        self.sink_config = Map::new();
        self.sink_label = None;
        self
    }

    // ------------------------------------------------------------------
    // Metadata
    // ------------------------------------------------------------------

    /// Sets / overrides the pipeline name.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        require_nonblank("name", &name);
        self.name = Some(name);
        self
    }

    /// Sets the pipeline description.
    pub fn described_as(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets the display label for the streaming agent node.
    pub fn with_agent_label(mut self, label: impl Into<String>) -> Self {
        let label = label.into();
        require_nonblank("label", &label);
        self.agent_label = Some(label);
        self
    }

    // ------------------------------------------------------------------
    // Compilation
    // ------------------------------------------------------------------

    /// Returns a read-only view of the recorded operator chain.
    pub fn operators(&self) -> &[Map<String, Value>] {
        &self.operators
    }

    /// Compile the chain into a Pulse pipeline dict ready for POST.
    pub fn build(&self) -> Result<Value, PulseError> {
        self.build_inner(None)
    }

    /// Same as [`build`](Self::build) but overrides the pipeline name.
    pub fn build_with_name(&self, name: &str) -> Result<Value, PulseError> {
        require_nonblank("name", name);
        self.build_inner(Some(name.to_string()))
    }

    fn build_inner(&self, override_name: Option<String>) -> Result<Value, PulseError> {
        let pipeline_name = override_name.or_else(|| self.name.clone()).ok_or_else(|| {
            PulseError::InvalidConfig(
                "pipeline name required — pass to StreamBuilder::new or build_with_name".into(),
            )
        })?;
        let input_topic = self.input_topic.as_ref().ok_or_else(|| {
            PulseError::InvalidConfig("no source — call .from_topic(...) before build()".into())
        })?;
        if self.operators.is_empty() {
            return Err(PulseError::InvalidConfig(
                "no operators — chain at least one of .filter/.map/.key_by/... before build()"
                    .into(),
            ));
        }

        let source_engine = self.source_engine.as_deref().unwrap_or("kafka");

        let mut nodes: Vec<Value> = Vec::with_capacity(3);

        // Source node
        let mut src_config = Map::new();
        src_config.insert("engine".into(), Value::String(source_engine.to_string()));
        src_config.insert("inputTopic".into(), Value::String(input_topic.clone()));
        for (k, v) in &self.source_config {
            src_config.insert(k.clone(), v.clone());
        }
        let src_label = self
            .source_label
            .clone()
            .unwrap_or_else(|| format!("{source_engine} source"));
        nodes.push(json!({
            "type": "source",
            "label": src_label,
            "config": Value::Object(src_config),
        }));

        // Agent node
        let mut agent_config = Map::new();
        agent_config.insert("engine".into(), Value::String("streaming".into()));
        agent_config.insert("inputTopic".into(), Value::String(input_topic.clone()));
        let ops_value: Vec<Value> = self
            .operators
            .iter()
            .map(|op| Value::Object(op.clone()))
            .collect();
        agent_config.insert("operators".into(), Value::Array(ops_value));
        if let Some(ref out) = self.output_topic {
            agent_config.insert("outputTopic".into(), Value::String(out.clone()));
        }
        let agent_label = self
            .agent_label
            .clone()
            .unwrap_or_else(|| pipeline_name.clone());
        nodes.push(json!({
            "type": "agent",
            "label": agent_label,
            "config": Value::Object(agent_config),
        }));

        // Sink node — only when both output_topic AND sink_channel are set
        if let (Some(out), Some(ch)) = (self.output_topic.as_ref(), self.sink_channel.as_ref()) {
            let mut sink_conf = Map::new();
            sink_conf.insert("channel".into(), Value::String(ch.clone()));
            sink_conf.insert("inputTopic".into(), Value::String(out.clone()));
            for (k, v) in &self.sink_config {
                sink_conf.insert(k.clone(), v.clone());
            }
            let sink_label = self
                .sink_label
                .clone()
                .unwrap_or_else(|| format!("{ch} sink"));
            nodes.push(json!({
                "type": "sink",
                "label": sink_label,
                "config": Value::Object(sink_conf),
            }));
        }

        let mut pipeline = Map::new();
        pipeline.insert("name".into(), Value::String(pipeline_name));
        pipeline.insert("nodes".into(), Value::Array(nodes));
        if let Some(ref desc) = self.description {
            pipeline.insert("description".into(), Value::String(desc.clone()));
        }
        Ok(Value::Object(pipeline))
    }
}

// ---------------------------------------------------------------------------
// StreamsResource — the client.streams() accessor
// ---------------------------------------------------------------------------

/// `client.streams()` — compile + deploy [`StreamBuilder`] pipelines.
///
/// Sugar over `client.pipelines().create()` — the compile happens client-side,
/// the deploy is the same POST.
pub struct StreamsResource<'c> {
    pub(crate) client: &'c PulseClient,
}

impl<'c> StreamsResource<'c> {
    /// Compile the builder to a pipeline dict WITHOUT deploying.
    pub fn compile(&self, builder: &StreamBuilder) -> Result<Value, PulseError> {
        builder.build()
    }

    /// Compile with a name override WITHOUT deploying.
    pub fn compile_with_name(
        &self,
        builder: &StreamBuilder,
        name: &str,
    ) -> Result<Value, PulseError> {
        builder.build_with_name(name)
    }

    /// Compile + POST to `/api/pulse/pipelines`. Returns the server response.
    pub async fn deploy(&self, builder: &StreamBuilder) -> Result<Value, PulseError> {
        let definition = builder.build()?;
        self.client
            .request(
                Method::POST,
                "/api/pulse/pipelines",
                Some(&definition),
                true,
            )
            .await
    }

    /// Compile with a name override + POST to `/api/pulse/pipelines`.
    pub async fn deploy_with_name(
        &self,
        builder: &StreamBuilder,
        name: &str,
    ) -> Result<Value, PulseError> {
        let definition = builder.build_with_name(name)?;
        self.client
            .request(
                Method::POST,
                "/api/pulse/pipelines",
                Some(&definition),
                true,
            )
            .await
    }
}

impl std::fmt::Debug for StreamsResource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsResource").finish()
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn require_nonblank(name: &str, value: &str) {
    if value.trim().is_empty() {
        panic!("{name} must be a non-empty string");
    }
}

/// Panics if `on_failure` is set to an invalid value (B-109).
fn check_failure(on_failure: &Option<String>) {
    if let Some(f) = on_failure {
        if f != "EMIT_ERROR" && f != "DROP" && f != "PASS_THROUGH" {
            panic!("on_failure must be EMIT_ERROR, DROP, or PASS_THROUGH, got {f:?}");
        }
    }
}

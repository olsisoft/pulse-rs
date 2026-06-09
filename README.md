# pulse-client — Rust SDK for StreamFlow Pulse

Official Rust client for [Pulse](https://github.com/olsisoft/pulse-rs) — the AI Agent Platform. Async-first, **`reqwest` + `serde`** stack, **MSRV 1.82**.

```rust
use pulse_client::PulseClient;

#[tokio::main]
async fn main() -> Result<(), pulse_client::PulseError> {
    let client = PulseClient::builder()
        .base_url("http://localhost:9090")
        .build()?;

    client.auth().login("alice", "secret").await?;

    for pipeline in client.pipelines().list().await? {
        println!("{}", pipeline["name"]);
    }
    Ok(())
}
```

## Install

```toml
[dependencies]
pulse-client = "2.6.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Requires **Rust 1.82+** as a best-effort MSRV (declared in `Cargo.toml`). CI tests against **stable only** — the transitive dep graph (reqwest → hyper-util → tokio-rustls → base64ct → …) shifts its own floor frequently, so chasing an MSRV in CI produces flaky red builds for reasons unrelated to this code. If you hit a build error on a Rust older than stable, bump your toolchain.

## Why pulse-client (Rust)

- **Async-first** — every method returns `Future`. Drops naturally into tokio + axum + actix.
- **Three external deps** — `reqwest` (HTTP, the de facto standard) + `serde` + `serde_json`. No Hyper-direct fiddling, no custom transports.
- **`rustls` by default** — no system OpenSSL dance. Cross-compile works out of the box.
- **Sibling parity** — same surface + naming as the Python (`pulse-py`), JavaScript (`@olsisoft/pulse-client`), Java (`com.streamflow:pulse-client`), and Go (`github.com/olsisoft/pulse-go`) SDKs.
- **Cheap to clone** — `PulseClient: Clone`, the underlying `reqwest::Client` pools connections, the token sits behind `Arc<RwLock>`. Share a single instance across tasks.
- **Spec-aligned** — every method corresponds 1:1 to an endpoint in the [Pulse OpenAPI 3.1 spec](../streamflow-pulse/src/main/resources/openapi/openapi.yaml). Drift caught at PR time by the in-tree spec invariant tests (B-103).

## Quick start

```rust
use std::time::Duration;
use pulse_client::{PulseClient, PulseError};

#[tokio::main]
async fn main() -> Result<(), PulseError> {
    let client = PulseClient::builder()
        .base_url(std::env::var("PULSE_URL").unwrap())
        .timeout(Duration::from_secs(10))
        .build()?;

    // Login — token cached on the client automatically
    if let Err(err) = client
        .auth()
        .login(
            &std::env::var("PULSE_USER").unwrap(),
            &std::env::var("PULSE_PASSWORD").unwrap(),
        )
        .await
    {
        if err.is_auth_error() {
            panic!("bad credentials");
        }
        return Err(err);
    }

    // List + inspect
    for p in client.pipelines().list().await? {
        println!("{} — {}", p["name"], p["status"]);
    }

    // Create from a template
    let new_pipeline = client
        .pipelines()
        .create(&serde_json::json!({
            "name": "my-fraud-detector",
            "templateId": "fintech-fraud-detection-realtime",
            "nodes": [
                {"id": "src",  "type": "source", "subType": "kafka-source"},
                {"id": "agt",  "type": "agent",  "subType": "streaming"},
                {"id": "snk",  "type": "sink",   "subType": "telegram"}
            ]
        }))
        .await?;
    println!("created: {}", new_pipeline["id"]);
    Ok(())
}
```

## Supported surfaces (v2.6.0)

| Resource | Methods | Notes |
|---|---|---|
| `client.auth()` | `login(user, pass)`, `refresh(refresh_token)`, `organizations()`, `switch_org(org_id)` | Auto-caches JWT after login / refresh / switch_org. |
| `client.pipelines()` | `list()`, `get(id)`, `create(definition)`, `delete(id)` | `definition` follows the CreatePipelineRequest schema. |
| `client.agents()` | `list()`, `get(id)` | Read-only — agents are owned by pipelines. |
| `client.templates()` | `list()` | The 223+ first-party templates. |
| `client.users()` | `list()` | Requires USERS_LIST permission (Owner / Platform Admin personas). |
| `client.version()` | top-level | Public — no JWT required. |

Every method returns `impl Future<Output = Result<Value, PulseError>>`. `Value` is the re-exported `serde_json::Value` — full document, no schema-bound DTOs (yet). Schema-bound types land in v3.0.

Full ~112-endpoint surface documented in Swagger UI at `<pulse-server>/api-docs`. Less-used methods land opportunistically as user-facing demand surfaces.

## Embedded ML inference & duplex

Score events with an uploaded ONNX model in-process (B-112), and open a
bidirectional duplex channel for synchronous decisions (B-114). Full guide:
[ML inference & duplex](https://github.com/olsisoft/pulse-rs/blob/dev/docs/SDK-ML-INFERENCE-AND-DUPLEX.md).

```rust
use pulse_client::{ModelUpload, MlPredictOptions};
use std::collections::BTreeMap;

// Upload + score with an ONNX model (no model-server hop)
let schema = BTreeMap::from([("amount".into(), "float".into()), ("country".into(), "float".into())]);
client.models().upload(ModelUpload::from_path("fraud", "./fraud.onnx").input_schema(schema)).await?;
builder.from_topic("transactions")
    .ml_predict(MlPredictOptions {
        model: "fraud".into(),
        input_fields: vec!["amount".into(), "country".into()],
        output_field: "prediction".into(),
        ..Default::default()
    })
    .filter("prediction.fraud_score > 0.8").to_topic("flagged");

// Duplex: one connection, send in / receive the correlated output
let mut ch = client.duplex("fraud-detector").await?;
let cid = ch.send(&serde_json::json!({ "amount": 5000 }), Some("tx-1")).await?;
let out = ch.recv().await?;   // out.correlation_id == Some("tx-1")
ch.close().await?;
```

## Authentication

Three patterns:

```rust
// 1. Username + password (interactive / CLI tools)
let client = PulseClient::builder()
    .base_url("http://localhost:9090")
    .build()?;
client.auth().login("alice", "secret").await?;

// 2. Pre-minted JWT (CI / service accounts)
let client = PulseClient::builder()
    .base_url("http://localhost:9090")
    .token(std::env::var("PULSE_JWT").unwrap())
    .build()?;

// 3. Hot token rotation (long-running daemons)
client.set_token(freshly_minted_token);
client.clear_token();  // log out
```

For long-running processes, persist `refreshToken` from `login()` and call `client.auth().refresh(&refresh_token)` before the JWT expires (default 1 h TTL).

## Error handling

```rust
use pulse_client::PulseError;

match client.pipelines().get("nope").await {
    Ok(p) => println!("{p:?}"),
    Err(PulseError::NotFound { .. }) => println!("doesn't exist — fine"),
    Err(PulseError::RateLimit { retry_after_seconds, .. }) => {
        let wait = retry_after_seconds.unwrap_or(60);
        tokio::time::sleep(std::time::Duration::from_secs(wait as u64)).await;
        // retry
    }
    Err(err) => {
        eprintln!("Pulse call failed: {err}");
        if let Some(code) = err.status_code() {
            eprintln!("status={code}");
        }
        if let Some(body) = err.body() {
            eprintln!("body={body}");
        }
    }
}
```

Convenience predicates: `err.is_auth_error()`, `is_not_found()`, `is_validation_error()`, `is_rate_limited()`. Every error carries `status_code()`, `path()`, `body()`.

## Custom reqwest::Client (proxies, mTLS, shared pools, tracing)

```rust
let shared = reqwest::Client::builder()
    .timeout(std::time::Duration::from_secs(5))
    .proxy(reqwest::Proxy::all("http://proxy.acme.com:3128")?)
    // .add_root_certificate(...)   // for mTLS / internal CAs
    .build()?;

let client = PulseClient::builder()
    .base_url("http://pulse.acme.com")
    .http_client(shared)
    .build()?;
```

## Development

```bash
git clone https://github.com/olsisoft/pulse-rs.git
cd pulse-rs

cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps --open
```

CI runs the same on every push touching `pulse-rs/` — see `.github/workflows/pulse-rs.yaml`.

## Automatic retry (opt-in)

Off by default — one attempt per request. Enable bounded, full-jitter
exponential-backoff retries via `RetryPolicy`:

```rust
use pulse_client::RetryPolicy;

let client = PulseClient::builder()
    .base_url("http://localhost:9090")
    .retry(RetryPolicy::with_max_retries(3))   // or a full RetryPolicy { .. }
    .build()?;
```

429 (rate limited) is retried for any method, honouring `Retry-After`;
`on_status` 5xx (default `502/503/504`) and transport errors are retried only for
idempotent methods (GET/HEAD/PUT/DELETE) unless `retry_non_idempotent`; terminal
4xx are never retried.

## Local pipeline simulation (Python-only today)

The streams DSL is **client-side declaration, server-side execution**:
`streams().compile(&builder)` builds the pipeline JSON locally (no network) and
`streams().deploy(&builder)` runs it on the Pulse engine. This SDK has **no
in-process simulator** — to validate a pipeline before deploy, `compile()` and
inspect the JSON, or deploy to a dev Pulse.

> A local `TopologyTestDriver`-style executor that runs a streams pipeline
> in-process over sample events (`StreamBuilder::simulate(events)`) currently
> exists **only in the Python SDK** (`streamflow-pulse-client`). Cross-language
> parity is tracked as **B-169** (issue #311); until then, local simulation is a
> Python-exclusive capability.

## Roadmap

- **v2.5.x** — current async API, 5 core resources, `version()`.
- **v2.6.x** — expanded resource coverage: backups, schedules, credentials, settings, approvals, chat.
- **v3.0** — schema-bound DTOs (typed structs instead of `serde_json::Value`); event-stream consumer as a `Stream<Item = Event>` consuming `/api/pulse/events/stream` (SSE).
- **B-098 satellite** — once `olsisoft/pulse-rs` exists, this in-tree code lifts out and publishes to crates.io. `cargo add pulse-client` will switch to the satellite; in-tree continues to mirror for one release cycle.

Track progress in [`docs/STREAMFLOW-BACKLOG.md`](../docs/STREAMFLOW-BACKLOG.md) under item **B-098**.

## License

Apache 2.0 — same as the parent Pulse repository.

# Pulse Rust SDK — Examples

Five runnable examples showing how an application drives the **StreamFlow event
mesh** through Pulse. The SDK *declares* the work; Pulse runs it on the cluster
(sharded, replicated) — `app → SDK → Pulse API → bridge → mesh`.

## Use cases

| # | File | What it shows |
|---|------|---------------|
| 1 | [`realtime_windowed_aggregation.rs`](realtime_windowed_aggregation.rs) | Per-merchant 1-minute tumbling-window rollup (`count`/`sum`/`avg`/`max`) → topic |
| 2 | [`events_live_and_replay.rs`](events_live_and_replay.rs) | Tail the live event `Stream` **and** replay a key's committed state history (time-travel) |
| 3 | [`interactive_query.rs`](interactive_query.rs) | Interactive Query — `summary` / point `get` / bounded `scan` / filtered + grouped `query` |
| 4 | [`ai_enrichment_pipeline.rs`](ai_enrichment_pipeline.rs) | Agentic stream — LLM sentiment → `extract` structured fields → MCP CRM lookup |
| 5 | [`stream_to_connector.rs`](stream_to_connector.rs) | Discover sink connectors, then `filter` → sink a stream to a ClickHouse connector |

## Prerequisites

- **Rust 1.75+** and the SDK as a dependency: `pulse-client = "2"` (it pulls a
  Tokio runtime transitively; the examples use `#[tokio::main]`).
- A reachable **Pulse** instance — embedded mesh, or attached to a StreamFlow
  cluster (Settings → Data Plane → REMOTE).

## Run

```bash
export PULSE_URL=http://localhost:9090      # your Pulse base URL

cargo run --example realtime_windowed_aggregation
cargo run --example events_live_and_replay
cargo run --example interactive_query
cargo run --example ai_enrichment_pipeline
cargo run --example stream_to_connector
```

Compile them all without running: `cargo build --examples`.

## Use-case ladder (simplest → most complex)

A graduated set of five examples sharing ONE domain — **card-payments fraud
monitoring** on the `card-authorizations` topic (events `{cardId, merchantId,
amount, ts}`; fraud rule = more than 5 authorizations on one card in a 60s
tumbling window). Each rung adds one capability on top of the last.

| # | File | What it shows | Run |
|---|------|---------------|-----|
| 1 | [`usecase_1_connect_and_list.rs`](usecase_1_connect_and_list.rs) | Connectivity hello-world — `version()`, optional login, list pipelines + connectors | `cargo run --example usecase_1_connect_and_list` |
| 2 | [`usecase_2_deploy_velocity_pipeline.rs`](usecase_2_deploy_velocity_pipeline.rs) | Streams DSL — declare "card-velocity-60s" (filter → key_by → 60s tumbling aggs → filter → sink), print compiled JSON, deploy | `cargo run --example usecase_2_deploy_velocity_pipeline` |
| 3 | [`usecase_3_interactive_query.rs`](usecase_3_interactive_query.rs) | Interactive Query — `summary`, filtered `query` (`txCount > 5`), point `get`, with caller-side rate-limit retry | `cargo run --example usecase_3_interactive_query` |
| 4 | [`usecase_4_events_and_replay.rs`](usecase_4_events_and_replay.rs) | Live SSE `fraud-alert` events (bounded) + `replay` one card's state-change history | `cargo run --example usecase_4_events_and_replay` |
| 5 | [`usecase_5_synchronous_decision.rs`](usecase_5_synchronous_decision.rs) | Duplex channel (B-114) — send charges to "fraud-decider", recv correlated ALLOW/DENY | `cargo run --example usecase_5_synchronous_decision` |

These examples talk to a **live Pulse at `PULSE_URL`** (default
`http://localhost:9090`). Set `PULSE_TOKEN`, or `PULSE_USER` + `PULSE_PASSWORD`,
for the authenticated rungs (2–5); rung 1 degrades gracefully without them.

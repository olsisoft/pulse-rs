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

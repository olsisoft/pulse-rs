//! Use case 1 — real-time windowed aggregation on the event mesh.
//!
//! Declares a streaming pipeline that rolls up payment transactions per merchant
//! in 1-minute tumbling windows and writes the rollups back to a mesh topic.
//! Deployed to a Pulse attached to a StreamFlow cluster, this runs on the mesh.
//!
//! Run:  PULSE_URL=http://localhost:9090 cargo run --example realtime_windowed_aggregation

use std::collections::BTreeMap;

use pulse_client::{aggs, windows, PulseClient, StreamBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut aggregations = BTreeMap::new();
    aggregations.insert("txn_count".to_string(), aggs::count());
    aggregations.insert("total_amount".to_string(), aggs::sum("amount"));
    aggregations.insert("avg_amount".to_string(), aggs::avg("amount"));
    aggregations.insert("max_amount".to_string(), aggs::max("amount"));

    let builder = StreamBuilder::new("merchant-rollups-1m")
        .from_topic("transactions")
        .filter("amount > 0")
        .key_by("merchant_id")
        .window_with_aggs(windows::tumbling("1m"), aggregations)
        .to_topic("merchant-rollups-1m");

    println!("Pipeline spec: {}", builder.build()?);

    let client = PulseClient::builder().base_url(base_url()).build()?;
    let deployed = client.streams().deploy(&builder).await?;
    println!("Deployed: {deployed}");
    Ok(())
}

fn base_url() -> String {
    std::env::var("PULSE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

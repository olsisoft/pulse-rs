//! Use case 5 — sink a mesh stream to an external connector.
//!
//! Discovers the available sink connectors, then declares a stream that delivers
//! the per-merchant rollups to a ClickHouse warehouse via a connector sink.
//!
//! Run:  PULSE_URL=http://localhost:9090 cargo run --example stream_to_connector

use pulse_client::{PulseClient, StreamBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PulseClient::builder().base_url(base_url()).build()?;

    let sinks = client.connectors().sinks().await?;
    println!("{} sink connector(s) available", sinks.len());

    let builder = StreamBuilder::new("rollups-to-warehouse")
        .from_topic("merchant-rollups-1m")
        .filter("total_amount > 0")
        .to_connector("clickhouse")
        .with_sink_config("url", serde_json::json!("http://clickhouse:8123"))
        .with_sink_config("table", serde_json::json!("merchant_rollups"));

    println!("Pipeline spec: {}", builder.build()?);

    let deployed = client.streams().deploy(&builder).await?;
    println!("Deployed: {deployed}");
    Ok(())
}

fn base_url() -> String {
    std::env::var("PULSE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

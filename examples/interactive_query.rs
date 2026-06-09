//! Use case 3 — Interactive Query over mesh-materialized agent state.
//!
//! Reads the live, queryable state an agent maintains on the mesh: a summary, a
//! point lookup by key, a bounded scan, and a filtered/grouped query.
//!
//! Run:  PULSE_URL=http://localhost:9090 cargo run --example interactive_query

use pulse_client::{IQQueryOptions, IQScanOptions, PulseClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PulseClient::builder().base_url(base_url()).build()?;
    let agent = "merchant-rollups-1m";

    println!("Summary: {}", client.iq().summary(agent).await?);

    // Point lookup for one merchant's current rollup.
    println!(
        "merchant-7: {}",
        client.iq().get(agent, "merchant-7").await?
    );

    // Bounded scan of the keyspace.
    let scan = client
        .iq()
        .scan(
            agent,
            IQScanOptions {
                limit: Some(10),
                ..Default::default()
            },
        )
        .await?;
    println!("Scan (first 10): {scan}");

    // Filtered + grouped query.
    let result = client
        .iq()
        .query(
            agent,
            IQQueryOptions {
                filter: Some(
                    serde_json::json!({"field": "total_amount", "op": "gt", "value": 1000}),
                ),
                group_by: Some("region".to_string()),
                limit: Some(20),
                ..Default::default()
            },
        )
        .await?;
    println!("High-volume merchants by region: {result}");
    Ok(())
}

fn base_url() -> String {
    std::env::var("PULSE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

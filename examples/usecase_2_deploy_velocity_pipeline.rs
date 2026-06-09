//! Use-case ladder 2 — deploy the card-velocity fraud pipeline (streams DSL).
//!
//! What it shows: declare pipeline "card-velocity-60s" with the streams DSL —
//! `card-authorizations` → filter → key_by(cardId) → 60s tumbling window
//! (txCount/totalAmount/maxAmount) → filter(txCount > 5) → `fraud-alerts` with a
//! "dashboard" sink channel. Prints the compiled pipeline JSON (offline) then
//! deploys it.
//!
//! Prerequisites:
//!   * A reachable Pulse at `PULSE_URL` (default `http://localhost:9090`).
//!   * Auth: `PULSE_TOKEN`, or `PULSE_USER` + `PULSE_PASSWORD` (deploy needs a JWT).
//!     The compile step prints with no token; only the deploy call needs auth.
//!
//! Build + run:
//!   cargo build --example usecase_2_deploy_velocity_pipeline
//!   PULSE_URL=http://localhost:9090 cargo run --example usecase_2_deploy_velocity_pipeline

use std::collections::BTreeMap;

use pulse_client::{aggs, windows, PulseClient, StreamBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // fraud rule: more than 5 authorizations on one card in a 60s tumbling window.
    let mut aggregations = BTreeMap::new();
    aggregations.insert("txCount".to_string(), aggs::count());
    aggregations.insert("totalAmount".to_string(), aggs::sum("amount"));
    aggregations.insert("maxAmount".to_string(), aggs::max("amount"));

    let builder = StreamBuilder::new("card-velocity-60s")
        .described_as("Flag cards with >5 authorizations in a 60s window")
        .from_topic("card-authorizations")
        .filter("amount > 0")
        .key_by("cardId")
        .window_with_aggs(windows::tumbling("60s"), aggregations)
        .filter("txCount > 5")
        .to_topic_with_channel("fraud-alerts", "dashboard");

    // Compile offline (no network) and print the pipeline JSON the server sees.
    let compiled = builder.build()?;
    println!(
        "Compiled pipeline JSON:\n{}",
        serde_json::to_string_pretty(&compiled)?
    );

    let client = PulseClient::builder().base_url(base_url()).build()?;
    authenticate(&client).await?;

    let deployed = client.streams().deploy(&builder).await?;
    println!("Deployed: {deployed}");
    Ok(())
}

async fn authenticate(client: &PulseClient) -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(token) = std::env::var("PULSE_TOKEN") {
        if !token.is_empty() {
            client.set_token(token);
            return Ok(());
        }
    }
    if let (Ok(user), Ok(password)) = (std::env::var("PULSE_USER"), std::env::var("PULSE_PASSWORD"))
    {
        if !user.is_empty() && !password.is_empty() {
            client.auth().login(&user, &password).await?;
        }
    }
    Ok(())
}

fn base_url() -> String {
    std::env::var("PULSE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

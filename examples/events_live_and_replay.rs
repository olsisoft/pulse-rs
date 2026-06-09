//! Use case 2 — consume live mesh events and replay state history.
//!
//!   * tail the live event stream — bounded here to the first 10 events;
//!   * replay the committed state-change history for one key (time-travel).
//!
//! Run:  PULSE_URL=http://localhost:9090 cargo run --example events_live_and_replay

use futures_util::StreamExt;
use pulse_client::PulseClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PulseClient::builder().base_url(base_url()).build()?;

    // Replay the last hour of committed state changes for one account key.
    let changes = client
        .events()
        .replay("balance", "acct-42", "-1h", "now", 50)
        .await?;
    println!("Replayed {} state change(s) for acct-42", changes.len());

    // Tail the live event stream — stop after the first 10 events.
    let mut stream = client.events().stream().await?;
    println!("Tailing live events (first 10)…");
    let mut seen = 0;
    while let Some(event) = stream.next().await {
        let event = event?;
        println!("  event: {event}");
        seen += 1;
        if seen >= 10 {
            break;
        }
    }
    Ok(())
}

fn base_url() -> String {
    std::env::var("PULSE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

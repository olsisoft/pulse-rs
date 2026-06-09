//! Use-case ladder 4 — live fraud-alert events + state-change replay.
//!
//! What it shows: subscribe to the live SSE event stream and print a handful of
//! `fraud-alert` events (stopping after a few or a short timeout), then `replay`
//! one card's committed state-change history (time-travel) and print it.
//!
//! Prerequisites:
//!   * A reachable Pulse at `PULSE_URL` (default `http://localhost:9090`).
//!   * Auth: `PULSE_TOKEN`, or `PULSE_USER` + `PULSE_PASSWORD` (the SSE stream
//!     requires a JWT — without one this example exits early with a clear note).
//!   * The "card-velocity-60s" agent deployed (see usecase_2) and producing alerts.
//!
//! Build + run:
//!   cargo build --example usecase_4_events_and_replay
//!   PULSE_URL=http://localhost:9090 cargo run --example usecase_4_events_and_replay

use std::time::Duration;

use futures_util::StreamExt;
use pulse_client::PulseClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PulseClient::builder().base_url(base_url()).build()?;
    if !authenticate(&client).await? {
        println!(
            "No PULSE_TOKEN or PULSE_USER/PULSE_PASSWORD set — the SSE stream needs \
             a JWT. Set credentials to tail live fraud alerts."
        );
        return Ok(());
    }

    // Tail the live event stream — stop after a few fraud-alert events or a 15s
    // timeout, whichever comes first.
    let mut stream = client.events().stream().await?;
    println!("Tailing live events for fraud alerts (≤5, 15s budget)…");
    let mut shown = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => {
                println!("Timeout reached — stopping the live tail.");
                break;
            }
            next = stream.next() => {
                match next {
                    Some(event) => {
                        let event = event?;
                        let kind = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
                        if kind == "fraud-alert" {
                            println!("  fraud-alert: {event}");
                            shown += 1;
                            if shown >= 5 {
                                break;
                            }
                        }
                    }
                    None => {
                        println!("Stream ended.");
                        break;
                    }
                }
            }
        }
    }
    drop(stream); // closes the subscription

    // Replay one card's committed state-change history (time-travel).
    let changes = client
        .events()
        .replay("card-velocity-60s", "card-007", "-1h", "now", 50)
        .await?;
    println!("Replayed {} state change(s) for card-007:", changes.len());
    for change in &changes {
        println!("  {change}");
    }

    Ok(())
}

/// Returns `Ok(true)` if a token is now set, `Ok(false)` if no credentials.
async fn authenticate(client: &PulseClient) -> Result<bool, Box<dyn std::error::Error>> {
    if let Ok(token) = std::env::var("PULSE_TOKEN") {
        if !token.is_empty() {
            client.set_token(token);
            return Ok(true);
        }
    }
    match (std::env::var("PULSE_USER"), std::env::var("PULSE_PASSWORD")) {
        (Ok(user), Ok(password)) if !user.is_empty() && !password.is_empty() => {
            client.auth().login(&user, &password).await?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn base_url() -> String {
    std::env::var("PULSE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

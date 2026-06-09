//! Use-case ladder 5 — synchronous fraud decision over a duplex channel (B-114).
//!
//! What it shows: open ONE WebSocket to agent "fraud-decider", send a couple of
//! charges {cardId, amount} (one hot card expected DENY, one fresh card expected
//! ALLOW), `recv` each correlated decision, and print ALLOW/DENY + the correlation
//! id. This is the synchronous-decision path: publish-in and decision-out ride the
//! same connection, matched by correlation id.
//!
//! Prerequisites:
//!   * A reachable Pulse at `PULSE_URL` (default `http://localhost:9090`); the
//!     duplex endpoint runs on the WebSocket port (REST port + 1, derived for you).
//!   * Auth: `PULSE_TOKEN`, or `PULSE_USER` + `PULSE_PASSWORD` (the token rides the
//!     WS upgrade). Without one this example exits early with a clear note.
//!   * A "fraud-decider" agent deployed that emits a decision per input charge.
//!
//! Build + run:
//!   cargo build --example usecase_5_synchronous_decision
//!   PULSE_URL=http://localhost:9090 cargo run --example usecase_5_synchronous_decision

use serde_json::json;

use pulse_client::PulseClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PulseClient::builder().base_url(base_url()).build()?;
    if !authenticate(&client).await? {
        println!(
            "No PULSE_TOKEN or PULSE_USER/PULSE_PASSWORD set — the duplex channel \
             needs a JWT. Set credentials to make synchronous decisions."
        );
        return Ok(());
    }

    let mut channel = client.duplex("fraud-decider").await?;

    // A hot card (expected DENY) and a fresh card (expected ALLOW).
    let charges = [
        ("card-007", 4200, "charge-hot"),
        ("card-999", 1899, "charge-fresh"),
    ];

    for (card_id, amount, correlation) in charges {
        let cid = channel
            .send(
                &json!({ "cardId": card_id, "amount": amount }),
                Some(correlation),
            )
            .await?;
        let output = channel.recv().await?;

        // The decision lives on the agent's output event payload.
        let decision = output
            .event
            .get("payload")
            .and_then(|p| p.get("decision"))
            .or_else(|| output.event.get("decision"))
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");
        println!(
            "{card_id} (${amount}) → {decision}  [sent cid={cid}, recv cid={recv:?}]",
            recv = output.correlation_id,
        );
    }

    channel.close().await?;
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

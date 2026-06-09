//! Use-case ladder 3 — Interactive Query the live fraud state.
//!
//! What it shows: IQ against agent "card-velocity-60s" — print the state summary,
//! run a filtered query for cards with `txCount > 5` (built with the `iq_leaf`
//! `gt` filter leaf), and a point `get` for "card-007". The query is wrapped in a
//! small caller-side retry that honours `retry_after_seconds` — demonstrating the
//! SDK's deliberate no-auto-retry design (it surfaces `PulseError::RateLimit` and
//! lets the caller decide).
//!
//! Prerequisites:
//!   * A reachable Pulse at `PULSE_URL` (default `http://localhost:9090`).
//!   * Auth: `PULSE_TOKEN`, or `PULSE_USER` + `PULSE_PASSWORD` (IQ needs AGENT_READ).
//!   * The "card-velocity-60s" agent deployed (see usecase_2) and ingesting.
//!
//! Build + run:
//!   cargo build --example usecase_3_interactive_query
//!   PULSE_URL=http://localhost:9090 cargo run --example usecase_3_interactive_query

use std::time::Duration;

use pulse_client::{iq_leaf, IQQueryOptions, PulseClient, PulseError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PulseClient::builder().base_url(base_url()).build()?;
    authenticate(&client).await?;

    let agent = "card-velocity-60s";

    println!("Summary: {}", client.iq().summary(agent).await?);

    // Filtered query: cards over the velocity threshold (txCount > 5).
    let opts = IQQueryOptions {
        filter: Some(iq_leaf("txCount", "gt", 5)),
        limit: Some(50),
        ..Default::default()
    };
    let flagged = query_with_retry(&client, agent, &opts, 3).await?;
    println!("Cards with txCount > 5: {flagged}");

    // Point get for one card's current window state.
    match client.iq().get(agent, "card-007").await {
        Ok(state) => println!("card-007: {state}"),
        Err(e) if e.is_not_found() => println!("card-007: no live state for this key"),
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

/// Runs an IQ query, retrying on `PulseError::RateLimit` up to `max_attempts`
/// times, sleeping for the server-advised `retry_after_seconds` (defaulting to
/// 1s when the server gives no hint). The SDK never auto-retries — back-pressure
/// handling is a caller concern, shown here.
async fn query_with_retry(
    client: &PulseClient,
    agent: &str,
    opts: &IQQueryOptions,
    max_attempts: u32,
) -> Result<pulse_client::Value, PulseError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match client.iq().query(agent, opts.clone()).await {
            Ok(v) => return Ok(v),
            Err(PulseError::RateLimit {
                retry_after_seconds,
                ..
            }) if attempt < max_attempts => {
                let wait = retry_after_seconds.unwrap_or(1);
                println!("Rate-limited; retrying in {wait}s (attempt {attempt}/{max_attempts})…");
                tokio::time::sleep(Duration::from_secs(wait as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
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

//! Use-case ladder 1 — connect & list (hello-world / connectivity check).
//!
//! What it shows: construct the client for `PULSE_URL`, call `version()` (public,
//! no JWT), log in if `PULSE_USER`/`PULSE_PASSWORD` (or a `PULSE_TOKEN`) are set —
//! degrading gracefully when they aren't — then list pipelines and connectors.
//!
//! Prerequisites:
//!   * A reachable Pulse at `PULSE_URL` (default `http://localhost:9090`).
//!   * Optional auth: `PULSE_USER` + `PULSE_PASSWORD`, or `PULSE_TOKEN`.
//!     Without them the authenticated listings are skipped, not fatal.
//!
//! Build + run:
//!   cargo build --example usecase_1_connect_and_list
//!   PULSE_URL=http://localhost:9090 cargo run --example usecase_1_connect_and_list

use pulse_client::PulseClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PulseClient::builder().base_url(base_url()).build()?;

    // version() is public — no token required.
    println!("Pulse version: {}", client.version().await?);

    // Authenticate if credentials are present; degrade gracefully otherwise.
    if !authenticate(&client).await? {
        println!(
            "No PULSE_TOKEN or PULSE_USER/PULSE_PASSWORD set — skipping the \
             authenticated listings (set them to see pipelines + connectors)."
        );
        return Ok(());
    }

    let pipelines = client.pipelines().list().await?;
    println!("{} pipeline(s):", pipelines.len());
    for p in &pipelines {
        println!("  - {}", p.get("name").unwrap_or(p));
    }

    let connectors = client.connectors().list().await?;
    let sources = connectors
        .get("sources")
        .and_then(|v| v.as_array())
        .map(Vec::len)
        .unwrap_or(0);
    let sinks = connectors
        .get("sinks")
        .and_then(|v| v.as_array())
        .map(Vec::len)
        .unwrap_or(0);
    println!(
        "Connectors: {sources} source(s), {sink_count} sink(s)",
        sink_count = sinks
    );
    for sink in client.connectors().sinks().await? {
        println!(
            "  sink: {}",
            sink.get("subType")
                .or_else(|| sink.get("displayName"))
                .unwrap_or(&sink)
        );
    }

    Ok(())
}

/// Logs the client in from the environment. Returns `Ok(true)` if a token is now
/// set, `Ok(false)` if no credentials were supplied.
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

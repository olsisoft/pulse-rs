//! Use case 4 — agentic enrichment pipeline (LLM + extract + MCP) on the mesh.
//!
//! Enriches support tickets streaming through the mesh: classify sentiment with
//! an LLM, pull structured fields out of free text, then call an MCP tool to
//! look the customer up — a declarative stream that runs on the cluster.
//!
//! Run:  PULSE_URL=http://localhost:9090 cargo run --example ai_enrichment_pipeline

use std::collections::BTreeMap;

use pulse_client::{ExtractOptions, MapLlmOptions, McpCallOptions, PulseClient, StreamBuilder};
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut schema = BTreeMap::new();
    schema.insert("product".to_string(), "string".to_string());
    schema.insert("requested_action".to_string(), "string".to_string());

    let mut args = BTreeMap::new();
    args.insert(
        "email".to_string(),
        Value::String("${customer_email}".to_string()),
    );

    let builder = StreamBuilder::new("ticket-enrichment")
        .from_topic("support-tickets")
        .filter("priority != 'spam'")
        .map_llm(
            "Classify the ticket sentiment as positive, neutral, or negative.",
            MapLlmOptions {
                output_field: "sentiment".to_string(),
                ..Default::default()
            },
        )
        .extract(ExtractOptions {
            instruction: "Extract the product name and the customer's requested action."
                .to_string(),
            schema,
            ..Default::default()
        })
        .mcp_call(
            "crm.lookup_customer",
            McpCallOptions {
                args: Some(args),
                output_field: Some("customer".to_string()),
                ..Default::default()
            },
        )
        .to_topic("tickets-enriched");

    println!("Pipeline spec: {}", builder.build()?);

    let client = PulseClient::builder().base_url(base_url()).build()?;
    let deployed = client.streams().deploy(&builder).await?;
    println!("Deployed: {deployed}");
    Ok(())
}

fn base_url() -> String {
    std::env::var("PULSE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

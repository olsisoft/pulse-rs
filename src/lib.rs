//! Official Rust client for [StreamFlow Pulse](https://github.com/olsisoft/pulse-rs)
//! — the AI Agent Platform.
//!
//! # Quick start
//!
//! ```no_run
//! use pulse_client::PulseClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), pulse_client::PulseError> {
//!     let client = PulseClient::builder()
//!         .base_url("http://localhost:9090")
//!         .build()?;
//!
//!     client.auth().login("alice", "secret").await?;
//!
//!     for pipeline in client.pipelines().list().await? {
//!         println!("{}", pipeline["name"]);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Surface
//!
//! Mirrors the Python (`pulse-py`), JavaScript (`@olsisoft/pulse-client`),
//! Java (`com.streamflow:pulse-client`) and Go (`github.com/olsisoft/pulse-go`)
//! SDKs 1:1:
//!
//! - [`auth()`](PulseClient::auth) — login, refresh, organisations, switch org
//! - [`pipelines()`](PulseClient::pipelines) — list, get, create, delete
//! - [`agents()`](PulseClient::agents) — list, get
//! - [`templates()`](PulseClient::templates) — list
//! - [`users()`](PulseClient::users) — list (admin only)
//! - [`events()`](PulseClient::events) — Server-Sent Events stream
//! - [`iq()`](PulseClient::iq) — B-106 Interactive Queries on agent state
//! - [`streams()`](PulseClient::streams) — B-107 Kafka-Streams-like DSL
//! - [`version()`](PulseClient::version) — public, no JWT required
//!
//! # Wire format
//!
//! Every method corresponds 1:1 to an endpoint in the Pulse OpenAPI 3.1 spec
//! (`streamflow-pulse/src/main/resources/openapi/openapi.yaml`). Drift caught
//! at PR time by the in-tree spec invariant tests (B-103).

#![doc(html_root_url = "https://docs.rs/pulse-client/2.6.0")]
#![warn(missing_debug_implementations)]
#![warn(rust_2018_idioms)]

mod client;
mod duplex;
mod error;
mod events;
mod iq;
mod resources;
mod streams;

pub use client::{PulseClient, PulseClientBuilder};
pub use duplex::{derive_ws_url, DuplexChannel, DuplexOutput};
pub use error::PulseError;
pub use events::{EventsResource, EventsStream};
pub use iq::{iq_and, iq_leaf, iq_not, iq_or, IQQueryOptions, IQResource, IQScanOptions};
pub use resources::{
    AgentsResource, AuthResource, ConnectorsResource, ModelUpload, ModelsResource,
    PipelinesResource, TemplatesResource, UsersResource,
};
pub use streams::{
    aggs, windows, BranchSpec, BroadcastJoinOptions, CdcJoinOptions, CepOptions,
    EnrichAsyncOptions, ExtractOptions, MapLlmOptions, MapOptions, McpCallOptions,
    MlPredictOptions, StreamBuilder, StreamsResource, WindowOptions, WindowSpec,
};

// Re-export serde_json::Value so callers don't need to add serde_json to
// their direct dependencies just to inspect responses.
pub use serde_json::Value;

/// Current SDK version (matches `Cargo.toml` and the Pulse server it targets).
pub const VERSION: &str = "2.6.0";

impl std::fmt::Debug for PulseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulseClient")
            .field("base_url", &self.inner.base_url)
            .field("token", &self.token().map(|_| "<set>"))
            .finish()
    }
}

impl<'c> std::fmt::Debug for AuthResource<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthResource").finish()
    }
}

impl<'c> std::fmt::Debug for PipelinesResource<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelinesResource").finish()
    }
}

impl<'c> std::fmt::Debug for AgentsResource<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentsResource").finish()
    }
}

impl<'c> std::fmt::Debug for TemplatesResource<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemplatesResource").finish()
    }
}

impl<'c> std::fmt::Debug for UsersResource<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsersResource").finish()
    }
}

impl<'c> std::fmt::Debug for ConnectorsResource<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorsResource").finish()
    }
}

impl<'c> std::fmt::Debug for ModelsResource<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelsResource").finish()
    }
}

//! Resource accessors — one per OpenAPI tag.

use reqwest::Method;
use serde_json::{json, Value};

use crate::client::PulseClient;
use crate::error::PulseError;

// ---------------------------------------------------------------------------
// AuthResource — client.auth()
// ---------------------------------------------------------------------------

pub struct AuthResource<'c> {
    pub(crate) client: &'c PulseClient,
}

impl AuthResource<'_> {
    /// `POST /api/auth/login` — exchanges username + password for a JWT.
    ///
    /// On success, the returned token is cached on the parent client so
    /// subsequent calls authenticate automatically.
    pub async fn login(&self, username: &str, password: &str) -> Result<Value, PulseError> {
        let body = json!({ "username": username, "password": password });
        let response = self
            .client
            .request(Method::POST, "/api/auth/login", Some(&body), false)
            .await?;
        cache_token(self.client, &response);
        Ok(response)
    }

    /// `POST /api/auth/refresh` — exchanges a refresh token for a fresh JWT.
    pub async fn refresh(&self, refresh_token: &str) -> Result<Value, PulseError> {
        let body = json!({ "refreshToken": refresh_token });
        let response = self
            .client
            .request(Method::POST, "/api/auth/refresh", Some(&body), false)
            .await?;
        cache_token(self.client, &response);
        Ok(response)
    }

    /// `GET /api/auth/organizations` — orgs the current user is a member of.
    pub async fn organizations(&self) -> Result<Vec<Value>, PulseError> {
        let result = self
            .client
            .request(Method::GET, "/api/auth/organizations", None::<&()>, true)
            .await?;
        Ok(unwrap_list(&result, "organizations"))
    }

    /// `POST /api/auth/switch-org` — switches the active organisation.
    /// The new JWT (with updated orgId claim) is cached on the parent client.
    pub async fn switch_org(&self, org_id: &str) -> Result<Value, PulseError> {
        let body = json!({ "orgId": org_id });
        let response = self
            .client
            .request(Method::POST, "/api/auth/switch-org", Some(&body), true)
            .await?;
        cache_token(self.client, &response);
        Ok(response)
    }
}

fn cache_token(client: &PulseClient, response: &Value) {
    if let Some(token) = response.get("token").and_then(Value::as_str) {
        if !token.is_empty() {
            client.set_token(token);
        }
    }
}

// ---------------------------------------------------------------------------
// PipelinesResource — client.pipelines()
// ---------------------------------------------------------------------------

pub struct PipelinesResource<'c> {
    pub(crate) client: &'c PulseClient,
}

impl PipelinesResource<'_> {
    /// `GET /api/pulse/pipelines` — every pipeline in the current org.
    pub async fn list(&self) -> Result<Vec<Value>, PulseError> {
        let result = self
            .client
            .request(Method::GET, "/api/pulse/pipelines", None::<&()>, true)
            .await?;
        Ok(unwrap_list(&result, "pipelines"))
    }

    /// `GET /api/pulse/pipelines/{id}` — one pipeline by id.
    pub async fn get(&self, pipeline_id: &str) -> Result<Value, PulseError> {
        let path = format!("/api/pulse/pipelines/{}", encode_path(pipeline_id));
        self.client
            .request(Method::GET, &path, None::<&()>, true)
            .await
    }

    /// `POST /api/pulse/pipelines` — creates + deploys a new pipeline.
    ///
    /// The definition must follow the `CreatePipelineRequest` schema (see
    /// openapi.yaml). At minimum: `name` + `nodes`.
    pub async fn create(&self, definition: &Value) -> Result<Value, PulseError> {
        self.client
            .request(Method::POST, "/api/pulse/pipelines", Some(definition), true)
            .await
    }

    /// `DELETE /api/pulse/pipelines/{id}` — tears down the pipeline.
    pub async fn delete(&self, pipeline_id: &str) -> Result<(), PulseError> {
        let path = format!("/api/pulse/pipelines/{}", encode_path(pipeline_id));
        self.client
            .request(Method::DELETE, &path, None::<&()>, true)
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AgentsResource — client.agents()
// ---------------------------------------------------------------------------

pub struct AgentsResource<'c> {
    pub(crate) client: &'c PulseClient,
}

impl AgentsResource<'_> {
    /// `GET /api/pulse/agents` — every deployed agent in the current org.
    pub async fn list(&self) -> Result<Vec<Value>, PulseError> {
        let result = self
            .client
            .request(Method::GET, "/api/pulse/agents", None::<&()>, true)
            .await?;
        Ok(unwrap_list(&result, "agents"))
    }

    /// `GET /api/pulse/agents/{id}` — one agent by id.
    pub async fn get(&self, agent_id: &str) -> Result<Value, PulseError> {
        let path = format!("/api/pulse/agents/{}", encode_path(agent_id));
        self.client
            .request(Method::GET, &path, None::<&()>, true)
            .await
    }

    /// B-115 Phase 1 — `PUT /api/pulse/agents/{id}`: replace the agent's config.
    ///
    /// `config` is the FULL agent config (not a partial merge) — at minimum
    /// `name`. Optional fields (`engineType`, `inputTopic`, `outputTopic`,
    /// `description`, `instances`, `monthlyBudget`, `config`) fall back to safe
    /// defaults when omitted. See the `UpdateAgentRequest` schema in
    /// `openapi.yaml`.
    ///
    /// Today this triggers a full stop + persist + start cycle on the engine
    /// side — the agent is briefly unavailable while the swap happens.
    /// Existing state in the agent's keyed store is preserved. Phase 2
    /// (B-115-engine) will add atomic event-boundary swap so hot-reloadable
    /// changes apply with no downtime.
    ///
    /// Returns the post-update agent snapshot (same shape as [`get`](Self::get)).
    ///
    /// # Errors
    ///
    /// - [`PulseError::Validation`] on a bad config (self-loop, invalid
    ///   streaming operators)
    /// - [`PulseError::NotFound`] if the agent doesn't exist
    pub async fn update(&self, agent_id: &str, config: &Value) -> Result<Value, PulseError> {
        let path = format!("/api/pulse/agents/{}", encode_path(agent_id));
        self.client
            .request(Method::PUT, &path, Some(config), true)
            .await
    }

    /// `DELETE /api/pulse/agents/{id}` — stop the agent + remove its config row.
    ///
    /// The agent's keyed state store is also dropped. Requires the
    /// `AGENT_DELETE` permission.
    pub async fn delete(&self, agent_id: &str) -> Result<(), PulseError> {
        let path = format!("/api/pulse/agents/{}", encode_path(agent_id));
        self.client
            .request::<()>(Method::DELETE, &path, None, true)
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TemplatesResource — client.templates()
// ---------------------------------------------------------------------------

pub struct TemplatesResource<'c> {
    pub(crate) client: &'c PulseClient,
}

impl TemplatesResource<'_> {
    /// `GET /api/pulse/templates` — the 223+ first-party templates.
    pub async fn list(&self) -> Result<Vec<Value>, PulseError> {
        let result = self
            .client
            .request(Method::GET, "/api/pulse/templates", None::<&()>, true)
            .await?;
        Ok(unwrap_list(&result, "templates"))
    }
}

// ---------------------------------------------------------------------------
// UsersResource — client.users()
// ---------------------------------------------------------------------------

pub struct UsersResource<'c> {
    pub(crate) client: &'c PulseClient,
}

impl UsersResource<'_> {
    /// `GET /api/pulse/users` — every user in the current org.
    ///
    /// Requires the caller to have the `USERS_LIST` permission atom (Owner /
    /// Platform Admin personas by default — see B-105).
    pub async fn list(&self) -> Result<Vec<Value>, PulseError> {
        let result = self
            .client
            .request(Method::GET, "/api/pulse/users", None::<&()>, true)
            .await?;
        Ok(unwrap_list(&result, "users"))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extracts a `Vec<Value>` from `result[key]`. Returns an empty Vec for
/// missing / malformed envelopes — never panics — so callers can iterate
/// safely.
fn unwrap_list(result: &Value, key: &str) -> Vec<Value> {
    result
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// URL-encodes a path-param segment so ids containing `/`, spaces, etc.
/// round-trip safely. Uses the same character set as the `pulse-go`
/// `url.PathEscape` and `pulse-java` `URLEncoder` — `+` is encoded as `%20`.
fn encode_path(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

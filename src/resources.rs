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
// ModelsResource — client.models()
// ---------------------------------------------------------------------------

/// `client.models()` — B-112 embedded ML model registry.
///
/// Upload ONNX models that the streaming `ml_predict` operator scores events
/// against, in-process on the Pulse engine (no model-server hop). Models are
/// org-scoped; upload / delete require the ADMIN role.
///
/// # Example
///
/// ```no_run
/// use pulse_client::{PulseClient, ModelUpload};
/// use std::collections::BTreeMap;
///
/// # async fn run(client: &PulseClient) -> Result<(), pulse_client::PulseError> {
/// let mut input = BTreeMap::new();
/// input.insert("amount".to_string(), "float".to_string());
/// input.insert("country".to_string(), "string".to_string());
///
/// client
///     .models()
///     .upload(
///         ModelUpload::from_path("fraud-classifier", "./model.onnx")
///             .input_schema(input),
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ModelsResource<'c> {
    pub(crate) client: &'c PulseClient,
}

/// B-112 — describes a model upload to [`ModelsResource::upload`].
///
/// Supply the model bytes either by file `path` (read at upload time) or as
/// raw `data`. Exactly one of the two must be set — [`ModelsResource::upload`]
/// returns a [`PulseError::InvalidConfig`] otherwise.
#[derive(Debug, Clone, Default)]
pub struct ModelUpload {
    /// Model name referenced by `ml_predict(model = ...)`.
    pub name: String,
    /// Filesystem path to the `.onnx` file. Mutually exclusive with `data`.
    pub path: Option<String>,
    /// Raw model bytes. Mutually exclusive with `path`.
    pub data: Option<Vec<u8>>,
    /// Model runtime — only `"onnx"` is supported today. Defaults to `"onnx"`.
    pub runtime: Option<String>,
    /// Ordered feature-name → type map, used to pack features into the input
    /// tensor (in the model's input order).
    pub input_schema: Option<std::collections::BTreeMap<String, String>>,
    /// Output-name → type map (informational).
    pub output_schema: Option<std::collections::BTreeMap<String, String>>,
}

impl ModelUpload {
    /// Upload from a filesystem path to the `.onnx` file.
    pub fn from_path(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: Some(path.into()),
            ..Self::default()
        }
    }

    /// Upload from raw model bytes.
    pub fn from_bytes(name: impl Into<String>, data: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            data: Some(data),
            ..Self::default()
        }
    }

    /// Override the runtime (default `"onnx"`).
    pub fn runtime(mut self, runtime: impl Into<String>) -> Self {
        self.runtime = Some(runtime.into());
        self
    }

    /// Set the ordered input feature schema.
    pub fn input_schema(mut self, schema: std::collections::BTreeMap<String, String>) -> Self {
        self.input_schema = Some(schema);
        self
    }

    /// Set the (informational) output schema.
    pub fn output_schema(mut self, schema: std::collections::BTreeMap<String, String>) -> Self {
        self.output_schema = Some(schema);
        self
    }
}

impl ModelsResource<'_> {
    /// `POST /api/pulse/ml-models` — upload (or replace) a model.
    ///
    /// Sent as `multipart/form-data`: a file part named `model` carrying the
    /// bytes, plus text parts `name`, `runtime`, and (when set) `inputSchema` /
    /// `outputSchema` as JSON strings. Replacing an existing name hot-swaps the
    /// model with no agent restart.
    ///
    /// Returns the persisted model metadata (name, runtime, sha256, version, …).
    ///
    /// # Errors
    ///
    /// - [`PulseError::InvalidConfig`] if `name` is blank, if neither or both
    ///   of `path`/`data` are set, or if the model bytes are empty.
    /// - [`PulseError::Transport`] if reading the file at `path` fails.
    pub async fn upload(&self, upload: ModelUpload) -> Result<Value, PulseError> {
        if upload.name.trim().is_empty() {
            return Err(PulseError::InvalidConfig(
                "model name must be a non-empty string".to_string(),
            ));
        }
        if upload.path.is_some() == upload.data.is_some() {
            return Err(PulseError::InvalidConfig(
                "provide exactly one of 'path' or 'data'".to_string(),
            ));
        }

        let (blob, filename) = match (&upload.path, upload.data) {
            (Some(path), None) => {
                let bytes = std::fs::read(path)
                    .map_err(|e| PulseError::InvalidConfig(format!("read {path}: {e}")))?;
                let filename = path
                    .rsplit(['/', '\\'])
                    .next()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("model.onnx")
                    .to_string();
                (bytes, filename)
            }
            (None, Some(data)) => (data, format!("{}.onnx", upload.name)),
            // Unreachable — guarded by the XOR check above.
            _ => unreachable!("exactly one of path/data enforced above"),
        };
        if blob.is_empty() {
            return Err(PulseError::InvalidConfig(
                "model bytes are empty".to_string(),
            ));
        }

        let runtime = upload.runtime.unwrap_or_else(|| "onnx".to_string());
        let model_part = reqwest::multipart::Part::bytes(blob)
            .file_name(filename)
            .mime_str("application/octet-stream")
            .map_err(PulseError::Transport)?;
        let mut form = reqwest::multipart::Form::new()
            .text("name", upload.name)
            .text("runtime", runtime)
            .part("model", model_part);
        if let Some(schema) = upload.input_schema {
            form = form.text("inputSchema", serde_json::to_string(&schema)?);
        }
        if let Some(schema) = upload.output_schema {
            form = form.text("outputSchema", serde_json::to_string(&schema)?);
        }

        self.client
            .request_multipart("/api/pulse/ml-models", form)
            .await
    }

    /// `GET /api/pulse/ml-models` — models registered for the caller's org.
    pub async fn list(&self) -> Result<Vec<Value>, PulseError> {
        let result = self
            .client
            .request(Method::GET, "/api/pulse/ml-models", None::<&()>, true)
            .await?;
        Ok(unwrap_list(&result, "models"))
    }

    /// `GET /api/pulse/ml-models/{name}` — metadata for one model.
    pub async fn get(&self, name: &str) -> Result<Value, PulseError> {
        let path = format!("/api/pulse/ml-models/{}", encode_path(name));
        self.client
            .request(Method::GET, &path, None::<&()>, true)
            .await
    }

    /// `DELETE /api/pulse/ml-models/{name}` — remove a model (ADMIN).
    pub async fn delete(&self, name: &str) -> Result<(), PulseError> {
        let path = format!("/api/pulse/ml-models/{}", encode_path(name));
        self.client
            .request::<()>(Method::DELETE, &path, None, true)
            .await?;
        Ok(())
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

use crate::db::LogFilter;
use crate::error::SwarmError;
use crate::harness::CliKind;
use crate::orchestrator::{DoneReport, Orchestrator};
use axum::extract::ws;
use axum::extract::{Path, Query, State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::services::{ServeDir, ServeFile};

#[derive(Embed)]
#[folder = "frontend/dist/"]
struct DashboardAssets;

type AppState = Arc<Orchestrator>;
const PEERS_HINT: &str = "run swarm peers to list agents";

#[derive(Deserialize)]
pub struct SpawnRequest {
    pub role: String,
    pub harness: String,
    #[serde(default)]
    pub system_prompt: String,
    pub parent_id: Option<String>,
    #[serde(default = "default_comms")]
    pub comms: String,
    pub model: Option<String>,
    #[serde(default)]
    pub worktree: bool,
}

fn default_comms() -> String {
    "mesh".to_string()
}

#[derive(Deserialize)]
pub struct DoneRequest {
    pub message: Option<String>,
    pub outcome: Option<String>,
    pub deliverable: Option<String>,
    pub checks: Option<String>,
    pub risk: Option<String>,
    pub next_action: Option<String>,
}

#[derive(Deserialize)]
pub struct SendRequest {
    pub from: String,
    pub to: String,
    pub content: String,
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub perspective: Option<String>,
    #[serde(default)]
    pub all: bool,
}

#[derive(Deserialize)]
pub struct LogQuery {
    #[serde(default = "default_log_limit")]
    pub n: usize,
    #[serde(rename = "type", default)]
    pub filter_type: Option<String>,
    pub q: Option<String>,
}

fn default_log_limit() -> usize {
    20
}

#[derive(Deserialize)]
pub struct InboxQuery {
    #[serde(default = "default_log_limit")]
    pub n: usize,
    pub from: Option<String>,
    pub q: Option<String>,
}

#[derive(Deserialize)]
pub struct CleanupQuery {
    #[serde(default)]
    pub delete_branch: bool,
}

#[derive(Deserialize)]
pub struct EventQuery {
    pub since: Option<String>,
    pub agent_id: Option<String>,
    #[serde(default = "default_event_limit")]
    pub limit: usize,
}

fn default_event_limit() -> usize {
    100
}

#[derive(Deserialize)]
pub struct BriefQuery {
    #[serde(default = "default_brief_limit")]
    pub limit: usize,
    pub q: Option<String>,
}

fn default_brief_limit() -> usize {
    20
}

#[derive(Serialize)]
struct ModelsResponse {
    harness: String,
    default_model: String,
    models: Vec<String>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    uptime: u64,
    version: &'static str,
    data_dir: String,
    project_dir: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    hint: String,
}

fn json_error(status: StatusCode, error: impl Into<String>, hint: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: error.into(),
            hint: hint.into(),
        }),
    )
        .into_response()
}

fn swarm_error_response(error: SwarmError) -> Response {
    match error {
        SwarmError::AgentNotFound(id) => json_error(
            StatusCode::NOT_FOUND,
            format!("agent not found: {id}"),
            PEERS_HINT,
        ),
        SwarmError::AgentInactive { id, status } => json_error(
            StatusCode::CONFLICT,
            format!("agent {id} is not accepting messages; status is {status}"),
            PEERS_HINT,
        ),
        SwarmError::InvalidInput(message) => json_error(
            StatusCode::BAD_REQUEST,
            message,
            "use only letters, numbers, underscores, and hyphens for role names and agent IDs",
        ),
        SwarmError::InvalidRequest(message) => json_error(
            StatusCode::BAD_REQUEST,
            message,
            "check the request and retry",
        ),
        SwarmError::Timeout(message) => json_error(
            StatusCode::REQUEST_TIMEOUT,
            format!("timeout: {message}"),
            "retry the request",
        ),
        other => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            other.to_string(),
            "check the swarm server logs and retry the request",
        ),
    }
}

async fn blocking_orchestrator<T, F>(context: &'static str, f: F) -> Result<T, SwarmError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SwarmError> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| SwarmError::Internal(format!("{context} task failed: {e}")))?
}

pub fn router(state: AppState) -> Router {
    router_with_dashboard(state, None)
}

pub fn router_with_dashboard(state: AppState, dashboard_dir: Option<PathBuf>) -> Router {
    let api = Router::new()
        .route("/api/health", get(health))
        .route("/api/brief", get(get_run_brief))
        .route("/api/stats", get(get_stats))
        .route("/api/agents", get(list_agents).post(spawn_agent))
        .route("/api/agents/{id}", get(get_agent).delete(kill_agent))
        .route("/api/agents/{id}/brief", get(get_agent_brief))
        .route("/api/agents/{id}/done", post(done_agent))
        .route("/api/agents/{id}/cleanup", post(cleanup_agent))
        .route("/api/agents/{id}/inbox", get(get_agent_inbox))
        .route("/api/agents/{id}/log", get(get_agent_log))
        .route("/api/agents/{id}/worktree", get(get_agent_worktree))
        .route("/api/messages", post(send_message))
        .route("/api/events", get(list_events))
        .route("/api/models", get(list_models))
        .route("/ws", get(ws_handler))
        .with_state(state);

    match dashboard_dir {
        Some(dir) if dir.exists() => {
            let index = dir.join("index.html");
            api.fallback_service(ServeDir::new(&dir).not_found_service(ServeFile::new(index)))
        }
        _ if DashboardAssets::get("index.html").is_some() => api.fallback(serve_embedded),
        _ => api.fallback(not_found),
    }
}

async fn serve_embedded(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match DashboardAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                [(axum::http::header::CONTENT_TYPE, mime.as_ref().to_string())],
                content.data.to_vec(),
            )
                .into_response()
        }
        None => match DashboardAssets::get("index.html") {
            Some(content) => (
                [(axum::http::header::CONTENT_TYPE, "text/html".to_string())],
                content.data.to_vec(),
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
    }
}

async fn health(State(orch): State<AppState>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        uptime: orch.uptime_seconds(),
        version: env!("CARGO_PKG_VERSION"),
        data_dir: orch.data_dir().to_string_lossy().to_string(),
        project_dir: orch.project_dir().to_string_lossy().to_string(),
    })
}

async fn get_stats(State(orch): State<AppState>) -> impl IntoResponse {
    match blocking_orchestrator("get stats", move || orch.stats()).await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn get_run_brief(
    State(orch): State<AppState>,
    Query(params): Query<BriefQuery>,
) -> impl IntoResponse {
    match blocking_orchestrator("get run brief", move || {
        orch.run_brief(params.limit, params.q.as_deref())
    })
    .await
    {
        Ok(brief) => Json(brief).into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn list_agents(
    State(orch): State<AppState>,
    Query(params): Query<ListQuery>,
) -> impl IntoResponse {
    if let Some(perspective) = params.perspective {
        match blocking_orchestrator("list agents", move || {
            orch.list_agents_with_perspective_all(&perspective, params.all)
        })
        .await
        {
            Ok(views) => Json(views).into_response(),
            Err(e) => swarm_error_response(e),
        }
    } else if params.all {
        match blocking_orchestrator("list agents", move || orch.list_all_agents()).await {
            Ok(agents) => Json(agents).into_response(),
            Err(e) => swarm_error_response(e),
        }
    } else {
        match blocking_orchestrator("list agents", move || orch.list_agents()).await {
            Ok(agents) => Json(agents).into_response(),
            Err(e) => swarm_error_response(e),
        }
    }
}

async fn get_agent(State(orch): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match blocking_orchestrator("get agent", {
        let id = id.clone();
        move || orch.get_agent(&id)
    })
    .await
    {
        Ok(Some(agent)) => Json(agent).into_response(),
        Ok(None) => swarm_error_response(SwarmError::AgentNotFound(id)),
        Err(e) => swarm_error_response(e),
    }
}

async fn spawn_agent(
    State(orch): State<AppState>,
    Json(req): Json<SpawnRequest>,
) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        orch.spawn_agent_with_model(
            &req.role,
            &req.harness,
            req.model.as_deref(),
            &req.system_prompt,
            req.parent_id.as_deref(),
            &req.comms,
            req.worktree,
        )
    })
    .await;

    match result {
        Ok(Ok(agent)) => (StatusCode::CREATED, Json(agent)).into_response(),
        Ok(Err(e)) => swarm_error_response(e),
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("spawn task failed: {e}"),
            "check the swarm server logs and retry the request",
        ),
    }
}

async fn send_message(
    State(orch): State<AppState>,
    Json(req): Json<SendRequest>,
) -> impl IntoResponse {
    match orch.send_message(&req.from, &req.to, &req.content).await {
        Ok(msg) => Json(msg).into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn get_agent_log(
    State(orch): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<LogQuery>,
) -> impl IntoResponse {
    let filter = match params.filter_type.as_deref() {
        Some("messages") => LogFilter::Messages,
        Some("output") => LogFilter::Output,
        _ => LogFilter::All,
    };
    match blocking_orchestrator("get agent log", {
        let id = id.clone();
        move || orch.search_agent_log(&id, params.n, filter, params.q.as_deref())
    })
    .await
    {
        Ok(entries) => Json(entries).into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn get_agent_inbox(
    State(orch): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<InboxQuery>,
) -> impl IntoResponse {
    match blocking_orchestrator("get agent inbox", {
        let id = id.clone();
        move || orch.search_inbox(&id, params.from.as_deref(), params.n, params.q.as_deref())
    })
    .await
    {
        Ok(entries) => Json(entries).into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn get_agent_brief(
    State(orch): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<BriefQuery>,
) -> impl IntoResponse {
    match blocking_orchestrator("get agent brief", {
        let id = id.clone();
        move || orch.agent_brief(&id, params.limit, params.q.as_deref())
    })
    .await
    {
        Ok(brief) => Json(brief).into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn get_agent_worktree(
    State(orch): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match blocking_orchestrator("get agent worktree", {
        let id = id.clone();
        move || orch.worktree_info(&id)
    })
    .await
    {
        Ok(Some(info)) => Json(info).into_response(),
        Ok(None) => json_error(
            StatusCode::NOT_FOUND,
            format!("worktree not found for agent: {id}"),
            "spawn the agent with --worktree or check swarm peers --all",
        ),
        Err(e) => swarm_error_response(e),
    }
}

async fn done_agent(
    State(orch): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<DoneRequest>,
) -> impl IntoResponse {
    let report = DoneReport {
        summary: req.message,
        outcome: req.outcome,
        deliverable: req.deliverable,
        checks: req.checks,
        risk: req.risk,
        next_action: req.next_action,
    };
    match orch.done_agent_with_report(&id, report).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn cleanup_agent(
    State(orch): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<CleanupQuery>,
) -> impl IntoResponse {
    let result =
        tokio::task::spawn_blocking(move || orch.cleanup_agent(&id, params.delete_branch)).await;

    match result {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) => swarm_error_response(e),
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cleanup task failed: {e}"),
            "check the swarm server logs and retry the request",
        ),
    }
}

async fn kill_agent(State(orch): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match orch.kill_agent(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn list_events(
    State(orch): State<AppState>,
    Query(params): Query<EventQuery>,
) -> impl IntoResponse {
    match blocking_orchestrator("list events", move || {
        orch.list_events(
            params.since.as_deref(),
            params.agent_id.as_deref(),
            params.limit,
        )
    })
    .await
    {
        Ok(events) => Json(events).into_response(),
        Err(e) => swarm_error_response(e),
    }
}

async fn list_models() -> impl IntoResponse {
    let harnesses = [
        CliKind::Claude,
        CliKind::Gemini,
        CliKind::Codex,
        CliKind::Grok,
    ];
    let models: Vec<ModelsResponse> = harnesses
        .iter()
        .map(|kind| ModelsResponse {
            harness: kind.default_binary().to_string(),
            default_model: kind.default_model().to_string(),
            models: kind.known_models().iter().map(|s| s.to_string()).collect(),
        })
        .collect();
    Json(models)
}

async fn ws_handler(State(orch): State<AppState>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| handle_ws(socket, orch))
}

async fn not_found() -> impl IntoResponse {
    json_error(StatusCode::NOT_FOUND, "route not found", "run swarm --help")
}

async fn handle_ws(mut socket: ws::WebSocket, orch: AppState) {
    let mut rx = orch.subscribe();
    loop {
        match rx.recv().await {
            Ok(event) => {
                if let Ok(json) = serde_json::to_string(&event) {
                    if socket.send(ws::Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!("websocket skipped {skipped} lagged event(s)");
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

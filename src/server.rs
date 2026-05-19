use crate::orchestrator::Orchestrator;
use axum::extract::ws;
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use std::sync::Arc;

type AppState = Arc<Orchestrator>;

#[derive(Deserialize)]
pub struct SpawnRequest {
    pub role: String,
    pub harness: String,
    #[serde(default)]
    pub system_prompt: String,
    pub parent_id: Option<String>,
    #[serde(default = "default_comms")]
    pub comms: String,
}

fn default_comms() -> String {
    "mesh".to_string()
}

#[derive(Deserialize)]
pub struct SendRequest {
    pub from: String,
    pub to: String,
    pub content: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/agents", get(list_agents).post(spawn_agent))
        .route(
            "/api/agents/{id}",
            get(get_agent).delete(kill_agent),
        )
        .route("/api/messages", post(send_message))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

async fn list_agents(State(orch): State<AppState>) -> impl IntoResponse {
    match orch.list_agents() {
        Ok(agents) => Json(agents).into_response(),
        Err(e) => {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn get_agent(State(orch): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match orch.get_agent(&id) {
        Ok(Some(agent)) => Json(agent).into_response(),
        Ok(None) => axum::http::StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn spawn_agent(
    State(orch): State<AppState>,
    Json(req): Json<SpawnRequest>,
) -> impl IntoResponse {
    match orch.spawn_agent(
        &req.role,
        &req.harness,
        &req.system_prompt,
        req.parent_id.as_deref(),
        &req.comms,
    ) {
        Ok(agent) => (axum::http::StatusCode::CREATED, Json(agent)).into_response(),
        Err(e) => {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn send_message(
    State(orch): State<AppState>,
    Json(req): Json<SendRequest>,
) -> impl IntoResponse {
    match orch.send_message(&req.from, &req.to, &req.content).await {
        Ok(msg) => Json(msg).into_response(),
        Err(e) => {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn kill_agent(State(orch): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match orch.kill_agent(&id).await {
        Ok(()) => axum::http::StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn ws_handler(State(orch): State<AppState>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| handle_ws(socket, orch))
}

async fn handle_ws(mut socket: ws::WebSocket, orch: AppState) {
    let mut rx = orch.subscribe();
    while let Ok(event) = rx.recv().await {
        if let Ok(json) = serde_json::to_string(&event) {
            if socket
                .send(ws::Message::Text(json.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    }
}

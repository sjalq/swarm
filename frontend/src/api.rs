use crate::state::{Agent, LogEntry, Stats, SwarmEvent, WsState};
use futures::StreamExt;
use gloo_net::http::Request;
use gloo_net::websocket::{futures::WebSocket, Message as WsMessage};
use leptos::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use wasm_bindgen_futures::spawn_local;

fn api_base() -> String {
    let location = web_sys::window().unwrap().location();
    let protocol = location.protocol().unwrap_or_else(|_| "http:".into());
    let host = location.host().unwrap_or_else(|_| "127.0.0.1:9800".into());
    format!("{}//{}/api", protocol, host)
}

fn ws_url() -> String {
    let location = web_sys::window().unwrap().location();
    let protocol = location.protocol().unwrap_or_else(|_| "http:".into());
    let ws_protocol = if protocol == "https:" { "wss:" } else { "ws:" };
    let host = location.host().unwrap_or_else(|_| "127.0.0.1:9800".into());
    format!("{}//{}/ws", ws_protocol, host)
}

pub async fn fetch_agents(include_all: bool) -> Result<Vec<Agent>, String> {
    let url = if include_all {
        format!("{}/agents?all=true", api_base())
    } else {
        format!("{}/agents", api_base())
    };

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| format!("fetch failed: {}", e))?;

    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }

    resp.json::<Vec<Agent>>()
        .await
        .map_err(|e| format!("parse failed: {}", e))
}

pub async fn fetch_stats() -> Result<Stats, String> {
    let url = format!("{}/stats", api_base());

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| format!("fetch failed: {}", e))?;

    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }

    resp.json::<Stats>()
        .await
        .map_err(|e| format!("parse failed: {}", e))
}

pub async fn fetch_agent_log(agent_id: &str, limit: usize) -> Result<Vec<LogEntry>, String> {
    let url = format!("{}/agents/{}/log?n={}", api_base(), agent_id, limit);

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| format!("fetch failed: {}", e))?;

    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }

    resp.json::<Vec<LogEntry>>()
        .await
        .map_err(|e| format!("parse failed: {}", e))
}

#[derive(Serialize)]
struct SendMessageRequest<'a> {
    from: &'a str,
    to: &'a str,
    content: &'a str,
}

pub async fn send_user_message(agent_id: &str, content: &str) -> Result<(), String> {
    let url = format!("{}/messages", api_base());
    let body = SendMessageRequest {
        from: "user",
        to: agent_id,
        content,
    };

    let resp = Request::post(&url)
        .json(&body)
        .map_err(|e| format!("encode failed: {}", e))?
        .send()
        .await
        .map_err(|e| format!("send failed: {}", e))?;

    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }

    Ok(())
}

pub fn connect_websocket(
    agents: RwSignal<Vec<Agent>>,
    activity_map: RwSignal<HashMap<String, String>>,
    ws_state: RwSignal<WsState>,
) {
    spawn_local(async move {
        ws_state.set(WsState::Connecting);
        ws_connect_loop(agents, activity_map, ws_state).await;
    });
}

async fn ws_connect_loop(
    agents: RwSignal<Vec<Agent>>,
    activity_map: RwSignal<HashMap<String, String>>,
    ws_state: RwSignal<WsState>,
) {
    let url = ws_url();

    loop {
        let ws = match WebSocket::open(&url) {
            Ok(ws) => {
                ws_state.set(WsState::Connected);
                ws
            }
            Err(e) => {
                web_sys::console::error_1(&format!("ws connect failed: {:?}", e).into());
                let current = ws_state.get_untracked();
                let next_attempt = match current {
                    WsState::Reconnecting { attempt } => attempt + 1,
                    _ => 1,
                };
                ws_state.set(WsState::Reconnecting {
                    attempt: next_attempt,
                });
                let delay = ws_state.get_untracked().reconnect_delay_ms();
                gloo_timers::future::TimeoutFuture::new(delay).await;
                continue;
            }
        };

        let (_write, mut read) = ws.split();

        while let Some(msg) = read.next().await {
            match msg {
                Ok(WsMessage::Text(text)) => {
                    if let Ok(event) = serde_json::from_str::<SwarmEvent>(&text) {
                        agents.update(|a| {
                            activity_map.update(|am| {
                                crate::state::apply_event(a, am, &event);
                            });
                        });
                    }
                }
                Ok(WsMessage::Bytes(_)) => {}
                Err(e) => {
                    web_sys::console::error_1(&format!("ws error: {:?}", e).into());
                    break;
                }
            }
        }

        ws_state.set(WsState::Reconnecting { attempt: 1 });
        let delay = ws_state.get_untracked().reconnect_delay_ms();
        gloo_timers::future::TimeoutFuture::new(delay).await;
    }
}

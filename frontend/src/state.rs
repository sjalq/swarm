use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Remote data FSM (mirrors twolebot's RemoteData) ──

#[derive(Clone, Debug, PartialEq)]
pub enum RemoteData<T> {
    NotAsked,
    Loading,
    Success(T),
    Failure(String),
}

// ── WebSocket connection FSM ──

#[derive(Clone, Debug, PartialEq)]
pub enum WsState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting { attempt: u32 },
}

impl WsState {
    pub fn reconnect_delay_ms(&self) -> u32 {
        match self {
            WsState::Reconnecting { attempt } => {
                let base = 1000u32;
                let max = 30_000u32;
                base.saturating_mul(2u32.saturating_pow(*attempt)).min(max)
            }
            _ => 1000,
        }
    }

    pub fn css_class(&self) -> &'static str {
        match self {
            WsState::Disconnected => "",
            WsState::Connecting | WsState::Reconnecting { .. } => "connecting",
            WsState::Connected => "connected",
        }
    }

    pub fn label(&self) -> String {
        match self {
            WsState::Disconnected => "disconnected".into(),
            WsState::Connecting => "connecting".into(),
            WsState::Connected => "connected".into(),
            WsState::Reconnecting { attempt } => format!("reconnecting ({})", attempt),
        }
    }
}

// ── Sort state machine ──

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SortField {
    CreatedAt,
    LastActivity,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SortState {
    pub field: SortField,
    pub direction: SortDirection,
}

impl Default for SortState {
    fn default() -> Self {
        Self {
            field: SortField::LastActivity,
            direction: SortDirection::Desc,
        }
    }
}

impl SortState {
    pub fn toggle_field(self, field: SortField) -> Self {
        if self.field == field {
            Self {
                direction: match self.direction {
                    SortDirection::Asc => SortDirection::Desc,
                    SortDirection::Desc => SortDirection::Asc,
                },
                ..self
            }
        } else {
            Self {
                field,
                direction: SortDirection::Desc,
            }
        }
    }
}

// ── Data types (mirror backend) ──

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Agent {
    pub id: String,
    pub label: String,
    pub harness: String,
    pub model: String,
    pub status: String,
    pub parent_id: Option<String>,
    pub system_prompt: String,
    pub work_dir: String,
    pub comms: String,
    pub created_at: String,
    pub ended_at: Option<String>,
    pub worktree_branch: Option<String>,
    pub project_dir: Option<String>,
    #[serde(default)]
    pub user_launched: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub timestamp: String,
    pub kind: String,
    pub peer: String,
    pub content: String,
}

impl LogEntry {
    pub fn bubble_class(&self) -> &str {
        match self.kind.as_str() {
            "recv" if self.peer == "user" => "chat-bubble incoming external",
            "sent" if self.peer == "user" => "chat-bubble outgoing external",
            "recv" => "chat-bubble incoming",
            "sent" => "chat-bubble outgoing",
            _ => "chat-bubble system",
        }
    }

    pub fn label(&self) -> String {
        match self.kind.as_str() {
            "recv" if self.peer.is_empty() => "received".into(),
            "recv" => format!("received from {}", self.peer),
            "sent" if self.peer.is_empty() => "sent".into(),
            "sent" => format!("sent to {}", self.peer),
            "output" => "agent output".into(),
            "interrupted" => "interrupted output".into(),
            "error" => "error".into(),
            "timeout" => "timeout".into(),
            _ => self.kind.clone(),
        }
    }
}

impl Agent {
    pub fn status_class(&self) -> &str {
        match self.status.as_str() {
            "idle" => "idle",
            "working" => "working",
            "done" => "done",
            _ => "error",
        }
    }

    pub fn harness_class(&self) -> &str {
        match self.harness.as_str() {
            "claude" => "claude",
            "gemini" => "gemini",
            "codex" => "codex",
            "grok" => "grok",
            "echo" => "echo",
            _ => "echo",
        }
    }

    pub fn display_model(&self) -> &str {
        if self.model.is_empty() {
            "(default)"
        } else {
            &self.model
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Stats {
    pub total: u64,
    pub alive: u64,
    pub done: u64,
    pub messages: u64,
    pub errors: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwarmEvent {
    TopicStarted {
        agent: Agent,
    },
    AgentDone {
        agent_id: String,
        message: Option<String>,
    },
    AgentKilled {
        agent_id: String,
    },
    AgentStatus {
        agent_id: String,
        status: String,
    },
    AgentOutput {
        agent_id: String,
        text: String,
    },
    AgentError {
        agent_id: String,
        error: String,
    },
    MessageRouted {
        from: String,
        to: String,
    },
    UserNotification {
        from: String,
        content: String,
    },
}

// ── Tree node (computed from flat agent list) ──

#[derive(Clone, Debug, PartialEq)]
pub struct AgentTreeNode {
    pub agent: Agent,
    pub children: Vec<AgentTreeNode>,
    pub last_activity: String,
}

// ── Pure functions ──

pub fn build_tree(agents: &[Agent], activity_map: &HashMap<String, String>) -> Vec<AgentTreeNode> {
    let agent_map: HashMap<&str, &Agent> = agents.iter().map(|a| (a.id.as_str(), a)).collect();

    let root_ids: Vec<&str> = agents
        .iter()
        .filter(|a| {
            a.parent_id
                .as_ref()
                .map_or(true, |pid| !agent_map.contains_key(pid.as_str()))
        })
        .map(|a| a.id.as_str())
        .collect();

    let children_map: HashMap<&str, Vec<&str>> = {
        let mut map: HashMap<&str, Vec<&str>> = HashMap::new();
        for agent in agents {
            if let Some(ref pid) = agent.parent_id {
                if agent_map.contains_key(pid.as_str()) {
                    map.entry(pid.as_str()).or_default().push(agent.id.as_str());
                }
            }
        }
        map
    };

    fn build_node(
        id: &str,
        agent_map: &HashMap<&str, &Agent>,
        children_map: &HashMap<&str, Vec<&str>>,
        activity_map: &HashMap<String, String>,
    ) -> Option<AgentTreeNode> {
        let agent = agent_map.get(id)?;
        let children: Vec<AgentTreeNode> = children_map
            .get(id)
            .map(|child_ids| {
                child_ids
                    .iter()
                    .filter_map(|cid| build_node(cid, agent_map, children_map, activity_map))
                    .collect()
            })
            .unwrap_or_default();

        let own_activity = activity_map
            .get(agent.id.as_str())
            .cloned()
            .unwrap_or_else(|| agent.created_at.clone());

        let last_activity = children
            .iter()
            .map(|c| c.last_activity.as_str())
            .chain(std::iter::once(own_activity.as_str()))
            .max()
            .unwrap_or(own_activity.as_str())
            .to_string();

        Some(AgentTreeNode {
            agent: (*agent).clone(),
            children,
            last_activity,
        })
    }

    root_ids
        .iter()
        .filter_map(|id| build_node(id, &agent_map, &children_map, activity_map))
        .collect()
}

pub fn sort_tree(nodes: &mut [AgentTreeNode], sort: SortState) {
    let cmp = |a: &AgentTreeNode, b: &AgentTreeNode| {
        let ord = match sort.field {
            SortField::CreatedAt => a.agent.created_at.cmp(&b.agent.created_at),
            SortField::LastActivity => a.last_activity.cmp(&b.last_activity),
        };
        match sort.direction {
            SortDirection::Asc => ord,
            SortDirection::Desc => ord.reverse(),
        }
    };

    nodes.sort_by(cmp);
    for node in nodes.iter_mut() {
        sort_tree(&mut node.children, sort);
    }
}

pub fn apply_event(
    agents: &mut Vec<Agent>,
    activity_map: &mut HashMap<String, String>,
    event: &SwarmEvent,
) {
    let now = chrono::Utc::now().to_rfc3339();

    match event {
        SwarmEvent::TopicStarted { agent } => {
            if !agents.iter().any(|a| a.id == agent.id) {
                agents.push(agent.clone());
            }
            activity_map.insert(agent.id.clone(), now);
        }
        SwarmEvent::AgentDone { agent_id, .. } => {
            if let Some(agent) = agents.iter_mut().find(|a| a.id == *agent_id) {
                agent.status = "done".into();
                agent.ended_at = Some(now.clone());
            }
            activity_map.insert(agent_id.clone(), now);
        }
        SwarmEvent::AgentKilled { agent_id } => {
            if let Some(agent) = agents.iter_mut().find(|a| a.id == *agent_id) {
                agent.status = "done".into();
                agent.ended_at = Some(now.clone());
            }
            activity_map.insert(agent_id.clone(), now);
        }
        SwarmEvent::AgentStatus { agent_id, status } => {
            if let Some(agent) = agents.iter_mut().find(|a| a.id == *agent_id) {
                agent.status = status.clone();
            }
            activity_map.insert(agent_id.clone(), now);
        }
        SwarmEvent::AgentOutput { agent_id, .. } => {
            activity_map.insert(agent_id.clone(), now);
        }
        SwarmEvent::AgentError { agent_id, .. } => {
            activity_map.insert(agent_id.clone(), now);
        }
        SwarmEvent::MessageRouted { from, to } => {
            activity_map.insert(from.clone(), now.clone());
            activity_map.insert(to.clone(), now);
        }
        SwarmEvent::UserNotification { from, .. } => {
            activity_map.insert(from.clone(), now);
        }
    }
}

pub fn format_timestamp(ts: &str) -> String {
    let date = js_sys::Date::new(&wasm_bindgen::JsValue::from_str(ts));
    if date.get_time().is_nan() {
        return if ts.len() >= 19 {
            ts[..19].replace('T', " ")
        } else {
            ts.to_string()
        };
    }

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        date.get_full_year(),
        date.get_month() + 1,
        date.get_date(),
        date.get_hours(),
        date.get_minutes(),
        date.get_seconds()
    )
}

pub fn format_relative_time(ts: &str) -> String {
    let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) else {
        return ts.to_string();
    };
    let now = chrono::Utc::now();
    let diff = now.signed_duration_since(dt);
    let total_seconds = diff.num_seconds().max(0);
    let days = total_seconds / 86_400;
    let hours = (total_seconds / 3_600) % 24;
    let minutes = (total_seconds / 60) % 60;
    let seconds = total_seconds % 60;

    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    parts.push(format!("{seconds}s"));

    parts.join(" ")
}

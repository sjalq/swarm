use serde::{Deserialize, Serialize};
use std::fmt;

pub const USER_TOPIC_ID: &str = "user";
pub const SWARM_PROTOCOL_VERSION: &str = "topic-label-v1";

pub trait SqlEnum: Sized {
    fn as_sql(&self) -> &'static str;
    fn from_sql(value: &str) -> std::result::Result<Self, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicStatus {
    Idle,
    Working,
    Error,
    #[serde(rename = "done")]
    Paused,
}

impl TopicStatus {
    pub fn is_active(self) -> bool {
        !matches!(self, Self::Paused)
    }
}

impl SqlEnum for TopicStatus {
    fn as_sql(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Error => "error",
            // Keep the SQLite/API value stable while the domain model uses
            // "paused" for a durable topic stream that can be resumed later.
            Self::Paused => "done",
        }
    }

    fn from_sql(value: &str) -> std::result::Result<Self, String> {
        match value {
            "idle" => Ok(Self::Idle),
            "working" => Ok(Self::Working),
            "error" => Ok(Self::Error),
            "done" | "paused" => Ok(Self::Paused),
            other => Err(format!("unknown topic status: {other}")),
        }
    }
}

impl fmt::Display for TopicStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_sql())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommsMode {
    #[serde(rename = "mesh")]
    Mesh,
    #[serde(rename = "parent-only")]
    ParentOnly,
}

impl SqlEnum for CommsMode {
    fn as_sql(&self) -> &'static str {
        match self {
            Self::Mesh => "mesh",
            Self::ParentOnly => "parent-only",
        }
    }

    fn from_sql(value: &str) -> std::result::Result<Self, String> {
        match value {
            "mesh" => Ok(Self::Mesh),
            "parent-only" | "parent_only" => Ok(Self::ParentOnly),
            other => Err(format!("unknown comms mode: {other}")),
        }
    }
}

impl fmt::Display for CommsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_sql())
    }
}

impl Default for CommsMode {
    fn default() -> Self {
        Self::Mesh
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRow {
    pub id: String,
    pub label: String,
    pub harness: String,
    pub model: String,
    pub status: TopicStatus,
    pub parent_id: Option<String>,
    pub system_prompt: String,
    pub work_dir: String,
    pub comms: CommsMode,
    pub created_at: String,
    pub ended_at: Option<String>,
    pub worktree_branch: Option<String>,
    pub project_dir: Option<String>,
    pub user_launched: bool,
}

impl AgentRow {
    pub fn parent_or_user(&self) -> &str {
        self.parent_id.as_deref().unwrap_or(USER_TOPIC_ID)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRow {
    pub id: String,
    pub event_type: String,
    pub agent_id: Option<String>,
    pub payload: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRow {
    pub id: String,
    pub from_agent: String,
    pub to_agent: String,
    pub content: String,
    pub delivered: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputLogRow {
    pub id: String,
    pub agent_id: String,
    pub content: String,
    pub kind: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoverRow {
    pub id: String,
    pub agent_id: String,
    pub summary: Option<String>,
    pub outcome: Option<String>,
    pub deliverable: Option<String>,
    pub checks: Option<String>,
    pub risk: Option<String>,
    pub next_action: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub kind: String,
    pub peer: String,
    pub content: String,
}

pub enum LogFilter {
    All,
    Messages,
    Output,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbStats {
    pub total: u64,
    pub alive: u64,
    pub done: u64,
    pub messages: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwarmEvent {
    TopicStarted {
        agent: AgentRow,
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
        status: TopicStatus,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentView {
    #[serde(flatten)]
    pub agent: AgentRow,
    pub relation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeInfo {
    pub branch: String,
    pub head: String,
    pub dirty: bool,
    pub path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DoneReport {
    pub summary: Option<String>,
    pub outcome: Option<String>,
    pub deliverable: Option<String>,
    pub checks: Option<String>,
    pub risk: Option<String>,
    pub next_action: Option<String>,
}

impl DoneReport {
    pub fn has_content(&self) -> bool {
        self.summary
            .as_ref()
            .or(self.outcome.as_ref())
            .or(self.deliverable.as_ref())
            .or(self.checks.as_ref())
            .or(self.risk.as_ref())
            .or(self.next_action.as_ref())
            .is_some()
    }
}

#[derive(Debug, Clone, Default)]
pub struct StartTopicOptions<'a> {
    pub model: Option<&'a str>,
    pub parent_id: Option<&'a str>,
    pub comms: CommsMode,
    pub use_worktree: bool,
    pub user_launched: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BriefLogEntry {
    pub timestamp: String,
    pub kind: String,
    pub peer: String,
    pub content_chars: usize,
    pub preview: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentBrief {
    pub id: String,
    pub label: String,
    pub harness: String,
    pub model: String,
    pub status: TopicStatus,
    pub parent_id: Option<String>,
    pub created_at: String,
    pub ended_at: Option<String>,
    pub worktree_branch: Option<String>,
    pub prompt_chars: usize,
    pub latest_handover: Option<HandoverRow>,
    pub recent_log: Vec<BriefLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentBriefSummary {
    pub id: String,
    pub label: String,
    pub harness: String,
    pub status: TopicStatus,
    pub parent_id: Option<String>,
    pub created_at: String,
    pub ended_at: Option<String>,
    pub worktree_branch: Option<String>,
    pub prompt_chars: usize,
    pub latest_handover: Option<HandoverRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmBrief {
    pub stats: DbStats,
    pub agents: Vec<AgentBriefSummary>,
    pub recent_handovers: Vec<HandoverRow>,
}

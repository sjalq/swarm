use crate::db::{AgentRow, Db, DbStats, EventRow, LogEntry, LogFilter, MessageRow, OutputLogRow};
use crate::error::{Result, SwarmError};
use crate::harness::{Harness, HarnessOutput, HarnessRegistry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const SWARM_PREAMBLE: &str = "\
You have access to the `swarm` CLI for multi-agent coordination:

Commands:
  swarm peers [--json]                 List visible agents (your parent, siblings, and all descendants) with relation labels.
  swarm peers --all [--json]           Include done agents.
  swarm send <agent-id> \"message\"      Send a message to another agent.
  swarm send user \"message\"            Notify the operator running the orchestrator.
  swarm spawn --role <name> --harness <cli> --prompt \"instructions...\"
                                       Create a new child agent. Harnesses: claude, gemini, codex, grok, echo.
  swarm spawn --role <name> --harness claude --model claude-sonnet-4-6 --worktree --prompt \"...\"
                                       Spawn with a specific model and an isolated git worktree.
  swarm cleanup <agent-id>             Remove a done agent's git worktree.
  swarm cleanup <agent-id> --delete-branch  Also delete the agent's branch.
  swarm models [--json]                List available models for each harness.
  swarm log <agent-id> [--json]        View an agent's recent activity (messages sent/received and output).
  swarm log <agent-id> -n <count>      Show the last N entries (default: 20).
  swarm log <agent-id> --truncate <N>  Truncate text log content to N chars (default: 500; 0 disables).
  swarm log <agent-id> --messages      Show only messages (sent and received).
  swarm log <agent-id> --output        Show only harness output.
  swarm status [--json]                Show your own agent status (includes model info).
  swarm done \"optional final message\"   Signal that you have finished your task. Sends a message to your parent and exits gracefully.
  swarm kill <agent-id>                Stop an agent and mark it done.

Communication guidelines:
- Always reply via `swarm send` when the sender asks a question or directly requests a result.
- Never reply to status broadcasts or FYI progress updates unless they ask for action.
- Keep swarm messages under 300 words unless the requester explicitly asks for more.
- All swarm commands are idempotent and safe to retry.
- Use `swarm send user \"message\"` when you need to notify the human operator directly.
- For long-running work, send brief progress updates to the requestor so they know you are active. Keep updates short - the recipient has a limited context window, and every message you send consumes part of it.
- Do not send unnecessary messages. If you have nothing meaningful to report, stay silent. Silence is better than noise.
- Use `swarm log <agent-id>` to check on agents you have delegated work to, rather than interrupting them with a status request.
- `swarm peers` shows only your parent, siblings, and descendants. Agents outside your family tree are not visible to you.
- When your assigned task is complete, call `swarm done \"summary of what you did\"` to signal completion and report back to your parent. Do not stay idle - finish and exit.
- If you spawn child agents, their relationship to you is preserved when you finish. You do not need to clean them up unless they are stuck or no longer useful.
- Git worktrees (`--worktree` on spawn) give an agent its own branch and file checkout. Use your judgment:
  - DO use worktrees when multiple agents will edit files in the same compiled project (Rust, Elm, etc.) - the compiler will fail if two agents edit concurrently.
  - DO NOT use worktrees for read-only tasks: reviewing, critiquing, searching, analyzing. Those agents should see the real source.
  - DO NOT use worktrees for a single editing agent when no other agent is editing the same codebase.
  - When in doubt: if the agent's job is to read, no worktree. If it will write and others are also writing, worktree.
- When you finish work in a worktree, `git add` and `git commit` your changes before calling `swarm done`. Uncommitted work in a worktree is invisible to other agents and the coordinator.
- Worktrees are NOT automatically cleaned up. Use `swarm cleanup <agent-id>` only after you have reviewed or merged the agent's work.
- If you are a coordinator and your child agents worked in worktrees, merge their branches into the base branch (usually main) before calling `swarm done`. Use `git merge <branch>` from the main checkout. Resolve conflicts if needed - that is part of your coordination role.";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwarmEvent {
    AgentSpawned {
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

enum WorkerCmd {
    NewMessage,
    Shutdown,
}

struct AgentWorker {
    cmd_tx: mpsc::UnboundedSender<WorkerCmd>,
    _handle: JoinHandle<()>,
}

struct WorkerConfig {
    db: Arc<Db>,
    harness: Arc<dyn Harness>,
    agent_id: String,
    agent_role: String,
    model: Option<String>,
    topic_dir: PathBuf,
    project_dir: PathBuf,
    swarm_addr: String,
    event_tx: broadcast::Sender<SwarmEvent>,
    resume_conversation: bool,
}

pub struct Orchestrator {
    db: Arc<Db>,
    registry: HarnessRegistry,
    workers: Mutex<HashMap<String, AgentWorker>>,
    event_tx: broadcast::Sender<SwarmEvent>,
    addr: String,
    project_dir: PathBuf,
    data_dir: PathBuf,
    start_time: Instant,
}

impl Orchestrator {
    pub fn new(
        db: Arc<Db>,
        registry: HarnessRegistry,
        addr: String,
        project_dir: PathBuf,
        data_dir: PathBuf,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            db,
            registry,
            workers: Mutex::new(HashMap::new()),
            event_tx,
            addr,
            project_dir,
            data_dir,
            start_time: Instant::now(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SwarmEvent> {
        self.event_tx.subscribe()
    }

    fn workers(&self) -> Result<MutexGuard<'_, HashMap<String, AgentWorker>>> {
        self.workers
            .lock()
            .map_err(|_| SwarmError::Internal("worker registry mutex poisoned".to_string()))
    }

    fn active_status(status: &str) -> bool {
        status != "done"
    }

    fn validate_identifier(kind: &str, value: &str) -> Result<()> {
        if value.is_empty() {
            return Err(SwarmError::InvalidInput(format!(
                "{kind} must not be empty"
            )));
        }

        if !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(SwarmError::InvalidInput(format!(
                "{kind} contains invalid characters; allowed characters are [a-zA-Z0-9_-]"
            )));
        }

        Ok(())
    }

    fn validate_role(role: &str) -> Result<()> {
        Self::validate_identifier("role", role)
    }

    fn validate_agent_id(agent_id: &str) -> Result<()> {
        Self::validate_identifier("agent_id", agent_id)
    }

    fn notify_worker(&self, agent_id: &str, cmd: WorkerCmd) -> Result<bool> {
        let workers = self.workers()?;
        if let Some(worker) = workers.get(agent_id) {
            worker.cmd_tx.send(cmd).map_err(|_| {
                SwarmError::Internal(format!("agent worker is stopped: {agent_id}"))
            })?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn has_worker(&self, agent_id: &str) -> Result<bool> {
        Ok(self.workers()?.contains_key(agent_id))
    }

    fn git_output(current_dir: &Path, args: &[&str]) -> Result<String> {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(current_dir)
            .output()
            .map_err(|e| SwarmError::Process(format!("git {} failed: {e}", args.join(" "))))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SwarmError::Process(format!(
                "git {} failed: {}",
                args.join(" "),
                stderr.trim()
            )));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    async fn blocking_db<T, F>(context: &'static str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| SwarmError::Internal(format!("{context} task failed: {e}")))?
    }

    async fn update_status_if_active_async(db: Arc<Db>, agent_id: String, status: String) {
        if let Err(e) = Self::blocking_db("update agent status", move || {
            db.update_agent_status_if_active(&agent_id, &status)
        })
        .await
        {
            tracing::error!("failed to update agent status: {e}");
        }
    }

    async fn update_status_async(db: Arc<Db>, agent_id: String, status: String) {
        if let Err(e) = Self::blocking_db("update agent status", move || {
            db.update_agent_status(&agent_id, &status)
        })
        .await
        {
            tracing::error!("failed to update agent status: {e}");
        }
    }

    async fn insert_output_log_async(db: Arc<Db>, entry: OutputLogRow, context: &'static str) {
        if let Err(e) = Self::blocking_db(context, move || db.insert_output_log(&entry)).await {
            tracing::error!("{context}: {e}");
        }
    }

    fn start_worker_for_agent(&self, agent: &AgentRow, resume_conversation: bool) -> Result<bool> {
        if self.workers()?.contains_key(&agent.id) {
            return Ok(false);
        }

        let harness = self
            .registry
            .get(&agent.harness)
            .ok_or_else(|| SwarmError::Internal(format!("unknown harness: {}", agent.harness)))?;

        let topic_dir = PathBuf::from(&agent.work_dir);
        let agent_project_dir = self
            .worktree_dir(&agent.id)?
            .unwrap_or_else(|| self.project_dir.clone());
        let model = if agent.model.is_empty() {
            None
        } else {
            Some(agent.model.clone())
        };

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(Self::agent_worker_loop(
            WorkerConfig {
                db: self.db.clone(),
                harness,
                agent_id: agent.id.clone(),
                agent_role: agent.role.clone(),
                model,
                topic_dir,
                project_dir: agent_project_dir,
                swarm_addr: self.addr.clone(),
                event_tx: self.event_tx.clone(),
                resume_conversation,
            },
            cmd_rx,
        ));

        let mut workers = self.workers()?;
        if workers.contains_key(&agent.id) {
            if cmd_tx.send(WorkerCmd::Shutdown).is_err() {
                tracing::debug!("duplicate worker already stopped: {}", agent.id);
            }
            return Ok(false);
        }

        workers.insert(
            agent.id.clone(),
            AgentWorker {
                cmd_tx,
                _handle: handle,
            },
        );
        Ok(true)
    }

    async fn reactivate_agent_async(&self, agent: &AgentRow) -> Result<()> {
        if Self::active_status(&agent.status) {
            return Ok(());
        }

        self.start_worker_for_agent(agent, true)?;
        let db = self.db.clone();
        let agent_id = agent.id.clone();
        Self::blocking_db("reactivate agent", move || {
            db.update_agent_status(&agent_id, "idle")
        })
        .await?;
        self.emit_event_async(SwarmEvent::AgentStatus {
            agent_id: agent.id.clone(),
            status: "idle".to_string(),
        })
        .await;
        Ok(())
    }

    pub fn list_agents(&self) -> Result<Vec<AgentRow>> {
        self.db.list_agents()
    }

    pub fn list_all_agents(&self) -> Result<Vec<AgentRow>> {
        self.db.list_all_agents()
    }

    pub fn get_agent(&self, id: &str) -> Result<Option<AgentRow>> {
        self.db.get_agent(id)
    }

    pub fn get_agent_log(
        &self,
        agent_id: &str,
        limit: usize,
        filter: LogFilter,
    ) -> Result<Vec<LogEntry>> {
        self.db.get_agent_log(agent_id, limit, filter)
    }

    pub fn list_events(
        &self,
        since: Option<&str>,
        agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventRow>> {
        self.db.list_events(since, agent_id, limit)
    }

    pub fn stats(&self) -> Result<DbStats> {
        self.db.stats()
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn project_dir(&self) -> &Path {
        &self.project_dir
    }

    pub fn list_agents_with_perspective(&self, perspective_id: &str) -> Result<Vec<AgentView>> {
        self.list_agents_with_perspective_all(perspective_id, false)
    }

    pub fn list_agents_with_perspective_all(
        &self,
        perspective_id: &str,
        include_inactive: bool,
    ) -> Result<Vec<AgentView>> {
        let all = self.db.list_all_agents()?;
        let self_parent: Option<String> = all
            .iter()
            .find(|a| a.id == perspective_id)
            .and_then(|a| a.parent_id.clone());

        fn collect_descendants(
            root: &str,
            agents: &[AgentRow],
            out: &mut std::collections::HashSet<String>,
        ) {
            for a in agents {
                if a.parent_id.as_deref() == Some(root) {
                    out.insert(a.id.clone());
                    collect_descendants(&a.id, agents, out);
                }
            }
        }
        let descendants = {
            let mut set = std::collections::HashSet::new();
            collect_descendants(perspective_id, &all, &mut set);
            set
        };

        let views = all
            .into_iter()
            .filter(|a| include_inactive || Self::active_status(&a.status))
            .filter_map(|a| {
                let relation = if a.id == perspective_id {
                    "self"
                } else if self_parent.as_deref() == Some(a.id.as_str()) {
                    "parent"
                } else if a.parent_id.as_deref() == Some(perspective_id) {
                    "child"
                } else if self_parent.is_some() && a.parent_id == self_parent {
                    "sibling"
                } else if descendants.contains(&a.id) {
                    "descendant"
                } else {
                    return None;
                };
                Some(AgentView {
                    agent: a,
                    relation: relation.to_string(),
                })
            })
            .collect();
        Ok(views)
    }

    pub fn spawn_agent(
        &self,
        role: &str,
        harness_name: &str,
        system_prompt: &str,
        parent_id: Option<&str>,
        comms: &str,
    ) -> Result<AgentRow> {
        self.spawn_agent_with_model(
            role,
            harness_name,
            None,
            system_prompt,
            parent_id,
            comms,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_agent_with_model(
        &self,
        role: &str,
        harness_name: &str,
        model: Option<&str>,
        prompt: &str,
        parent_id: Option<&str>,
        comms: &str,
        use_worktree: bool,
    ) -> Result<AgentRow> {
        Self::validate_role(role)?;

        let _harness = self
            .registry
            .get(harness_name)
            .ok_or_else(|| SwarmError::Internal(format!("unknown harness: {harness_name}")))?;

        let uuid = uuid::Uuid::new_v4().to_string();
        let short_id = &uuid[..8];
        let id = format!("{role}-{short_id}");
        Self::validate_agent_id(&id)?;
        let topic_dir = self.data_dir.join("agents").join(&id);
        std::fs::create_dir_all(&topic_dir)?;
        let worktree_branch = use_worktree.then(|| Self::worktree_branch_name(&id));

        if use_worktree {
            self.create_worktree(&id)?;
        }

        let now = chrono::Utc::now().to_rfc3339();
        let model_str = model.unwrap_or("").to_string();
        let agent = AgentRow {
            id: id.clone(),
            role: role.to_string(),
            harness: harness_name.to_string(),
            model: model_str.clone(),
            status: "idle".to_string(),
            parent_id: parent_id.map(String::from),
            system_prompt: prompt.to_string(),
            work_dir: topic_dir.to_string_lossy().to_string(),
            comms: comms.to_string(),
            created_at: now,
            ended_at: None,
            worktree_branch,
            project_dir: Some(self.project_dir.to_string_lossy().to_string()),
        };

        if let Err(e) = self.db.insert_agent(&agent) {
            if use_worktree {
                if let Err(cleanup_err) = self.cleanup_agent(&id, true) {
                    tracing::error!(
                        "failed to clean up worktree after spawn failure: {cleanup_err}"
                    );
                }
            }
            return Err(e);
        }

        if let Err(e) = self.start_worker_for_agent(&agent, false) {
            if use_worktree {
                if let Err(cleanup_err) = self.cleanup_agent(&id, true) {
                    tracing::error!(
                        "failed to clean up worktree after worker start failure: {cleanup_err}"
                    );
                }
            }
            if let Err(delete_err) = self.db.delete_agent(&id) {
                tracing::error!("failed to delete agent after worker start failure: {delete_err}");
            }
            return Err(e);
        }

        self.emit_event(SwarmEvent::AgentSpawned {
            agent: agent.clone(),
        });

        // Auto-send prompt as first message if non-empty
        if !prompt.is_empty() {
            let from = parent_id.unwrap_or("user").to_string();
            let msg = MessageRow {
                id: uuid::Uuid::new_v4().to_string(),
                from_agent: from.clone(),
                to_agent: id.clone(),
                content: prompt.to_string(),
                delivered: false,
                created_at: chrono::Utc::now().to_rfc3339(),
            };
            self.db.enqueue_message(&msg)?;
            self.notify_worker(&id, WorkerCmd::NewMessage)?;
            self.emit_event(SwarmEvent::MessageRouted { from, to: id });
        }

        Ok(agent)
    }

    fn worktree_branch_name(agent_id: &str) -> String {
        format!("swarm/{agent_id}")
    }

    fn create_worktree(&self, agent_id: &str) -> Result<PathBuf> {
        Self::validate_agent_id(agent_id)?;

        let worktree_dir = self.data_dir.join("worktrees").join(agent_id);
        let branch_name = Self::worktree_branch_name(agent_id);

        std::fs::create_dir_all(self.data_dir.join("worktrees"))?;

        let output = std::process::Command::new("git")
            .args([
                "worktree",
                "add",
                "-b",
                &branch_name,
                &worktree_dir.to_string_lossy(),
            ])
            .current_dir(&self.project_dir)
            .output()
            .map_err(|e| SwarmError::Process(format!("git worktree add failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SwarmError::Process(format!(
                "git worktree add failed: {}",
                stderr.trim()
            )));
        }

        Ok(worktree_dir)
    }

    pub fn worktree_dir(&self, agent_id: &str) -> Result<Option<PathBuf>> {
        Self::validate_agent_id(agent_id)?;

        let dir = self.data_dir.join("worktrees").join(agent_id);
        if dir.exists() {
            Ok(Some(dir))
        } else {
            Ok(None)
        }
    }

    pub fn worktree_info(&self, agent_id: &str) -> Result<Option<WorktreeInfo>> {
        Self::validate_agent_id(agent_id)?;

        if self.db.get_agent(agent_id)?.is_none() {
            return Err(SwarmError::AgentNotFound(agent_id.to_string()));
        }

        let Some(dir) = self.worktree_dir(agent_id)? else {
            return Ok(None);
        };

        let branch = Self::git_output(&dir, &["branch", "--show-current"])?;
        let head = Self::git_output(&dir, &["rev-parse", "HEAD"])?;
        let status = Self::git_output(&dir, &["status", "--porcelain"])?;

        Ok(Some(WorktreeInfo {
            branch,
            head,
            dirty: !status.is_empty(),
            path: dir.to_string_lossy().to_string(),
        }))
    }

    pub fn resume_existing_workers(&self) -> Result<usize> {
        let agents = self.db.list_agents()?;
        let mut resumed = 0;

        for agent in agents {
            if self.workers()?.contains_key(&agent.id) {
                continue;
            }

            if self.registry.get(&agent.harness).is_none() {
                self.db.update_agent_status(&agent.id, "error")?;
                tracing::error!(
                    "cannot resume agent {}: unknown harness {}",
                    agent.id,
                    agent.harness
                );
                continue;
            }

            if !self.start_worker_for_agent(&agent, true)? {
                continue;
            }

            if self.db.has_pending_messages(&agent.id)? {
                self.notify_worker(&agent.id, WorkerCmd::NewMessage)?;
            }

            resumed += 1;
        }

        Ok(resumed)
    }

    pub fn cleanup_agent(&self, agent_id: &str, delete_branch: bool) -> Result<()> {
        Self::validate_agent_id(agent_id)?;

        let worktree_dir = self.data_dir.join("worktrees").join(agent_id);
        if !worktree_dir.exists() {
            return Ok(());
        }

        let output = std::process::Command::new("git")
            .args([
                "worktree",
                "remove",
                "--force",
                &worktree_dir.to_string_lossy(),
            ])
            .current_dir(&self.project_dir)
            .output()
            .map_err(|e| SwarmError::Process(format!("git worktree remove failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SwarmError::Process(format!(
                "git worktree remove failed: {}",
                stderr.trim()
            )));
        }

        if delete_branch {
            let branch_name = self
                .db
                .get_agent(agent_id)?
                .and_then(|agent| agent.worktree_branch)
                .unwrap_or_else(|| Self::worktree_branch_name(agent_id));
            let output = std::process::Command::new("git")
                .args(["branch", "-D", &branch_name])
                .current_dir(&self.project_dir)
                .output()
                .map_err(|e| SwarmError::Process(format!("git branch delete failed: {e}")))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(SwarmError::Process(format!(
                    "git branch delete failed: {}",
                    stderr.trim()
                )));
            }
        }

        Ok(())
    }

    fn event_agent_id(event: &SwarmEvent) -> Option<String> {
        match event {
            SwarmEvent::AgentSpawned { agent } => Some(agent.id.clone()),
            SwarmEvent::AgentDone { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentKilled { agent_id } => Some(agent_id.clone()),
            SwarmEvent::AgentStatus { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentOutput { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentError { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::MessageRouted { .. } => None,
            SwarmEvent::UserNotification { from, .. } => Some(from.clone()),
        }
    }

    fn event_type(event: &SwarmEvent) -> &'static str {
        match event {
            SwarmEvent::AgentSpawned { .. } => "agent_spawned",
            SwarmEvent::AgentDone { .. } => "agent_done",
            SwarmEvent::AgentKilled { .. } => "agent_killed",
            SwarmEvent::AgentStatus { .. } => "agent_status",
            SwarmEvent::AgentOutput { .. } => "agent_output",
            SwarmEvent::AgentError { .. } => "agent_error",
            SwarmEvent::MessageRouted { .. } => "message_routed",
            SwarmEvent::UserNotification { .. } => "user_notification",
        }
    }

    fn emit_event(&self, event: SwarmEvent) {
        let agent_id = Self::event_agent_id(&event);
        let event_type = Self::event_type(&event);
        if let Ok(payload) = serde_json::to_string(&event) {
            if let Err(e) = self.db.insert_event(&EventRow {
                id: uuid::Uuid::new_v4().to_string(),
                event_type: event_type.to_string(),
                agent_id,
                payload,
                created_at: chrono::Utc::now().to_rfc3339(),
            }) {
                tracing::error!("failed to persist event: {e}");
            }
        }
        let _ = self.event_tx.send(event);
    }

    async fn emit_event_async(&self, event: SwarmEvent) {
        let agent_id = Self::event_agent_id(&event);
        let event_type = Self::event_type(&event);
        if let Ok(payload) = serde_json::to_string(&event) {
            let db = self.db.clone();
            let event = EventRow {
                id: uuid::Uuid::new_v4().to_string(),
                event_type: event_type.to_string(),
                agent_id,
                payload,
                created_at: chrono::Utc::now().to_rfc3339(),
            };
            if let Err(e) =
                Self::blocking_db("persist event", move || db.insert_event(&event)).await
            {
                tracing::error!("failed to persist event: {e}");
            }
        }
        let _ = self.event_tx.send(event);
    }

    pub async fn send_message(&self, from: &str, to: &str, content: &str) -> Result<MessageRow> {
        if to == "user" {
            let msg = MessageRow {
                id: uuid::Uuid::new_v4().to_string(),
                from_agent: from.to_string(),
                to_agent: to.to_string(),
                content: content.to_string(),
                delivered: true,
                created_at: chrono::Utc::now().to_rfc3339(),
            };

            let db = self.db.clone();
            let msg_for_insert = msg.clone();
            Self::blocking_db("enqueue user notification", move || {
                db.enqueue_message(&msg_for_insert)
            })
            .await?;
            self.emit_event_async(SwarmEvent::UserNotification {
                from: from.to_string(),
                content: content.to_string(),
            })
            .await;

            return Ok(msg);
        }

        let db = self.db.clone();
        let to_id = to.to_string();
        let agent = Self::blocking_db("get agent", move || db.get_agent(&to_id))
            .await?
            .ok_or_else(|| SwarmError::AgentNotFound(to.to_string()))?;

        if agent.comms == "parent-only" {
            match agent.parent_id.as_deref() {
                Some(parent) if from == parent || from == "user" || from == "system" => {}
                Some(parent) => {
                    return Err(SwarmError::InvalidRequest(format!(
                        "agent {to} only accepts messages from its parent ({parent})"
                    )));
                }
                None if from == "user" || from == "system" => {}
                None => {
                    return Err(SwarmError::InvalidRequest(format!(
                        "agent {to} has parent-only comms but no parent"
                    )));
                }
            }
        }

        if !Self::active_status(&agent.status) {
            self.reactivate_agent_async(&agent).await?;
        }

        if !self.has_worker(to)? {
            return Err(SwarmError::Internal(format!(
                "agent {to} has no running worker"
            )));
        }

        let msg = MessageRow {
            id: uuid::Uuid::new_v4().to_string(),
            from_agent: from.to_string(),
            to_agent: to.to_string(),
            content: content.to_string(),
            delivered: false,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let db = self.db.clone();
        let msg_for_insert = msg.clone();
        if !Self::blocking_db("enqueue message", move || {
            db.enqueue_message_for_active_agent(&msg_for_insert)
        })
        .await?
        {
            return Err(SwarmError::Internal(format!(
                "agent {to} is not accepting messages"
            )));
        }
        self.notify_worker(to, WorkerCmd::NewMessage)?;

        self.emit_event_async(SwarmEvent::MessageRouted {
            from: from.to_string(),
            to: to.to_string(),
        })
        .await;

        Ok(msg)
    }

    pub async fn done_agent(&self, id: &str, message: Option<&str>) -> Result<()> {
        let db = self.db.clone();
        let agent_id = id.to_string();
        let agent = Self::blocking_db("get agent", move || db.get_agent(&agent_id))
            .await?
            .ok_or_else(|| SwarmError::AgentNotFound(id.to_string()))?;

        if !Self::active_status(&agent.status) {
            return Ok(());
        }

        if let Some(msg_content) = message {
            if let Some(ref parent_id) = agent.parent_id {
                let msg = MessageRow {
                    id: uuid::Uuid::new_v4().to_string(),
                    from_agent: id.to_string(),
                    to_agent: parent_id.clone(),
                    content: msg_content.to_string(),
                    delivered: false,
                    created_at: chrono::Utc::now().to_rfc3339(),
                };
                let db = self.db.clone();
                Self::blocking_db("enqueue done message", move || db.enqueue_message(&msg)).await?;
                if let Err(e) = self.notify_worker(parent_id, WorkerCmd::NewMessage) {
                    tracing::error!("failed to notify parent agent {parent_id}: {e}");
                }
                self.emit_event_async(SwarmEvent::MessageRouted {
                    from: id.to_string(),
                    to: parent_id.clone(),
                })
                .await;
            }
        }

        let db = self.db.clone();
        let agent_id = id.to_string();
        Self::blocking_db("mark agent done", move || {
            db.update_agent_status(&agent_id, "done")
        })
        .await?;

        let worker = self.workers()?.remove(id);
        if let Some(worker) = worker {
            if worker.cmd_tx.send(WorkerCmd::Shutdown).is_err() {
                tracing::debug!("agent worker already stopped: {id}");
            }
        }
        self.emit_event_async(SwarmEvent::AgentDone {
            agent_id: id.to_string(),
            message: message.map(String::from),
        })
        .await;
        Ok(())
    }

    pub async fn kill_agent(&self, id: &str) -> Result<()> {
        let db = self.db.clone();
        let agent_id = id.to_string();
        let agent = Self::blocking_db("get agent", move || db.get_agent(&agent_id))
            .await?
            .ok_or_else(|| SwarmError::AgentNotFound(id.to_string()))?;
        if !Self::active_status(&agent.status) {
            return Ok(());
        }

        let db = self.db.clone();
        let agent_id = id.to_string();
        Self::blocking_db("mark agent killed", move || {
            db.update_agent_status(&agent_id, "done")
        })
        .await?;

        let worker = self.workers()?.remove(id);
        if let Some(worker) = worker {
            if worker.cmd_tx.send(WorkerCmd::Shutdown).is_err() {
                tracing::debug!("agent worker already stopped: {id}");
            }
        }
        self.emit_event_async(SwarmEvent::AgentKilled {
            agent_id: id.to_string(),
        })
        .await;
        Ok(())
    }

    pub async fn shutdown_all(&self) -> Result<()> {
        let workers = {
            let mut workers = self.workers()?;
            workers.drain().collect::<Vec<_>>()
        };

        for (agent_id, worker) in workers {
            let db = self.db.clone();
            let status_agent_id = agent_id.clone();
            Self::blocking_db("mark agent done", move || {
                db.update_agent_status(&status_agent_id, "done")
            })
            .await?;
            if worker.cmd_tx.send(WorkerCmd::Shutdown).is_err() {
                tracing::debug!("agent worker already stopped: {agent_id}");
            }
            self.emit_event_async(SwarmEvent::AgentDone {
                agent_id,
                message: None,
            })
            .await;
        }

        Ok(())
    }

    async fn agent_worker_loop(
        config: WorkerConfig,
        mut cmd_rx: mpsc::UnboundedReceiver<WorkerCmd>,
    ) {
        let WorkerConfig {
            db,
            harness,
            agent_id,
            agent_role,
            model,
            topic_dir,
            project_dir,
            swarm_addr,
            event_tx,
            resume_conversation,
        } = config;
        let mut first_message = !resume_conversation;
        let mut has_pending = false;
        let mut was_interrupted = false;

        loop {
            if !has_pending {
                let cmd = match cmd_rx.recv().await {
                    Some(cmd) => cmd,
                    None => break,
                };
                match cmd {
                    WorkerCmd::Shutdown => return,
                    WorkerCmd::NewMessage => {}
                }
            }
            has_pending = false;

            let mut messages = Vec::new();
            loop {
                let dequeue_db = db.clone();
                let dequeue_agent_id = agent_id.clone();
                match Self::blocking_db("dequeue message", move || {
                    dequeue_db.dequeue_message(&dequeue_agent_id)
                })
                .await
                {
                    Ok(Some(msg)) => messages.push(msg),
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!("failed to dequeue message: {e}");
                        break;
                    }
                }
            }
            if messages.is_empty() {
                continue;
            }

            // Drain stale NewMessage notifications for messages we just batch-dequeued
            loop {
                match cmd_rx.try_recv() {
                    Ok(WorkerCmd::NewMessage) => continue,
                    Ok(WorkerCmd::Shutdown) => return,
                    Err(_) => break,
                }
            }

            Self::update_status_if_active_async(
                db.clone(),
                agent_id.clone(),
                "working".to_string(),
            )
            .await;
            let _ = event_tx.send(SwarmEvent::AgentStatus {
                agent_id: agent_id.clone(),
                status: "working".to_string(),
            });

            let is_first = first_message;
            let msg_parts: Vec<String> = messages
                .iter()
                .map(|m| format!("[from: {}]\n{}", m.from_agent, m.content))
                .collect();
            let msg_block = msg_parts.join("\n\n");

            let prompt = if first_message {
                first_message = false;
                format!(
                    "{SWARM_PREAMBLE}\n\nYour agent ID: {agent_id}\nYour role: {agent_role}\nProject directory: {}\n\n{}",
                    project_dir.display(), msg_block
                )
            } else if was_interrupted {
                format!(
                    "[system: your previous response was interrupted by incoming messages]\n\n{}",
                    msg_block
                )
            } else {
                msg_block
            };
            was_interrupted = false;

            let mut env = HashMap::new();
            env.insert("SWARM_AGENT_ID".to_string(), agent_id.clone());
            env.insert("SWARM_SOCKET".to_string(), swarm_addr.clone());
            env.insert(
                "SWARM_PROJECT_DIR".to_string(),
                project_dir.to_string_lossy().to_string(),
            );

            let (tx, mut rx) = mpsc::channel(100);
            let h = harness.clone();
            let td = topic_dir.clone();
            let err_tx = tx.clone();
            let model_ref = model.clone();
            let continue_conv = !is_first;

            let harness_handle = tokio::spawn(async move {
                if let Err(e) = h
                    .run(&prompt, model_ref.as_deref(), continue_conv, &td, env, tx)
                    .await
                {
                    tracing::error!("harness error: {e}");
                    let _ = err_tx
                        .send(HarnessOutput::Error(format!("harness failed: {e}")))
                        .await;
                }
            });

            let mut had_error = false;
            let mut accumulated_output = String::new();
            let mut interrupted = false;
            let run_started = tokio::time::Instant::now();

            loop {
                tokio::select! {
                    biased;
                    output = rx.recv() => {
                        match output {
                            Some(HarnessOutput::Text(text)) => {
                                accumulated_output.push_str(&text);
                                accumulated_output.push('\n');
                                let _ = event_tx.send(SwarmEvent::AgentOutput {
                                    agent_id: agent_id.clone(),
                                    text,
                                });
                            }
                            Some(HarnessOutput::Complete(text)) => {
                                if !text.is_empty() {
                                    let _ = event_tx.send(SwarmEvent::AgentOutput {
                                        agent_id: agent_id.clone(),
                                        text: text.clone(),
                                    });
                                }
                                Self::insert_output_log_async(db.clone(), OutputLogRow {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    agent_id: agent_id.clone(),
                                    content: text,
                                    kind: "output".to_string(),
                                    created_at: chrono::Utc::now().to_rfc3339(),
                                }, "failed to persist output log").await;
                            }
                            Some(HarnessOutput::Error(err)) => {
                                had_error = true;
                                let _ = event_tx.send(SwarmEvent::AgentError {
                                    agent_id: agent_id.clone(),
                                    error: err.clone(),
                                });
                                Self::insert_output_log_async(db.clone(), OutputLogRow {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    agent_id: agent_id.clone(),
                                    content: err,
                                    kind: "error".to_string(),
                                    created_at: chrono::Utc::now().to_rfc3339(),
                                }, "failed to persist error log").await;
                            }
                            Some(HarnessOutput::Timeout(partial)) => {
                                had_error = true;
                                let _ = event_tx.send(SwarmEvent::AgentError {
                                    agent_id: agent_id.clone(),
                                    error: format!(
                                        "timeout, partial output: {} chars",
                                        partial.len()
                                    ),
                                });
                                Self::insert_output_log_async(db.clone(), OutputLogRow {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    agent_id: agent_id.clone(),
                                    content: partial,
                                    kind: "timeout".to_string(),
                                    created_at: chrono::Utc::now().to_rfc3339(),
                                }, "failed to persist timeout log").await;
                            }
                            None => break,
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(WorkerCmd::NewMessage) => {
                                // Async DB boundaries can yield between quick sends; give those
                                // follow-up notifications a moment to batch instead of aborting.
                                if run_started.elapsed() < std::time::Duration::from_millis(25) {
                                    has_pending = true;
                                    continue;
                                }
                                tracing::info!("agent {} interrupted by new message", agent_id);
                                harness_handle.abort();
                                interrupted = true;
                                has_pending = true;
                                break;
                            }
                            Some(WorkerCmd::Shutdown) => {
                                harness_handle.abort();
                                return;
                            }
                            None => {
                                harness_handle.abort();
                                Self::update_status_async(
                                    db.clone(),
                                    agent_id.clone(),
                                    "done".to_string(),
                                )
                                .await;
                                return;
                            }
                        }
                    }
                }
            }

            if interrupted {
                if !accumulated_output.is_empty() {
                    Self::insert_output_log_async(
                        db.clone(),
                        OutputLogRow {
                            id: uuid::Uuid::new_v4().to_string(),
                            agent_id: agent_id.clone(),
                            content: accumulated_output,
                            kind: "interrupted".to_string(),
                            created_at: chrono::Utc::now().to_rfc3339(),
                        },
                        "failed to persist interrupted log",
                    )
                    .await;
                }
                was_interrupted = true;
                continue;
            }

            let next_status = if had_error { "error" } else { "idle" };
            Self::update_status_if_active_async(
                db.clone(),
                agent_id.clone(),
                next_status.to_string(),
            )
            .await;
            let _ = event_tx.send(SwarmEvent::AgentStatus {
                agent_id: agent_id.clone(),
                status: next_status.to_string(),
            });

            let pending_db = db.clone();
            let pending_agent_id = agent_id.clone();
            match Self::blocking_db("check pending messages", move || {
                pending_db.has_pending_messages(&pending_agent_id)
            })
            .await
            {
                Ok(true) => has_pending = true,
                Ok(false) => {}
                Err(e) => tracing::error!("failed to check pending messages: {e}"),
            }
        }

        Self::update_status_async(db.clone(), agent_id.clone(), "done".to_string()).await;
    }
}

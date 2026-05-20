use crate::db::{AgentRow, Db, EventRow, LogEntry, LogFilter, MessageRow, OutputLogRow};
use crate::error::{Result, SwarmError};
use crate::harness::{Harness, HarnessOutput, HarnessRegistry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const SWARM_PREAMBLE: &str = "\
You have access to the `swarm` CLI for multi-agent coordination:

Commands:
  swarm peers                          List visible agents (your parent, siblings, and all descendants) with relation labels.
  swarm peers --all                    Include dead agents.
  swarm send <agent-id> \"message\"      Send a message to another agent.
  swarm spawn --role <name> --harness <cli> --prompt \"instructions...\"
                                       Create a new child agent. Harnesses: claude, gemini, codex, grok, echo.
  swarm spawn --role <name> --harness claude --model claude-sonnet-4-6 --worktree --prompt \"...\"
                                       Spawn with a specific model and an isolated git worktree.
  swarm cleanup <agent-id>             Remove a done/dead agent's git worktree.
  swarm cleanup <agent-id> --delete-branch  Also delete the agent's branch.
  swarm models                         List available models for each harness.
  swarm log <agent-id>                 View an agent's recent activity (messages sent/received and output).
  swarm log <agent-id> -n <count>      Show the last N entries (default: 20).
  swarm log <agent-id> --messages      Show only messages (sent and received).
  swarm log <agent-id> --output        Show only harness output.
  swarm status                         Show your own agent status (includes model info).
  swarm done \"optional final message\"   Signal that you have finished your task. Sends a message to your parent and exits gracefully.
  swarm kill <agent-id>                Terminate an agent. Its children are re-parented to the killed agent's parent.

Communication guidelines:
- When you receive a message, reply to the sender with your result via `swarm send` if a response is expected.
- For long-running work, send brief progress updates to the requestor so they know you are active. Keep updates short - the recipient has a limited context window, and every message you send consumes part of it.
- Do not send unnecessary messages. If you have nothing meaningful to report, stay silent. Silence is better than noise.
- Use `swarm log <agent-id>` to check on agents you have delegated work to, rather than interrupting them with a status request.
- `swarm peers` shows only your parent, siblings, and descendants. Agents outside your family tree are not visible to you.
- When your assigned task is complete, call `swarm done \"summary of what you did\"` to signal completion and report back to your parent. Do not stay idle - finish and exit.
- If you spawn child agents, they will be re-parented to your parent when you finish. You do not need to clean them up unless they are stuck or no longer useful.
- Git worktrees (`--worktree` on spawn) give an agent its own branch and file checkout. Use your judgment:
  - DO use worktrees when multiple agents will edit files in the same compiled project (Rust, Elm, etc.) - the compiler will fail if two agents edit concurrently.
  - DO NOT use worktrees for read-only tasks: reviewing, critiquing, searching, analyzing. Those agents should see the real source.
  - DO NOT use worktrees for a single editing agent when no other agent is editing the same codebase.
  - When in doubt: if the agent's job is to read, no worktree. If it will write and others are also writing, worktree.
- When you finish work in a worktree, `git add` and `git commit` your changes before calling `swarm done`. Uncommitted work in a worktree is invisible to other agents and the coordinator.
- Worktrees are NOT automatically cleaned up. Use `swarm cleanup <agent-id>` only after you have reviewed or merged the agent's work.
- If you are a coordinator and your child agents worked in worktrees, merge their branches into the base branch (usually main) before calling `swarm done`. Use `git merge <branch>` from the main checkout. Resolve conflicts if needed - that is part of your coordination role.";

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentView {
    #[serde(flatten)]
    pub agent: AgentRow,
    pub relation: String,
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
}

pub struct Orchestrator {
    db: Arc<Db>,
    registry: HarnessRegistry,
    workers: Mutex<HashMap<String, AgentWorker>>,
    event_tx: broadcast::Sender<SwarmEvent>,
    addr: String,
    project_dir: PathBuf,
    data_dir: PathBuf,
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
        !matches!(status, "dead" | "done")
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

    fn log_result<T>(context: &str, result: Result<T>) {
        if let Err(e) = result {
            tracing::error!("{context}: {e}");
        }
    }

    fn update_status_if_active(db: &Db, agent_id: &str, status: &str) {
        match db.update_agent_status_if_active(agent_id, status) {
            Ok(_) => {}
            Err(e) => tracing::error!("failed to update agent status: {e}"),
        }
    }

    pub fn list_agents(&self) -> Result<Vec<AgentRow>> {
        self.db.list_agents()
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

    pub fn list_agents_with_perspective(&self, perspective_id: &str) -> Result<Vec<AgentView>> {
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
            .filter(|a| Self::active_status(&a.status))
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

        let harness = self
            .registry
            .get(harness_name)
            .ok_or_else(|| SwarmError::Internal(format!("unknown harness: {harness_name}")))?;

        let uuid = uuid::Uuid::new_v4().to_string();
        let short_id = &uuid[..8];
        let id = format!("{role}-{short_id}");
        Self::validate_agent_id(&id)?;
        let topic_dir = self.data_dir.join("agents").join(&id);
        std::fs::create_dir_all(&topic_dir)?;

        let agent_project_dir = if use_worktree {
            self.create_worktree(&id)?
        } else {
            self.project_dir.clone()
        };

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

        let model_for_worker = if model_str.is_empty() {
            None
        } else {
            Some(model_str)
        };

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(Self::agent_worker_loop(
            WorkerConfig {
                db: self.db.clone(),
                harness,
                agent_id: id.clone(),
                agent_role: role.to_string(),
                model: model_for_worker,
                topic_dir,
                project_dir: agent_project_dir,
                swarm_addr: self.addr.clone(),
                event_tx: self.event_tx.clone(),
            },
            cmd_rx,
        ));

        self.workers()?.insert(
            id.clone(),
            AgentWorker {
                cmd_tx,
                _handle: handle,
            },
        );

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

    fn create_worktree(&self, agent_id: &str) -> Result<PathBuf> {
        Self::validate_agent_id(agent_id)?;

        let worktree_dir = self.data_dir.join("worktrees").join(agent_id);
        let branch_name = format!("swarm/{agent_id}");

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

    pub fn resume_existing_workers(&self) -> Result<usize> {
        let agents = self.db.list_agents()?;
        let mut resumed = 0;

        for agent in agents {
            if self.workers()?.contains_key(&agent.id) {
                continue;
            }

            let Some(harness) = self.registry.get(&agent.harness) else {
                self.db.update_agent_status(&agent.id, "error")?;
                tracing::error!(
                    "cannot resume agent {}: unknown harness {}",
                    agent.id,
                    agent.harness
                );
                continue;
            };

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
                },
                cmd_rx,
            ));

            self.workers()?.insert(
                agent.id.clone(),
                AgentWorker {
                    cmd_tx,
                    _handle: handle,
                },
            );

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
            let branch_name = format!("swarm/{agent_id}");
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

    fn emit_event(&self, event: SwarmEvent) {
        let agent_id = match &event {
            SwarmEvent::AgentSpawned { agent } => Some(agent.id.clone()),
            SwarmEvent::AgentDone { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentKilled { agent_id } => Some(agent_id.clone()),
            SwarmEvent::AgentStatus { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentOutput { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentError { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::MessageRouted { .. } => None,
        };
        let event_type = match &event {
            SwarmEvent::AgentSpawned { .. } => "agent_spawned",
            SwarmEvent::AgentDone { .. } => "agent_done",
            SwarmEvent::AgentKilled { .. } => "agent_killed",
            SwarmEvent::AgentStatus { .. } => "agent_status",
            SwarmEvent::AgentOutput { .. } => "agent_output",
            SwarmEvent::AgentError { .. } => "agent_error",
            SwarmEvent::MessageRouted { .. } => "message_routed",
        };
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

    pub async fn send_message(&self, from: &str, to: &str, content: &str) -> Result<MessageRow> {
        let agent = self
            .db
            .get_agent(to)?
            .ok_or_else(|| SwarmError::AgentNotFound(to.to_string()))?;

        if !Self::active_status(&agent.status) {
            return Err(SwarmError::AgentInactive {
                id: to.to_string(),
                status: agent.status,
            });
        }

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

        if !self.db.enqueue_message_for_active_agent(&msg)? {
            return Err(SwarmError::Internal(format!(
                "agent {to} is not accepting messages"
            )));
        }
        self.notify_worker(to, WorkerCmd::NewMessage)?;

        self.emit_event(SwarmEvent::MessageRouted {
            from: from.to_string(),
            to: to.to_string(),
        });

        Ok(msg)
    }

    pub async fn done_agent(&self, id: &str, message: Option<&str>) -> Result<()> {
        let agent = self
            .db
            .get_agent(id)?
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
                self.db.enqueue_message(&msg)?;
                if let Err(e) = self.notify_worker(parent_id, WorkerCmd::NewMessage) {
                    tracing::error!("failed to notify parent agent {parent_id}: {e}");
                }
                self.emit_event(SwarmEvent::MessageRouted {
                    from: id.to_string(),
                    to: parent_id.clone(),
                });
            }
        }

        self.db.reparent_children(id, agent.parent_id.as_deref())?;
        self.db.update_agent_status(id, "done")?;

        let worker = self.workers()?.remove(id);
        if let Some(worker) = worker {
            if worker.cmd_tx.send(WorkerCmd::Shutdown).is_err() {
                tracing::debug!("agent worker already stopped: {id}");
            }
        }
        self.emit_event(SwarmEvent::AgentDone {
            agent_id: id.to_string(),
            message: message.map(String::from),
        });
        Ok(())
    }

    pub async fn kill_agent(&self, id: &str) -> Result<()> {
        let agent = self
            .db
            .get_agent(id)?
            .ok_or_else(|| SwarmError::AgentNotFound(id.to_string()))?;
        let new_parent = agent.parent_id;
        self.db.reparent_children(id, new_parent.as_deref())?;
        self.db.update_agent_status(id, "dead")?;

        let worker = self.workers()?.remove(id);
        if let Some(worker) = worker {
            if worker.cmd_tx.send(WorkerCmd::Shutdown).is_err() {
                tracing::debug!("agent worker already stopped: {id}");
            }
        }
        self.emit_event(SwarmEvent::AgentKilled {
            agent_id: id.to_string(),
        });
        Ok(())
    }

    pub async fn shutdown_all(&self) -> Result<()> {
        let workers = {
            let mut workers = self.workers()?;
            workers.drain().collect::<Vec<_>>()
        };

        for (agent_id, worker) in workers {
            self.db.update_agent_status(&agent_id, "dead")?;
            if worker.cmd_tx.send(WorkerCmd::Shutdown).is_err() {
                tracing::debug!("agent worker already stopped: {agent_id}");
            }
            self.emit_event(SwarmEvent::AgentKilled { agent_id });
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
        } = config;
        let mut first_message = true;
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
            while let Ok(Some(msg)) = db.dequeue_message(&agent_id) {
                messages.push(msg);
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

            Self::update_status_if_active(&db, &agent_id, "working");
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
                                Self::log_result("failed to persist output log", db.insert_output_log(&OutputLogRow {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    agent_id: agent_id.clone(),
                                    content: text,
                                    kind: "output".to_string(),
                                    created_at: chrono::Utc::now().to_rfc3339(),
                                }));
                            }
                            Some(HarnessOutput::Error(err)) => {
                                had_error = true;
                                let _ = event_tx.send(SwarmEvent::AgentError {
                                    agent_id: agent_id.clone(),
                                    error: err.clone(),
                                });
                                Self::log_result("failed to persist error log", db.insert_output_log(&OutputLogRow {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    agent_id: agent_id.clone(),
                                    content: err,
                                    kind: "error".to_string(),
                                    created_at: chrono::Utc::now().to_rfc3339(),
                                }));
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
                                Self::log_result("failed to persist timeout log", db.insert_output_log(&OutputLogRow {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    agent_id: agent_id.clone(),
                                    content: partial,
                                    kind: "timeout".to_string(),
                                    created_at: chrono::Utc::now().to_rfc3339(),
                                }));
                            }
                            None => break,
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(WorkerCmd::NewMessage) => {
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
                                Self::log_result(
                                    "failed to mark agent dead",
                                    db.update_agent_status(&agent_id, "dead"),
                                );
                                return;
                            }
                        }
                    }
                }
            }

            if interrupted {
                if !accumulated_output.is_empty() {
                    Self::log_result(
                        "failed to persist interrupted log",
                        db.insert_output_log(&OutputLogRow {
                            id: uuid::Uuid::new_v4().to_string(),
                            agent_id: agent_id.clone(),
                            content: accumulated_output,
                            kind: "interrupted".to_string(),
                            created_at: chrono::Utc::now().to_rfc3339(),
                        }),
                    );
                }
                was_interrupted = true;
                continue;
            }

            let next_status = if had_error { "error" } else { "idle" };
            Self::update_status_if_active(&db, &agent_id, next_status);
            let _ = event_tx.send(SwarmEvent::AgentStatus {
                agent_id: agent_id.clone(),
                status: next_status.to_string(),
            });

            match db.has_pending_messages(&agent_id) {
                Ok(true) => has_pending = true,
                Ok(false) => {}
                Err(e) => tracing::error!("failed to check pending messages: {e}"),
            }
        }

        Self::log_result(
            "failed to mark agent dead",
            db.update_agent_status(&agent_id, "dead"),
        );
    }
}

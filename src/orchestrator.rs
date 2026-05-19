use crate::db::{AgentRow, Db, EventRow, LogEntry, LogFilter, MessageRow, OutputLogRow};
use crate::error::{Result, SwarmError};
use crate::harness::{Harness, HarnessOutput, HarnessRegistry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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
- Worktrees are NOT automatically cleaned up. Use `swarm cleanup <agent-id>` only after you have reviewed or merged the agent's work.";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwarmEvent {
    AgentSpawned { agent: AgentRow },
    AgentDone { agent_id: String, message: Option<String> },
    AgentKilled { agent_id: String },
    AgentStatus { agent_id: String, status: String },
    AgentOutput { agent_id: String, text: String },
    AgentError { agent_id: String, error: String },
    MessageRouted { from: String, to: String },
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
    cmd_tx: mpsc::Sender<WorkerCmd>,
    _handle: JoinHandle<()>,
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

    pub fn list_agents_with_perspective(
        &self,
        perspective_id: &str,
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
            .filter(|a| a.status != "dead")
            .filter_map(|a| {
                let relation = if a.id == perspective_id {
                    "self"
                } else if self_parent.as_deref() == Some(a.id.as_str()) {
                    "parent"
                } else if a.parent_id.as_deref() == Some(perspective_id) {
                    "child"
                } else if self_parent.is_some()
                    && a.parent_id == self_parent
                {
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
        self.spawn_agent_with_model(role, harness_name, None, system_prompt, parent_id, comms, false)
    }

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
        let harness = self.registry.get(harness_name).ok_or_else(|| {
            SwarmError::Internal(format!("unknown harness: {harness_name}"))
        })?;

        let short_id = &uuid::Uuid::new_v4().to_string()[..4];
        let id = format!("{role}-{short_id}");
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

        self.db.insert_agent(&agent)?;

        let model_for_worker = if model_str.is_empty() {
            None
        } else {
            Some(model_str)
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let handle = tokio::spawn(Self::agent_worker_loop(
            self.db.clone(),
            harness,
            id.clone(),
            role.to_string(),
            model_for_worker,
            topic_dir,
            agent_project_dir,
            self.addr.clone(),
            cmd_rx,
            self.event_tx.clone(),
        ));

        self.workers.lock().unwrap().insert(
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
            if let Some(worker) = self.workers.lock().unwrap().get(&id) {
                let _ = worker.cmd_tx.try_send(WorkerCmd::NewMessage);
            }
            self.emit_event(SwarmEvent::MessageRouted {
                from,
                to: id,
            });
        }

        Ok(agent)
    }

    fn create_worktree(&self, agent_id: &str) -> Result<PathBuf> {
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

    pub fn worktree_dir(&self, agent_id: &str) -> Option<PathBuf> {
        let dir = self.data_dir.join("worktrees").join(agent_id);
        if dir.exists() {
            Some(dir)
        } else {
            None
        }
    }

    pub fn cleanup_agent(&self, agent_id: &str, delete_branch: bool) -> Result<()> {
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
            std::process::Command::new("git")
                .args(["branch", "-D", &branch_name])
                .current_dir(&self.project_dir)
                .output()
                .ok();
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
            self.db
                .insert_event(&EventRow {
                    id: uuid::Uuid::new_v4().to_string(),
                    event_type: event_type.to_string(),
                    agent_id,
                    payload,
                    created_at: chrono::Utc::now().to_rfc3339(),
                })
                .ok();
        }
        let _ = self.event_tx.send(event);
    }

    pub async fn send_message(&self, from: &str, to: &str, content: &str) -> Result<MessageRow> {
        let agent = self
            .db
            .get_agent(to)?
            .ok_or_else(|| SwarmError::AgentNotFound(to.to_string()))?;

        if agent.comms == "parent-only" {
            if let Some(ref parent) = agent.parent_id {
                if from != parent && from != "user" && from != "system" {
                    return Err(SwarmError::Internal(format!(
                        "agent {to} only accepts messages from its parent ({parent})"
                    )));
                }
            }
        }

        let msg = MessageRow {
            id: uuid::Uuid::new_v4().to_string(),
            from_agent: from.to_string(),
            to_agent: to.to_string(),
            content: content.to_string(),
            delivered: false,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        self.db.enqueue_message(&msg)?;

        let workers = self.workers.lock().unwrap();
        if let Some(worker) = workers.get(to) {
            let _ = worker.cmd_tx.try_send(WorkerCmd::NewMessage);
        }

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
                if let Some(worker) = self.workers.lock().unwrap().get(parent_id) {
                    let _ = worker.cmd_tx.try_send(WorkerCmd::NewMessage);
                }
                self.emit_event(SwarmEvent::MessageRouted {
                    from: id.to_string(),
                    to: parent_id.clone(),
                });
            }
        }

        self.db.reparent_children(id, agent.parent_id.as_deref())?;

        let worker = self.workers.lock().unwrap().remove(id);
        if let Some(worker) = worker {
            let _ = worker.cmd_tx.send(WorkerCmd::Shutdown).await;
            worker._handle.abort();
        }
        self.db.update_agent_status(id, "done")?;
        self.emit_event(SwarmEvent::AgentDone {
            agent_id: id.to_string(),
            message: message.map(String::from),
        });
        Ok(())
    }

    pub async fn kill_agent(&self, id: &str) -> Result<()> {
        let agent = self.db.get_agent(id)?;
        let new_parent = agent.and_then(|a| a.parent_id);
        self.db.reparent_children(id, new_parent.as_deref())?;

        let worker = self.workers.lock().unwrap().remove(id);
        if let Some(worker) = worker {
            let _ = worker.cmd_tx.send(WorkerCmd::Shutdown).await;
            worker._handle.abort();
        }
        self.db.update_agent_status(id, "dead")?;
        self.emit_event(SwarmEvent::AgentKilled {
            agent_id: id.to_string(),
        });
        Ok(())
    }

    async fn agent_worker_loop(
        db: Arc<Db>,
        harness: Arc<dyn Harness>,
        agent_id: String,
        agent_role: String,
        model: Option<String>,
        topic_dir: PathBuf,
        project_dir: PathBuf,
        swarm_addr: String,
        mut cmd_rx: mpsc::Receiver<WorkerCmd>,
        event_tx: broadcast::Sender<SwarmEvent>,
    ) {
        let mut first_message = true;

        loop {
            let cmd = match cmd_rx.recv().await {
                Some(cmd) => cmd,
                None => break,
            };

            match cmd {
                WorkerCmd::Shutdown => break,
                WorkerCmd::NewMessage => {
                    while let Ok(Some(msg)) = db.dequeue_message(&agent_id) {
                        db.update_agent_status(&agent_id, "working").ok();
                        let _ = event_tx.send(SwarmEvent::AgentStatus {
                            agent_id: agent_id.clone(),
                            status: "working".to_string(),
                        });

                        let is_first = first_message;
                        let prompt = if first_message {
                            first_message = false;
                            format!(
                                "{SWARM_PREAMBLE}\n\nYour agent ID: {agent_id}\nYour role: {agent_role}\nProject directory: {}\n\n[from: {}]\n{}",
                                project_dir.display(), msg.from_agent, msg.content
                            )
                        } else {
                            format!("[from: {}]\n{}", msg.from_agent, msg.content)
                        };

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

                        tokio::spawn(async move {
                            if let Err(e) = h
                                .run(&prompt, model_ref.as_deref(), continue_conv, &td, env, tx)
                                .await
                            {
                                tracing::error!("harness error: {e}");
                                let _ = err_tx
                                    .send(HarnessOutput::Error(format!(
                                        "harness failed: {e}"
                                    )))
                                    .await;
                            }
                        });

                        let mut had_error = false;
                        while let Some(output) = rx.recv().await {
                            match output {
                                HarnessOutput::Text(text) => {
                                    let _ = event_tx.send(SwarmEvent::AgentOutput {
                                        agent_id: agent_id.clone(),
                                        text,
                                    });
                                }
                                HarnessOutput::Complete(text) => {
                                    if !text.is_empty() {
                                        let _ = event_tx.send(SwarmEvent::AgentOutput {
                                            agent_id: agent_id.clone(),
                                            text: text.clone(),
                                        });
                                    }
                                    db.insert_output_log(&OutputLogRow {
                                        id: uuid::Uuid::new_v4().to_string(),
                                        agent_id: agent_id.clone(),
                                        content: text,
                                        kind: "output".to_string(),
                                        created_at: chrono::Utc::now().to_rfc3339(),
                                    })
                                    .ok();
                                }
                                HarnessOutput::Error(err) => {
                                    had_error = true;
                                    let _ = event_tx.send(SwarmEvent::AgentError {
                                        agent_id: agent_id.clone(),
                                        error: err.clone(),
                                    });
                                    db.insert_output_log(&OutputLogRow {
                                        id: uuid::Uuid::new_v4().to_string(),
                                        agent_id: agent_id.clone(),
                                        content: err,
                                        kind: "error".to_string(),
                                        created_at: chrono::Utc::now().to_rfc3339(),
                                    })
                                    .ok();
                                }
                                HarnessOutput::Timeout(partial) => {
                                    had_error = true;
                                    let _ = event_tx.send(SwarmEvent::AgentError {
                                        agent_id: agent_id.clone(),
                                        error: format!(
                                            "timeout, partial output: {} chars",
                                            partial.len()
                                        ),
                                    });
                                    db.insert_output_log(&OutputLogRow {
                                        id: uuid::Uuid::new_v4().to_string(),
                                        agent_id: agent_id.clone(),
                                        content: partial,
                                        kind: "timeout".to_string(),
                                        created_at: chrono::Utc::now().to_rfc3339(),
                                    })
                                    .ok();
                                }
                            }
                        }

                        let next_status = if had_error { "error" } else { "idle" };
                        db.update_agent_status(&agent_id, next_status).ok();
                        let _ = event_tx.send(SwarmEvent::AgentStatus {
                            agent_id: agent_id.clone(),
                            status: next_status.to_string(),
                        });

                        // Check for shutdown between messages
                        if let Ok(WorkerCmd::Shutdown) = cmd_rx.try_recv() {
                            return;
                        }
                    }
                }
            }
        }

        db.update_agent_status(&agent_id, "dead").ok();
    }
}

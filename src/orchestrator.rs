use crate::db::{AgentRow, Db, MessageRow};
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
- `swarm peers` - list all agents in the swarm
- `swarm send <agent-id> \"message\"` - send a message to another agent
- `swarm spawn --role <name> --harness <cli> --prompt \"instructions...\"` - create a child agent
- `swarm status` - show your own status
- `swarm kill <agent-id>` - terminate an agent";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwarmEvent {
    AgentSpawned { agent: AgentRow },
    AgentKilled { agent_id: String },
    AgentStatus { agent_id: String, status: String },
    AgentOutput { agent_id: String, text: String },
    MessageRouted { from: String, to: String },
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
    #[allow(dead_code)]
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

    pub fn spawn_agent(
        &self,
        role: &str,
        harness_name: &str,
        system_prompt: &str,
        parent_id: Option<&str>,
        comms: &str,
    ) -> Result<AgentRow> {
        let harness = self.registry.get(harness_name).ok_or_else(|| {
            SwarmError::Internal(format!("unknown harness: {harness_name}"))
        })?;

        let short_id = &uuid::Uuid::new_v4().to_string()[..4];
        let id = format!("{role}-{short_id}");
        let work_dir = self.data_dir.join("agents").join(&id);
        std::fs::create_dir_all(&work_dir)?;

        let now = chrono::Utc::now().to_rfc3339();
        let agent = AgentRow {
            id: id.clone(),
            role: role.to_string(),
            harness: harness_name.to_string(),
            status: "idle".to_string(),
            parent_id: parent_id.map(String::from),
            system_prompt: system_prompt.to_string(),
            work_dir: work_dir.to_string_lossy().to_string(),
            comms: comms.to_string(),
            created_at: now,
        };

        self.db.insert_agent(&agent)?;

        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let handle = tokio::spawn(Self::agent_worker_loop(
            self.db.clone(),
            harness,
            id.clone(),
            role.to_string(),
            system_prompt.to_string(),
            work_dir,
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

        let _ = self.event_tx.send(SwarmEvent::AgentSpawned {
            agent: agent.clone(),
        });

        Ok(agent)
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

        let _ = self.event_tx.send(SwarmEvent::MessageRouted {
            from: from.to_string(),
            to: to.to_string(),
        });

        Ok(msg)
    }

    pub async fn kill_agent(&self, id: &str) -> Result<()> {
        let worker = self.workers.lock().unwrap().remove(id);
        if let Some(worker) = worker {
            let _ = worker.cmd_tx.send(WorkerCmd::Shutdown).await;
            worker._handle.abort();
        }
        self.db.update_agent_status(id, "dead")?;
        let _ = self.event_tx.send(SwarmEvent::AgentKilled {
            agent_id: id.to_string(),
        });
        Ok(())
    }

    async fn agent_worker_loop(
        db: Arc<Db>,
        harness: Arc<dyn Harness>,
        agent_id: String,
        agent_role: String,
        system_prompt: String,
        work_dir: PathBuf,
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

                        let prompt = if first_message {
                            first_message = false;
                            format!(
                                "{SWARM_PREAMBLE}\n\nYour agent ID: {agent_id}\nYour role: {agent_role}\n\n{system_prompt}\n\n[from: {}]\n{}",
                                msg.from_agent, msg.content
                            )
                        } else {
                            format!("[from: {}]\n{}", msg.from_agent, msg.content)
                        };

                        let mut env = HashMap::new();
                        env.insert("SWARM_AGENT_ID".to_string(), agent_id.clone());
                        env.insert("SWARM_SOCKET".to_string(), swarm_addr.clone());

                        let (tx, mut rx) = mpsc::channel(100);
                        let h = harness.clone();
                        let wd = work_dir.clone();

                        tokio::spawn(async move {
                            if let Err(e) = h.run(&prompt, &wd, env, tx).await {
                                tracing::error!("harness error: {e}");
                            }
                        });

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
                                            text,
                                        });
                                    }
                                }
                                HarnessOutput::Error(err) => {
                                    let _ = event_tx.send(SwarmEvent::AgentOutput {
                                        agent_id: agent_id.clone(),
                                        text: format!("[error] {err}"),
                                    });
                                }
                                HarnessOutput::Timeout(partial) => {
                                    let _ = event_tx.send(SwarmEvent::AgentOutput {
                                        agent_id: agent_id.clone(),
                                        text: format!(
                                            "[timeout] partial: {} chars",
                                            partial.len()
                                        ),
                                    });
                                }
                            }
                        }

                        db.update_agent_status(&agent_id, "idle").ok();
                        let _ = event_tx.send(SwarmEvent::AgentStatus {
                            agent_id: agent_id.clone(),
                            status: "idle".to_string(),
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

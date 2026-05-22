use crate::db::{Db, EventRow, LogFilter, MessageRow, OutputLogRow};
use crate::error::{Result, SwarmError};
use crate::harness::{Harness, HarnessOutput, HarnessRegistry};
use crate::types::{
    AgentBrief, AgentBriefSummary, AgentRow, AgentView, BriefLogEntry, CommsMode, DbStats,
    HandoverRow, LogEntry, SqlEnum, SwarmBrief, TopicStatus, WorktreeInfo, USER_TOPIC_ID,
};
pub use crate::types::{DoneReport, StartTopicOptions, SwarmEvent};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const SWARM_PREAMBLE: &str = "\
You are a durable swarm topic running inside an inter-harness coordination session.
Use the `swarm` CLI for all coordination.

Critical delivery rule:
- Send useful work to your parent with `swarm send parent \"message\"`.
- Terminal stdout is only process output; it is not a reliable reply to your parent or the user.
- Use `swarm done \"summary\"` after you have sent the useful result, to pause the topic and hand off status.
- Do not poll for messages in a wait loop. Incoming messages automatically interrupt and resume this topic with the new messages included.
- Use `swarm inbox --all` only for an occasional snapshot/debug check; use `swarm watch-inbox` from outside the running topic to monitor traffic.
- Start child topics with `swarm run --label <label> --harness <harness> \"task\"`; no separate delegation command exists.

Keep swarm messages concise. Send progress only when useful. Run `swarm --help` when command syntax matters.";

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
    topic_label: String,
    parent_id: String,
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
    project_key: String,
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
        let project_key = project_dir.to_string_lossy().to_string();
        if let Err(e) = db.assign_unscoped_agents_to_project(&project_key) {
            tracing::error!("failed to assign legacy agents to project: {e}");
        }
        Self {
            db,
            registry,
            workers: Mutex::new(HashMap::new()),
            event_tx,
            addr,
            project_dir,
            project_key,
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

    fn active_status(status: TopicStatus) -> bool {
        status.is_active()
    }

    fn brief_log_entry(entry: LogEntry) -> BriefLogEntry {
        let content_chars = entry.content.chars().count();
        let mut preview = entry
            .content
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        const PREVIEW_CHARS: usize = 240;
        if preview.chars().count() > PREVIEW_CHARS {
            let mut truncated = preview.chars().take(PREVIEW_CHARS).collect::<String>();
            truncated.push_str("...");
            preview = truncated;
        }

        BriefLogEntry {
            timestamp: entry.timestamp,
            kind: entry.kind,
            peer: entry.peer,
            content_chars,
            preview,
        }
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

    fn validate_label(label: &str) -> Result<()> {
        Self::validate_identifier("label", label)
    }

    fn validate_agent_id(agent_id: &str) -> Result<()> {
        Self::validate_identifier("agent_id", agent_id)
    }

    fn notify_worker(&self, agent_id: &str, cmd: WorkerCmd) -> Result<bool> {
        let workers = self.workers()?;
        if let Some(worker) = workers.get(agent_id) {
            worker.cmd_tx.send(cmd).map_err(|_| {
                SwarmError::Internal(format!("topic worker is stopped: {agent_id}"))
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

    async fn update_status_if_active_async(db: Arc<Db>, agent_id: String, status: TopicStatus) {
        if let Err(e) = Self::blocking_db("update agent status", move || {
            db.update_agent_status_if_active(&agent_id, status)
        })
        .await
        {
            tracing::error!("failed to update agent status: {e}");
        }
    }

    async fn update_status_async(db: Arc<Db>, agent_id: String, status: TopicStatus) {
        if let Err(e) = Self::blocking_db("update agent status", move || {
            db.update_agent_status(&agent_id, status)
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
                topic_label: agent.label.clone(),
                parent_id: agent.parent_or_user().to_string(),
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
        if Self::active_status(agent.status) {
            return Ok(());
        }

        self.start_worker_for_agent(agent, true)?;
        let db = self.db.clone();
        let agent_id = agent.id.clone();
        Self::blocking_db("reactivate agent", move || {
            db.update_agent_status(&agent_id, TopicStatus::Idle)
        })
        .await?;
        self.emit_event_async(SwarmEvent::AgentStatus {
            agent_id: agent.id.clone(),
            status: TopicStatus::Idle,
        })
        .await;
        Ok(())
    }

    pub fn list_agents(&self) -> Result<Vec<AgentRow>> {
        self.db.list_agents_for_project(&self.project_key)
    }

    pub fn list_all_agents(&self) -> Result<Vec<AgentRow>> {
        self.db.list_all_agents_for_project(&self.project_key)
    }

    pub fn get_agent(&self, id: &str) -> Result<Option<AgentRow>> {
        Ok(self
            .db
            .get_agent(id)?
            .filter(|agent| agent.project_dir.as_deref() == Some(self.project_key.as_str())))
    }

    pub fn get_agent_log(
        &self,
        agent_id: &str,
        limit: usize,
        filter: LogFilter,
    ) -> Result<Vec<LogEntry>> {
        if agent_id == "user" {
            return self
                .db
                .search_user_log_for_project(&self.project_key, limit, filter, None);
        }
        self.get_agent(agent_id)?
            .ok_or_else(|| SwarmError::AgentNotFound(agent_id.to_string()))?;
        self.db.get_agent_log(agent_id, limit, filter)
    }

    pub fn search_agent_log(
        &self,
        agent_id: &str,
        limit: usize,
        filter: LogFilter,
        query: Option<&str>,
    ) -> Result<Vec<LogEntry>> {
        if agent_id == "user" {
            return self
                .db
                .search_user_log_for_project(&self.project_key, limit, filter, query);
        }
        self.get_agent(agent_id)?
            .ok_or_else(|| SwarmError::AgentNotFound(agent_id.to_string()))?;
        self.db.search_agent_log(agent_id, limit, filter, query)
    }

    pub fn search_inbox(
        &self,
        target: &str,
        from_agent: Option<&str>,
        limit: usize,
        query: Option<&str>,
    ) -> Result<Vec<LogEntry>> {
        self.search_inbox_scoped(target, from_agent, limit, query, false, None)
    }

    pub fn search_inbox_scoped(
        &self,
        target: &str,
        from_agent: Option<&str>,
        limit: usize,
        query: Option<&str>,
        only_new: bool,
        since: Option<&str>,
    ) -> Result<Vec<LogEntry>> {
        let cursor = if only_new {
            self.db.get_inbox_cursor(target)?
        } else {
            None
        };
        let since = since.or(cursor.as_deref());

        if target == "user" {
            let entries = self.db.search_user_inbox_for_project(
                &self.project_key,
                from_agent,
                limit,
                query,
                since,
            )?;
            if only_new {
                if let Some(last) = entries.last() {
                    self.db.set_inbox_cursor(target, last.timestamp.as_str())?;
                }
            }
            return Ok(entries);
        }

        self.get_agent(target)?
            .ok_or_else(|| SwarmError::AgentNotFound(target.to_string()))?;
        let entries = self
            .db
            .search_agent_inbox(target, from_agent, limit, query, since)?;
        if only_new {
            if let Some(last) = entries.last() {
                self.db.set_inbox_cursor(target, last.timestamp.as_str())?;
            }
        }
        Ok(entries)
    }

    pub fn agent_brief(
        &self,
        agent_id: &str,
        limit: usize,
        query: Option<&str>,
    ) -> Result<AgentBrief> {
        let agent = self
            .get_agent(agent_id)?
            .ok_or_else(|| SwarmError::AgentNotFound(agent_id.to_string()))?;
        let latest_handover = self.db.latest_handover(agent_id)?;
        let recent_log = self
            .db
            .search_agent_log(agent_id, limit, LogFilter::All, query)?
            .into_iter()
            .map(Self::brief_log_entry)
            .collect();

        Ok(AgentBrief {
            id: agent.id,
            label: agent.label,
            harness: agent.harness,
            model: agent.model,
            status: agent.status,
            parent_id: agent.parent_id,
            created_at: agent.created_at,
            ended_at: agent.ended_at,
            worktree_branch: agent.worktree_branch,
            prompt_chars: agent.system_prompt.chars().count(),
            latest_handover,
            recent_log,
        })
    }

    pub fn swarm_brief(&self, limit: usize, query: Option<&str>) -> Result<SwarmBrief> {
        let mut agents = self.list_all_agents()?;
        agents.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        let query_lower = query.map(str::to_lowercase);
        let mut summaries = Vec::new();
        for agent in agents {
            let latest_handover = self.db.latest_handover(&agent.id)?;
            if let Some(query) = query_lower.as_deref() {
                let haystack = format!(
                    "{} {} {} {} {} {} {}",
                    agent.id,
                    agent.label,
                    agent.harness,
                    agent.status,
                    agent.parent_id.as_deref().unwrap_or(""),
                    latest_handover
                        .as_ref()
                        .and_then(|h| h.summary.as_deref())
                        .unwrap_or(""),
                    latest_handover
                        .as_ref()
                        .and_then(|h| h.next_action.as_deref())
                        .unwrap_or("")
                )
                .to_lowercase();
                if !haystack.contains(query) {
                    continue;
                }
            }

            summaries.push(AgentBriefSummary {
                id: agent.id,
                label: agent.label,
                harness: agent.harness,
                status: agent.status,
                parent_id: agent.parent_id,
                created_at: agent.created_at,
                ended_at: agent.ended_at,
                worktree_branch: agent.worktree_branch,
                prompt_chars: agent.system_prompt.chars().count(),
                latest_handover,
            });
            if summaries.len() >= limit {
                break;
            }
        }

        Ok(SwarmBrief {
            stats: self.stats()?,
            agents: summaries,
            recent_handovers: self
                .db
                .latest_handovers_for_project(&self.project_key, limit)?,
        })
    }

    pub fn list_events(
        &self,
        since: Option<&str>,
        agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventRow>> {
        self.db
            .list_events_for_project(&self.project_key, since, agent_id, limit)
    }

    pub fn stats(&self) -> Result<DbStats> {
        self.db.stats_for_project(&self.project_key)
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
        let all = self.list_all_agents()?;
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
            .filter(|a| include_inactive || Self::active_status(a.status))
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

    pub fn start_topic(
        &self,
        label: &str,
        harness_name: &str,
        system_prompt: &str,
        parent_id: Option<&str>,
        comms: &str,
    ) -> Result<AgentRow> {
        self.start_topic_with_model(
            label,
            harness_name,
            None,
            system_prompt,
            parent_id,
            comms,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start_topic_with_model(
        &self,
        label: &str,
        harness_name: &str,
        model: Option<&str>,
        prompt: &str,
        parent_id: Option<&str>,
        comms: &str,
        use_worktree: bool,
    ) -> Result<AgentRow> {
        let comms = CommsMode::from_sql(comms).map_err(SwarmError::InvalidInput)?;
        let user_launched = match parent_id {
            Some(USER_TOPIC_ID) | None => true,
            Some(_) => false,
        };
        self.start_topic_with_options(
            label,
            harness_name,
            prompt,
            StartTopicOptions {
                model,
                parent_id,
                comms,
                use_worktree,
                user_launched,
            },
        )
    }

    pub fn start_topic_with_options(
        &self,
        label: &str,
        harness_name: &str,
        prompt: &str,
        options: StartTopicOptions<'_>,
    ) -> Result<AgentRow> {
        Self::validate_label(label)?;

        let _harness = self
            .registry
            .get(harness_name)
            .ok_or_else(|| SwarmError::Internal(format!("unknown harness: {harness_name}")))?;

        let uuid = uuid::Uuid::new_v4().to_string();
        let short_id = &uuid[..8];
        let id = format!("{label}-{short_id}");
        Self::validate_agent_id(&id)?;
        let topic_dir = self.data_dir.join("agents").join(&id);
        std::fs::create_dir_all(&topic_dir)?;
        let worktree_branch = options
            .use_worktree
            .then(|| Self::worktree_branch_name(&id));

        if options.use_worktree {
            self.create_worktree(&id)?;
        }

        let now = chrono::Utc::now().to_rfc3339();
        let model_str = options.model.unwrap_or("").to_string();
        let parent_id = Some(options.parent_id.unwrap_or(USER_TOPIC_ID).to_string());
        let agent = AgentRow {
            id: id.clone(),
            label: label.to_string(),
            harness: harness_name.to_string(),
            model: model_str.clone(),
            status: TopicStatus::Idle,
            parent_id,
            system_prompt: prompt.to_string(),
            work_dir: topic_dir.to_string_lossy().to_string(),
            comms: options.comms,
            created_at: now,
            ended_at: None,
            worktree_branch,
            project_dir: Some(self.project_dir.to_string_lossy().to_string()),
            user_launched: options.user_launched,
        };

        if let Err(e) = self.db.insert_agent(&agent) {
            if options.use_worktree {
                if let Err(cleanup_err) = self.cleanup_agent(&id, true) {
                    tracing::error!(
                        "failed to clean up worktree after topic start failure: {cleanup_err}"
                    );
                }
            }
            return Err(e);
        }

        if let Err(e) = self.start_worker_for_agent(&agent, false) {
            if options.use_worktree {
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

        self.emit_event(SwarmEvent::TopicStarted {
            agent: agent.clone(),
        });

        // Auto-send prompt as first message if non-empty
        if !prompt.is_empty() {
            let from = agent.parent_or_user().to_string();
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

        if self.get_agent(agent_id)?.is_none() {
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
        let agents = self.list_agents()?;
        let mut resumed = 0;

        for agent in agents {
            if self.workers()?.contains_key(&agent.id) {
                continue;
            }

            if self.registry.get(&agent.harness).is_none() {
                self.db.update_agent_status(&agent.id, TopicStatus::Error)?;
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
        let agent = self.db.get_agent(agent_id)?;
        if let Some(agent) = agent.as_ref() {
            if agent.project_dir.as_deref() != Some(self.project_key.as_str()) {
                return Err(SwarmError::AgentNotFound(agent_id.to_string()));
            }
        }

        if !worktree_dir.exists() {
            if agent.is_some() {
                self.db.clear_agent_worktree_branch(agent_id)?;
            }
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
            let branch_name = agent
                .as_ref()
                .and_then(|agent| agent.worktree_branch.clone())
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

        self.db.clear_agent_worktree_branch(agent_id)?;

        Ok(())
    }

    fn event_agent_id(event: &SwarmEvent) -> Option<String> {
        match event {
            SwarmEvent::TopicStarted { agent } => Some(agent.id.clone()),
            SwarmEvent::AgentDone { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentKilled { agent_id } => Some(agent_id.clone()),
            SwarmEvent::AgentStatus { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentOutput { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::AgentError { agent_id, .. } => Some(agent_id.clone()),
            SwarmEvent::MessageRouted { from, to } => {
                if to == USER_TOPIC_ID {
                    Some(from.clone())
                } else {
                    Some(to.clone())
                }
            }
            SwarmEvent::UserNotification { from, .. } => Some(from.clone()),
        }
    }

    fn event_type(event: &SwarmEvent) -> &'static str {
        match event {
            SwarmEvent::TopicStarted { .. } => "topic_started",
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
        if to == USER_TOPIC_ID {
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
            self.emit_event_async(SwarmEvent::MessageRouted {
                from: from.to_string(),
                to: to.to_string(),
            })
            .await;
            self.emit_event_async(SwarmEvent::UserNotification {
                from: from.to_string(),
                content: content.to_string(),
            })
            .await;

            return Ok(msg);
        }

        let db = self.db.clone();
        let to_id = to.to_string();
        let project_key = self.project_key.clone();
        let agent = Self::blocking_db("get agent", move || db.get_agent(&to_id))
            .await?
            .filter(|agent| agent.project_dir.as_deref() == Some(project_key.as_str()))
            .ok_or_else(|| SwarmError::AgentNotFound(to.to_string()))?;

        if agent.comms == CommsMode::ParentOnly
            && !self.is_immediate_family_sender(from, &agent).await?
        {
            if let Some(parent) = agent.parent_id.as_deref() {
                return Err(SwarmError::InvalidRequest(format!(
                    "topic {to} only accepts messages from user/system, its parent ({parent}), its children, or its siblings"
                )));
            } else {
                return Err(SwarmError::InvalidRequest(format!(
                    "topic {to} has parent-only comms and only accepts messages from user/system or its children"
                )));
            }
        }

        if !Self::active_status(agent.status) {
            self.reactivate_agent_async(&agent).await?;
        }

        if !self.has_worker(to)? {
            return Err(SwarmError::Internal(format!(
                "topic {to} has no running worker"
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
                "topic {to} is not accepting messages"
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

    async fn is_immediate_family_sender(
        &self,
        sender_id: &str,
        recipient: &AgentRow,
    ) -> Result<bool> {
        if sender_id == USER_TOPIC_ID || sender_id == "system" {
            return Ok(true);
        }

        if recipient.parent_id.as_deref() == Some(sender_id) {
            return Ok(true);
        }

        let db = self.db.clone();
        let sender_id = sender_id.to_string();
        let project_key = self.project_key.clone();
        let sender = Self::blocking_db("get message sender", move || db.get_agent(&sender_id))
            .await?
            .filter(|agent| agent.project_dir.as_deref() == Some(project_key.as_str()));
        let Some(sender_parent) = sender.and_then(|agent| agent.parent_id) else {
            return Ok(false);
        };

        if sender_parent == recipient.id {
            return Ok(true);
        }

        Ok(recipient
            .parent_id
            .as_deref()
            .is_some_and(|recipient_parent| sender_parent == recipient_parent))
    }

    pub async fn done_agent(&self, id: &str, message: Option<&str>) -> Result<()> {
        self.done_agent_with_report(
            id,
            DoneReport {
                summary: message.map(str::to_string),
                ..DoneReport::default()
            },
        )
        .await
    }

    pub async fn done_agent_with_report(&self, id: &str, report: DoneReport) -> Result<()> {
        let db = self.db.clone();
        let agent_id = id.to_string();
        let project_key = self.project_key.clone();
        let agent = Self::blocking_db("get agent", move || db.get_agent(&agent_id))
            .await?
            .filter(|agent| agent.project_dir.as_deref() == Some(project_key.as_str()))
            .ok_or_else(|| SwarmError::AgentNotFound(id.to_string()))?;

        if !Self::active_status(agent.status) {
            return Ok(());
        }

        if report.has_content() {
            let handover = HandoverRow {
                id: uuid::Uuid::new_v4().to_string(),
                agent_id: id.to_string(),
                summary: report.summary.clone(),
                outcome: report.outcome.clone(),
                deliverable: report.deliverable.clone(),
                checks: report.checks.clone(),
                risk: report.risk.clone(),
                next_action: report.next_action.clone(),
                created_at: chrono::Utc::now().to_rfc3339(),
            };
            let db = self.db.clone();
            Self::blocking_db("insert handover", move || db.insert_handover(&handover)).await?;
        }

        if let Some(msg_content) = report.summary.as_deref() {
            let parent_id = agent.parent_or_user().to_string();
            self.send_message(id, &parent_id, msg_content).await?;
        }

        let db = self.db.clone();
        let agent_id = id.to_string();
        Self::blocking_db("mark agent done", move || {
            db.update_agent_status(&agent_id, TopicStatus::Paused)
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
            message: report.summary.clone(),
        })
        .await;
        Ok(())
    }

    pub async fn kill_agent(&self, id: &str) -> Result<()> {
        let db = self.db.clone();
        let agent_id = id.to_string();
        let project_key = self.project_key.clone();
        let agent = Self::blocking_db("get agent", move || db.get_agent(&agent_id))
            .await?
            .filter(|agent| agent.project_dir.as_deref() == Some(project_key.as_str()))
            .ok_or_else(|| SwarmError::AgentNotFound(id.to_string()))?;
        if !Self::active_status(agent.status) {
            return Ok(());
        }

        let db = self.db.clone();
        let agent_id = id.to_string();
        Self::blocking_db("mark agent killed", move || {
            db.update_agent_status(&agent_id, TopicStatus::Paused)
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
                db.update_agent_status(&status_agent_id, TopicStatus::Paused)
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
            topic_label,
            parent_id,
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

            Self::update_status_if_active_async(db.clone(), agent_id.clone(), TopicStatus::Working)
                .await;
            let _ = event_tx.send(SwarmEvent::AgentStatus {
                agent_id: agent_id.clone(),
                status: TopicStatus::Working,
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
                    "{SWARM_PREAMBLE}\n\n--- SWARM CONTEXT ---\nTopic ID: {agent_id}\nLabel: {topic_label}\nParent: {parent_id}\nProject directory: {}\n\n--- TASK / INCOMING MESSAGES ---\n{}",
                    project_dir.display(), msg_block
                )
            } else if was_interrupted {
                format!(
                    "[system: your previous response was interrupted by incoming messages]\n\n--- INCOMING MESSAGES ---\n{}",
                    msg_block
                )
            } else {
                format!("--- INCOMING MESSAGES ---\n{msg_block}")
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
                                    TopicStatus::Paused,
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

            let next_status = if had_error {
                TopicStatus::Error
            } else {
                TopicStatus::Idle
            };
            Self::update_status_if_active_async(db.clone(), agent_id.clone(), next_status).await;
            let _ = event_tx.send(SwarmEvent::AgentStatus {
                agent_id: agent_id.clone(),
                status: next_status,
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

        Self::update_status_async(db.clone(), agent_id.clone(), TopicStatus::Paused).await;
    }
}

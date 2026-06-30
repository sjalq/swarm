use crate::error::Result;
pub use crate::types::{
    AgentRow, CommsMode, DbStats, EventRow, HandoverRow, LeasedMessage, LeasedMessageBatch,
    LogEntry, LogFilter, MessageRow, MessageState, OutputLogRow, SqlEnum, TerminalCause,
    TopicStatus,
};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, ToSql};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

const AGENT_SELECT_COLUMNS: &str = "id, label, harness, model, status, parent_id, system_prompt, work_dir, comms, created_at, ended_at, terminal_cause, error_reason, worktree_branch, project_dir, user_launched";

pub struct Db {
    pool: Pool<SqliteConnectionManager>,
    write_lock: Mutex<()>,
}

fn escape_like(query: &str) -> String {
    query
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn query_log_entries(conn: &Connection, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<LogEntry>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt
        .query_map(params, |row| {
            Ok(LogEntry {
                timestamp: row.get(0)?,
                kind: row.get(1)?,
                peer: row.get(2)?,
                content: row.get(3)?,
                broadcast_id: None,
                broadcast_count: None,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    rows.reverse();
    Ok(rows)
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let is_memory = path == Path::new(":memory:");
        if !is_memory {
            let conn = Connection::open(path)?;
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
        }

        let manager = SqliteConnectionManager::file(path)
            .with_init(|conn| conn.execute_batch("PRAGMA busy_timeout=5000;"));
        let mut builder = Pool::builder().max_size(8);
        if is_memory {
            builder = builder.max_size(1);
        }
        let pool = builder.build(manager)?;
        let db = Self {
            pool,
            write_lock: Mutex::new(()),
        };
        db.init_tables()?;
        Ok(db)
    }

    fn conn(&self) -> Result<PooledConnection<SqliteConnectionManager>> {
        Ok(self.pool.get()?)
    }

    fn write_guard(&self) -> Result<MutexGuard<'_, ()>> {
        self.write_lock
            .lock()
            .map_err(|_| crate::error::SwarmError::Internal("database write lock poisoned".into()))
    }

    fn init_tables(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agents (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                harness TEXT NOT NULL,
                model TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'idle',
                parent_id TEXT,
                system_prompt TEXT NOT NULL DEFAULT '',
                work_dir TEXT NOT NULL,
                comms TEXT NOT NULL DEFAULT 'mesh',
                created_at TEXT NOT NULL,
                ended_at TEXT NULL,
                terminal_cause TEXT NULL,
                error_reason TEXT NULL,
                worktree_branch TEXT NULL,
                project_dir TEXT NULL,
                user_launched INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS inbox_cursors (
                recipient TEXT PRIMARY KEY,
                last_seen_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                from_agent TEXT NOT NULL,
                to_agent TEXT NOT NULL,
                content TEXT NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                leased_at TEXT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_messages_pending
                ON messages(to_agent, delivered, leased_at, created_at);
            CREATE TABLE IF NOT EXISTS output_log (
                id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                content TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'output',
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_output_log_agent
                ON output_log(agent_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_messages_from
                ON messages(from_agent, created_at);
            CREATE INDEX IF NOT EXISTS idx_messages_to
                ON messages(to_agent, created_at);
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                agent_id TEXT,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_time
                ON events(created_at);
            CREATE INDEX IF NOT EXISTS idx_events_agent
                ON events(agent_id, created_at);
            CREATE TABLE IF NOT EXISTS handovers (
                id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                summary TEXT,
                outcome TEXT,
                deliverable TEXT,
                checks TEXT,
                risk TEXT,
                next_action TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_handovers_agent
                ON handovers(agent_id, created_at);",
        )?;
        Self::migrate_agent_label_column(&conn)?;
        Self::ensure_agents_column(&conn, "ended_at", "TEXT NULL")?;
        Self::ensure_agents_column(&conn, "terminal_cause", "TEXT NULL")?;
        Self::ensure_agents_column(&conn, "error_reason", "TEXT NULL")?;
        Self::ensure_agents_column(&conn, "worktree_branch", "TEXT NULL")?;
        Self::ensure_agents_column(&conn, "project_dir", "TEXT NULL")?;
        Self::ensure_agents_column(&conn, "user_launched", "INTEGER NOT NULL DEFAULT 0")?;
        Self::migrate_inbox_cursors(&conn)?;
        Self::ensure_messages_broadcast_id(&conn)?;
        Self::ensure_messages_leased_at(&conn)?;
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_agents_status
                ON agents(status, created_at);
            CREATE INDEX IF NOT EXISTS idx_agents_parent_status
                ON agents(parent_id, status);
            CREATE INDEX IF NOT EXISTS idx_agents_project_status
                ON agents(project_dir, status, created_at);
            CREATE INDEX IF NOT EXISTS idx_agents_project_user_created
                ON agents(project_dir, user_launched, created_at);
            DROP INDEX IF EXISTS idx_messages_pending;
            CREATE INDEX IF NOT EXISTS idx_messages_pending
                ON messages(to_agent, delivered, leased_at, created_at);
            DROP INDEX IF EXISTS idx_agents_project_run_status;
            DROP INDEX IF EXISTS idx_runs_project_created;
            DROP TABLE IF EXISTS runs;
            UPDATE agents
                SET status = 'done',
                    ended_at = COALESCE(
                        ended_at,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    ),
                    terminal_cause = COALESCE(terminal_cause, 'startup_gc'),
                    error_reason = NULL
                WHERE status = 'dead';",
        )?;
        Ok(())
    }

    fn agent_column_names(conn: &Connection) -> Result<Vec<String>> {
        let mut stmt = conn.prepare("PRAGMA table_info(agents)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    fn migrate_agent_label_column(conn: &Connection) -> Result<()> {
        let columns = Self::agent_column_names(conn)?;
        let has_label = columns.iter().any(|name| name == "label");
        let has_role = columns.iter().any(|name| name == "role");

        if !has_label && has_role {
            conn.execute_batch("ALTER TABLE agents RENAME COLUMN role TO label;")?;
        } else if !has_label {
            conn.execute(
                "ALTER TABLE agents ADD COLUMN label TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }

        Ok(())
    }

    fn migrate_inbox_cursors(conn: &Connection) -> Result<()> {
        let has_run_id = {
            let mut stmt = conn.prepare("PRAGMA table_info(inbox_cursors)")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            let columns = rows.collect::<std::result::Result<Vec<_>, _>>()?;
            columns.iter().any(|name| name == "run_id")
        };

        if has_run_id {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS inbox_cursors_new (
                    recipient TEXT PRIMARY KEY,
                    last_seen_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                INSERT OR REPLACE INTO inbox_cursors_new (recipient, last_seen_at, updated_at)
                    SELECT c.recipient, c.last_seen_at, c.updated_at
                    FROM inbox_cursors c
                    INNER JOIN (
                        SELECT recipient, MAX(updated_at) AS updated_at
                        FROM inbox_cursors
                        GROUP BY recipient
                    ) latest
                    ON latest.recipient = c.recipient
                    AND latest.updated_at = c.updated_at;
                DROP TABLE inbox_cursors;
                ALTER TABLE inbox_cursors_new RENAME TO inbox_cursors;",
            )?;
        }

        Ok(())
    }

    fn ensure_messages_broadcast_id(conn: &Connection) -> Result<()> {
        let columns = Self::message_column_names(conn)?;
        if !columns.iter().any(|c| c == "broadcast_id") {
            conn.execute_batch(
                "ALTER TABLE messages ADD COLUMN broadcast_id TEXT NULL;
                 CREATE INDEX IF NOT EXISTS idx_messages_broadcast
                     ON messages(broadcast_id);",
            )?;
        }
        Ok(())
    }

    fn ensure_messages_leased_at(conn: &Connection) -> Result<()> {
        let columns = Self::message_column_names(conn)?;
        if !columns.iter().any(|c| c == "leased_at") {
            conn.execute_batch(
                "ALTER TABLE messages ADD COLUMN leased_at TEXT NULL;
                 CREATE INDEX IF NOT EXISTS idx_messages_pending
                     ON messages(to_agent, delivered, leased_at, created_at);",
            )?;
        }
        Ok(())
    }

    fn message_column_names(conn: &Connection) -> Result<Vec<String>> {
        let mut stmt = conn.prepare("PRAGMA table_info(messages)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    fn ensure_agents_column(conn: &Connection, column: &str, definition: &str) -> Result<()> {
        let exists = Self::agent_column_names(conn)?
            .iter()
            .any(|name| name == column);

        if !exists {
            conn.execute(
                &format!("ALTER TABLE agents ADD COLUMN {column} {definition}"),
                [],
            )?;
        }

        Ok(())
    }

    fn agent_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentRow> {
        let status_text: String = row.get(4)?;
        let status = TopicStatus::from_sql(&status_text).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
            )
        })?;
        let comms_text: String = row.get(8)?;
        let comms = CommsMode::from_sql(&comms_text).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
            )
        })?;
        let terminal_cause_text: Option<String> = row.get(11)?;
        let terminal_cause = terminal_cause_text
            .as_deref()
            .map(TerminalCause::from_sql)
            .transpose()
            .map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    11,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })?;
        Ok(AgentRow {
            id: row.get(0)?,
            label: row.get(1)?,
            harness: row.get(2)?,
            model: row.get(3)?,
            status,
            parent_id: row.get(5)?,
            system_prompt: row.get(6)?,
            work_dir: row.get(7)?,
            comms,
            created_at: row.get(9)?,
            ended_at: row.get(10)?,
            terminal_cause,
            error_reason: row.get(12)?,
            worktree_branch: row.get(13)?,
            project_dir: row.get(14)?,
            user_launched: row.get::<_, bool>(15)?,
        })
    }

    pub fn assign_unscoped_agents_to_project(&self, project_dir: &str) -> Result<usize> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        let count = conn.execute(
            "UPDATE agents SET project_dir = ?1 WHERE project_dir IS NULL",
            [project_dir],
        )?;
        Ok(count)
    }

    pub fn insert_agent(&self, agent: &AgentRow) -> Result<()> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO agents (id, label, harness, model, status, parent_id, system_prompt, work_dir, comms, created_at, ended_at, terminal_cause, error_reason, worktree_branch, project_dir, user_launched)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            rusqlite::params![
                agent.id,
                agent.label,
                agent.harness,
                agent.model,
                agent.status.as_sql(),
                agent.parent_id,
                agent.system_prompt,
                agent.work_dir,
                agent.comms.as_sql(),
                agent.created_at,
                agent.ended_at,
                agent.terminal_cause.map(|cause| cause.as_sql()),
                agent.error_reason,
                agent.worktree_branch,
                agent.project_dir,
                agent.user_launched as i32,
            ],
        )?;
        Ok(())
    }

    pub fn get_agent(&self, id: &str) -> Result<Option<AgentRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {AGENT_SELECT_COLUMNS} FROM agents WHERE id = ?1"
        ))?;
        let result = stmt.query_row([id], Self::agent_from_row);
        match result {
            Ok(agent) => Ok(Some(agent)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_agents(&self) -> Result<Vec<AgentRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {AGENT_SELECT_COLUMNS} FROM agents WHERE status != 'done' ORDER BY created_at"
        ))?;
        let agents = stmt
            .query_map([], Self::agent_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(agents)
    }

    pub fn list_agents_for_project(&self, project_dir: &str) -> Result<Vec<AgentRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {AGENT_SELECT_COLUMNS}
             FROM agents WHERE status != 'done' AND project_dir = ?1 ORDER BY created_at"
        ))?;
        let agents = stmt
            .query_map([project_dir], Self::agent_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(agents)
    }

    pub fn list_all_agents(&self) -> Result<Vec<AgentRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {AGENT_SELECT_COLUMNS} FROM agents ORDER BY created_at"
        ))?;
        let agents = stmt
            .query_map([], Self::agent_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(agents)
    }

    pub fn list_all_agents_for_project(&self, project_dir: &str) -> Result<Vec<AgentRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {AGENT_SELECT_COLUMNS} FROM agents WHERE project_dir = ?1 ORDER BY created_at"
        ))?;
        let agents = stmt
            .query_map([project_dir], Self::agent_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(agents)
    }

    pub fn insert_event(&self, event: &EventRow) -> Result<()> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO events (id, event_type, agent_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                event.id,
                event.event_type,
                event.agent_id,
                event.payload,
                event.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn list_events(
        &self,
        since: Option<&str>,
        agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventRow>> {
        let conn = self.conn()?;
        let (sql, params) = match (since, agent_id) {
            (Some(since), Some(aid)) => (
                "SELECT id, event_type, agent_id, payload, created_at
                 FROM events WHERE created_at >= ?1 AND agent_id = ?2
                 ORDER BY created_at ASC LIMIT ?3",
                vec![since.to_string(), aid.to_string(), limit.to_string()],
            ),
            (Some(since), None) => (
                "SELECT id, event_type, agent_id, payload, created_at
                 FROM events WHERE created_at >= ?1
                 ORDER BY created_at ASC LIMIT ?2",
                vec![since.to_string(), limit.to_string()],
            ),
            (None, Some(aid)) => (
                "SELECT id, event_type, agent_id, payload, created_at
                 FROM events WHERE agent_id = ?1
                 ORDER BY created_at ASC LIMIT ?2",
                vec![aid.to_string(), limit.to_string()],
            ),
            (None, None) => (
                "SELECT id, event_type, agent_id, payload, created_at
                 FROM events ORDER BY created_at ASC LIMIT ?1",
                vec![limit.to_string()],
            ),
        };
        let mut stmt = conn.prepare(sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params
            .iter()
            .map(|s| s as &dyn rusqlite::types::ToSql)
            .collect();
        let events = stmt
            .query_map(params_refs.as_slice(), |row| {
                Ok(EventRow {
                    id: row.get(0)?,
                    event_type: row.get(1)?,
                    agent_id: row.get(2)?,
                    payload: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(events)
    }

    pub fn list_events_for_project(
        &self,
        project_dir: &str,
        since: Option<&str>,
        agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventRow>> {
        let conn = self.conn()?;
        let (sql, params) = match (since, agent_id) {
            (Some(since), Some(aid)) => (
                "SELECT id, event_type, agent_id, payload, created_at
                 FROM events
                 WHERE created_at >= ?1
                   AND agent_id = ?2
                   AND agent_id IN (SELECT id FROM agents WHERE project_dir = ?3)
                 ORDER BY created_at ASC LIMIT ?4",
                vec![
                    since.to_string(),
                    aid.to_string(),
                    project_dir.to_string(),
                    limit.to_string(),
                ],
            ),
            (Some(since), None) => (
                "SELECT id, event_type, agent_id, payload, created_at
                 FROM events
                 WHERE created_at >= ?1
                   AND agent_id IN (SELECT id FROM agents WHERE project_dir = ?2)
                 ORDER BY created_at ASC LIMIT ?3",
                vec![
                    since.to_string(),
                    project_dir.to_string(),
                    limit.to_string(),
                ],
            ),
            (None, Some(aid)) => (
                "SELECT id, event_type, agent_id, payload, created_at
                 FROM events
                 WHERE agent_id = ?1
                   AND agent_id IN (SELECT id FROM agents WHERE project_dir = ?2)
                 ORDER BY created_at ASC LIMIT ?3",
                vec![aid.to_string(), project_dir.to_string(), limit.to_string()],
            ),
            (None, None) => (
                "SELECT id, event_type, agent_id, payload, created_at
                 FROM events
                 WHERE agent_id IN (SELECT id FROM agents WHERE project_dir = ?1)
                 ORDER BY created_at ASC LIMIT ?2",
                vec![project_dir.to_string(), limit.to_string()],
            ),
        };
        let mut stmt = conn.prepare(sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params
            .iter()
            .map(|s| s as &dyn rusqlite::types::ToSql)
            .collect();
        let events = stmt
            .query_map(params_refs.as_slice(), |row| {
                Ok(EventRow {
                    id: row.get(0)?,
                    event_type: row.get(1)?,
                    agent_id: row.get(2)?,
                    payload: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(events)
    }

    pub fn list_recent_events_for_project(
        &self,
        project_dir: &str,
        limit: usize,
    ) -> Result<Vec<EventRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, event_type, agent_id, payload, created_at
             FROM events
             WHERE agent_id IN (SELECT id FROM agents WHERE project_dir = ?1)
             ORDER BY created_at DESC LIMIT ?2",
        )?;
        let mut events = stmt
            .query_map(rusqlite::params![project_dir, limit as i64], |row| {
                Ok(EventRow {
                    id: row.get(0)?,
                    event_type: row.get(1)?,
                    agent_id: row.get(2)?,
                    payload: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        events.reverse();
        Ok(events)
    }

    pub fn reparent_children(&self, old_parent: &str, new_parent: Option<&str>) -> Result<usize> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        let count = conn.execute(
            "UPDATE agents SET parent_id = ?1 WHERE parent_id = ?2 AND status != 'done'",
            rusqlite::params![new_parent, old_parent],
        )?;
        Ok(count)
    }

    pub fn update_agent_status(&self, id: &str, status: TopicStatus) -> Result<()> {
        self.update_agent_status_with_details(
            id,
            status,
            Self::default_terminal_cause(status),
            None,
        )
        .map(|_| ())
    }

    pub fn update_agent_status_error(&self, id: &str, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        self.update_agent_status_with_details(id, TopicStatus::Error, None, Some(reason.as_str()))
            .map(|_| ())
    }

    pub fn update_agent_status_terminal(&self, id: &str, cause: TerminalCause) -> Result<()> {
        self.update_agent_status_with_details(id, TopicStatus::Paused, Some(cause), None)
            .map(|_| ())
    }

    fn default_terminal_cause(status: TopicStatus) -> Option<TerminalCause> {
        (!status.is_active()).then_some(TerminalCause::LoopExit)
    }

    fn update_agent_status_with_details(
        &self,
        id: &str,
        status: TopicStatus,
        terminal_cause: Option<TerminalCause>,
        error_reason: Option<&str>,
    ) -> Result<bool> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        let status_text = status.as_sql();
        let ended_at = (!status.is_active()).then(|| chrono::Utc::now().to_rfc3339());
        let terminal_cause = terminal_cause.map(|cause| cause.as_sql());
        let updated = conn.execute(
            "UPDATE agents
                SET status = ?1,
                    ended_at = CASE
                        WHEN ?1 = 'done' THEN COALESCE(ended_at, ?3)
                        ELSE NULL
                    END,
                    terminal_cause = CASE
                        WHEN ?1 = 'done' THEN ?4
                        ELSE NULL
                    END,
                    error_reason = CASE
                        WHEN ?1 = 'error' THEN ?5
                        ELSE NULL
                    END
                WHERE id = ?2",
            rusqlite::params![status_text, id, ended_at, terminal_cause, error_reason],
        )?;
        Ok(updated > 0)
    }

    pub fn update_agent_status_if_active(&self, id: &str, status: TopicStatus) -> Result<bool> {
        self.update_agent_status_if_active_with_details(
            id,
            status,
            Self::default_terminal_cause(status),
            None,
        )
    }

    pub fn update_agent_status_if_active_error(
        &self,
        id: &str,
        reason: impl Into<String>,
    ) -> Result<bool> {
        let reason = reason.into();
        self.update_agent_status_if_active_with_details(
            id,
            TopicStatus::Error,
            None,
            Some(reason.as_str()),
        )
    }

    pub fn update_agent_status_if_active_terminal(
        &self,
        id: &str,
        cause: TerminalCause,
    ) -> Result<bool> {
        self.update_agent_status_if_active_with_details(id, TopicStatus::Paused, Some(cause), None)
    }

    fn update_agent_status_if_active_with_details(
        &self,
        id: &str,
        status: TopicStatus,
        terminal_cause: Option<TerminalCause>,
        error_reason: Option<&str>,
    ) -> Result<bool> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        let status_text = status.as_sql();
        let ended_at = (!status.is_active()).then(|| chrono::Utc::now().to_rfc3339());
        let terminal_cause = terminal_cause.map(|cause| cause.as_sql());
        let updated = conn.execute(
            "UPDATE agents
                SET status = ?1,
                    ended_at = CASE
                        WHEN ?1 = 'done' THEN COALESCE(ended_at, ?3)
                        ELSE NULL
                    END,
                    terminal_cause = CASE
                        WHEN ?1 = 'done' THEN ?4
                        ELSE NULL
                    END,
                    error_reason = CASE
                        WHEN ?1 = 'error' THEN ?5
                        ELSE NULL
                    END
                WHERE id = ?2 AND status != 'done'",
            rusqlite::params![status_text, id, ended_at, terminal_cause, error_reason],
        )?;
        Ok(updated > 0)
    }

    pub fn clear_agent_worktree_branch(&self, id: &str) -> Result<()> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        conn.execute(
            "UPDATE agents SET worktree_branch = NULL WHERE id = ?1",
            [id],
        )?;
        Ok(())
    }

    pub fn delete_agent(&self, id: &str) -> Result<()> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        conn.execute("DELETE FROM agents WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn enqueue_message(&self, msg: &MessageRow) -> Result<()> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO messages (id, from_agent, to_agent, content, delivered, created_at, broadcast_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                msg.id,
                msg.from_agent,
                msg.to_agent,
                msg.content,
                msg.state.delivered_sql(),
                msg.created_at,
                msg.broadcast_id,
            ],
        )?;
        Ok(())
    }

    pub fn enqueue_message_for_active_agent(&self, msg: &MessageRow) -> Result<bool> {
        let _write = self.write_guard()?;
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let is_active: bool = tx.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM agents
                WHERE id = ?1 AND status != 'done'
            )",
            [&msg.to_agent],
            |row| row.get(0),
        )?;

        if !is_active {
            return Ok(false);
        }

        tx.execute(
            "INSERT INTO messages (id, from_agent, to_agent, content, delivered, created_at, broadcast_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                msg.id,
                msg.from_agent,
                msg.to_agent,
                msg.content,
                msg.state.delivered_sql(),
                msg.created_at,
                msg.broadcast_id,
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn insert_output_log(&self, entry: &OutputLogRow) -> Result<()> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO output_log (id, agent_id, content, kind, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                entry.id,
                entry.agent_id,
                entry.content,
                entry.kind,
                entry.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn insert_handover(&self, handover: &HandoverRow) -> Result<()> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO handovers (
                id, agent_id, summary, outcome, deliverable, checks, risk, next_action, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                handover.id,
                handover.agent_id,
                handover.summary,
                handover.outcome,
                handover.deliverable,
                handover.checks,
                handover.risk,
                handover.next_action,
                handover.created_at,
            ],
        )?;
        Ok(())
    }

    fn handover_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<HandoverRow> {
        Ok(HandoverRow {
            id: row.get(0)?,
            agent_id: row.get(1)?,
            summary: row.get(2)?,
            outcome: row.get(3)?,
            deliverable: row.get(4)?,
            checks: row.get(5)?,
            risk: row.get(6)?,
            next_action: row.get(7)?,
            created_at: row.get(8)?,
        })
    }

    pub fn latest_handover(&self, agent_id: &str) -> Result<Option<HandoverRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, agent_id, summary, outcome, deliverable, checks, risk, next_action, created_at
             FROM handovers WHERE agent_id = ?1 ORDER BY created_at DESC LIMIT 1",
        )?;
        let result = stmt.query_row([agent_id], Self::handover_from_row);
        match result {
            Ok(handover) => Ok(Some(handover)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn latest_handovers(&self, limit: usize) -> Result<Vec<HandoverRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, agent_id, summary, outcome, deliverable, checks, risk, next_action, created_at
             FROM handovers ORDER BY created_at DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map([limit], Self::handover_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn latest_handovers_for_project(
        &self,
        project_dir: &str,
        limit: usize,
    ) -> Result<Vec<HandoverRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT h.id, h.agent_id, h.summary, h.outcome, h.deliverable, h.checks, h.risk, h.next_action, h.created_at
             FROM handovers h
             INNER JOIN agents a ON a.id = h.agent_id
             WHERE a.project_dir = ?1
             ORDER BY h.created_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(
                rusqlite::params![project_dir, limit],
                Self::handover_from_row,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_agent_log(
        &self,
        agent_id: &str,
        limit: usize,
        filter: LogFilter,
    ) -> Result<Vec<LogEntry>> {
        self.search_agent_log(agent_id, limit, filter, None)
    }

    pub fn search_agent_log(
        &self,
        agent_id: &str,
        limit: usize,
        filter: LogFilter,
        query: Option<&str>,
    ) -> Result<Vec<LogEntry>> {
        let conn = self.conn()?;

        let sent_subquery = "SELECT MIN(created_at) AS timestamp, 'sent' AS kind, \
                 MIN(to_agent) AS peer, content, broadcast_id, COUNT(*) AS broadcast_count \
             FROM messages WHERE from_agent = ?1 \
             GROUP BY COALESCE(broadcast_id, id), content";

        let base = match filter {
            LogFilter::All => {
                format!(
                    "SELECT timestamp, kind, peer, content, broadcast_id, broadcast_count FROM (
                        SELECT created_at AS timestamp, 'recv' AS kind, from_agent AS peer,
                            content, broadcast_id, 1 AS broadcast_count
                        FROM messages WHERE to_agent = ?1
                        UNION ALL
                        {sent_subquery}
                        UNION ALL
                        SELECT created_at AS timestamp, kind, '' AS peer, content,
                            NULL AS broadcast_id, NULL AS broadcast_count
                        FROM output_log WHERE agent_id = ?1
                    )"
                )
            }
            LogFilter::Messages => {
                format!(
                    "SELECT timestamp, kind, peer, content, broadcast_id, broadcast_count FROM (
                        SELECT created_at AS timestamp, 'recv' AS kind, from_agent AS peer,
                            content, broadcast_id, 1 AS broadcast_count
                        FROM messages WHERE to_agent = ?1
                        UNION ALL
                        {sent_subquery}
                    )"
                )
            }
            LogFilter::Output => {
                "SELECT timestamp, kind, peer, content, NULL AS broadcast_id, NULL AS broadcast_count FROM (
                    SELECT created_at AS timestamp, kind, '' AS peer, content
                    FROM output_log WHERE agent_id = ?1
                )".to_string()
            }
        };

        let mut sql = base;
        let pattern = query.map(|q| format!("%{}%", escape_like(q)));
        if pattern.is_some() {
            sql.push_str(" WHERE LOWER(content) LIKE LOWER(?2) ESCAPE '\\'");
            sql.push_str(" ORDER BY timestamp DESC LIMIT ?3");
        } else {
            sql.push_str(" ORDER BY timestamp DESC LIMIT ?2");
        }

        let mut stmt = conn.prepare(&sql)?;
        let map_row = |row: &rusqlite::Row<'_>| {
            let bc: Option<i64> = row.get(5)?;
            Ok(LogEntry {
                timestamp: row.get(0)?,
                kind: row.get(1)?,
                peer: row.get(2)?,
                content: row.get(3)?,
                broadcast_id: row.get(4)?,
                broadcast_count: bc.map(|n| n as usize),
            })
        };
        let mut rows = if let Some(pattern) = pattern {
            stmt.query_map(rusqlite::params![agent_id, pattern, limit], map_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(rusqlite::params![agent_id, limit], map_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        rows.reverse();
        Ok(rows)
    }

    pub fn search_user_log_for_project(
        &self,
        project_dir: &str,
        limit: usize,
        filter: LogFilter,
        query: Option<&str>,
    ) -> Result<Vec<LogEntry>> {
        if matches!(filter, LogFilter::Output) {
            return Ok(Vec::new());
        }

        let conn = self.conn()?;
        let base = "SELECT timestamp, kind, peer, content, broadcast_id FROM (
            SELECT m.created_at AS timestamp, 'recv' AS kind, m.from_agent AS peer,
                m.content, m.broadcast_id
            FROM messages m
            INNER JOIN agents a ON a.id = m.from_agent
            WHERE m.to_agent = 'user' AND a.project_dir = ?1
            UNION ALL
            SELECT m.created_at AS timestamp, 'sent' AS kind, m.to_agent AS peer,
                m.content, m.broadcast_id
            FROM messages m
            INNER JOIN agents a ON a.id = m.to_agent
            WHERE m.from_agent = 'user' AND a.project_dir = ?1
        )";

        let mut sql = base.to_string();
        let pattern = query.map(|q| format!("%{}%", escape_like(q)));
        if pattern.is_some() {
            sql.push_str(" WHERE LOWER(content) LIKE LOWER(?2) ESCAPE '\\'");
            sql.push_str(" ORDER BY timestamp DESC LIMIT ?3");
        } else {
            sql.push_str(" ORDER BY timestamp DESC LIMIT ?2");
        }

        let mut stmt = conn.prepare(&sql)?;
        let map_row = |row: &rusqlite::Row<'_>| {
            Ok(LogEntry {
                timestamp: row.get(0)?,
                kind: row.get(1)?,
                peer: row.get(2)?,
                content: row.get(3)?,
                broadcast_id: row.get(4)?,
                broadcast_count: None,
            })
        };
        let mut rows = if let Some(pattern) = pattern {
            stmt.query_map(rusqlite::params![project_dir, pattern, limit], map_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(rusqlite::params![project_dir, limit], map_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        rows.reverse();
        Ok(rows)
    }

    pub fn search_agent_inbox(
        &self,
        agent_id: &str,
        from_agent: Option<&str>,
        limit: usize,
        query: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<LogEntry>> {
        let conn = self.conn()?;
        let mut sql = String::from(
            "SELECT created_at AS timestamp, 'recv' AS kind, from_agent AS peer, content
             FROM messages WHERE to_agent = ?1",
        );
        let agent_id = agent_id.to_string();
        let from_agent = from_agent.map(str::to_string);
        let pattern = query.map(|q| format!("%{}%", escape_like(q)));
        let since = since.map(str::to_string);
        let limit = limit as i64;
        let mut next_param = 2;

        if from_agent.is_some() {
            sql.push_str(&format!(" AND from_agent = ?{next_param}"));
            next_param += 1;
        }
        if since.is_some() {
            sql.push_str(&format!(" AND created_at > ?{next_param}"));
            next_param += 1;
        }
        if pattern.is_some() {
            sql.push_str(&format!(
                " AND LOWER(content) LIKE LOWER(?{next_param}) ESCAPE '\\'"
            ));
            next_param += 1;
        }
        sql.push_str(&format!(" ORDER BY created_at DESC LIMIT ?{next_param}"));

        let mut params: Vec<&dyn ToSql> = vec![&agent_id];
        if let Some(from_agent) = from_agent.as_ref() {
            params.push(from_agent);
        }
        if let Some(since) = since.as_ref() {
            params.push(since);
        }
        if let Some(pattern) = pattern.as_ref() {
            params.push(pattern);
        }
        params.push(&limit);
        query_log_entries(&conn, &sql, &params)
    }

    pub fn search_user_inbox_for_project(
        &self,
        project_dir: &str,
        from_agent: Option<&str>,
        limit: usize,
        query: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<LogEntry>> {
        let conn = self.conn()?;
        let mut sql = String::from(
            "SELECT m.created_at AS timestamp, 'recv' AS kind, m.from_agent AS peer, m.content
             FROM messages m
             INNER JOIN agents a ON a.id = m.from_agent
             WHERE m.to_agent = 'user' AND a.project_dir = ?1",
        );
        let project_dir = project_dir.to_string();
        let from_agent = from_agent.map(str::to_string);
        let pattern = query.map(|q| format!("%{}%", escape_like(q)));
        let since = since.map(str::to_string);
        let limit = limit as i64;
        let mut next_param = 2;

        if from_agent.is_some() {
            sql.push_str(&format!(" AND m.from_agent = ?{next_param}"));
            next_param += 1;
        }
        if since.is_some() {
            sql.push_str(&format!(" AND m.created_at > ?{next_param}"));
            next_param += 1;
        }
        if pattern.is_some() {
            sql.push_str(&format!(
                " AND LOWER(m.content) LIKE LOWER(?{next_param}) ESCAPE '\\'"
            ));
            next_param += 1;
        }
        sql.push_str(&format!(" ORDER BY m.created_at DESC LIMIT ?{next_param}"));

        let mut params: Vec<&dyn ToSql> = vec![&project_dir];
        if let Some(from_agent) = from_agent.as_ref() {
            params.push(from_agent);
        }
        if let Some(since) = since.as_ref() {
            params.push(since);
        }
        if let Some(pattern) = pattern.as_ref() {
            params.push(pattern);
        }
        params.push(&limit);
        query_log_entries(&conn, &sql, &params)
    }

    pub fn get_inbox_cursor(&self, recipient: &str) -> Result<Option<String>> {
        let conn = self.conn()?;
        let result = conn.query_row(
            "SELECT last_seen_at FROM inbox_cursors WHERE recipient = ?1",
            rusqlite::params![recipient],
            |row| row.get(0),
        );
        match result {
            Ok(timestamp) => Ok(Some(timestamp)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_inbox_cursor(&self, recipient: &str, last_seen_at: &str) -> Result<()> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        let updated_at = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO inbox_cursors (recipient, last_seen_at, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(recipient)
             DO UPDATE SET last_seen_at = excluded.last_seen_at,
                           updated_at = excluded.updated_at",
            rusqlite::params![recipient, last_seen_at, updated_at],
        )?;
        Ok(())
    }

    pub fn dequeue_message_batch(&self, agent_id: &str) -> Result<LeasedMessageBatch> {
        let mut messages = Vec::new();
        while let Some(message) = self.dequeue_message(agent_id)? {
            messages.push(message);
        }
        Ok(LeasedMessageBatch::new(messages))
    }

    pub fn dequeue_message(&self, agent_id: &str) -> Result<Option<LeasedMessage>> {
        let _write = self.write_guard()?;
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let leased_at = chrono::Utc::now().to_rfc3339();
        let result = {
            let mut stmt = tx.prepare(
                "SELECT id, from_agent, to_agent, content, delivered, created_at, broadcast_id
                 FROM messages
                 WHERE to_agent = ?1 AND delivered = 0 AND leased_at IS NULL
                 ORDER BY created_at LIMIT 1",
            )?;
            stmt.query_row([agent_id], |row| {
                Ok(MessageRow {
                    id: row.get(0)?,
                    from_agent: row.get(1)?,
                    to_agent: row.get(2)?,
                    content: row.get(3)?,
                    state: if row.get::<_, i32>(4)? != 0 {
                        MessageState::delivered()
                    } else {
                        MessageState::pending()
                    },
                    created_at: row.get(5)?,
                    broadcast_id: row.get(6)?,
                })
            })
        };
        match result {
            Ok(msg) => {
                let claimed = tx.execute(
                    "UPDATE messages SET leased_at = ?1
                     WHERE id = ?2 AND delivered = 0 AND leased_at IS NULL",
                    rusqlite::params![leased_at, &msg.id],
                )?;
                if claimed == 0 {
                    tx.commit()?;
                    return Ok(None);
                }
                tx.commit()?;
                Ok(Some(LeasedMessage {
                    id: msg.id,
                    from_agent: msg.from_agent,
                    to_agent: msg.to_agent,
                    content: msg.content,
                    created_at: msg.created_at,
                    leased_at,
                    broadcast_id: msg.broadcast_id,
                }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn mark_messages_delivered(&self, messages: &LeasedMessageBatch) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        let _write = self.write_guard()?;
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let mut updated = 0;
        for message in messages.iter() {
            updated += tx.execute(
                "UPDATE messages
                 SET delivered = 1, leased_at = NULL
                 WHERE id = ?1 AND delivered = 0 AND leased_at = ?2",
                rusqlite::params![message.id(), message.leased_at],
            )?;
        }
        tx.commit()?;
        Ok(updated)
    }

    pub fn release_messages(&self, messages: &LeasedMessageBatch) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        let _write = self.write_guard()?;
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let mut updated = 0;
        for message in messages.iter() {
            updated += tx.execute(
                "UPDATE messages
                 SET leased_at = NULL
                 WHERE id = ?1 AND delivered = 0 AND leased_at = ?2",
                rusqlite::params![message.id(), message.leased_at],
            )?;
        }
        tx.commit()?;
        Ok(updated)
    }

    pub fn release_inflight_messages_for_project(&self, project_dir: &str) -> Result<usize> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        let count = conn.execute(
            "UPDATE messages
             SET leased_at = NULL
             WHERE delivered = 0
               AND leased_at IS NOT NULL
               AND to_agent IN (
                   SELECT id FROM agents WHERE project_dir = ?1
               )",
            [project_dir],
        )?;
        Ok(count)
    }

    pub fn has_pending_messages(&self, agent_id: &str) -> Result<bool> {
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM messages
             WHERE to_agent = ?1 AND delivered = 0 AND leased_at IS NULL",
            [agent_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn mark_unfinished_agents_done(&self) -> Result<usize> {
        let _write = self.write_guard()?;
        let conn = self.conn()?;
        let ended_at = chrono::Utc::now().to_rfc3339();
        let count = conn.execute(
            "UPDATE agents
                SET status = 'done',
                    ended_at = COALESCE(ended_at, ?1),
                    terminal_cause = COALESCE(terminal_cause, 'startup_gc'),
                    error_reason = NULL
                WHERE status != 'done'",
            [ended_at],
        )?;
        Ok(count)
    }

    pub fn stats(&self) -> Result<DbStats> {
        let conn = self.conn()?;
        let total: i64 = conn.query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))?;
        let alive: i64 = conn.query_row(
            "SELECT COUNT(*) FROM agents WHERE status != 'done'",
            [],
            |row| row.get(0),
        )?;
        let done: i64 = conn.query_row(
            "SELECT COUNT(*) FROM agents WHERE status = 'done'",
            [],
            |row| row.get(0),
        )?;
        let messages: i64 =
            conn.query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))?;
        let errors: i64 = conn.query_row(
            "SELECT COUNT(*) FROM output_log WHERE kind IN ('error', 'timeout')",
            [],
            |row| row.get(0),
        )?;

        Ok(DbStats {
            total: total as u64,
            alive: alive as u64,
            done: done as u64,
            messages: messages as u64,
            errors: errors as u64,
        })
    }

    pub fn stats_for_project(&self, project_dir: &str) -> Result<DbStats> {
        let conn = self.conn()?;
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM agents WHERE project_dir = ?1",
            [project_dir],
            |row| row.get(0),
        )?;
        let alive: i64 = conn.query_row(
            "SELECT COUNT(*) FROM agents WHERE project_dir = ?1 AND status != 'done'",
            [project_dir],
            |row| row.get(0),
        )?;
        let done: i64 = conn.query_row(
            "SELECT COUNT(*) FROM agents WHERE project_dir = ?1 AND status = 'done'",
            [project_dir],
            |row| row.get(0),
        )?;
        let messages: i64 = conn.query_row(
            "SELECT COUNT(*) FROM messages
             WHERE from_agent IN (SELECT id FROM agents WHERE project_dir = ?1)
                OR to_agent IN (SELECT id FROM agents WHERE project_dir = ?1)",
            [project_dir],
            |row| row.get(0),
        )?;
        let errors: i64 = conn.query_row(
            "SELECT COUNT(*) FROM output_log
             WHERE kind IN ('error', 'timeout')
               AND agent_id IN (SELECT id FROM agents WHERE project_dir = ?1)",
            [project_dir],
            |row| row.get(0),
        )?;

        Ok(DbStats {
            total: total as u64,
            alive: alive as u64,
            done: done as u64,
            messages: messages as u64,
            errors: errors as u64,
        })
    }

    pub fn list_active_project_dirs(&self) -> Result<Vec<String>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT project_dir FROM agents WHERE project_dir IS NOT NULL AND status != 'done'",
        )?;
        let dirs = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(dirs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Db {
        Db::open(Path::new(":memory:")).unwrap()
    }

    fn test_agent(id: &str, status: &str) -> AgentRow {
        AgentRow {
            id: id.into(),
            label: "tester".into(),
            harness: "echo".into(),
            model: String::new(),
            status: TopicStatus::from_sql(status).unwrap(),
            parent_id: None,
            system_prompt: "you are a tester".into(),
            work_dir: "/tmp/test".into(),
            comms: CommsMode::Mesh,
            created_at: "2026-01-01T00:00:00Z".into(),
            ended_at: None,
            terminal_cause: None,
            error_reason: None,
            worktree_branch: None,
            project_dir: None,
            user_launched: false,
        }
    }

    fn agent_columns(db: &Db) -> Vec<String> {
        let conn = db.conn().unwrap();
        let mut stmt = conn.prepare("PRAGMA table_info(agents)").unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    }

    fn agent_indexes(db: &Db) -> Vec<String> {
        let conn = db.conn().unwrap();
        let mut stmt = conn.prepare("PRAGMA index_list(agents)").unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    }

    fn message_columns(db: &Db) -> Vec<String> {
        let conn = db.conn().unwrap();
        Db::message_column_names(&conn).unwrap()
    }

    #[test]
    fn initializes_agent_metadata_columns_and_indexes() {
        let db = test_db();

        let columns = agent_columns(&db);
        assert!(columns.contains(&"ended_at".to_string()));
        assert!(columns.contains(&"worktree_branch".to_string()));
        assert!(columns.contains(&"project_dir".to_string()));
        assert!(columns.contains(&"user_launched".to_string()));
        assert!(message_columns(&db).contains(&"leased_at".to_string()));

        let indexes = agent_indexes(&db);
        assert!(indexes.contains(&"idx_agents_status".to_string()));
        assert!(indexes.contains(&"idx_agents_parent_status".to_string()));
        assert!(indexes.contains(&"idx_agents_project_status".to_string()));
    }

    #[test]
    fn migrates_legacy_agents_table_metadata_and_label_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("swarm.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE agents (
                    id TEXT PRIMARY KEY,
                    role TEXT NOT NULL,
                    harness TEXT NOT NULL,
                    model TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'idle',
                    parent_id TEXT,
                    system_prompt TEXT NOT NULL DEFAULT '',
                    work_dir TEXT NOT NULL,
                    comms TEXT NOT NULL DEFAULT 'mesh',
                    created_at TEXT NOT NULL
                );
                INSERT INTO agents (id, role, harness, model, status, parent_id, system_prompt, work_dir, comms, created_at)
                VALUES ('legacy-agent', 'worker', 'echo', '', 'idle', NULL, '', '/tmp', 'mesh', '2026-01-01T00:00:00Z');",
            )
            .unwrap();
        }

        let db = Db::open(&db_path).unwrap();
        let columns = agent_columns(&db);
        assert!(columns.contains(&"label".to_string()));
        assert!(!columns.contains(&"role".to_string()));
        assert!(columns.contains(&"ended_at".to_string()));
        assert!(columns.contains(&"worktree_branch".to_string()));

        let agent = db.get_agent("legacy-agent").unwrap().unwrap();
        assert_eq!(agent.label, "worker");
        assert_eq!(agent.status, TopicStatus::Idle);
        assert_eq!(agent.ended_at, None);
        assert_eq!(agent.worktree_branch, None);
    }

    #[test]
    fn agent_crud() {
        let db = test_db();
        let agent = test_agent("test-1234", "idle");
        db.insert_agent(&agent).unwrap();

        let fetched = db.get_agent("test-1234").unwrap().unwrap();
        assert_eq!(fetched.label, "tester");
        assert_eq!(fetched.harness, "echo");

        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 1);

        db.update_agent_status("test-1234", TopicStatus::Working)
            .unwrap();
        let updated = db.get_agent("test-1234").unwrap().unwrap();
        assert_eq!(updated.status, TopicStatus::Working);

        db.delete_agent("test-1234").unwrap();
        assert!(db.get_agent("test-1234").unwrap().is_none());
    }

    #[test]
    fn inbox_cursor_round_trips() {
        let db = test_db();
        assert!(db.get_inbox_cursor("user").unwrap().is_none());
        db.set_inbox_cursor("user", "2026-01-01T00:00:00Z").unwrap();
        assert_eq!(
            db.get_inbox_cursor("user").unwrap().as_deref(),
            Some("2026-01-01T00:00:00Z")
        );
    }

    #[test]
    fn message_queue() {
        let db = test_db();
        let msg = MessageRow {
            id: "msg-1".into(),
            from_agent: "user".into(),
            to_agent: "agent-1".into(),
            content: "hello".into(),
            state: MessageState::pending(),
            created_at: "2026-01-01T00:00:00Z".into(),
            broadcast_id: None,
        };
        db.enqueue_message(&msg).unwrap();

        let dequeued = db.dequeue_message("agent-1").unwrap().unwrap();
        assert_eq!(dequeued.content, "hello");
        assert_eq!(dequeued.from_agent, "user");

        // Should be empty now
        assert!(db.dequeue_message("agent-1").unwrap().is_none());
    }

    #[test]
    fn message_leases_ack_and_release() {
        let db = test_db();
        let msg = MessageRow {
            id: "msg-lease".into(),
            from_agent: "user".into(),
            to_agent: "agent-1".into(),
            content: "lease me".into(),
            state: MessageState::pending(),
            created_at: "2026-01-01T00:00:00Z".into(),
            broadcast_id: None,
        };
        db.enqueue_message(&msg).unwrap();

        let first = db.dequeue_message("agent-1").unwrap().unwrap();
        assert_eq!(first.id, "msg-lease");
        assert!(!db.has_pending_messages("agent-1").unwrap());

        db.release_messages(&LeasedMessageBatch::new(vec![first]))
            .unwrap();
        assert!(db.has_pending_messages("agent-1").unwrap());

        let second = db.dequeue_message("agent-1").unwrap().unwrap();
        db.mark_messages_delivered(&LeasedMessageBatch::new(vec![second]))
            .unwrap();
        assert!(!db.has_pending_messages("agent-1").unwrap());
        assert!(db.dequeue_message("agent-1").unwrap().is_none());
    }

    #[test]
    fn stale_message_leases_cannot_ack_or_release_current_lease() {
        let db = test_db();
        db.enqueue_message(&MessageRow {
            id: "msg-stale-lease".into(),
            from_agent: "user".into(),
            to_agent: "agent-1".into(),
            content: "lease guarded".into(),
            state: MessageState::pending(),
            created_at: "2026-01-01T00:00:00Z".into(),
            broadcast_id: None,
        })
        .unwrap();

        let current = db.dequeue_message("agent-1").unwrap().unwrap();
        let mut stale = current.clone();
        stale.leased_at = "2026-01-01T00:00:01Z".into();
        let stale_batch = LeasedMessageBatch::new(vec![stale]);

        assert_eq!(db.mark_messages_delivered(&stale_batch).unwrap(), 0);
        assert_eq!(db.release_messages(&stale_batch).unwrap(), 0);
        assert!(!db.has_pending_messages("agent-1").unwrap());

        db.release_messages(&LeasedMessageBatch::new(vec![current]))
            .unwrap();
        assert!(db.has_pending_messages("agent-1").unwrap());
    }

    #[test]
    fn project_resume_releases_inflight_messages() {
        let db = test_db();
        let mut agent = test_agent("agent-1", "working");
        agent.project_dir = Some("/tmp/project".into());
        db.insert_agent(&agent).unwrap();
        db.enqueue_message(&MessageRow {
            id: "msg-resume".into(),
            from_agent: "user".into(),
            to_agent: "agent-1".into(),
            content: "resume me".into(),
            state: MessageState::pending(),
            created_at: "2026-01-01T00:00:00Z".into(),
            broadcast_id: None,
        })
        .unwrap();

        assert!(db.dequeue_message("agent-1").unwrap().is_some());
        assert!(!db.has_pending_messages("agent-1").unwrap());

        assert_eq!(
            db.release_inflight_messages_for_project("/tmp/project")
                .unwrap(),
            1
        );
        assert!(db.has_pending_messages("agent-1").unwrap());
    }

    #[test]
    fn message_ordering() {
        let db = test_db();
        for i in 0..3 {
            let msg = MessageRow {
                id: format!("msg-{i}"),
                from_agent: "user".into(),
                to_agent: "agent-1".into(),
                content: format!("message {i}"),
                state: MessageState::pending(),
                created_at: format!("2026-01-01T00:00:0{i}Z"),
                broadcast_id: None,
            };
            db.enqueue_message(&msg).unwrap();
        }

        for i in 0..3 {
            let msg = db.dequeue_message("agent-1").unwrap().unwrap();
            assert_eq!(msg.content, format!("message {i}"));
        }
        assert!(db.dequeue_message("agent-1").unwrap().is_none());
    }

    #[test]
    fn agent_log_interleaves_messages_and_output() {
        let db = test_db();

        db.enqueue_message(&MessageRow {
            id: "m1".into(),
            from_agent: "user".into(),
            to_agent: "agent-1".into(),
            content: "do something".into(),
            state: MessageState::pending(),
            created_at: "2026-01-01T00:00:01Z".into(),
            broadcast_id: None,
        })
        .unwrap();

        db.insert_output_log(&OutputLogRow {
            id: "o1".into(),
            agent_id: "agent-1".into(),
            content: "working on it".into(),
            kind: "output".into(),
            created_at: "2026-01-01T00:00:02Z".into(),
        })
        .unwrap();

        db.enqueue_message(&MessageRow {
            id: "m2".into(),
            from_agent: "agent-1".into(),
            to_agent: "user".into(),
            content: "done".into(),
            state: MessageState::pending(),
            created_at: "2026-01-01T00:00:03Z".into(),
            broadcast_id: None,
        })
        .unwrap();

        let all = db.get_agent_log("agent-1", 50, LogFilter::All).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].kind, "recv");
        assert_eq!(all[0].content, "do something");
        assert_eq!(all[1].kind, "output");
        assert_eq!(all[1].content, "working on it");
        assert_eq!(all[2].kind, "sent");
        assert_eq!(all[2].content, "done");

        let msgs = db
            .get_agent_log("agent-1", 50, LogFilter::Messages)
            .unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].kind, "recv");
        assert_eq!(msgs[1].kind, "sent");

        let output = db.get_agent_log("agent-1", 50, LogFilter::Output).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].kind, "output");

        let limited = db.get_agent_log("agent-1", 2, LogFilter::All).unwrap();
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].content, "working on it");
        assert_eq!(limited[1].content, "done");
    }

    #[test]
    fn agent_log_search_filters_content() {
        let db = test_db();

        for (id, content, created_at) in [
            ("m1", "alpha planning", "2026-01-01T00:00:01Z"),
            ("m2", "beta checkpoint", "2026-01-01T00:00:02Z"),
            ("m3", "alpha handoff", "2026-01-01T00:00:03Z"),
        ] {
            db.enqueue_message(&MessageRow {
                id: id.into(),
                from_agent: "user".into(),
                to_agent: "agent-1".into(),
                content: content.into(),
                state: MessageState::pending(),
                created_at: created_at.into(),
                broadcast_id: None,
            })
            .unwrap();
        }

        let matches = db
            .search_agent_log("agent-1", 10, LogFilter::Messages, Some("ALPHA"))
            .unwrap();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].content, "alpha planning");
        assert_eq!(matches[1].content, "alpha handoff");
    }

    #[test]
    fn handovers_store_structured_finish_report() {
        let db = test_db();
        db.insert_handover(&HandoverRow {
            id: "h1".into(),
            agent_id: "agent-1".into(),
            summary: Some("implemented".into()),
            outcome: Some("done".into()),
            deliverable: Some("branch swarm/agent-1".into()),
            checks: Some("cargo test".into()),
            risk: Some("none known".into()),
            next_action: Some("review".into()),
            created_at: "2026-01-01T00:00:01Z".into(),
        })
        .unwrap();

        let latest = db.latest_handover("agent-1").unwrap().unwrap();
        assert_eq!(latest.summary.as_deref(), Some("implemented"));
        assert_eq!(latest.next_action.as_deref(), Some("review"));
    }

    #[test]
    fn done_agents_hidden_from_list() {
        let db = test_db();
        let agent = test_agent("done-agent", "done");
        db.insert_agent(&agent).unwrap();
        assert!(db.list_agents().unwrap().is_empty());
        assert!(db.get_agent("done-agent").unwrap().is_some());
    }

    #[test]
    fn active_agents_visible_from_list() {
        let db = test_db();
        let agent = test_agent("active-agent", "idle");
        db.insert_agent(&agent).unwrap();
        assert_eq!(db.list_agents().unwrap().len(), 1);
        assert!(db.get_agent("active-agent").unwrap().is_some());
    }

    #[test]
    fn terminal_agents_reject_atomic_status_and_message_updates() {
        let db = test_db();
        let agent = test_agent("done-agent", "done");
        db.insert_agent(&agent).unwrap();

        assert!(!db
            .update_agent_status_if_active("done-agent", TopicStatus::Idle)
            .unwrap());
        assert_eq!(
            db.get_agent("done-agent").unwrap().unwrap().status,
            TopicStatus::Paused
        );

        let msg = MessageRow {
            id: "msg-terminal".into(),
            from_agent: "user".into(),
            to_agent: "done-agent".into(),
            content: "hello?".into(),
            state: MessageState::pending(),
            created_at: "2026-01-01T00:00:01Z".into(),
            broadcast_id: None,
        };
        assert!(!db.enqueue_message_for_active_agent(&msg).unwrap());
        assert!(!db.has_pending_messages("done-agent").unwrap());
    }

    #[test]
    fn done_status_sets_ended_at_and_reactivation_clears_it() {
        let db = test_db();
        db.insert_agent(&test_agent("agent-1", "idle")).unwrap();

        db.update_agent_status("agent-1", TopicStatus::Paused)
            .unwrap();
        let done_agent = db.get_agent("agent-1").unwrap().unwrap();
        assert_eq!(done_agent.status, TopicStatus::Paused);
        assert_eq!(done_agent.terminal_cause, Some(TerminalCause::LoopExit));
        assert_eq!(done_agent.error_reason, None);
        assert!(
            done_agent.ended_at.is_some(),
            "done transition should set ended_at"
        );

        db.update_agent_status("agent-1", TopicStatus::Idle)
            .unwrap();
        let active_agent = db.get_agent("agent-1").unwrap().unwrap();
        assert_eq!(active_agent.status, TopicStatus::Idle);
        assert_eq!(active_agent.ended_at, None);
        assert_eq!(active_agent.terminal_cause, None);
        assert_eq!(active_agent.error_reason, None);
    }
}

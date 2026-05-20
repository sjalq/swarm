use crate::error::{Result, SwarmError};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRow {
    pub id: String,
    pub role: String,
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

pub struct Db {
    conn: Mutex<Connection>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.init_tables()?;
        Ok(db)
    }

    fn conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| SwarmError::Internal("database mutex poisoned".to_string()))
    }

    fn init_tables(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agents (
                id TEXT PRIMARY KEY,
                role TEXT NOT NULL,
                harness TEXT NOT NULL,
                model TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'idle',
                parent_id TEXT,
                system_prompt TEXT NOT NULL DEFAULT '',
                work_dir TEXT NOT NULL,
                comms TEXT NOT NULL DEFAULT 'mesh',
                created_at TEXT NOT NULL,
                ended_at TEXT NULL,
                worktree_branch TEXT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                from_agent TEXT NOT NULL,
                to_agent TEXT NOT NULL,
                content TEXT NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_messages_pending
                ON messages(to_agent, delivered, created_at);
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
                ON events(agent_id, created_at);",
        )?;
        Self::ensure_agents_column(&conn, "ended_at", "TEXT NULL")?;
        Self::ensure_agents_column(&conn, "worktree_branch", "TEXT NULL")?;
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_agents_status
                ON agents(status, created_at);
            CREATE INDEX IF NOT EXISTS idx_agents_parent_status
                ON agents(parent_id, status);
            UPDATE agents
                SET status = 'done',
                    ended_at = COALESCE(
                        ended_at,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    )
                WHERE status = 'dead';",
        )?;
        Ok(())
    }

    fn ensure_agents_column(conn: &Connection, column: &str, definition: &str) -> Result<()> {
        let exists = {
            let mut stmt = conn.prepare("PRAGMA table_info(agents)")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            let columns = rows.collect::<std::result::Result<Vec<_>, _>>()?;
            columns.iter().any(|name| name == column)
        };

        if !exists {
            conn.execute(
                &format!("ALTER TABLE agents ADD COLUMN {column} {definition}"),
                [],
            )?;
        }

        Ok(())
    }

    fn agent_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentRow> {
        Ok(AgentRow {
            id: row.get(0)?,
            role: row.get(1)?,
            harness: row.get(2)?,
            model: row.get(3)?,
            status: row.get(4)?,
            parent_id: row.get(5)?,
            system_prompt: row.get(6)?,
            work_dir: row.get(7)?,
            comms: row.get(8)?,
            created_at: row.get(9)?,
            ended_at: row.get(10)?,
            worktree_branch: row.get(11)?,
        })
    }

    pub fn insert_agent(&self, agent: &AgentRow) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO agents (id, role, harness, model, status, parent_id, system_prompt, work_dir, comms, created_at, ended_at, worktree_branch)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                agent.id,
                agent.role,
                agent.harness,
                agent.model,
                agent.status,
                agent.parent_id,
                agent.system_prompt,
                agent.work_dir,
                agent.comms,
                agent.created_at,
                agent.ended_at,
                agent.worktree_branch,
            ],
        )?;
        Ok(())
    }

    pub fn get_agent(&self, id: &str) -> Result<Option<AgentRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, role, harness, model, status, parent_id, system_prompt, work_dir, comms, created_at, ended_at, worktree_branch
             FROM agents WHERE id = ?1",
        )?;
        let result = stmt.query_row([id], Self::agent_from_row);
        match result {
            Ok(agent) => Ok(Some(agent)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_agents(&self) -> Result<Vec<AgentRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, role, harness, model, status, parent_id, system_prompt, work_dir, comms, created_at, ended_at, worktree_branch
             FROM agents WHERE status != 'done' ORDER BY created_at",
        )?;
        let agents = stmt
            .query_map([], Self::agent_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(agents)
    }

    pub fn list_all_agents(&self) -> Result<Vec<AgentRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, role, harness, model, status, parent_id, system_prompt, work_dir, comms, created_at, ended_at, worktree_branch
             FROM agents ORDER BY created_at",
        )?;
        let agents = stmt
            .query_map([], Self::agent_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(agents)
    }

    pub fn insert_event(&self, event: &EventRow) -> Result<()> {
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

    pub fn reparent_children(&self, old_parent: &str, new_parent: Option<&str>) -> Result<usize> {
        let conn = self.conn()?;
        let count = conn.execute(
            "UPDATE agents SET parent_id = ?1 WHERE parent_id = ?2 AND status != 'done'",
            rusqlite::params![new_parent, old_parent],
        )?;
        Ok(count)
    }

    pub fn update_agent_status(&self, id: &str, status: &str) -> Result<()> {
        let conn = self.conn()?;
        let ended_at = (status == "done").then(|| chrono::Utc::now().to_rfc3339());
        conn.execute(
            "UPDATE agents
                SET status = ?1,
                    ended_at = CASE
                        WHEN ?1 = 'done' THEN COALESCE(ended_at, ?3)
                        ELSE NULL
                    END
                WHERE id = ?2",
            rusqlite::params![status, id, ended_at],
        )?;
        Ok(())
    }

    pub fn update_agent_status_if_active(&self, id: &str, status: &str) -> Result<bool> {
        let conn = self.conn()?;
        let ended_at = (status == "done").then(|| chrono::Utc::now().to_rfc3339());
        let updated = conn.execute(
            "UPDATE agents
                SET status = ?1,
                    ended_at = CASE
                        WHEN ?1 = 'done' THEN COALESCE(ended_at, ?3)
                        ELSE NULL
                    END
                WHERE id = ?2 AND status != 'done'",
            rusqlite::params![status, id, ended_at],
        )?;
        Ok(updated > 0)
    }

    pub fn delete_agent(&self, id: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute("DELETE FROM agents WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn enqueue_message(&self, msg: &MessageRow) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO messages (id, from_agent, to_agent, content, delivered, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                msg.id,
                msg.from_agent,
                msg.to_agent,
                msg.content,
                msg.delivered as i32,
                msg.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn enqueue_message_for_active_agent(&self, msg: &MessageRow) -> Result<bool> {
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
            "INSERT INTO messages (id, from_agent, to_agent, content, delivered, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                msg.id,
                msg.from_agent,
                msg.to_agent,
                msg.content,
                msg.delivered as i32,
                msg.created_at,
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn insert_output_log(&self, entry: &OutputLogRow) -> Result<()> {
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

    pub fn get_agent_log(
        &self,
        agent_id: &str,
        limit: usize,
        filter: LogFilter,
    ) -> Result<Vec<LogEntry>> {
        let conn = self.conn()?;

        let entries = match filter {
            LogFilter::All => {
                let mut stmt = conn.prepare(
                    "SELECT timestamp, kind, peer, content FROM (
                        SELECT created_at AS timestamp, 'recv' AS kind, from_agent AS peer, content
                        FROM messages WHERE to_agent = ?1
                        UNION ALL
                        SELECT created_at AS timestamp, 'sent' AS kind, to_agent AS peer, content
                        FROM messages WHERE from_agent = ?1
                        UNION ALL
                        SELECT created_at AS timestamp, kind, '' AS peer, content
                        FROM output_log WHERE agent_id = ?1
                    ) ORDER BY timestamp ASC LIMIT ?2",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![agent_id, limit], |row| {
                        Ok(LogEntry {
                            timestamp: row.get(0)?,
                            kind: row.get(1)?,
                            peer: row.get(2)?,
                            content: row.get(3)?,
                        })
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                rows
            }
            LogFilter::Messages => {
                let mut stmt = conn.prepare(
                    "SELECT timestamp, kind, peer, content FROM (
                        SELECT created_at AS timestamp, 'recv' AS kind, from_agent AS peer, content
                        FROM messages WHERE to_agent = ?1
                        UNION ALL
                        SELECT created_at AS timestamp, 'sent' AS kind, to_agent AS peer, content
                        FROM messages WHERE from_agent = ?1
                    ) ORDER BY timestamp ASC LIMIT ?2",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![agent_id, limit], |row| {
                        Ok(LogEntry {
                            timestamp: row.get(0)?,
                            kind: row.get(1)?,
                            peer: row.get(2)?,
                            content: row.get(3)?,
                        })
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                rows
            }
            LogFilter::Output => {
                let mut stmt = conn.prepare(
                    "SELECT created_at, kind, '' AS peer, content
                     FROM output_log WHERE agent_id = ?1
                     ORDER BY created_at ASC LIMIT ?2",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![agent_id, limit], |row| {
                        Ok(LogEntry {
                            timestamp: row.get(0)?,
                            kind: row.get(1)?,
                            peer: row.get(2)?,
                            content: row.get(3)?,
                        })
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                rows
            }
        };

        Ok(entries)
    }

    pub fn dequeue_message(&self, agent_id: &str) -> Result<Option<MessageRow>> {
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let result = {
            let mut stmt = tx.prepare(
                "SELECT id, from_agent, to_agent, content, delivered, created_at
                 FROM messages WHERE to_agent = ?1 AND delivered = 0
                 ORDER BY created_at LIMIT 1",
            )?;
            stmt.query_row([agent_id], |row| {
                Ok(MessageRow {
                    id: row.get(0)?,
                    from_agent: row.get(1)?,
                    to_agent: row.get(2)?,
                    content: row.get(3)?,
                    delivered: row.get::<_, i32>(4)? != 0,
                    created_at: row.get(5)?,
                })
            })
        };
        match result {
            Ok(msg) => {
                tx.execute("UPDATE messages SET delivered = 1 WHERE id = ?1", [&msg.id])?;
                tx.commit()?;
                Ok(Some(msg))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn has_pending_messages(&self, agent_id: &str) -> Result<bool> {
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM messages WHERE to_agent = ?1 AND delivered = 0",
            [agent_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn mark_unfinished_agents_done(&self) -> Result<usize> {
        let conn = self.conn()?;
        let ended_at = chrono::Utc::now().to_rfc3339();
        let count = conn.execute(
            "UPDATE agents
                SET status = 'done',
                    ended_at = COALESCE(ended_at, ?1)
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
            role: "tester".into(),
            harness: "echo".into(),
            model: String::new(),
            status: status.into(),
            parent_id: None,
            system_prompt: "you are a tester".into(),
            work_dir: "/tmp/test".into(),
            comms: "mesh".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            ended_at: None,
            worktree_branch: None,
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

    #[test]
    fn initializes_agent_metadata_columns_and_indexes() {
        let db = test_db();

        let columns = agent_columns(&db);
        assert!(columns.contains(&"ended_at".to_string()));
        assert!(columns.contains(&"worktree_branch".to_string()));

        let indexes = agent_indexes(&db);
        assert!(indexes.contains(&"idx_agents_status".to_string()));
        assert!(indexes.contains(&"idx_agents_parent_status".to_string()));
    }

    #[test]
    fn migrates_legacy_agents_table_metadata_columns() {
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
        assert!(columns.contains(&"ended_at".to_string()));
        assert!(columns.contains(&"worktree_branch".to_string()));

        let agent = db.get_agent("legacy-agent").unwrap().unwrap();
        assert_eq!(agent.status, "idle");
        assert_eq!(agent.ended_at, None);
        assert_eq!(agent.worktree_branch, None);
    }

    #[test]
    fn agent_crud() {
        let db = test_db();
        let agent = test_agent("test-1234", "idle");
        db.insert_agent(&agent).unwrap();

        let fetched = db.get_agent("test-1234").unwrap().unwrap();
        assert_eq!(fetched.role, "tester");
        assert_eq!(fetched.harness, "echo");

        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 1);

        db.update_agent_status("test-1234", "working").unwrap();
        let updated = db.get_agent("test-1234").unwrap().unwrap();
        assert_eq!(updated.status, "working");

        db.delete_agent("test-1234").unwrap();
        assert!(db.get_agent("test-1234").unwrap().is_none());
    }

    #[test]
    fn message_queue() {
        let db = test_db();
        let msg = MessageRow {
            id: "msg-1".into(),
            from_agent: "user".into(),
            to_agent: "agent-1".into(),
            content: "hello".into(),
            delivered: false,
            created_at: "2026-01-01T00:00:00Z".into(),
        };
        db.enqueue_message(&msg).unwrap();

        let dequeued = db.dequeue_message("agent-1").unwrap().unwrap();
        assert_eq!(dequeued.content, "hello");
        assert_eq!(dequeued.from_agent, "user");

        // Should be empty now
        assert!(db.dequeue_message("agent-1").unwrap().is_none());
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
                delivered: false,
                created_at: format!("2026-01-01T00:00:0{i}Z"),
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
            delivered: false,
            created_at: "2026-01-01T00:00:01Z".into(),
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
            delivered: false,
            created_at: "2026-01-01T00:00:03Z".into(),
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
            .update_agent_status_if_active("done-agent", "idle")
            .unwrap());
        assert_eq!(db.get_agent("done-agent").unwrap().unwrap().status, "done");

        let msg = MessageRow {
            id: "msg-terminal".into(),
            from_agent: "user".into(),
            to_agent: "done-agent".into(),
            content: "hello?".into(),
            delivered: false,
            created_at: "2026-01-01T00:00:01Z".into(),
        };
        assert!(!db.enqueue_message_for_active_agent(&msg).unwrap());
        assert!(!db.has_pending_messages("done-agent").unwrap());
    }

    #[test]
    fn done_status_sets_ended_at_and_reactivation_clears_it() {
        let db = test_db();
        db.insert_agent(&test_agent("agent-1", "idle")).unwrap();

        db.update_agent_status("agent-1", "done").unwrap();
        let done_agent = db.get_agent("agent-1").unwrap().unwrap();
        assert_eq!(done_agent.status, "done");
        assert!(
            done_agent.ended_at.is_some(),
            "done transition should set ended_at"
        );

        db.update_agent_status("agent-1", "idle").unwrap();
        let active_agent = db.get_agent("agent-1").unwrap().unwrap();
        assert_eq!(active_agent.status, "idle");
        assert_eq!(active_agent.ended_at, None);
    }
}

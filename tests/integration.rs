use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use swarm::db::{AgentRow, CommsMode, Db, LogFilter, MessageRow, OutputLogRow, TopicStatus};
use swarm::error::Result as SwarmResult;
use swarm::harness::{CliHarness, CliKind, Harness, HarnessOutput, HarnessRegistry};
use swarm::orchestrator::{DoneReport, Orchestrator, SwarmEvent};
use tokio::sync::mpsc;

struct ResumeProbeHarness;

impl Harness for ResumeProbeHarness {
    fn name(&self) -> &str {
        "resume-probe"
    }

    fn run(
        &self,
        prompt: &str,
        _model: Option<&str>,
        continue_conversation: bool,
        _work_dir: &Path,
        _env_extra: HashMap<String, String>,
        tx: mpsc::Sender<HarnessOutput>,
    ) -> Pin<Box<dyn Future<Output = SwarmResult<()>> + Send>> {
        let response = format!("resume={continue_conversation}; prompt={}", prompt.trim());
        Box::pin(async move {
            tx.send(HarnessOutput::Complete(response))
                .await
                .map_err(|e| swarm::error::SwarmError::Internal(e.to_string()))?;
            Ok(())
        })
    }
}

fn setup() -> (tempfile::TempDir, Arc<Orchestrator>) {
    setup_with_registry(HarnessRegistry::new())
}

fn setup_with_registry(registry: HarnessRegistry) -> (tempfile::TempDir, Arc<Orchestrator>) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    let agents_dir = dir.path().join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        "http://127.0.0.1:0".to_string(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));
    (dir, orch)
}

async fn setup_http_server() -> (tempfile::TempDir, Arc<Orchestrator>, String) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    std::fs::create_dir_all(dir.path().join("agents")).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let router = swarm::server::router(orch.clone());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    (dir, orch, addr)
}

async fn start_http_server(orch: Arc<Orchestrator>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");
    let router = swarm::server::router(orch);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn wait_for_agent_output(
    rx: &mut tokio::sync::broadcast::Receiver<SwarmEvent>,
    agent_id: &str,
    expected: &str,
) -> String {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentOutput { agent_id: id, text }) => {
                    if id == agent_id && text.contains(expected) {
                        return text;
                    }
                }
                Err(e) => panic!("event stream closed before output arrived: {e}"),
                _ => continue,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {agent_id} output containing {expected:?}"))
}

fn setup_with_git() -> (tempfile::TempDir, Arc<Orchestrator>) {
    setup_with_git_registry(HarnessRegistry::new())
}

fn setup_with_git_registry(registry: HarnessRegistry) -> (tempfile::TempDir, Arc<Orchestrator>) {
    let dir = tempfile::tempdir().unwrap();
    let project_dir = dir.path().join("project");
    std::fs::create_dir_all(&project_dir).unwrap();

    // Initialize a git repo with an initial commit
    std::process::Command::new("git")
        .args(["init"])
        .current_dir(&project_dir)
        .output()
        .unwrap();
    std::process::Command::new("git")
        .args(["config", "user.email", "test@test.com"])
        .current_dir(&project_dir)
        .output()
        .unwrap();
    std::process::Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(&project_dir)
        .output()
        .unwrap();
    std::fs::write(project_dir.join("README.md"), "# test project\n").unwrap();
    std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(&project_dir)
        .output()
        .unwrap();
    std::process::Command::new("git")
        .args(["commit", "-m", "initial"])
        .current_dir(&project_dir)
        .output()
        .unwrap();

    let data_dir = project_dir.join(".swarm");
    std::fs::create_dir_all(data_dir.join("agents")).unwrap();

    let db = Arc::new(Db::open(&data_dir.join("swarm.db")).unwrap());
    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        "http://127.0.0.1:0".to_string(),
        project_dir,
        data_dir,
    ));
    (dir, orch)
}

#[tokio::test]
async fn start_topic_and_list_agents() {
    let (_dir, orch) = setup();

    let a = orch
        .start_topic("researcher", "echo", "find things", None, "mesh")
        .unwrap();
    assert!(a.id.starts_with("researcher-"));
    assert_eq!(a.harness, "echo");
    assert_eq!(a.status, TopicStatus::Idle);
    assert_eq!(a.parent_id.as_deref(), Some("user"));

    let b = orch
        .start_topic("writer", "echo", "write things", Some(&a.id), "mesh")
        .unwrap();
    assert_eq!(b.parent_id.as_deref(), Some(a.id.as_str()));

    let agents = orch.list_agents().unwrap();
    assert_eq!(agents.len(), 2);
}

#[tokio::test]
async fn global_data_dir_scopes_agents_to_current_project() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let project_a = dir.path().join("project-a");
    let project_b = dir.path().join("project-b");
    std::fs::create_dir_all(data_dir.join("agents")).unwrap();
    std::fs::create_dir_all(&project_a).unwrap();
    std::fs::create_dir_all(&project_b).unwrap();

    let db = Arc::new(Db::open(&data_dir.join("swarm.db")).unwrap());
    for (id, project_dir) in [("worker-a", &project_a), ("worker-b", &project_b)] {
        db.insert_agent(&AgentRow {
            id: id.into(),
            label: "worker".into(),
            harness: "echo".into(),
            model: String::new(),
            status: TopicStatus::Idle,
            parent_id: None,
            system_prompt: String::new(),
            work_dir: data_dir
                .join("agents")
                .join(id)
                .to_string_lossy()
                .to_string(),
            comms: CommsMode::Mesh,
            created_at: "2026-01-01T00:00:00Z".into(),
            ended_at: None,
            worktree_branch: None,
            project_dir: Some(project_dir.to_string_lossy().to_string()),
            user_launched: false,
        })
        .unwrap();
    }

    db.enqueue_message(&MessageRow {
        id: "message-a".into(),
        from_agent: "user".into(),
        to_agent: "worker-a".into(),
        content: "project a".into(),
        delivered: false,
        created_at: "2026-01-01T00:00:01Z".into(),
        broadcast_id: None,
    })
    .unwrap();
    db.enqueue_message(&MessageRow {
        id: "message-b".into(),
        from_agent: "user".into(),
        to_agent: "worker-b".into(),
        content: "project b".into(),
        delivered: false,
        created_at: "2026-01-01T00:00:01Z".into(),
        broadcast_id: None,
    })
    .unwrap();

    let orch_a = Arc::new(Orchestrator::new(
        db.clone(),
        HarnessRegistry::new(),
        "http://127.0.0.1:0".into(),
        project_a.clone(),
        data_dir.clone(),
    ));
    let orch_b = Arc::new(Orchestrator::new(
        db,
        HarnessRegistry::new(),
        "http://127.0.0.1:0".into(),
        project_b,
        data_dir,
    ));

    assert_eq!(orch_a.list_agents().unwrap().len(), 1);
    assert!(orch_a.get_agent("worker-a").unwrap().is_some());
    assert!(orch_a.get_agent("worker-b").unwrap().is_none());
    assert_eq!(orch_a.stats().unwrap().messages, 1);
    assert_eq!(orch_a.resume_existing_workers().unwrap(), 1);

    assert_eq!(orch_b.list_agents().unwrap().len(), 1);
    assert!(orch_b.get_agent("worker-a").unwrap().is_none());
    assert!(orch_b.get_agent("worker-b").unwrap().is_some());
    assert_eq!(orch_b.stats().unwrap().messages, 1);
    assert_eq!(orch_b.resume_existing_workers().unwrap(), 1);
}

#[tokio::test]
async fn echo_agent_processes_message() {
    let (_dir, orch, _addr) = setup_http_server().await;

    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic("tester", "echo", "", None, "mesh")
        .unwrap();

    orch.send_message("user", &agent.id, "hello world")
        .await
        .unwrap();

    // Wait for the echo harness to route its reply back to the sender.
    let saw_reply = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::UserNotification { from, content }) => {
                    if from == agent.id && content == "(echo) hello world" {
                        return true;
                    }
                }
                Err(_) => return false,
                _ => continue,
            }
        }
    })
    .await;

    assert_eq!(
        saw_reply,
        Ok(true),
        "echo agent should route the message back to the sender"
    );
}

#[tokio::test]
async fn echo_parent_child_messages_do_not_bounce_forever() {
    let (_dir, orch, _addr) = setup_http_server().await;

    let parent = orch
        .start_topic("parent", "echo", "", None, "mesh")
        .unwrap();
    let child = orch
        .start_topic("child", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.send_message(&parent.id, &child.id, "one child task")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let parent_log = orch
        .get_agent_log(&parent.id, 50, LogFilter::Messages)
        .unwrap();
    let child_replies = parent_log
        .iter()
        .filter(|entry| {
            entry.kind == "recv"
                && entry.peer == child.id
                && entry.content.contains("one child task")
        })
        .count();
    assert_eq!(child_replies, 1, "parent should get one child reply");

    let child_log = orch
        .get_agent_log(&child.id, 50, LogFilter::Messages)
        .unwrap();
    let child_sent = child_log
        .iter()
        .filter(|entry| entry.kind == "sent" && entry.peer == parent.id)
        .count();
    assert_eq!(
        child_sent, 1,
        "child should not echo the parent's echo response back again"
    );
}

#[tokio::test]
async fn agent_status_transitions() {
    let (_dir, orch, _addr) = setup_http_server().await;

    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic("worker", "echo", "", None, "mesh")
        .unwrap();
    assert_eq!(agent.status, TopicStatus::Idle);

    orch.send_message("user", &agent.id, "do something")
        .await
        .unwrap();

    let mut saw_working = false;
    let mut saw_idle_after = false;

    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentStatus { status, .. }) => {
                    if status == TopicStatus::Working {
                        saw_working = true;
                    }
                    if status == TopicStatus::Idle && saw_working {
                        saw_idle_after = true;
                        return;
                    }
                }
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    assert!(saw_working, "agent should transition to working");
    assert!(
        saw_idle_after,
        "agent should return to idle after processing"
    );
}

#[tokio::test]
async fn kill_agent() {
    let (_dir, orch) = setup();

    let agent = orch
        .start_topic("doomed", "echo", "", None, "mesh")
        .unwrap();
    assert_eq!(orch.list_agents().unwrap().len(), 1);

    orch.kill_agent(&agent.id).await.unwrap();

    // Done agents are hidden from active list
    assert_eq!(orch.list_agents().unwrap().len(), 0);

    // But still fetchable directly
    let fetched = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(fetched.status, TopicStatus::Paused);
    assert!(fetched.ended_at.is_some(), "kill should populate ended_at");
}

#[tokio::test]
async fn parent_only_comms_enforced() {
    let (_dir, orch) = setup();

    let parent = orch.start_topic("boss", "echo", "", None, "mesh").unwrap();
    let child = orch
        .start_topic("worker", "echo", "", Some(&parent.id), "parent-only")
        .unwrap();

    // Parent can message child
    let result = orch.send_message(&parent.id, &child.id, "do this").await;
    assert!(result.is_ok());

    // Same-parent siblings can coordinate with parent-only topics.
    let sibling = orch
        .start_topic("sibling", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let result = orch.send_message(&sibling.id, &child.id, "hey").await;
    assert!(
        result.is_ok(),
        "sibling should be able to message parent-only child"
    );

    // User can always message (special sender)
    let result = orch.send_message("user", &child.id, "override").await;
    assert!(result.is_ok());

    let outsider_parent = orch
        .start_topic("outsider-parent", "echo", "", None, "mesh")
        .unwrap();
    let outsider = orch
        .start_topic("outsider", "echo", "", Some(&outsider_parent.id), "mesh")
        .unwrap();
    let result = orch
        .send_message(&outsider.id, &child.id, "not family")
        .await;
    assert!(
        result.is_err(),
        "unrelated topics should not be able to message parent-only child"
    );
}

#[tokio::test]
async fn parent_only_topics_accept_messages_from_direct_children_and_siblings() {
    let (_dir, orch) = setup();

    let grandparent = orch.start_topic("root", "echo", "", None, "mesh").unwrap();
    let parent = orch
        .start_topic(
            "limited-parent",
            "echo",
            "",
            Some(&grandparent.id),
            "parent-only",
        )
        .unwrap();
    let child = orch
        .start_topic("child", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let sibling = orch
        .start_topic("sibling", "echo", "", Some(&grandparent.id), "mesh")
        .unwrap();
    let unrelated_root = orch
        .start_topic("other-root", "echo", "", None, "mesh")
        .unwrap();
    let unrelated = orch
        .start_topic("unrelated", "echo", "", Some(&unrelated_root.id), "mesh")
        .unwrap();

    let result = orch
        .send_message(&child.id, &parent.id, "child report")
        .await;
    assert!(
        result.is_ok(),
        "direct children must be able to report to parent-only parents"
    );

    let result = orch
        .send_message(&sibling.id, &parent.id, "sibling report")
        .await;
    assert!(
        result.is_ok(),
        "same-parent siblings must be able to message parent-only topics"
    );

    let result = orch
        .send_message(&unrelated.id, &parent.id, "unrelated report")
        .await;
    assert!(
        result.is_err(),
        "parent-only topics should still reject unrelated peers"
    );
}

#[tokio::test]
async fn message_to_nonexistent_agent_fails() {
    let (_dir, orch) = setup();
    let result = orch.send_message("user", "ghost-1234", "hello").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn user_is_valid_message_target_for_direct_messages() {
    let (_dir, orch) = setup();

    let agent = orch
        .start_topic("notifier", "echo", "", None, "mesh")
        .unwrap();
    assert!(orch.get_agent("user").unwrap().is_none());

    let mut rx = orch.subscribe();
    let msg = orch
        .send_message(&agent.id, "user", "user heads up")
        .await
        .unwrap();

    assert_eq!(msg.from_agent, agent.id);
    assert_eq!(msg.to_agent, "user");
    assert_eq!(msg.content, "user heads up");
    assert!(msg.delivered);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::UserNotification { from, content })
                    if from == agent.id && content == "user heads up" =>
                {
                    return;
                }
                Err(e) => panic!("event stream closed before notification arrived: {e}"),
                _ => continue,
            }
        }
    })
    .await
    .expect("timed out waiting for user notification event");

    let log = orch
        .get_agent_log(&agent.id, 50, LogFilter::Messages)
        .unwrap();
    assert!(
        log.iter().any(|entry| {
            entry.kind == "sent" && entry.peer == "user" && entry.content == "user heads up"
        }),
        "agent log should include the persisted user notification"
    );

    let user_log = orch.get_agent_log("user", 50, LogFilter::Messages).unwrap();
    assert!(
        user_log.iter().any(|entry| {
            entry.kind == "recv" && entry.peer == agent.id && entry.content == "user heads up"
        }),
        "user log should include direct responses sent to the user"
    );

    let user_matches = orch
        .search_agent_log("user", 50, LogFilter::Messages, Some("heads up"))
        .unwrap();
    assert_eq!(user_matches.len(), 1);

    let user_inbox = orch
        .search_inbox("user", Some(&agent.id), 50, None)
        .unwrap();
    assert_eq!(user_inbox.len(), 1);
    assert_eq!(user_inbox[0].kind, "recv");
    assert_eq!(user_inbox[0].peer, agent.id);
    assert_eq!(user_inbox[0].content, "user heads up");

    let user_output = orch.get_agent_log("user", 50, LogFilter::Output).unwrap();
    assert!(user_output.is_empty());

    let agent_events = orch.list_events(None, Some(&agent.id), 1000).unwrap();
    let notifications: Vec<_> = agent_events
        .iter()
        .filter(|event| event.event_type == "user_notification")
        .collect();
    assert_eq!(notifications.len(), 1);

    let payload: serde_json::Value = serde_json::from_str(&notifications[0].payload).unwrap();
    assert_eq!(payload["type"], "user_notification");
    assert_eq!(payload["from"], agent.id);
    assert_eq!(payload["content"], "user heads up");

    let routed = agent_events
        .iter()
        .find(|event| event.event_type == "message_routed")
        .expect("user-directed sends should also emit a routing event");
    let payload: serde_json::Value = serde_json::from_str(&routed.payload).unwrap();
    assert_eq!(payload["type"], "message_routed");
    assert_eq!(payload["from"], agent.id);
    assert_eq!(payload["to"], "user");
}

#[tokio::test]
async fn multiple_messages_processed_in_order() {
    let (_dir, orch, _addr) = setup_http_server().await;

    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic("orderer", "echo", "", None, "mesh")
        .unwrap();

    // Send 3 messages quickly
    for i in 0..3 {
        orch.send_message("user", &agent.id, &format!("msg-{i}"))
            .await
            .unwrap();
    }

    // Collect routed replies (messages may be batched into fewer harness turns).
    let mut all_replies = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::UserNotification { from, content }) if from == agent.id => {
                    all_replies.push_str(&content);
                    if all_replies.contains("msg-0")
                        && all_replies.contains("msg-1")
                        && all_replies.contains("msg-2")
                    {
                        return;
                    }
                }
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    assert!(all_replies.contains("msg-0"), "should contain msg-0");
    assert!(all_replies.contains("msg-1"), "should contain msg-1");
    assert!(all_replies.contains("msg-2"), "should contain msg-2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn message_interrupts_running_agent() {
    let (_dir, orch, _addr) = setup_http_server().await;
    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic("worker", "echo", "", None, "mesh")
        .unwrap();

    // Now send a message, then quickly send another to interrupt
    orch.send_message("user", &agent.id, "first task")
        .await
        .unwrap();

    // Small delay to let worker pick up the first message
    tokio::time::sleep(Duration::from_millis(50)).await;

    orch.send_message("user", &agent.id, "interrupt!")
        .await
        .unwrap();

    // Collect routed replies.
    let mut all_replies = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::UserNotification { from, content }) if from == agent.id => {
                    all_replies.push_str(&content);
                    if all_replies.contains("interrupt!") {
                        return;
                    }
                }
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    assert!(
        all_replies.contains("interrupt!"),
        "interrupt message should be processed"
    );
}

#[tokio::test]
async fn http_api_start_topic_and_list() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    let agents_dir = dir.path().join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();

    // Find a free port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let router = swarm::server::router(orch);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // Give server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // Start topic via HTTP
    let resp = client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "label": "tester",
            "harness": "echo",
            "system_prompt": "test agent",
            "comms": "mesh"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let agent: serde_json::Value = resp.json().await.unwrap();
    let agent_id = agent["id"].as_str().unwrap().to_string();
    assert!(agent_id.starts_with("tester-"));
    assert_eq!(agent["parent_id"].as_str(), Some("user"));

    // List via HTTP
    let resp = client
        .get(format!("{addr}/api/agents"))
        .send()
        .await
        .unwrap();
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["id"].as_str().unwrap(), agent_id);

    // Get single agent
    let resp = client
        .get(format!("{addr}/api/agents/{agent_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Send message via HTTP
    let resp = client
        .post(format!("{addr}/api/messages"))
        .json(&serde_json::json!({
            "from": "user",
            "to": agent_id,
            "content": "http test"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Kill via HTTP
    let resp = client
        .delete(format!("{addr}/api/agents/{agent_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Verify stopped
    let resp = client
        .get(format!("{addr}/api/agents"))
        .send()
        .await
        .unwrap();
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 0);

    let resp = client
        .get(format!("{addr}/api/agents?all=true"))
        .send()
        .await
        .unwrap();
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["id"].as_str().unwrap(), agent_id);
    assert_eq!(agents[0]["status"], "done");
}

#[tokio::test]
async fn http_api_health_returns_status_uptime_and_version() {
    let (_dir, _orch, addr) = setup_http_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{addr}/api/health"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert!(body["uptime"].as_u64().is_some());
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn http_api_stats_returns_counts() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    std::fs::create_dir_all(dir.path().join("agents")).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let orch = Arc::new(Orchestrator::new(
        db.clone(),
        HarnessRegistry::new(),
        "http://127.0.0.1:0".to_string(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));
    let addr = start_http_server(orch.clone()).await;

    let alive = orch.start_topic("alive", "echo", "", None, "mesh").unwrap();
    let done = orch.start_topic("done", "echo", "", None, "mesh").unwrap();
    orch.done_agent(&done.id, None).await.unwrap();

    db.enqueue_message(&MessageRow {
        id: "stats-message".into(),
        from_agent: "user".into(),
        to_agent: alive.id.clone(),
        content: "queued for stats".into(),
        delivered: false,
        created_at: "2026-01-01T00:00:00Z".into(),
        broadcast_id: None,
    })
    .unwrap();
    db.insert_output_log(&OutputLogRow {
        id: "stats-error".into(),
        agent_id: alive.id,
        content: "failed for stats".into(),
        kind: "error".into(),
        created_at: "2026-01-01T00:00:01Z".into(),
    })
    .unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/stats"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["total"], 2);
    assert_eq!(body["alive"], 1);
    assert_eq!(body["done"], 1);
    assert_eq!(body["messages"], 1);
    assert_eq!(body["errors"], 1);
}

#[tokio::test]
async fn http_api_agent_worktree_returns_git_details() {
    let (_dir, orch) = setup_with_git();
    let addr = start_http_server(orch.clone()).await;

    let agent = orch
        .start_topic_with_model("editor", "echo", None, "", None, "mesh", true)
        .unwrap();
    let worktree = orch
        .worktree_dir(&agent.id)
        .unwrap()
        .expect("worktree should exist");

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/agents/{}/worktree", agent.id))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["branch"], format!("swarm/{}", agent.id));
    assert_eq!(body["path"].as_str().unwrap(), worktree.to_string_lossy());
    assert_eq!(body["dirty"], false);
    assert_eq!(body["head"].as_str().unwrap().len(), 40);

    std::fs::write(worktree.join("README.md"), "# changed\n").unwrap();

    let resp = client
        .get(format!("{addr}/api/agents/{}/worktree", agent.id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["dirty"], true);
}

#[tokio::test]
async fn agent_preamble_is_concise_and_topic_focused() {
    let mut registry = HarnessRegistry::new();
    registry.register(ResumeProbeHarness);
    let (_dir, orch) = setup_with_registry(registry);
    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic("probe", "resume-probe", "inspect preamble", None, "mesh")
        .unwrap();

    let output = wait_for_agent_output(&mut rx, &agent.id, "Critical delivery rule").await;
    assert!(output.contains(
        "You are a durable swarm topic running inside an inter-harness coordination session"
    ));
    assert!(output.contains("Use the `swarm` CLI for all coordination"));
    assert!(output.contains("Terminal stdout is only process output"));
    assert!(output.contains("`swarm send parent"));
    assert!(output.contains("swarm watch-inbox"));
    assert!(output.contains("no separate delegation command exists"));
    assert!(output.contains("--- SWARM CONTEXT ---"));
    assert!(output.contains("Parent: user"));
    assert!(output.contains("--- TASK / INCOMING MESSAGES ---"));
    assert!(output.contains("Keep swarm messages concise"));
}

#[tokio::test]
async fn http_api_missing_agent_returns_json_error() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    let agents_dir = dir.path().join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let router = swarm::server::router(orch);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/agents/missing-agent"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "topic not found: missing-agent");
    assert!(body["hint"].as_str().unwrap().contains("swarm peers"));
}

#[tokio::test]
async fn http_api_agent_not_found_returns_json_error() {
    let (_dir, _orch, addr) = setup_http_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{addr}/api/agents/missing-agent"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "topic not found: missing-agent");
    assert_eq!(body["hint"], "run swarm peers to list topics");
}

#[tokio::test]
async fn http_api_send_to_done_agent_reactivates_it() {
    let (_dir, orch, addr) = setup_http_server().await;
    let client = reqwest::Client::new();

    let done_agent = orch.start_topic("done", "echo", "", None, "mesh").unwrap();
    orch.done_agent(&done_agent.id, None).await.unwrap();

    let resp = client
        .post(format!("{addr}/api/messages"))
        .json(&serde_json::json!({
            "from": "user",
            "to": done_agent.id,
            "content": "hello?"
        }))
        .send()
        .await
        .unwrap();

    assert!(resp.status().is_success());

    let fetched = orch.get_agent(&done_agent.id).unwrap().unwrap();
    assert_ne!(fetched.status, TopicStatus::Paused);
}

#[tokio::test]
async fn cli_send_to_done_agent_succeeds() {
    let (dir, orch, addr) = setup_http_server().await;
    let agent = orch.start_topic("done", "echo", "", None, "mesh").unwrap();
    orch.done_agent(&agent.id, None).await.unwrap();
    let agent_id = agent.id.clone();
    let project_dir = dir.path().to_path_buf();

    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_swarm"))
            .env("SWARM_SOCKET", addr)
            .env("SWARM_PROJECT_DIR", project_dir)
            .args(["send", &agent_id, "hello?"])
            .output()
            .unwrap()
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "send should reactivate a done agent: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("sent to"), "{stdout}");
}

#[tokio::test]
async fn unknown_harness_rejected() {
    let (_dir, orch) = setup();
    let result = orch.start_topic("test", "nonexistent-harness", "", None, "mesh");
    assert!(result.is_err());
}

#[tokio::test]
async fn start_topic_rejects_unsafe_labels() {
    let (dir, orch) = setup();

    for label in ["../../../etc/passwd", "foo;rm -rf /", ""] {
        let err = orch
            .start_topic(label, "echo", "", None, "mesh")
            .expect_err("unsafe label should be rejected");
        assert!(
            err.to_string().contains("invalid input: label"),
            "unexpected error for label {label:?}: {err}"
        );
    }

    assert!(
        std::fs::read_dir(dir.path().join("agents"))
            .unwrap()
            .next()
            .is_none(),
        "invalid labels should not create topic dirs"
    );
}

#[tokio::test]
async fn echo_agent_log_captures_received_and_sent_messages() {
    let (_dir, orch, _addr) = setup_http_server().await;

    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic("logger", "echo", "", None, "mesh")
        .unwrap();

    orch.send_message("user", &agent.id, "test message")
        .await
        .unwrap();

    // Wait for processing to complete
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentStatus {
                    status: TopicStatus::Idle,
                    ..
                }) => return,
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    // Check all log entries
    let all = orch.get_agent_log(&agent.id, 50, LogFilter::All).unwrap();
    assert!(
        all.len() >= 2,
        "should have at least a recv and sent message, got {}",
        all.len()
    );

    let recv_entries: Vec<_> = all.iter().filter(|e| e.kind == "recv").collect();
    assert_eq!(recv_entries.len(), 1);
    assert_eq!(recv_entries[0].content, "test message");
    assert_eq!(recv_entries[0].peer, "user");

    let sent_entries: Vec<_> = all.iter().filter(|e| e.kind == "sent").collect();
    assert_eq!(sent_entries.len(), 1);
    assert_eq!(sent_entries[0].content, "(echo) test message");
    assert_eq!(sent_entries[0].peer, "user");

    // Check messages-only filter
    let msgs = orch
        .get_agent_log(&agent.id, 50, LogFilter::Messages)
        .unwrap();
    assert!(msgs.iter().all(|e| e.kind == "recv" || e.kind == "sent"));

    // Check output-only filter
    let outs = orch
        .get_agent_log(&agent.id, 50, LogFilter::Output)
        .unwrap();
    assert!(outs.is_empty(), "echo should communicate via messages only");
}

#[tokio::test]
async fn http_api_agent_log() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    let agents_dir = dir.path().join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let mut rx = orch.subscribe();
    let router = swarm::server::router(orch);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // Start topic
    let resp = client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "label": "logtest",
            "harness": "echo",
            "system_prompt": "",
            "comms": "mesh"
        }))
        .send()
        .await
        .unwrap();
    let agent: serde_json::Value = resp.json().await.unwrap();
    let agent_id = agent["id"].as_str().unwrap().to_string();

    // Send message and wait for processing
    client
        .post(format!("{addr}/api/messages"))
        .json(&serde_json::json!({
            "from": "user",
            "to": agent_id,
            "content": "log test"
        }))
        .send()
        .await
        .unwrap();

    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentStatus {
                    status: TopicStatus::Idle,
                    ..
                }) => return,
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    // Fetch log via HTTP
    let resp = client
        .get(format!("{addr}/api/agents/{agent_id}/log"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let entries: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(entries.len() >= 2);

    // Test messages-only filter
    let resp = client
        .get(format!("{addr}/api/agents/{agent_id}/log?type=messages"))
        .send()
        .await
        .unwrap();
    let msgs: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(msgs
        .iter()
        .all(|e| e["kind"] == "recv" || e["kind"] == "sent"));

    let resp = client
        .post(format!("{addr}/api/messages"))
        .json(&serde_json::json!({
            "from": agent_id,
            "to": "user",
            "content": "http user reply"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(format!("{addr}/api/agents/user/log?type=messages"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let user_msgs: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(
        user_msgs.iter().any(|entry| {
            entry["kind"] == "recv"
                && entry["peer"] == agent_id
                && entry["content"] == "http user reply"
        }),
        "user log should include direct responses sent through the HTTP API"
    );

    let resp = client
        .get(format!(
            "{addr}/api/agents/user/inbox?from={agent_id}&n=5&q=http%20user%20reply"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let user_inbox: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(user_inbox.len(), 1);
    assert_eq!(user_inbox[0]["kind"], "recv");
    assert_eq!(user_inbox[0]["peer"], agent_id);
    assert_eq!(user_inbox[0]["content"], "http user reply");

    // Test limit param
    let resp = client
        .get(format!("{addr}/api/agents/{agent_id}/log?n=1"))
        .send()
        .await
        .unwrap();
    let limited: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(limited.len(), 1);

    // Test search param
    let resp = client
        .get(format!("{addr}/api/agents/{agent_id}/log?q=log%20test"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let search_matches: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(search_matches
        .iter()
        .all(|entry| entry["content"].as_str().unwrap().contains("log test")));

    // Fetch compact brief via HTTP
    let resp = client
        .get(format!("{addr}/api/agents/{agent_id}/brief?limit=5"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let brief: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(brief["id"].as_str(), Some(agent_id.as_str()));
    assert!(brief["recent_log"].as_array().is_some());
}

#[tokio::test]
async fn harness_error_surfaces_in_events_and_log() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    std::fs::create_dir_all(dir.path().join("agents")).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let mut registry = HarnessRegistry::new();
    registry
        .register(CliHarness::new(CliKind::Claude).with_binary("/nonexistent/binary".to_string()));

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        "http://127.0.0.1:0".to_string(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic("failbot", "claude", "test", None, "mesh")
        .unwrap();

    orch.send_message("user", &agent.id, "this will fail")
        .await
        .unwrap();

    // Wait for error event
    let mut saw_error_event = false;
    let mut saw_error_status = false;
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentError { error, .. }) => {
                    assert!(
                        error.contains("harness failed") || error.contains("failed to start"),
                        "error should mention topic start failure, got: {error}"
                    );
                    saw_error_event = true;
                }
                Ok(SwarmEvent::AgentStatus {
                    status: TopicStatus::Error,
                    ..
                }) => {
                    saw_error_status = true;
                    if saw_error_event {
                        return;
                    }
                }
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    assert!(
        saw_error_event,
        "should emit AgentError event on topic start failure"
    );
    assert!(
        saw_error_status,
        "agent status should transition to 'error'"
    );

    // Error should appear in agent log
    let log = orch.get_agent_log(&agent.id, 50, LogFilter::All).unwrap();
    let errors: Vec<_> = log.iter().filter(|e| e.kind == "error").collect();
    assert!(!errors.is_empty(), "error should be recorded in agent log");
    assert!(
        errors[0].content.contains("harness failed")
            || errors[0].content.contains("failed to start")
    );
}

#[tokio::test]
async fn start_topic_with_model_override() {
    let (_dir, orch) = setup();

    let agent = orch
        .start_topic_with_model(
            "modeler",
            "echo",
            Some("harness-supported-model"),
            "test model",
            None,
            "mesh",
            false,
        )
        .unwrap();
    assert_eq!(agent.model, "harness-supported-model");

    let fetched = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(fetched.model, "harness-supported-model");

    let default_agent = orch
        .start_topic("defaulter", "echo", "no model", None, "mesh")
        .unwrap();
    assert_eq!(default_agent.model, "");
}

#[tokio::test]
async fn perspective_shows_family_relations() {
    let (_dir, orch) = setup();

    let grandparent = orch
        .start_topic("grandparent", "echo", "", None, "mesh")
        .unwrap();
    let parent = orch
        .start_topic("parent", "echo", "", Some(&grandparent.id), "mesh")
        .unwrap();
    let child_a = orch
        .start_topic("child-a", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let child_b = orch
        .start_topic("child-b", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let grandchild = orch
        .start_topic("grandchild", "echo", "", Some(&child_a.id), "mesh")
        .unwrap();
    let unrelated = orch
        .start_topic("unrelated", "echo", "", None, "mesh")
        .unwrap();

    let views = orch.list_agents_with_perspective(&child_a.id).unwrap();

    let find_relation = |id: &str| -> String {
        views
            .iter()
            .find(|v| v.agent.id == id)
            .map(|v| v.relation.clone())
            .unwrap_or_else(|| "not found".to_string())
    };

    assert_eq!(find_relation(&child_a.id), "self");
    assert_eq!(find_relation(&parent.id), "parent");
    assert_eq!(find_relation(&child_b.id), "sibling");
    assert_eq!(find_relation(&grandchild.id), "child");
    assert_eq!(find_relation(&grandparent.id), "not found");
    assert_eq!(find_relation(&unrelated.id), "not found");
}

#[tokio::test]
async fn perspective_hides_done_agents() {
    let (_dir, orch) = setup();

    let alive = orch.start_topic("alive", "echo", "", None, "mesh").unwrap();
    let doomed = orch
        .start_topic("doomed", "echo", "", None, "mesh")
        .unwrap();
    orch.kill_agent(&doomed.id).await.unwrap();

    let views = orch.list_agents_with_perspective(&alive.id).unwrap();
    assert!(
        views.iter().all(|v| v.agent.status != TopicStatus::Paused),
        "perspective should not include done agents"
    );
    assert_eq!(views.len(), 1);
}

#[tokio::test]
async fn events_are_persisted_and_queryable() {
    let (_dir, orch) = setup();

    let agent = orch
        .start_topic("eventer", "echo", "", None, "mesh")
        .unwrap();

    orch.send_message("user", &agent.id, "event test")
        .await
        .unwrap();

    // Wait for processing
    let mut rx = orch.subscribe();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentStatus {
                    status: TopicStatus::Idle,
                    ..
                }) => return,
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    let all_events = orch.list_events(None, None, 1000).unwrap();
    assert!(
        all_events.len() >= 2,
        "should have at least topic start + status events, got {}",
        all_events.len()
    );

    let topic_started_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.event_type == "topic_started")
        .collect();
    assert_eq!(topic_started_events.len(), 1);

    let agent_events = orch.list_events(None, Some(&agent.id), 1000).unwrap();
    assert!(
        agent_events
            .iter()
            .all(|e| e.agent_id.as_deref() == Some(&agent.id)),
        "agent_id filter should only return events for that agent"
    );

    if let Some(first_event) = all_events.first() {
        let since_events = orch
            .list_events(Some(&first_event.created_at), None, 1000)
            .unwrap();
        assert!(!since_events.is_empty());
    }
}

#[tokio::test]
async fn http_api_models_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    std::fs::create_dir_all(dir.path().join("agents")).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let router = swarm::server::router(orch);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{addr}/api/models"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let models: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(models.len(), 5);

    let claude = models.iter().find(|m| m["harness"] == "claude").unwrap();
    assert_eq!(claude["default_model"], "CLI default");
    assert!(claude["models"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn http_api_events_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    std::fs::create_dir_all(dir.path().join("agents")).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let router = swarm::server::router(orch.clone());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // Start a topic to generate events
    client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "label": "evt-test",
            "harness": "echo",
            "system_prompt": "",
            "comms": "mesh"
        }))
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{addr}/api/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let events: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(!events.is_empty());
    assert!(events[0]["event_type"].as_str().is_some());
    assert!(events[0]["payload"].as_str().is_some());

    let resp = client
        .get(format!("{addr}/api/events?limit=1"))
        .send()
        .await
        .unwrap();
    let limited: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(limited.len(), 1);
}

#[tokio::test]
async fn http_api_start_topic_with_model() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    std::fs::create_dir_all(dir.path().join("agents")).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let router = swarm::server::router(orch);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "label": "model-test",
            "harness": "echo",
            "system_prompt": "",
            "comms": "mesh",
            "model": "harness-supported-model"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let agent: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(agent["model"], "harness-supported-model");
}

#[tokio::test]
async fn http_api_perspective_query() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    std::fs::create_dir_all(dir.path().join("agents")).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let router = swarm::server::router(orch.clone());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // Start parent and child topics
    let resp = client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "label": "parent",
            "harness": "echo",
            "system_prompt": "",
            "comms": "mesh"
        }))
        .send()
        .await
        .unwrap();
    let parent: serde_json::Value = resp.json().await.unwrap();
    let parent_id = parent["id"].as_str().unwrap().to_string();

    let resp = client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "label": "child",
            "harness": "echo",
            "system_prompt": "",
            "parent_id": parent_id,
            "comms": "mesh"
        }))
        .send()
        .await
        .unwrap();
    let child: serde_json::Value = resp.json().await.unwrap();
    let child_id = child["id"].as_str().unwrap().to_string();

    // Without perspective - returns flat AgentRow
    let resp = client
        .get(format!("{addr}/api/agents"))
        .send()
        .await
        .unwrap();
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 2);
    assert!(agents[0].get("relation").is_none());

    // With perspective - returns AgentView with relation
    let resp = client
        .get(format!("{addr}/api/agents?perspective={child_id}"))
        .send()
        .await
        .unwrap();
    let views: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(views.len(), 2);

    let child_view = views.iter().find(|v| v["id"] == child_id).unwrap();
    assert_eq!(child_view["relation"], "self");

    let parent_view = views.iter().find(|v| v["id"] == parent_id).unwrap();
    assert_eq!(parent_view["relation"], "parent");
}

#[tokio::test]
async fn done_agent_sends_message_to_parent() {
    let (_dir, orch) = setup();

    let parent = orch.start_topic("boss", "echo", "", None, "mesh").unwrap();
    let child = orch
        .start_topic("worker", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.done_agent(&child.id, Some("task complete"))
        .await
        .unwrap();

    let fetched = orch.get_agent(&child.id).unwrap().unwrap();
    assert_eq!(fetched.status, TopicStatus::Paused);

    let log = orch.get_agent_log(&parent.id, 50, LogFilter::All).unwrap();
    let recv: Vec<_> = log
        .iter()
        .filter(|e| e.kind == "recv" && e.content == "task complete")
        .collect();
    assert_eq!(recv.len(), 1, "parent should receive the done message");
}

#[tokio::test]
async fn done_agent_structured_report_is_available_in_brief() {
    let (_dir, orch) = setup();

    let parent = orch.start_topic("boss", "echo", "", None, "mesh").unwrap();
    let child = orch
        .start_topic(
            "worker",
            "echo",
            "large prompt body",
            Some(&parent.id),
            "mesh",
        )
        .unwrap();

    orch.done_agent_with_report(
        &child.id,
        DoneReport {
            summary: Some("implemented report".into()),
            outcome: Some("done".into()),
            deliverable: Some("branch swarm/worker".into()),
            checks: Some("cargo test".into()),
            risk: Some("browser not checked".into()),
            next_action: Some("review branch".into()),
        },
    )
    .await
    .unwrap();

    let parent_log = orch.get_agent_log(&parent.id, 50, LogFilter::All).unwrap();
    assert!(parent_log
        .iter()
        .any(|e| e.kind == "recv" && e.content == "implemented report"));

    let brief = orch.agent_brief(&child.id, 5, None).unwrap();
    assert_eq!(brief.prompt_chars, "large prompt body".len());
    let handover = brief.latest_handover.expect("handover should be stored");
    assert_eq!(handover.summary.as_deref(), Some("implemented report"));
    assert_eq!(handover.next_action.as_deref(), Some("review branch"));
}

#[tokio::test]
async fn done_agent_is_idempotent() {
    let (_dir, orch) = setup();

    let parent = orch.start_topic("boss", "echo", "", None, "mesh").unwrap();
    let child = orch
        .start_topic("worker", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.done_agent(&child.id, Some("first done"))
        .await
        .unwrap();
    orch.done_agent(&child.id, Some("second done"))
        .await
        .unwrap();

    let fetched = orch.get_agent(&child.id).unwrap().unwrap();
    assert_eq!(fetched.status, TopicStatus::Paused);

    let log = orch.get_agent_log(&parent.id, 50, LogFilter::All).unwrap();
    let first_messages = log
        .iter()
        .filter(|e| e.kind == "recv" && e.content == "first done")
        .count();
    let second_messages = log
        .iter()
        .filter(|e| e.kind == "recv" && e.content == "second done")
        .count();
    assert_eq!(first_messages, 1, "first done message should be sent once");
    assert_eq!(
        second_messages, 0,
        "second done call should not enqueue a parent message"
    );

    let done_events = orch
        .list_events(None, Some(&child.id), 1000)
        .unwrap()
        .into_iter()
        .filter(|e| e.event_type == "agent_done")
        .count();
    assert_eq!(done_events, 1, "done event should only be emitted once");
}

#[tokio::test]
async fn done_agent_without_parent_still_works() {
    let (_dir, orch) = setup();

    let agent = orch
        .start_topic("orphan", "echo", "", None, "mesh")
        .unwrap();

    orch.done_agent(&agent.id, Some("finished")).await.unwrap();

    let fetched = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(fetched.status, TopicStatus::Paused);
    assert!(fetched.ended_at.is_some(), "done should populate ended_at");
}

#[tokio::test]
async fn send_to_done_agent_resumes_conversation_and_can_done_again() {
    let mut registry = HarnessRegistry::new();
    registry.register(ResumeProbeHarness);
    let (_dir, orch) = setup_with_registry(registry);
    let mut rx = orch.subscribe();

    let parent = orch.start_topic("boss", "echo", "", None, "mesh").unwrap();
    let agent = orch
        .start_topic("worker", "resume-probe", "", Some(&parent.id), "mesh")
        .unwrap();
    let child = orch
        .start_topic("child", "echo", "", Some(&agent.id), "mesh")
        .unwrap();

    orch.send_message("user", &agent.id, "first task")
        .await
        .unwrap();
    let first = wait_for_agent_output(&mut rx, &agent.id, "first task").await;
    assert!(first.contains("resume=false"), "{first}");

    orch.done_agent(&agent.id, Some("first done"))
        .await
        .unwrap();
    let done_agent = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(done_agent.status, TopicStatus::Paused);
    assert!(
        done_agent.ended_at.is_some(),
        "done should populate ended_at"
    );
    assert_eq!(
        orch.get_agent(&child.id)
            .unwrap()
            .unwrap()
            .parent_id
            .as_deref(),
        Some(agent.id.as_str()),
        "child relationship should survive while parent is done"
    );

    orch.send_message("user", &agent.id, "second task")
        .await
        .unwrap();
    let resumed = wait_for_agent_output(&mut rx, &agent.id, "second task").await;
    assert!(resumed.contains("resume=true"), "{resumed}");

    let fetched = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_ne!(fetched.status, TopicStatus::Paused);
    assert_eq!(
        fetched.ended_at.as_deref(),
        None,
        "resuming a done agent should clear ended_at"
    );
    assert_eq!(fetched.parent_id.as_deref(), Some(parent.id.as_str()));
    assert_eq!(
        orch.get_agent(&child.id)
            .unwrap()
            .unwrap()
            .parent_id
            .as_deref(),
        Some(agent.id.as_str()),
        "child relationship should survive after resume"
    );

    let log = orch.get_agent_log(&agent.id, 100, LogFilter::All).unwrap();
    assert!(log.iter().any(|e| e.content.contains("first task")));
    assert!(log.iter().any(|e| e.content.contains("second task")));

    orch.done_agent(&agent.id, Some("second done"))
        .await
        .unwrap();
    assert_eq!(
        orch.get_agent(&agent.id).unwrap().unwrap().status,
        TopicStatus::Paused
    );

    let done_events = orch
        .list_events(None, Some(&agent.id), 1000)
        .unwrap()
        .into_iter()
        .filter(|e| e.event_type == "agent_done")
        .count();
    assert_eq!(
        done_events, 2,
        "agent should be able to finish again after resume"
    );
}

#[tokio::test]
async fn kill_preserves_child_parent_links() {
    let (_dir, orch) = setup();

    let grandparent = orch.start_topic("gp", "echo", "", None, "mesh").unwrap();
    let parent = orch
        .start_topic("parent", "echo", "", Some(&grandparent.id), "mesh")
        .unwrap();
    let child_a = orch
        .start_topic("child-a", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let child_b = orch
        .start_topic("child-b", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.kill_agent(&parent.id).await.unwrap();

    let a = orch.get_agent(&child_a.id).unwrap().unwrap();
    assert_eq!(
        a.parent_id.as_deref(),
        Some(parent.id.as_str()),
        "child should keep its parent link after kill"
    );

    let b = orch.get_agent(&child_b.id).unwrap().unwrap();
    assert_eq!(
        b.parent_id.as_deref(),
        Some(parent.id.as_str()),
        "child should keep its parent link after kill"
    );

    let p = orch.get_agent(&parent.id).unwrap().unwrap();
    assert_eq!(p.parent_id.as_deref(), Some(grandparent.id.as_str()));
    assert_eq!(p.status, TopicStatus::Paused);
}

#[tokio::test]
async fn done_preserves_child_parent_links() {
    let (_dir, orch) = setup();

    let grandparent = orch.start_topic("gp", "echo", "", None, "mesh").unwrap();
    let parent = orch
        .start_topic("parent", "echo", "", Some(&grandparent.id), "mesh")
        .unwrap();
    let child = orch
        .start_topic("child", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.done_agent(&parent.id, None).await.unwrap();

    let c = orch.get_agent(&child.id).unwrap().unwrap();
    assert_eq!(
        c.parent_id.as_deref(),
        Some(parent.id.as_str()),
        "child should keep its parent link after parent done"
    );

    let p = orch.get_agent(&parent.id).unwrap().unwrap();
    assert_eq!(p.parent_id.as_deref(), Some(grandparent.id.as_str()));
    assert_eq!(p.status, TopicStatus::Paused);
}

#[tokio::test]
async fn kill_root_agent_preserves_child_parent_link() {
    let (_dir, orch) = setup();

    let root = orch.start_topic("root", "echo", "", None, "mesh").unwrap();
    let child = orch
        .start_topic("child", "echo", "", Some(&root.id), "mesh")
        .unwrap();

    orch.kill_agent(&root.id).await.unwrap();

    let c = orch.get_agent(&child.id).unwrap().unwrap();
    assert_eq!(
        c.parent_id.as_deref(),
        Some(root.id.as_str()),
        "child should keep its root parent link after kill"
    );
}

#[tokio::test]
async fn done_agent_hidden_from_active_views_until_message_reactivates() {
    let (_dir, orch) = setup();

    let parent = orch
        .start_topic("parent", "echo", "", None, "mesh")
        .unwrap();
    let child = orch
        .start_topic("child", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.done_agent(&child.id, None).await.unwrap();

    let active_agents = orch.list_agents().unwrap();
    assert!(
        active_agents.iter().all(|a| a.id != child.id),
        "done agents should not appear in active agent lists"
    );

    let views = orch.list_agents_with_perspective(&parent.id).unwrap();
    let child_view = views.iter().find(|v| v.agent.id == child.id);
    assert!(
        child_view.is_none(),
        "done agents should not appear in active perspective views"
    );

    orch.send_message("user", &child.id, "are you there?")
        .await
        .unwrap();

    let fetched = orch.get_agent(&child.id).unwrap().unwrap();
    assert_ne!(fetched.status, TopicStatus::Paused);
}

#[tokio::test]
async fn worktree_creates_isolated_branch() {
    let (_dir, orch) = setup_with_git();

    let agent = orch
        .start_topic_with_model("editor", "echo", None, "edit files", None, "mesh", true)
        .unwrap();
    let branch_name = format!("swarm/{}", agent.id);
    assert_eq!(agent.worktree_branch.as_deref(), Some(branch_name.as_str()));
    assert_eq!(
        orch.get_agent(&agent.id)
            .unwrap()
            .unwrap()
            .worktree_branch
            .as_deref(),
        Some(branch_name.as_str())
    );

    let worktree = orch.worktree_dir(&agent.id).unwrap();
    assert!(
        worktree.is_some(),
        "worktree dir should exist after topic start"
    );
    let wt = worktree.unwrap();
    assert!(
        wt.join("README.md").exists(),
        "worktree should have project files"
    );

    // Check the branch was created
    let output = std::process::Command::new("git")
        .args(["branch", "--list", &format!("swarm/{}", agent.id)])
        .current_dir(&wt)
        .output()
        .unwrap();
    let branches = String::from_utf8_lossy(&output.stdout);
    assert!(
        branches.contains(&branch_name),
        "branch swarm/{} should exist",
        agent.id
    );
}

#[tokio::test]
async fn resumed_agent_keeps_worktree_branch() {
    let mut registry = HarnessRegistry::new();
    registry.register(ResumeProbeHarness);
    let (_dir, orch) = setup_with_git_registry(registry);
    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic_with_model("editor", "resume-probe", None, "", None, "mesh", true)
        .unwrap();

    let worktree = orch
        .worktree_dir(&agent.id)
        .unwrap()
        .expect("worktree should exist");

    orch.done_agent(&agent.id, None).await.unwrap();
    orch.send_message("user", &agent.id, "resume on same branch")
        .await
        .unwrap();

    let output = wait_for_agent_output(&mut rx, &agent.id, "resume on same branch").await;
    assert!(output.contains("resume on same branch"), "{output}");

    assert_eq!(
        orch.worktree_dir(&agent.id).unwrap().as_deref(),
        Some(worktree.as_path())
    );
    let branch_name = format!("swarm/{}", agent.id);
    let output = std::process::Command::new("git")
        .args(["branch", "--show-current"])
        .current_dir(&worktree)
        .output()
        .unwrap();
    let current_branch = String::from_utf8_lossy(&output.stdout);
    assert_eq!(current_branch.trim(), branch_name);
}

#[tokio::test]
async fn no_worktree_by_default() {
    let (_dir, orch) = setup_with_git();

    let agent = orch
        .start_topic("reviewer", "echo", "review code", None, "mesh")
        .unwrap();
    assert_eq!(agent.worktree_branch, None);

    let worktree = orch.worktree_dir(&agent.id).unwrap();
    assert!(
        worktree.is_none(),
        "no worktree should exist for default topic start"
    );
}

#[tokio::test]
async fn cleanup_removes_worktree() {
    let (_dir, orch) = setup_with_git();

    let agent = orch
        .start_topic_with_model("cleaner", "echo", None, "edit", None, "mesh", true)
        .unwrap();

    assert!(orch.worktree_dir(&agent.id).unwrap().is_some());

    orch.cleanup_agent(&agent.id, false).unwrap();
    assert!(
        orch.worktree_dir(&agent.id).unwrap().is_none(),
        "worktree should be gone after cleanup"
    );
    let agent = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(
        agent.worktree_branch, None,
        "cleanup should clear stale worktree metadata"
    );
}

#[tokio::test]
async fn cleanup_and_worktree_lookup_reject_unsafe_agent_ids() {
    let (_dir, orch) = setup();

    for agent_id in ["../../../etc/passwd", "foo;rm -rf /", ""] {
        let err = orch
            .cleanup_agent(agent_id, false)
            .expect_err("unsafe agent_id should be rejected by cleanup");
        assert!(
            err.to_string().contains("invalid input: agent_id"),
            "unexpected cleanup error for agent_id {agent_id:?}: {err}"
        );

        let err = orch
            .worktree_dir(agent_id)
            .expect_err("unsafe agent_id should be rejected by worktree lookup");
        assert!(
            err.to_string().contains("invalid input: agent_id"),
            "unexpected worktree lookup error for agent_id {agent_id:?}: {err}"
        );
    }
}

#[tokio::test]
async fn cleanup_with_branch_delete() {
    let (_dir, orch) = setup_with_git();

    let agent = orch
        .start_topic_with_model("brancher", "echo", None, "edit", None, "mesh", true)
        .unwrap();

    let branch_name = format!("swarm/{}", agent.id);

    orch.cleanup_agent(&agent.id, true).unwrap();
    let agent = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(
        agent.worktree_branch, None,
        "cleanup should clear stale worktree metadata"
    );

    // Verify branch is gone too
    let output = std::process::Command::new("git")
        .args(["branch", "--list", &branch_name])
        .current_dir(_dir.path().join("project"))
        .output()
        .unwrap();
    let branches = String::from_utf8_lossy(&output.stdout);
    assert!(
        !branches.contains(&branch_name),
        "branch should be deleted with --delete-branch"
    );
}

#[tokio::test]
async fn cleanup_noop_without_worktree() {
    let (_dir, orch) = setup();

    let agent = orch.start_topic("plain", "echo", "", None, "mesh").unwrap();

    // Should not error even though there's no worktree
    orch.cleanup_agent(&agent.id, false).unwrap();
}

#[tokio::test]
async fn broadcast_family_sends_to_parent_siblings_and_children() {
    let (_dir, orch) = setup();

    let grandparent = orch
        .start_topic("grandparent", "echo", "", None, "mesh")
        .unwrap();
    let parent = orch
        .start_topic("parent", "echo", "", Some(&grandparent.id), "mesh")
        .unwrap();
    let sender = orch
        .start_topic("sender", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let sibling = orch
        .start_topic("sibling", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let child_a = orch
        .start_topic("child-a", "echo", "", Some(&sender.id), "mesh")
        .unwrap();
    let child_b = orch
        .start_topic("child-b", "echo", "", Some(&sender.id), "mesh")
        .unwrap();
    let _unrelated = orch
        .start_topic("unrelated", "echo", "", None, "mesh")
        .unwrap();

    let msgs = orch
        .broadcast_family(&sender.id, "family update")
        .await
        .unwrap();

    let targets: Vec<&str> = msgs.iter().map(|m| m.to_agent.as_str()).collect();
    assert!(
        targets.contains(&parent.id.as_str()),
        "should send to parent"
    );
    assert!(
        targets.contains(&sibling.id.as_str()),
        "should send to sibling"
    );
    assert!(
        targets.contains(&child_a.id.as_str()),
        "should send to child a"
    );
    assert!(
        targets.contains(&child_b.id.as_str()),
        "should send to child b"
    );
    assert_eq!(
        targets.len(),
        4,
        "should send to exactly parent + sibling + 2 children"
    );

    assert!(
        !targets.contains(&sender.id.as_str()),
        "should not send to self"
    );
    assert!(
        !targets.contains(&grandparent.id.as_str()),
        "should not send to grandparent"
    );
    assert!(!targets.contains(&"user"), "should not send to user");

    let broadcast_id = msgs[0]
        .broadcast_id
        .as_ref()
        .expect("should have broadcast_id");
    for msg in &msgs {
        assert_eq!(msg.from_agent, sender.id);
        assert_eq!(msg.content, "family update");
        assert_eq!(
            msg.broadcast_id.as_ref(),
            Some(broadcast_id),
            "all messages in a broadcast should share the same broadcast_id"
        );
    }
}

#[tokio::test]
async fn broadcast_family_deduplicates_in_sender_log() {
    let (_dir, orch) = setup();

    let parent = orch
        .start_topic("parent", "echo", "", None, "mesh")
        .unwrap();
    let sender = orch
        .start_topic("sender", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let _child = orch
        .start_topic("child", "echo", "", Some(&sender.id), "mesh")
        .unwrap();

    orch.broadcast_family(&sender.id, "heads up").await.unwrap();

    let log = orch
        .get_agent_log(&sender.id, 50, LogFilter::Messages)
        .unwrap();
    let sent: Vec<_> = log.iter().filter(|e| e.kind == "sent").collect();
    assert_eq!(
        sent.len(),
        1,
        "broadcast should show as single entry in sender log, got {}",
        sent.len()
    );
    assert_eq!(sent[0].content, "heads up");
    assert!(
        sent[0].broadcast_id.is_some(),
        "sent entry should have broadcast_id"
    );
    assert_eq!(
        sent[0].broadcast_count,
        Some(2),
        "broadcast_count should reflect number of recipients"
    );
}

#[tokio::test]
async fn broadcast_family_excludes_done_agents() {
    let (_dir, orch) = setup();

    let parent = orch
        .start_topic("parent", "echo", "", None, "mesh")
        .unwrap();
    let sender = orch
        .start_topic("sender", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let sibling = orch
        .start_topic("sibling", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.done_agent(&sibling.id, None).await.unwrap();

    let msgs = orch.broadcast_family(&sender.id, "update").await.unwrap();

    let targets: Vec<&str> = msgs.iter().map(|m| m.to_agent.as_str()).collect();
    assert!(
        targets.contains(&parent.id.as_str()),
        "should send to active parent"
    );
    assert!(
        !targets.contains(&sibling.id.as_str()),
        "should not send to done sibling"
    );
}

#[tokio::test]
async fn broadcast_family_returns_empty_for_isolated_agent() {
    let (_dir, orch) = setup();

    let agent = orch.start_topic("loner", "echo", "", None, "mesh").unwrap();

    let msgs = orch.broadcast_family(&agent.id, "hello?").await.unwrap();
    assert!(
        msgs.is_empty(),
        "isolated agent has no family to broadcast to"
    );
}

#[tokio::test]
async fn broadcast_family_http_endpoint() {
    let (_dir, orch, addr) = setup_http_server().await;
    let client = reqwest::Client::new();

    let parent = orch
        .start_topic("parent", "echo", "", None, "mesh")
        .unwrap();
    let sender = orch
        .start_topic("sender", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let child = orch
        .start_topic("child", "echo", "", Some(&sender.id), "mesh")
        .unwrap();

    let resp = client
        .post(format!("{addr}/api/messages/family"))
        .json(&serde_json::json!({
            "from": sender.id,
            "content": "family broadcast via http"
        }))
        .send()
        .await
        .unwrap();

    assert!(resp.status().is_success());
    let msgs: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(msgs.len(), 2);

    let targets: Vec<&str> = msgs.iter().filter_map(|m| m["to_agent"].as_str()).collect();
    assert!(targets.contains(&parent.id.as_str()));
    assert!(targets.contains(&child.id.as_str()));
}

#[tokio::test]
async fn broadcast_family_preamble_mentions_send_family() {
    let mut registry = HarnessRegistry::new();
    registry.register(ResumeProbeHarness);
    let (_dir, orch) = setup_with_registry(registry);
    let mut rx = orch.subscribe();

    let agent = orch
        .start_topic("probe", "resume-probe", "check preamble", None, "mesh")
        .unwrap();

    let output = wait_for_agent_output(&mut rx, &agent.id, "send-family").await;
    assert!(
        output.contains("swarm send-family"),
        "preamble should mention send-family command"
    );
}

use std::sync::Arc;
use std::time::Duration;
use swarm::db::{Db, LogFilter};
use swarm::harness::{CliHarness, CliKind, HarnessRegistry};
use swarm::orchestrator::{Orchestrator, SwarmEvent};

fn setup() -> (tempfile::TempDir, Arc<Orchestrator>) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    let agents_dir = dir.path().join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let registry = HarnessRegistry::new();
    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        "http://127.0.0.1:0".to_string(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));
    (dir, orch)
}

fn setup_with_git() -> (tempfile::TempDir, Arc<Orchestrator>) {
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
    let registry = HarnessRegistry::new();
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
async fn spawn_and_list_agents() {
    let (_dir, orch) = setup();

    let a = orch
        .spawn_agent("researcher", "echo", "find things", None, "mesh")
        .unwrap();
    assert!(a.id.starts_with("researcher-"));
    assert_eq!(a.harness, "echo");
    assert_eq!(a.status, "idle");

    let b = orch
        .spawn_agent("writer", "echo", "write things", Some(&a.id), "mesh")
        .unwrap();
    assert_eq!(b.parent_id.as_deref(), Some(a.id.as_str()));

    let agents = orch.list_agents().unwrap();
    assert_eq!(agents.len(), 2);
}

#[tokio::test]
async fn echo_agent_processes_message() {
    let (_dir, orch) = setup();

    let mut rx = orch.subscribe();

    let agent = orch
        .spawn_agent("tester", "echo", "", None, "mesh")
        .unwrap();

    orch.send_message("user", &agent.id, "hello world")
        .await
        .unwrap();

    // Wait for the echo harness to process and emit output
    let saw_output = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentOutput { text, .. }) => {
                    if text.contains("hello world") {
                        return true;
                    }
                }
                Err(_) => return false,
                _ => continue,
            }
        }
    })
    .await;

    assert_eq!(saw_output, Ok(true), "echo agent should process the message");
}

#[tokio::test]
async fn agent_status_transitions() {
    let (_dir, orch) = setup();

    let mut rx = orch.subscribe();

    let agent = orch
        .spawn_agent("worker", "echo", "", None, "mesh")
        .unwrap();
    assert_eq!(agent.status, "idle");

    orch.send_message("user", &agent.id, "do something")
        .await
        .unwrap();

    let mut saw_working = false;
    let mut saw_idle_after = false;

    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentStatus { status, .. }) => {
                    if status == "working" {
                        saw_working = true;
                    }
                    if status == "idle" && saw_working {
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
    assert!(saw_idle_after, "agent should return to idle after processing");
}

#[tokio::test]
async fn kill_agent() {
    let (_dir, orch) = setup();

    let agent = orch
        .spawn_agent("doomed", "echo", "", None, "mesh")
        .unwrap();
    assert_eq!(orch.list_agents().unwrap().len(), 1);

    orch.kill_agent(&agent.id).await.unwrap();

    // Dead agents are hidden from list
    assert_eq!(orch.list_agents().unwrap().len(), 0);

    // But still fetchable directly (marked dead)
    let fetched = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(fetched.status, "dead");
}

#[tokio::test]
async fn parent_only_comms_enforced() {
    let (_dir, orch) = setup();

    let parent = orch
        .spawn_agent("boss", "echo", "", None, "mesh")
        .unwrap();
    let child = orch
        .spawn_agent("worker", "echo", "", Some(&parent.id), "parent-only")
        .unwrap();

    // Parent can message child
    let result = orch.send_message(&parent.id, &child.id, "do this").await;
    assert!(result.is_ok());

    // Create a sibling that tries to message the child
    let sibling = orch
        .spawn_agent("sibling", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let result = orch.send_message(&sibling.id, &child.id, "hey").await;
    assert!(result.is_err(), "sibling should not be able to message parent-only child");

    // User can always message (special sender)
    let result = orch.send_message("user", &child.id, "override").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn message_to_nonexistent_agent_fails() {
    let (_dir, orch) = setup();
    let result = orch.send_message("user", "ghost-1234", "hello").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn multiple_messages_processed_in_order() {
    let (_dir, orch) = setup();

    let mut rx = orch.subscribe();

    let agent = orch
        .spawn_agent("orderer", "echo", "", None, "mesh")
        .unwrap();

    // Send 3 messages quickly
    for i in 0..3 {
        orch.send_message("user", &agent.id, &format!("msg-{i}"))
            .await
            .unwrap();
    }

    // Collect output in order
    let mut outputs = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentOutput { text, .. }) => {
                    if text.contains("msg-") {
                        outputs.push(text);
                        if outputs.len() >= 3 {
                            return;
                        }
                    }
                }
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    assert_eq!(outputs.len(), 3, "should receive all 3 echo responses");
    assert!(outputs[0].contains("msg-0"));
    assert!(outputs[1].contains("msg-1"));
    assert!(outputs[2].contains("msg-2"));
}

#[tokio::test]
async fn http_api_spawn_and_list() {
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

    // Spawn via HTTP
    let resp = client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "role": "tester",
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

    // Verify killed
    let resp = client
        .get(format!("{addr}/api/agents"))
        .send()
        .await
        .unwrap();
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 0);
}

#[tokio::test]
async fn unknown_harness_rejected() {
    let (_dir, orch) = setup();
    let result = orch.spawn_agent("test", "nonexistent-harness", "", None, "mesh");
    assert!(result.is_err());
}

#[tokio::test]
async fn echo_agent_log_captures_messages_and_output() {
    let (_dir, orch) = setup();

    let mut rx = orch.subscribe();

    let agent = orch
        .spawn_agent("logger", "echo", "", None, "mesh")
        .unwrap();

    orch.send_message("user", &agent.id, "test message")
        .await
        .unwrap();

    // Wait for processing to complete
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentStatus { status, .. }) if status == "idle" => return,
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    // Check all log entries
    let all = orch
        .get_agent_log(&agent.id, 50, LogFilter::All)
        .unwrap();
    assert!(all.len() >= 2, "should have at least a recv message and output, got {}", all.len());

    let recv_entries: Vec<_> = all.iter().filter(|e| e.kind == "recv").collect();
    assert_eq!(recv_entries.len(), 1);
    assert_eq!(recv_entries[0].content, "test message");
    assert_eq!(recv_entries[0].peer, "user");

    let output_entries: Vec<_> = all.iter().filter(|e| e.kind == "output").collect();
    assert_eq!(output_entries.len(), 1);
    assert!(output_entries[0].content.contains("test message"));

    // Check messages-only filter
    let msgs = orch
        .get_agent_log(&agent.id, 50, LogFilter::Messages)
        .unwrap();
    assert!(msgs.iter().all(|e| e.kind == "recv" || e.kind == "sent"));

    // Check output-only filter
    let outs = orch
        .get_agent_log(&agent.id, 50, LogFilter::Output)
        .unwrap();
    assert!(outs.iter().all(|e| e.kind == "output" || e.kind == "error" || e.kind == "timeout"));
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

    // Spawn agent
    let resp = client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "role": "logtest",
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
                Ok(SwarmEvent::AgentStatus { status, .. }) if status == "idle" => return,
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

    // Test limit param
    let resp = client
        .get(format!("{addr}/api/agents/{agent_id}/log?n=1"))
        .send()
        .await
        .unwrap();
    let limited: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(limited.len(), 1);
}

#[tokio::test]
async fn harness_error_surfaces_in_events_and_log() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("swarm.db");
    std::fs::create_dir_all(dir.path().join("agents")).unwrap();

    let db = Arc::new(Db::open(&db_path).unwrap());
    let mut registry = HarnessRegistry::new();
    registry.register(
        CliHarness::new(CliKind::Claude).with_binary("/nonexistent/binary".to_string()),
    );

    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        "http://127.0.0.1:0".to_string(),
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
    ));

    let mut rx = orch.subscribe();

    let agent = orch
        .spawn_agent("failbot", "claude", "test", None, "mesh")
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
                        error.contains("harness failed") || error.contains("spawn"),
                        "error should mention spawn failure, got: {error}"
                    );
                    saw_error_event = true;
                }
                Ok(SwarmEvent::AgentStatus { status, .. }) if status == "error" => {
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

    assert!(saw_error_event, "should emit AgentError event on spawn failure");
    assert!(saw_error_status, "agent status should transition to 'error'");

    // Error should appear in agent log
    let log = orch
        .get_agent_log(&agent.id, 50, LogFilter::All)
        .unwrap();
    let errors: Vec<_> = log.iter().filter(|e| e.kind == "error").collect();
    assert!(
        !errors.is_empty(),
        "error should be recorded in agent log"
    );
    assert!(errors[0].content.contains("harness failed") || errors[0].content.contains("spawn"));
}

#[tokio::test]
async fn spawn_with_model_override() {
    let (_dir, orch) = setup();

    let agent = orch
        .spawn_agent_with_model(
            "modeler",
            "echo",
            Some("claude-sonnet-4-6"),
            "test model",
            None,
            "mesh",
            false,
        )
        .unwrap();
    assert_eq!(agent.model, "claude-sonnet-4-6");

    let fetched = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(fetched.model, "claude-sonnet-4-6");

    let default_agent = orch
        .spawn_agent("defaulter", "echo", "no model", None, "mesh")
        .unwrap();
    assert_eq!(default_agent.model, "");
}

#[tokio::test]
async fn perspective_shows_family_relations() {
    let (_dir, orch) = setup();

    let grandparent = orch
        .spawn_agent("grandparent", "echo", "", None, "mesh")
        .unwrap();
    let parent = orch
        .spawn_agent("parent", "echo", "", Some(&grandparent.id), "mesh")
        .unwrap();
    let child_a = orch
        .spawn_agent("child-a", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let child_b = orch
        .spawn_agent("child-b", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let grandchild = orch
        .spawn_agent("grandchild", "echo", "", Some(&child_a.id), "mesh")
        .unwrap();
    let unrelated = orch
        .spawn_agent("unrelated", "echo", "", None, "mesh")
        .unwrap();

    let views = orch
        .list_agents_with_perspective(&child_a.id)
        .unwrap();

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
async fn perspective_hides_dead_agents() {
    let (_dir, orch) = setup();

    let alive = orch
        .spawn_agent("alive", "echo", "", None, "mesh")
        .unwrap();
    let doomed = orch
        .spawn_agent("doomed", "echo", "", None, "mesh")
        .unwrap();
    orch.kill_agent(&doomed.id).await.unwrap();

    let views = orch
        .list_agents_with_perspective(&alive.id)
        .unwrap();
    assert!(
        views.iter().all(|v| v.agent.status != "dead"),
        "perspective should not include dead agents"
    );
    assert_eq!(views.len(), 1);
}

#[tokio::test]
async fn events_are_persisted_and_queryable() {
    let (_dir, orch) = setup();

    let agent = orch
        .spawn_agent("eventer", "echo", "", None, "mesh")
        .unwrap();

    orch.send_message("user", &agent.id, "event test")
        .await
        .unwrap();

    // Wait for processing
    let mut rx = orch.subscribe();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::AgentStatus { status, .. }) if status == "idle" => return,
                Err(_) => return,
                _ => continue,
            }
        }
    })
    .await;

    let all_events = orch.list_events(None, None, 1000).unwrap();
    assert!(
        all_events.len() >= 2,
        "should have at least spawn + status events, got {}",
        all_events.len()
    );

    let spawn_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.event_type == "agent_spawned")
        .collect();
    assert_eq!(spawn_events.len(), 1);

    let agent_events = orch
        .list_events(None, Some(&agent.id), 1000)
        .unwrap();
    assert!(
        agent_events.iter().all(|e| e.agent_id.as_deref() == Some(&agent.id)),
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
    assert_eq!(models.len(), 4);

    let claude = models.iter().find(|m| m["harness"] == "claude").unwrap();
    assert_eq!(claude["default_model"], "claude-opus-4-6");
    assert!(claude["models"].as_array().unwrap().len() >= 3);
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

    // Spawn an agent to generate events
    client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "role": "evt-test",
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
async fn http_api_spawn_with_model() {
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
            "role": "model-test",
            "harness": "echo",
            "system_prompt": "",
            "comms": "mesh",
            "model": "claude-sonnet-4-6"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let agent: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(agent["model"], "claude-sonnet-4-6");
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

    // Spawn parent and child
    let resp = client
        .post(format!("{addr}/api/agents"))
        .json(&serde_json::json!({
            "role": "parent",
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
            "role": "child",
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

    let parent = orch
        .spawn_agent("boss", "echo", "", None, "mesh")
        .unwrap();
    let child = orch
        .spawn_agent("worker", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.done_agent(&child.id, Some("task complete")).await.unwrap();

    let fetched = orch.get_agent(&child.id).unwrap().unwrap();
    assert_eq!(fetched.status, "done");

    let log = orch
        .get_agent_log(&parent.id, 50, LogFilter::All)
        .unwrap();
    let recv: Vec<_> = log.iter().filter(|e| e.kind == "recv" && e.content == "task complete").collect();
    assert_eq!(recv.len(), 1, "parent should receive the done message");
}

#[tokio::test]
async fn done_agent_without_parent_still_works() {
    let (_dir, orch) = setup();

    let agent = orch
        .spawn_agent("orphan", "echo", "", None, "mesh")
        .unwrap();

    orch.done_agent(&agent.id, Some("finished")).await.unwrap();

    let fetched = orch.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(fetched.status, "done");
}

#[tokio::test]
async fn kill_reparents_children() {
    let (_dir, orch) = setup();

    let grandparent = orch
        .spawn_agent("gp", "echo", "", None, "mesh")
        .unwrap();
    let parent = orch
        .spawn_agent("parent", "echo", "", Some(&grandparent.id), "mesh")
        .unwrap();
    let child_a = orch
        .spawn_agent("child-a", "echo", "", Some(&parent.id), "mesh")
        .unwrap();
    let child_b = orch
        .spawn_agent("child-b", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.kill_agent(&parent.id).await.unwrap();

    let a = orch.get_agent(&child_a.id).unwrap().unwrap();
    assert_eq!(a.parent_id.as_deref(), Some(grandparent.id.as_str()),
        "child should be re-parented to grandparent");

    let b = orch.get_agent(&child_b.id).unwrap().unwrap();
    assert_eq!(b.parent_id.as_deref(), Some(grandparent.id.as_str()),
        "child should be re-parented to grandparent");
}

#[tokio::test]
async fn done_reparents_children() {
    let (_dir, orch) = setup();

    let grandparent = orch
        .spawn_agent("gp", "echo", "", None, "mesh")
        .unwrap();
    let parent = orch
        .spawn_agent("parent", "echo", "", Some(&grandparent.id), "mesh")
        .unwrap();
    let child = orch
        .spawn_agent("child", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.done_agent(&parent.id, None).await.unwrap();

    let c = orch.get_agent(&child.id).unwrap().unwrap();
    assert_eq!(c.parent_id.as_deref(), Some(grandparent.id.as_str()),
        "child should be re-parented to grandparent after parent done");

    let p = orch.get_agent(&parent.id).unwrap().unwrap();
    assert_eq!(p.status, "done");
}

#[tokio::test]
async fn kill_root_agent_orphans_children() {
    let (_dir, orch) = setup();

    let root = orch
        .spawn_agent("root", "echo", "", None, "mesh")
        .unwrap();
    let child = orch
        .spawn_agent("child", "echo", "", Some(&root.id), "mesh")
        .unwrap();

    orch.kill_agent(&root.id).await.unwrap();

    let c = orch.get_agent(&child.id).unwrap().unwrap();
    assert_eq!(c.parent_id, None,
        "child of root should become a root agent when root is killed");
}

#[tokio::test]
async fn done_agent_visible_in_perspective() {
    let (_dir, orch) = setup();

    let parent = orch
        .spawn_agent("parent", "echo", "", None, "mesh")
        .unwrap();
    let child = orch
        .spawn_agent("child", "echo", "", Some(&parent.id), "mesh")
        .unwrap();

    orch.done_agent(&child.id, None).await.unwrap();

    let views = orch.list_agents_with_perspective(&parent.id).unwrap();
    let child_view = views.iter().find(|v| v.agent.id == child.id);
    assert!(child_view.is_some(), "done agents should still be visible in perspective");
    assert_eq!(child_view.unwrap().agent.status, "done");
}

#[tokio::test]
async fn worktree_creates_isolated_branch() {
    let (_dir, orch) = setup_with_git();

    let agent = orch
        .spawn_agent_with_model("editor", "echo", None, "edit files", None, "mesh", true)
        .unwrap();

    let worktree = orch.worktree_dir(&agent.id);
    assert!(worktree.is_some(), "worktree dir should exist after spawn");
    let wt = worktree.unwrap();
    assert!(wt.join("README.md").exists(), "worktree should have project files");

    // Check the branch was created
    let output = std::process::Command::new("git")
        .args(["branch", "--list", &format!("swarm/{}", agent.id)])
        .current_dir(&wt)
        .output()
        .unwrap();
    let branches = String::from_utf8_lossy(&output.stdout);
    assert!(branches.contains(&format!("swarm/{}", agent.id)),
        "branch swarm/{} should exist", agent.id);
}

#[tokio::test]
async fn no_worktree_by_default() {
    let (_dir, orch) = setup_with_git();

    let agent = orch
        .spawn_agent("reviewer", "echo", "review code", None, "mesh")
        .unwrap();

    let worktree = orch.worktree_dir(&agent.id);
    assert!(worktree.is_none(), "no worktree should exist for default spawn");
}

#[tokio::test]
async fn cleanup_removes_worktree() {
    let (_dir, orch) = setup_with_git();

    let agent = orch
        .spawn_agent_with_model("cleaner", "echo", None, "edit", None, "mesh", true)
        .unwrap();

    assert!(orch.worktree_dir(&agent.id).is_some());

    orch.cleanup_agent(&agent.id, false).unwrap();
    assert!(orch.worktree_dir(&agent.id).is_none(),
        "worktree should be gone after cleanup");
}

#[tokio::test]
async fn cleanup_with_branch_delete() {
    let (_dir, orch) = setup_with_git();

    let agent = orch
        .spawn_agent_with_model("brancher", "echo", None, "edit", None, "mesh", true)
        .unwrap();

    let branch_name = format!("swarm/{}", agent.id);

    orch.cleanup_agent(&agent.id, true).unwrap();

    // Verify branch is gone too
    let output = std::process::Command::new("git")
        .args(["branch", "--list", &branch_name])
        .current_dir(_dir.path().join("project"))
        .output()
        .unwrap();
    let branches = String::from_utf8_lossy(&output.stdout);
    assert!(!branches.contains(&branch_name),
        "branch should be deleted with --delete-branch");
}

#[tokio::test]
async fn cleanup_noop_without_worktree() {
    let (_dir, orch) = setup();

    let agent = orch
        .spawn_agent("plain", "echo", "", None, "mesh")
        .unwrap();

    // Should not error even though there's no worktree
    orch.cleanup_agent(&agent.id, false).unwrap();
}

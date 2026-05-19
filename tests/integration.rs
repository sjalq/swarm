use std::sync::Arc;
use std::time::Duration;
use swarm::db::Db;
use swarm::harness::HarnessRegistry;
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

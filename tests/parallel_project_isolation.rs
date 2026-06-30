use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use swarm::db::{AgentRow, CommsMode, Db, LogFilter, TopicStatus};
use swarm::harness::HarnessRegistry;
use swarm::orchestrator::{Orchestrator, OrchestratorRegistry, SwarmEvent};

struct ProjectHarness {
    orch: Arc<Orchestrator>,
    addr: String,
    project_dir: PathBuf,
}

fn test_client(project_dir: &std::path::Path) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        swarm::server::PROJECT_HEADER,
        reqwest::header::HeaderValue::from_str(&project_dir.to_string_lossy()).unwrap(),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}

async fn setup_multi_project(n: usize) -> (tempfile::TempDir, Arc<Db>, Vec<ProjectHarness>) {
    let root = tempfile::tempdir().unwrap();
    let data_dir = root.path().join("shared-data");
    std::fs::create_dir_all(data_dir.join("agents")).unwrap();

    let db = Arc::new(Db::open(&data_dir.join("swarm.db")).unwrap());

    let mut projects = Vec::with_capacity(n);
    for i in 0..n {
        let project_dir = root.path().join(format!("project-{i}"));
        std::fs::create_dir_all(&project_dir).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("http://127.0.0.1:{port}");

        let orch = Arc::new(Orchestrator::new(
            db.clone(),
            HarnessRegistry::new(),
            addr.clone(),
            project_dir.clone(),
            data_dir.clone(),
        ));

        let registry_arc = Arc::new(OrchestratorRegistry::with_orchestrator(orch.clone()));
        let router = swarm::server::router(registry_arc);
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        projects.push(ProjectHarness {
            orch,
            addr,
            project_dir,
        });
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    (root, db, projects)
}

async fn wait_for_user_notification(
    rx: &mut tokio::sync::broadcast::Receiver<SwarmEvent>,
    agent_id: &str,
    timeout_secs: u64,
) -> Option<String> {
    let agent_id = agent_id.to_string();
    tokio::time::timeout(Duration::from_secs(timeout_secs), async {
        loop {
            match rx.recv().await {
                Ok(SwarmEvent::UserNotification { from, content }) if from == agent_id => {
                    return content;
                }
                Err(_) => return String::new(),
                _ => continue,
            }
        }
    })
    .await
    .ok()
}

async fn join_all_handles<T: Send + 'static>(handles: Vec<tokio::task::JoinHandle<T>>) -> Vec<T> {
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    results
}

// ---------------------------------------------------------------------------
// Test 1: Multiple projects sharing a DB see only their own agents
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_projects_see_only_own_agents() {
    let (_root, _db, projects) = setup_multi_project(4).await;

    let mut agent_ids: Vec<Vec<String>> = Vec::new();
    for (i, p) in projects.iter().enumerate() {
        let mut ids = Vec::new();
        for j in 0..3 {
            let agent = p
                .orch
                .start_topic(
                    &format!("worker-{j}"),
                    "echo",
                    &format!("project {i} task {j}"),
                    None,
                    "mesh",
                )
                .unwrap();
            ids.push(agent.id);
        }
        agent_ids.push(ids);
    }

    for (i, p) in projects.iter().enumerate() {
        let agents = p.orch.list_agents().unwrap();
        assert_eq!(
            agents.len(),
            3,
            "project {i} should see exactly 3 agents, got {}",
            agents.len()
        );

        for agent in &agents {
            assert!(
                agent_ids[i].contains(&agent.id),
                "project {i} sees foreign agent {}",
                agent.id
            );
        }

        for (j, other_ids) in agent_ids.iter().enumerate() {
            if j == i {
                continue;
            }
            for other_id in other_ids {
                assert!(
                    p.orch.get_agent(other_id).unwrap().is_none(),
                    "project {i} should not see agent {other_id} from project {j}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test 2: Concurrent echo agents process messages without cross-contamination
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_echo_agents_across_projects_no_cross_contamination() {
    let (_root, _db, projects) = setup_multi_project(3).await;

    let mut handles = Vec::new();

    for (i, p) in projects.iter().enumerate() {
        let orch = p.orch.clone();
        let mut rx = orch.subscribe();
        let project_idx = i;

        let handle = tokio::spawn(async move {
            let agent = orch
                .start_topic("echoer", "echo", "", None, "mesh")
                .unwrap();

            let unique_msg = format!(
                "project-{project_idx}-unique-payload-{}",
                uuid::Uuid::new_v4()
            );
            orch.send_message("user", &agent.id, &unique_msg)
                .await
                .unwrap();

            let reply = wait_for_user_notification(&mut rx, &agent.id, 5).await;
            assert!(
                reply.is_some(),
                "project {project_idx} echo agent should reply"
            );
            let reply = reply.unwrap();
            assert!(
                reply.contains(&unique_msg),
                "project {project_idx} reply should contain our unique payload, got: {reply}"
            );

            (project_idx, agent.id, unique_msg)
        });
        handles.push(handle);
    }

    let results: Vec<(usize, String, String)> = join_all_handles(handles).await;

    for (i, p) in projects.iter().enumerate() {
        let log = p
            .orch
            .get_agent_log("user", 100, LogFilter::Messages)
            .unwrap();

        for (j, (_, _, ref unique_msg)) in results.iter().enumerate() {
            if i == j {
                assert!(
                    log.iter().any(|e| e.content.contains(unique_msg.as_str())),
                    "project {i} user log should contain its own unique payload"
                );
            } else {
                assert!(
                    !log.iter().any(|e| e.content.contains(unique_msg.as_str())),
                    "project {i} user log should NOT contain project {j}'s payload"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test 3: Stats are properly scoped per project
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stats_scoped_per_project() {
    let (_root, _db, projects) = setup_multi_project(3).await;

    projects[0]
        .orch
        .start_topic("w1", "echo", "", None, "mesh")
        .unwrap();
    projects[0]
        .orch
        .start_topic("w2", "echo", "", None, "mesh")
        .unwrap();

    projects[1]
        .orch
        .start_topic("w1", "echo", "", None, "mesh")
        .unwrap();

    let stats0 = projects[0].orch.stats().unwrap();
    let stats1 = projects[1].orch.stats().unwrap();
    let stats2 = projects[2].orch.stats().unwrap();

    assert_eq!(stats0.alive, 2, "project 0 should have 2 alive agents");
    assert_eq!(stats1.alive, 1, "project 1 should have 1 alive agent");
    assert_eq!(stats2.alive, 0, "project 2 should have 0 alive agents");
    assert_eq!(stats2.total, 0, "project 2 total should be 0");
}

// ---------------------------------------------------------------------------
// Test 4: Events are scoped per project
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn events_scoped_per_project() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    let mut rx0 = projects[0].orch.subscribe();
    let mut rx1 = projects[1].orch.subscribe();

    let a0 = projects[0]
        .orch
        .start_topic("eventer", "echo", "task a", None, "mesh")
        .unwrap();
    let a1 = projects[1]
        .orch
        .start_topic("eventer", "echo", "task b", None, "mesh")
        .unwrap();

    // Wait for echo processing to complete (idle after working)
    for (rx, agent_id) in [(&mut rx0, &a0.id), (&mut rx1, &a1.id)] {
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match rx.recv().await {
                    Ok(SwarmEvent::AgentStatus {
                        agent_id: id,
                        status: TopicStatus::Idle,
                    }) if id == *agent_id => return,
                    Err(_) => return,
                    _ => continue,
                }
            }
        })
        .await;
    }

    let events0 = projects[0].orch.list_events(None, None, 1000).unwrap();
    let events1 = projects[1].orch.list_events(None, None, 1000).unwrap();

    let event0_agent_ids: Vec<_> = events0
        .iter()
        .filter_map(|e| e.agent_id.as_deref())
        .collect();
    let event1_agent_ids: Vec<_> = events1
        .iter()
        .filter_map(|e| e.agent_id.as_deref())
        .collect();

    assert!(
        !event0_agent_ids.contains(&a1.id.as_str()),
        "project 0 events should not contain project 1's agent"
    );
    assert!(
        !event1_agent_ids.contains(&a0.id.as_str()),
        "project 1 events should not contain project 0's agent"
    );
    assert!(
        event0_agent_ids.contains(&a0.id.as_str()),
        "project 0 events should contain its own agent"
    );
    assert!(
        event1_agent_ids.contains(&a1.id.as_str()),
        "project 1 events should contain its own agent"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Cross-project message send fails (agent not found)
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_project_message_send_fails() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    let a0 = projects[0]
        .orch
        .start_topic("target", "echo", "", None, "mesh")
        .unwrap();
    let a1 = projects[1]
        .orch
        .start_topic("sender", "echo", "", None, "mesh")
        .unwrap();

    let err = projects[1]
        .orch
        .send_message(&a1.id, &a0.id, "cross-project hack")
        .await
        .expect_err("sending from project 1 to project 0's agent should fail");
    assert!(
        err.to_string().contains("not found"),
        "error should be 'not found', got: {err}"
    );

    let err = projects[0]
        .orch
        .send_message("user", &a1.id, "cross-project inject")
        .await
        .expect_err("sending from project 0's user to project 1's agent should fail");
    assert!(
        err.to_string().contains("not found"),
        "error should be 'not found', got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Test 6: Done/kill on one project doesn't affect another
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn done_and_kill_isolated_between_projects() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    let a0 = projects[0]
        .orch
        .start_topic("survivor", "echo", "", None, "mesh")
        .unwrap();
    let a1 = projects[1]
        .orch
        .start_topic("doomed", "echo", "", None, "mesh")
        .unwrap();

    projects[1].orch.kill_agent(&a1.id).await.unwrap();

    let survivor = projects[0].orch.get_agent(&a0.id).unwrap().unwrap();
    assert_eq!(
        survivor.status,
        TopicStatus::Idle,
        "killing an agent in project 1 should not affect project 0's agent"
    );
    assert_eq!(projects[0].orch.list_agents().unwrap().len(), 1);

    let result = projects[0].orch.kill_agent(&a1.id).await;
    assert!(
        result.is_err(),
        "project 0 should not be able to kill project 1's agent"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Done on foreign agent fails
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn done_on_foreign_agent_fails() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    let a0 = projects[0]
        .orch
        .start_topic("worker", "echo", "", None, "mesh")
        .unwrap();

    let err = projects[1]
        .orch
        .done_agent(&a0.id, Some("hacked"))
        .await
        .expect_err("project 1 should not be able to mark project 0's agent as done");
    assert!(
        err.to_string().contains("not found"),
        "error should be 'not found', got: {err}"
    );

    let agent = projects[0].orch.get_agent(&a0.id).unwrap().unwrap();
    assert_eq!(
        agent.status,
        TopicStatus::Idle,
        "agent should still be idle after foreign done attempt"
    );
}

// ---------------------------------------------------------------------------
// Test 8: Resume only resumes own project's workers
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_only_resumes_own_projects_workers() {
    let root = tempfile::tempdir().unwrap();
    let data_dir = root.path().join("shared-data");
    std::fs::create_dir_all(data_dir.join("agents")).unwrap();

    let db = Arc::new(Db::open(&data_dir.join("swarm.db")).unwrap());

    let project_a_dir = root.path().join("project-a");
    let project_b_dir = root.path().join("project-b");
    std::fs::create_dir_all(&project_a_dir).unwrap();
    std::fs::create_dir_all(&project_b_dir).unwrap();

    for (id, project_dir) in [
        ("worker-a-00000001", &project_a_dir),
        ("worker-a-00000002", &project_a_dir),
        ("worker-b-00000001", &project_b_dir),
    ] {
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
            terminal_cause: None,
            error_reason: None,
            worktree_branch: None,
            project_dir: Some(project_dir.to_string_lossy().to_string()),
            user_launched: true,
        })
        .unwrap();
        std::fs::create_dir_all(data_dir.join("agents").join(id)).unwrap();
    }

    let orch_a = Arc::new(Orchestrator::new(
        db.clone(),
        HarnessRegistry::new(),
        "http://127.0.0.1:0".into(),
        project_a_dir,
        data_dir.clone(),
    ));
    let orch_b = Arc::new(Orchestrator::new(
        db,
        HarnessRegistry::new(),
        "http://127.0.0.1:0".into(),
        project_b_dir,
        data_dir,
    ));

    let resumed_a = orch_a.resume_existing_workers().unwrap();
    let resumed_b = orch_b.resume_existing_workers().unwrap();

    assert_eq!(resumed_a, 2, "project A should resume 2 workers");
    assert_eq!(resumed_b, 1, "project B should resume 1 worker");
}

// ---------------------------------------------------------------------------
// Test 9: HTTP API isolation - each server serves only its project
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_api_isolation_between_projects() {
    let (_root, _db, projects) = setup_multi_project(2).await;
    let client0 = test_client(&projects[0].project_dir);
    let client1 = test_client(&projects[1].project_dir);

    let resp = client0
        .post(format!("{}/api/agents", projects[0].addr))
        .json(&serde_json::json!({
            "label": "http-agent",
            "harness": "echo",
            "system_prompt": "project 0 only",
            "comms": "mesh"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let agent0: serde_json::Value = resp.json().await.unwrap();
    let agent0_id = agent0["id"].as_str().unwrap();

    let resp = client1
        .post(format!("{}/api/agents", projects[1].addr))
        .json(&serde_json::json!({
            "label": "http-agent",
            "harness": "echo",
            "system_prompt": "project 1 only",
            "comms": "mesh"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let agent1: serde_json::Value = resp.json().await.unwrap();
    let agent1_id = agent1["id"].as_str().unwrap();

    let resp = client0
        .get(format!("{}/api/agents", projects[0].addr))
        .send()
        .await
        .unwrap();
    let agents0: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents0.len(), 1);
    assert_eq!(agents0[0]["id"].as_str().unwrap(), agent0_id);

    let resp = client1
        .get(format!("{}/api/agents", projects[1].addr))
        .send()
        .await
        .unwrap();
    let agents1: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents1.len(), 1);
    assert_eq!(agents1[0]["id"].as_str().unwrap(), agent1_id);

    let resp = client0
        .get(format!("{}/api/agents/{agent1_id}", projects[0].addr))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "project 0 should not serve project 1's agent"
    );

    let resp = client1
        .get(format!("{}/api/agents/{agent0_id}", projects[1].addr))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "project 1 should not serve project 0's agent"
    );
}

// ---------------------------------------------------------------------------
// Test 10: Concurrent full lifecycle across 4 projects simultaneously
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_full_lifecycle_across_four_projects() {
    let (_root, _db, projects) = setup_multi_project(4).await;

    let mut handles = Vec::new();

    for (i, p) in projects.iter().enumerate() {
        let orch = p.orch.clone();
        let project_idx = i;

        let handle = tokio::spawn(async move {
            let mut rx = orch.subscribe();

            let parent = orch
                .start_topic("coordinator", "echo", "", None, "mesh")
                .unwrap();

            let child = orch
                .start_topic("worker", "echo", "", Some(&parent.id), "mesh")
                .unwrap();

            let unique = format!("p{project_idx}-{}", uuid::Uuid::new_v4());
            orch.send_message("user", &child.id, &unique).await.unwrap();

            let reply = wait_for_user_notification(&mut rx, &child.id, 5).await;
            assert!(
                reply.is_some(),
                "project {project_idx}: child should echo back"
            );

            orch.done_agent(&child.id, Some(&format!("done-{project_idx}")))
                .await
                .unwrap();

            let child_status = orch.get_agent(&child.id).unwrap().unwrap().status;
            assert!(
                matches!(child_status, TopicStatus::Paused | TopicStatus::Idle),
                "project {project_idx}: child should be done or echo-reactivated, got {child_status:?}"
            );

            let parent_log = orch
                .get_agent_log(&parent.id, 50, LogFilter::Messages)
                .unwrap();
            assert!(
                parent_log
                    .iter()
                    .any(|e| e.content == format!("done-{project_idx}")),
                "project {project_idx}: parent should receive done message"
            );

            let agents = orch.list_agents().unwrap();
            assert!(
                agents.len() <= 2,
                "project {project_idx}: parent and at most 1 echo-reactivated child should be active, got {}",
                agents.len()
            );

            orch.kill_agent(&parent.id).await.unwrap();

            // The echo harness may bounce the done message back to the
            // child, reactivating it (at most 1 extra).
            let remaining = orch.list_agents().unwrap();
            assert!(
                remaining.len() <= 1,
                "project {project_idx}: at most 1 echo-reactivated agent expected, got {}",
                remaining.len()
            );
            for agent in remaining {
                orch.kill_agent(&agent.id).await.unwrap();
            }

            let stats = orch.stats().unwrap();
            assert_eq!(
                stats.alive, 0,
                "project {project_idx}: no alive agents after full lifecycle"
            );
            assert!(
                stats.done >= 2 && stats.done <= 3,
                "project {project_idx}: expected 2-3 done agents, got {}",
                stats.done
            );

            project_idx
        });
        handles.push(handle);
    }

    let results: Vec<usize> = join_all_handles(handles).await;
    assert_eq!(results.len(), 4, "all 4 project lifecycles should complete");
}

// ---------------------------------------------------------------------------
// Test 11: Broadcast family does not leak across projects
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_family_does_not_leak_across_projects() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    let p0_parent = projects[0]
        .orch
        .start_topic("parent", "echo", "", None, "mesh")
        .unwrap();
    let p0_child = projects[0]
        .orch
        .start_topic("child", "echo", "", Some(&p0_parent.id), "mesh")
        .unwrap();

    let p1_parent = projects[1]
        .orch
        .start_topic("parent", "echo", "", None, "mesh")
        .unwrap();
    let p1_child = projects[1]
        .orch
        .start_topic("child", "echo", "", Some(&p1_parent.id), "mesh")
        .unwrap();

    let msgs0 = projects[0]
        .orch
        .broadcast_family(&p0_child.id, "p0 broadcast")
        .await
        .unwrap();

    let msgs1 = projects[1]
        .orch
        .broadcast_family(&p1_child.id, "p1 broadcast")
        .await
        .unwrap();

    let targets0: Vec<&str> = msgs0.iter().map(|m| m.to_agent.as_str()).collect();
    let targets1: Vec<&str> = msgs1.iter().map(|m| m.to_agent.as_str()).collect();

    assert!(targets0.contains(&p0_parent.id.as_str()));
    assert!(!targets0.contains(&p1_parent.id.as_str()));
    assert!(!targets0.contains(&p1_child.id.as_str()));

    assert!(targets1.contains(&p1_parent.id.as_str()));
    assert!(!targets1.contains(&p0_parent.id.as_str()));
    assert!(!targets1.contains(&p0_child.id.as_str()));
}

// ---------------------------------------------------------------------------
// Test 12: Same label agents in different projects are fully independent
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_label_agents_different_projects_are_independent() {
    let (_root, _db, projects) = setup_multi_project(3).await;

    let mut agents = Vec::new();
    for (i, p) in projects.iter().enumerate() {
        let agent = p
            .orch
            .start_topic(
                "analyzer",
                "echo",
                &format!("analyze project {i}"),
                None,
                "mesh",
            )
            .unwrap();
        agents.push(agent);
    }

    assert_ne!(agents[0].id, agents[1].id);
    assert_ne!(agents[1].id, agents[2].id);

    for agent in &agents {
        assert_eq!(agent.label, "analyzer");
    }

    for (i, p) in projects.iter().enumerate() {
        let found = p.orch.get_agent(&agents[i].id).unwrap();
        assert!(found.is_some(), "project {i} should find its own agent");

        for (j, other_agent) in agents.iter().enumerate() {
            if j == i {
                continue;
            }
            let found = p.orch.get_agent(&other_agent.id).unwrap();
            assert!(
                found.is_none(),
                "project {i} should not find project {j}'s agent"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test 13: Parallel high-volume messaging across projects
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_high_volume_messaging_across_projects() {
    let num_projects: usize = 3;
    let (_root, _db, projects) = setup_multi_project(num_projects).await;
    let messages_per_project: usize = 10;

    let mut handles = Vec::new();

    for (i, p) in projects.iter().enumerate() {
        let orch = p.orch.clone();
        let project_idx = i;
        let msg_count = messages_per_project;

        let mut rx = orch.subscribe();
        let handle = tokio::spawn(async move {
            let agent = orch
                .start_topic("bulk-worker", "echo", "", None, "mesh")
                .unwrap();

            for j in 0..msg_count {
                orch.send_message("user", &agent.id, &format!("p{project_idx}-msg-{j}"))
                    .await
                    .unwrap();
            }

            let mut replies = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match rx.recv().await {
                        Ok(SwarmEvent::UserNotification { from, content }) if from == agent.id => {
                            replies.push(content);
                            if replies.len() >= msg_count {
                                return;
                            }
                        }
                        Err(_) => return,
                        _ => continue,
                    }
                }
            })
            .await;

            let all_text: String = replies.join(" ");
            for j in 0..msg_count {
                assert!(
                    all_text.contains(&format!("p{project_idx}-msg-{j}")),
                    "project {project_idx} should see all its messages echoed back, missing msg-{j}"
                );
            }

            for other_p in 0..num_projects {
                if other_p == project_idx {
                    continue;
                }
                assert!(
                    !all_text.contains(&format!("p{other_p}-msg-")),
                    "project {project_idx} replies should not contain project {other_p}'s messages"
                );
            }

            project_idx
        });
        handles.push(handle);
    }

    let results: Vec<usize> = join_all_handles(handles).await;
    assert_eq!(results.len(), 3, "all 3 projects should complete");
}

// ---------------------------------------------------------------------------
// Test 14: HTTP stats endpoint isolation under concurrent load
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_stats_isolation_under_concurrent_load() {
    let (_root, _db, projects) = setup_multi_project(3).await;

    let mut handles = Vec::new();
    for (i, p) in projects.iter().enumerate() {
        let addr = p.addr.clone();
        let client = test_client(&p.project_dir);
        let agent_count = i + 1;

        let handle = tokio::spawn(async move {
            for j in 0..agent_count {
                client
                    .post(format!("{addr}/api/agents"))
                    .json(&serde_json::json!({
                        "label": format!("worker-{j}"),
                        "harness": "echo",
                        "system_prompt": "",
                        "comms": "mesh"
                    }))
                    .send()
                    .await
                    .unwrap();
            }
            (i, agent_count)
        });
        handles.push(handle);
    }

    let created: Vec<(usize, usize)> = join_all_handles(handles).await;

    for (i, expected_count) in &created {
        let client = test_client(&projects[*i].project_dir);
        let resp = client
            .get(format!("{}/api/stats", projects[*i].addr))
            .send()
            .await
            .unwrap();
        let stats: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            stats["alive"].as_u64().unwrap(),
            *expected_count as u64,
            "project {i} stats should show {expected_count} alive agents"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 15: Perspective view does not leak agents across projects
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn perspective_view_does_not_leak_across_projects() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    let p0_parent = projects[0]
        .orch
        .start_topic("parent", "echo", "", None, "mesh")
        .unwrap();
    let p0_child = projects[0]
        .orch
        .start_topic("child", "echo", "", Some(&p0_parent.id), "mesh")
        .unwrap();

    let _p1_agent = projects[1]
        .orch
        .start_topic("worker", "echo", "", None, "mesh")
        .unwrap();

    let views = projects[0]
        .orch
        .list_agents_with_perspective(&p0_child.id)
        .unwrap();

    let view_ids: Vec<&str> = views.iter().map(|v| v.agent.id.as_str()).collect();
    assert!(view_ids.contains(&p0_child.id.as_str()));
    assert!(view_ids.contains(&p0_parent.id.as_str()));
    assert_eq!(
        view_ids.len(),
        2,
        "perspective should only include agents from project 0"
    );
}

// ---------------------------------------------------------------------------
// Test 16: Swarm brief is scoped per project
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swarm_brief_scoped_per_project() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    projects[0]
        .orch
        .start_topic("alpha", "echo", "alpha task", None, "mesh")
        .unwrap();
    projects[0]
        .orch
        .start_topic("beta", "echo", "beta task", None, "mesh")
        .unwrap();

    projects[1]
        .orch
        .start_topic("gamma", "echo", "gamma task", None, "mesh")
        .unwrap();

    let brief0 = projects[0].orch.swarm_brief(100, None).unwrap();
    let brief1 = projects[1].orch.swarm_brief(100, None).unwrap();

    assert_eq!(brief0.stats.total, 2, "project 0 brief should show 2 total");
    assert_eq!(brief1.stats.total, 1, "project 1 brief should show 1 total");

    let brief0_labels: Vec<&str> = brief0.agents.iter().map(|a| a.label.as_str()).collect();
    assert!(brief0_labels.contains(&"alpha"));
    assert!(brief0_labels.contains(&"beta"));
    assert!(!brief0_labels.contains(&"gamma"));

    let brief1_labels: Vec<&str> = brief1.agents.iter().map(|a| a.label.as_str()).collect();
    assert!(brief1_labels.contains(&"gamma"));
    assert!(!brief1_labels.contains(&"alpha"));
}

// ---------------------------------------------------------------------------
// Test 17: list_all_agents (including done) is scoped per project
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_all_agents_scoped_per_project() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    let a0_alive = projects[0]
        .orch
        .start_topic("alive", "echo", "", None, "mesh")
        .unwrap();
    let a0_done = projects[0]
        .orch
        .start_topic("finished", "echo", "", None, "mesh")
        .unwrap();
    projects[0]
        .orch
        .done_agent(&a0_done.id, Some("complete"))
        .await
        .unwrap();

    let a1 = projects[1]
        .orch
        .start_topic("other", "echo", "", None, "mesh")
        .unwrap();

    let all0 = projects[0].orch.list_all_agents().unwrap();
    let all1 = projects[1].orch.list_all_agents().unwrap();

    assert_eq!(
        all0.len(),
        2,
        "project 0 should have 2 total agents (1 alive + 1 done)"
    );
    assert_eq!(all1.len(), 1, "project 1 should have 1 total agent");

    let all0_ids: Vec<&str> = all0.iter().map(|a| a.id.as_str()).collect();
    assert!(all0_ids.contains(&a0_alive.id.as_str()));
    assert!(all0_ids.contains(&a0_done.id.as_str()));
    assert!(!all0_ids.contains(&a1.id.as_str()));
}

// ---------------------------------------------------------------------------
// Test 18: get_agent_log rejects queries for foreign agents
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_agent_log_rejects_foreign_agents() {
    let (_root, _db, projects) = setup_multi_project(2).await;

    let a0 = projects[0]
        .orch
        .start_topic("logger", "echo", "", None, "mesh")
        .unwrap();

    projects[0]
        .orch
        .send_message("user", &a0.id, "test message")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let own_log = projects[0]
        .orch
        .get_agent_log(&a0.id, 50, LogFilter::All)
        .unwrap();
    assert!(
        !own_log.is_empty(),
        "own project should see the agent's log"
    );

    let err = projects[1]
        .orch
        .get_agent_log(&a0.id, 50, LogFilter::All)
        .expect_err("foreign project should not be able to read agent log");
    assert!(
        err.to_string().contains("not found"),
        "error should be 'not found', got: {err}"
    );
}

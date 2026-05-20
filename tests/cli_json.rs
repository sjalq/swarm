use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct SwarmRun {
    child: Child,
}

impl Drop for SwarmRun {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn start_swarm() -> (tempfile::TempDir, SwarmRun, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let addr = format!("http://127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_swarm"))
        .args([
            "run",
            "--project-dir",
            dir.path().to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--harness",
            "echo",
            "--role",
            "cli-json",
            "--no-gitignore",
        ])
        .env_remove("SWARM_SOCKET")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start swarm run");

    let run = SwarmRun { child };
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if let Ok(resp) = client.get(format!("{addr}/api/agents")).send().await {
            if resp.status().is_success() {
                let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
                if let Some(agent) = agents.first() {
                    let agent_id = agent["id"].as_str().unwrap().to_string();
                    return (dir, run, addr, agent_id);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("swarm server did not start");
}

fn run_swarm_json(args: &[&str], addr: &str, agent_id: Option<&str>) -> serde_json::Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_swarm"));
    command.args(args).env("SWARM_SOCKET", addr);
    if let Some(agent_id) = agent_id {
        command.env("SWARM_AGENT_ID", agent_id);
    } else {
        command.env_remove("SWARM_AGENT_ID");
    }

    let output = command.output().expect("failed to run swarm command");
    assert!(
        output.status.success(),
        "command {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "command {:?} did not emit parseable JSON: {err}; stdout: {}",
            args,
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

#[tokio::test]
async fn peers_status_and_log_json_flags_parse() {
    let (_dir, _run, addr, agent_id) = start_swarm().await;

    let peers = run_swarm_json(&["peers", "--json"], &addr, None);
    let peers = peers.as_array().expect("peers JSON should be an array");
    assert!(peers
        .iter()
        .any(|agent| agent["id"].as_str() == Some(agent_id.as_str())));

    let status = run_swarm_json(&["status", "--json"], &addr, Some(&agent_id));
    assert_eq!(status["id"].as_str(), Some(agent_id.as_str()));
    assert_eq!(status["harness"], "echo");

    let long_message = "x".repeat(700);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/messages"))
        .json(&serde_json::json!({
            "from": "user",
            "to": agent_id,
            "content": long_message,
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let log = run_swarm_json(
        &["log", &agent_id, "--json", "--truncate", "5"],
        &addr,
        None,
    );
    let entries = log.as_array().expect("log JSON should be an array");
    assert!(
        entries.iter().any(|entry| entry["kind"] == "recv"
            && entry["content"].as_str() == Some(long_message.as_str())),
        "JSON log output should preserve untruncated content"
    );
}

#[tokio::test]
async fn peers_status_and_log_piped_output_is_json_without_flag() {
    let (_dir, _run, addr, agent_id) = start_swarm().await;

    let peers = run_swarm_json(&["peers"], &addr, None);
    assert!(peers
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["id"].as_str() == Some(agent_id.as_str())));

    let status = run_swarm_json(&["status"], &addr, Some(&agent_id));
    assert_eq!(status["id"].as_str(), Some(agent_id.as_str()));

    let log = run_swarm_json(&["log", &agent_id], &addr, None);
    assert!(log.as_array().is_some());

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{addr}/api/agents/{agent_id}"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let peers = run_swarm_json(&["peers", "--all", "--json"], &addr, None);
    assert!(peers.as_array().unwrap().iter().any(|a| {
        a["id"].as_str() == Some(agent_id.as_str()) && a["status"].as_str() == Some("dead")
    }));
}

use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
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

struct WatchRun {
    child: Child,
    rx: mpsc::Receiver<String>,
}

impl WatchRun {
    fn wait_for_output(&self) -> String {
        self.rx
            .recv_timeout(Duration::from_secs(8))
            .expect("watch did not print the expected message")
    }
}

impl Drop for WatchRun {
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
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_swarm"))
        .args([
            "serve",
            "--project-dir",
            dir.path().to_str().unwrap(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--no-gitignore",
        ])
        .env_remove("SWARM_SOCKET")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start swarm serve");

    let run = SwarmRun { child };
    let client = reqwest::Client::new();
    let mut server_ready = false;
    for _ in 0..100 {
        if let Ok(resp) = client.get(format!("{addr}/api/health")).send().await {
            if resp.status().is_success() {
                server_ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(server_ready, "swarm server did not start");

    let output = Command::new(env!("CARGO_BIN_EXE_swarm"))
        .args([
            "run",
            "--project-dir",
            dir.path().to_str().unwrap(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--harness",
            "echo",
            "--label",
            "cli-json",
            "--detach",
            "--no-gitignore",
            "cli json smoke",
        ])
        .env("SWARM_SOCKET", &addr)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run swarm topic");
    assert!(
        output.status.success(),
        "failed to start topic: {}",
        String::from_utf8_lossy(&output.stderr)
    );

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

    panic!("swarm topic did not start");
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

fn run_swarm_ok(args: &[&str], addr: &str, agent_id: Option<&str>) {
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
}

fn start_watch_until(args: &[&str], addr: &str, expected: &str) -> WatchRun {
    let mut child = Command::new(env!("CARGO_BIN_EXE_swarm"))
        .args(args)
        .env("SWARM_SOCKET", addr)
        .env_remove("SWARM_AGENT_ID")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start swarm watch");

    let stdout = child.stdout.take().expect("stdout should be piped");
    let expected = expected.to_string();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let mut output = String::new();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => {
                    let _ = tx.send(output);
                    return;
                }
                Ok(_) => {
                    output.push_str(&line);
                    if output.contains(&expected) {
                        let _ = tx.send(output);
                        return;
                    }
                }
            }
        }
    });

    WatchRun { child, rx }
}

#[tokio::test]
async fn run_inline_watch_prints_echo_reply() {
    let (_dir, _run, addr, _agent_id) = start_swarm().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_swarm"))
        .args([
            "run",
            "--harness",
            "echo",
            "--label",
            "watch-smoke",
            "inline watch smoke",
        ])
        .env("SWARM_SOCKET", &addr)
        .env_remove("SWARM_AGENT_ID")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run swarm topic");

    let stdout = child.stdout.take().expect("stdout should be piped");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let mut output = String::new();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => {
                    let _ = tx.send(output);
                    return;
                }
                Ok(_) => {
                    output.push_str(&line);
                    if output.contains("(echo) inline watch smoke") {
                        let _ = tx.send(output);
                        return;
                    }
                }
            }
        }
    });

    let received = tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_secs(8)))
        .await
        .expect("reader task should not panic");

    let _ = child.kill();
    let _ = child.wait();

    let output = received.expect("inline run did not print the echo reply");

    assert!(
        output.contains("(echo) inline watch smoke"),
        "stdout should include the echoed parent response; stdout was: {output}"
    );
}

#[tokio::test]
async fn watch_uses_session_local_cursor() {
    let (_dir, _run, addr, agent_id) = start_swarm().await;

    let watch = start_watch_until(
        &["watch", "--all", "--json"],
        &addr,
        "session local watch smoke",
    );
    tokio::time::sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/messages"))
        .json(&serde_json::json!({
            "from": agent_id,
            "to": "user",
            "content": "session local watch smoke",
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let watch_output = watch.wait_for_output();
    assert!(
        watch_output.contains("session local watch smoke"),
        "watch should print the message; stdout was: {watch_output}"
    );
    drop(watch);

    let inbox = run_swarm_json(&["inbox", "--all", "--new", "--json"], &addr, None);
    assert!(
        inbox
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["content"].as_str() == Some("session local watch smoke")),
        "watch should not advance the persistent inbox cursor; inbox was: {inbox}"
    );
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
        a["id"].as_str() == Some(agent_id.as_str()) && a["status"].as_str() == Some("done")
    }));
}

#[tokio::test]
async fn brief_and_structured_done_json_flags_parse() {
    let (_dir, _run, addr, agent_id) = start_swarm().await;

    run_swarm_ok(
        &[
            "done",
            "handover summary",
            "--outcome",
            "done",
            "--deliverable",
            "branch swarm/test",
            "--checks",
            "cargo test",
            "--risk",
            "none",
            "--next-action",
            "review",
        ],
        &addr,
        Some(&agent_id),
    );

    let brief = run_swarm_json(&["brief", &agent_id, "--json"], &addr, None);
    assert_eq!(brief["id"].as_str(), Some(agent_id.as_str()));
    assert_eq!(
        brief["latest_handover"]["summary"].as_str(),
        Some("handover summary")
    );
    assert_eq!(
        brief["latest_handover"]["next_action"].as_str(),
        Some("review")
    );

    let overview = run_swarm_json(&["brief", "--json"], &addr, None);
    assert!(overview["agents"].as_array().is_some());
}

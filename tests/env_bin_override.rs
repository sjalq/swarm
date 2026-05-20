use std::process::Command;

#[test]
fn env_bin_override_preflight_error() {
    let output = Command::new("cargo")
        .args(["run", "--", "run", "--harness", "claude", "--port", "0"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("SWARM_CLAUDE_BIN", "/nonexistent/foo")
        .env_remove("SWARM_SOCKET")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("failed to run swarm run");

    assert!(
        !output.status.success(),
        "should exit non-zero when SWARM_CLAUDE_BIN points to nonexistent binary"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("/nonexistent/foo") || stderr.contains("SWARM_CLAUDE_BIN"),
        "stderr should mention the override path or env var, got: {}",
        stderr
    );
}

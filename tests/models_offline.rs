use std::process::Command;

#[test]
fn models_offline_succeeds_without_server() {
    let output = Command::new("cargo")
        .args(["run", "--", "models"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env_remove("SWARM_SOCKET")
        .output()
        .expect("failed to run swarm models");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "models should exit 0 offline, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(stdout.contains("claude"), "should list claude models");
    assert!(stdout.contains("gemini"), "should list gemini models");
    assert!(stdout.contains("codex"), "should list codex models");
    assert!(stdout.contains("grok"), "should list grok models");

    assert!(
        stdout.contains("(default)"),
        "should indicate a default model"
    );
}

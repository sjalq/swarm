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

    let models: serde_json::Value =
        serde_json::from_str(&stdout).expect("captured models output should be JSON");
    let models = models.as_array().expect("models output should be an array");

    for harness in ["claude", "gemini", "codex", "grok"] {
        let entry = models
            .iter()
            .find(|entry| entry["harness"] == harness)
            .unwrap_or_else(|| panic!("should list {harness} models"));
        assert!(entry["default_model"].as_str().is_some());
        assert!(entry["models"].as_array().unwrap().len() >= 2);
    }
}

#[test]
fn models_json_flag_parses() {
    let output = Command::new("cargo")
        .args(["run", "--", "models", "--json"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env_remove("SWARM_SOCKET")
        .output()
        .expect("failed to run swarm models --json");

    assert!(
        output.status.success(),
        "models --json should exit 0, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let models: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("models --json should be parseable JSON");
    assert_eq!(models.as_array().unwrap().len(), 4);
}

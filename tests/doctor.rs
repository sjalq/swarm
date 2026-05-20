use std::process::Command;

#[test]
fn doctor_lists_all_harnesses_and_git() {
    let output = Command::new("cargo")
        .args(["run", "--", "doctor"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("failed to run swarm doctor");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "doctor should exit 0, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(stdout.contains("claude"), "output should mention claude");
    assert!(stdout.contains("codex"), "output should mention codex");
    assert!(stdout.contains("gemini"), "output should mention gemini");
    assert!(stdout.contains("grok"), "output should mention grok");
    assert!(stdout.contains("git"), "output should mention git");
}

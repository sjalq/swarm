use std::process::Command;

#[test]
fn completions_bash_emits_function() {
    let output = Command::new("cargo")
        .args(["run", "--", "completions", "bash"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("failed to run swarm completions bash");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "completions should exit 0, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        stdout.contains("_swarm"),
        "bash completions should contain _swarm function, got: {}",
        &stdout[..stdout.len().min(200)]
    );
}

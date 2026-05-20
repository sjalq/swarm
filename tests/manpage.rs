use std::process::Command;

#[test]
fn manpage_emits_roff_header() {
    let output = Command::new("cargo")
        .args(["run", "--", "manpage"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("failed to run swarm manpage");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "manpage should exit 0, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        stdout.contains(".TH") && (stdout.contains("swarm") || stdout.contains("SWARM")),
        "manpage should contain roff .TH header for swarm, got: {}",
        &stdout[..stdout.len().min(300)]
    );
}

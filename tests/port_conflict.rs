use std::net::TcpListener;
use std::process::Command;

#[test]
fn port_conflict_detected() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
    let port = listener.local_addr().unwrap().port();

    let child = Command::new("cargo")
        .args(["run", "--", "serve", "--port", &port.to_string()])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env_remove("SWARM_SOCKET")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to start swarm serve");

    let output = child.wait_with_output().expect("failed to wait");

    drop(listener);

    assert!(
        !output.status.success(),
        "should exit non-zero on port conflict"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("in use"),
        "stderr should mention 'in use', got: {}",
        stderr
    );
}

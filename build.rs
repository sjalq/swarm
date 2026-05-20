use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=frontend/src");
    println!("cargo:rerun-if-changed=frontend/style.css");
    println!("cargo:rerun-if-changed=frontend/index.html");
    println!("cargo:rerun-if-changed=frontend/Cargo.toml");

    let dist_index = Path::new("frontend/dist/index.html");
    if dist_index.exists() {
        return;
    }

    if !Path::new("frontend/Cargo.toml").exists() {
        println!("cargo:warning=frontend/ directory not found; dashboard will not be embedded");
        std::fs::create_dir_all("frontend/dist").ok();
        return;
    }

    let trunk = Command::new("trunk").arg("--version").output();

    match trunk {
        Ok(output) if output.status.success() => {
            let status = Command::new("trunk")
                .args(["build", "--release"])
                .current_dir("frontend")
                .status()
                .expect("failed to run trunk build");
            if !status.success() {
                println!("cargo:warning=trunk build failed; dashboard will not be embedded");
                std::fs::create_dir_all("frontend/dist").ok();
            }
        }
        _ => {
            println!(
                "cargo:warning=trunk not found; dashboard will not be embedded. \
                 Install with: cargo install trunk"
            );
            std::fs::create_dir_all("frontend/dist").ok();
        }
    }
}

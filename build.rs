use std::path::Path;
use std::process::Command;
use std::time::SystemTime;

fn newest_mtime(path: &Path) -> Option<SystemTime> {
    if path.is_file() {
        return std::fs::metadata(path).and_then(|m| m.modified()).ok();
    }
    if path.is_dir() {
        let mut newest: Option<SystemTime> = None;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let child_newest = newest_mtime(&entry.path());
                newest = match (newest, child_newest) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                };
            }
        }
        return newest;
    }
    None
}

fn clear_dist() {
    let dist = Path::new("frontend/dist");
    if dist.exists() {
        std::fs::remove_dir_all(dist).ok();
    }
    std::fs::create_dir_all(dist).ok();
}

fn main() {
    println!("cargo:rerun-if-changed=frontend/src");
    println!("cargo:rerun-if-changed=frontend/style.css");
    println!("cargo:rerun-if-changed=frontend/index.html");
    println!("cargo:rerun-if-changed=frontend/Cargo.toml");

    // Only skip trunk build if the dist exists AND no tracked source is newer.
    // This prevents stale embedded assets when frontend code changes.
    let dist_index = Path::new("frontend/dist/index.html");
    if dist_index.exists() {
        let dist_mtime = std::fs::metadata(dist_index)
            .and_then(|m| m.modified())
            .ok();
        let source_dirs = [
            "frontend/src",
            "frontend/style.css",
            "frontend/index.html",
            "frontend/Cargo.toml",
        ];
        let any_newer = dist_mtime.is_some_and(|dist_t| {
            source_dirs
                .iter()
                .any(|src| newest_mtime(Path::new(src)).is_some_and(|src_t| src_t > dist_t))
        });
        if !any_newer {
            return;
        }
    }

    if !Path::new("frontend/Cargo.toml").exists() {
        println!("cargo:warning=frontend/ directory not found; dashboard will not be embedded");
        clear_dist();
        return;
    }

    let trunk = Command::new("trunk")
        .arg("--version")
        .env_remove("NO_COLOR")
        .output();

    match trunk {
        Ok(output) if output.status.success() => {
            let status = Command::new("trunk")
                .args(["build", "--release"])
                .current_dir("frontend")
                .env_remove("NO_COLOR")
                .status()
                .expect("failed to run trunk build");
            if !status.success() {
                clear_dist();
                panic!("trunk build failed");
            }
        }
        _ => {
            println!(
                "cargo:warning=trunk not found; dashboard will not be embedded. \
                 Install with: cargo install trunk"
            );
            clear_dist();
        }
    }
}

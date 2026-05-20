use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::sync::Arc;
use swarm::config::SwarmConfig;
use swarm::db::Db;
use swarm::harness::{CliKind, HarnessRegistry};
use swarm::orchestrator::{Orchestrator, SwarmEvent};

#[derive(Parser)]
#[command(
    name = "swarm",
    about = "Multi-agent CLI orchestrator",
    long_about = "Multi-agent CLI orchestrator that coordinates Claude, Codex, Gemini, and Grok CLIs.\n\n\
        Environment variables:\n  \
        SWARM_CLAUDE_BIN  Override the claude binary path\n  \
        SWARM_CODEX_BIN   Override the codex binary path\n  \
        SWARM_GEMINI_BIN  Override the gemini binary path\n  \
        SWARM_GROK_BIN    Override the grok binary path"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the orchestrator and parent agent
    Run {
        /// Project directory (agents work here)
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,

        /// Server port
        #[arg(long)]
        port: Option<u16>,

        /// Harness for the parent agent (claude, gemini, codex, grok, echo)
        #[arg(long)]
        harness: Option<String>,

        /// Initial prompt for the parent agent
        #[arg(long, default_value = "")]
        prompt: String,

        /// Role name for the parent agent
        #[arg(long, default_value = "coordinator")]
        role: String,

        /// Skip automatic .gitignore update
        #[arg(long)]
        no_gitignore: bool,
    },

    /// List all agents in the swarm
    Peers {
        /// Include dead agents
        #[arg(long)]
        all: bool,
    },

    /// Send a message to an agent
    Send {
        /// Target agent ID
        target: String,
        /// Message content
        message: String,
    },

    /// Spawn a new child agent
    Spawn {
        /// Agent role name
        #[arg(long)]
        role: String,

        /// Harness to use (claude, gemini, codex, grok, echo)
        #[arg(long)]
        harness: Option<String>,

        /// System prompt for the agent
        #[arg(long, default_value = "")]
        prompt: String,

        /// Communication mode: mesh or parent-only
        #[arg(long)]
        comms: Option<String>,

        /// Model override (e.g. claude-sonnet-4-6, gemini-2.5-flash)
        #[arg(long)]
        model: Option<String>,

        /// Give the agent its own git worktree (isolated branch)
        #[arg(long)]
        worktree: bool,
    },

    /// List available models for each harness (offline catalog)
    Models,

    /// Show own agent status
    Status,

    /// View an agent's recent activity (messages and output)
    Log {
        /// Agent ID to inspect
        target: String,

        /// Number of entries to show
        #[arg(short = 'n', long = "last", default_value = "20")]
        last: usize,

        /// Show only messages (sent and received)
        #[arg(long, conflicts_with = "output")]
        messages: bool,

        /// Show only harness output
        #[arg(long, conflicts_with = "messages")]
        output: bool,
    },

    /// Clean up an agent's worktree and optionally its branch
    Cleanup {
        /// Agent ID to clean up
        target: String,

        /// Also delete the git branch
        #[arg(long)]
        delete_branch: bool,
    },

    /// Signal that you have finished your task (self-termination)
    Done {
        /// Optional final message to send to your parent
        message: Option<String>,
    },

    /// Kill an agent
    Kill {
        /// Agent ID to terminate
        target: String,
    },

    /// Check harness availability, versions, and API keys
    Doctor,

    /// Print shell completion script to stdout
    Completions {
        /// Target shell
        shell: ShellArg,
    },

    /// Print roff manpage to stdout
    Manpage,
}

#[derive(Clone, ValueEnum)]
enum ShellArg {
    Bash,
    Zsh,
    Fish,
    Powershell,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            project_dir,
            port,
            harness,
            prompt,
            role,
            no_gitignore,
        } => {
            let config = SwarmConfig::load(Some(&project_dir));
            let port = port.unwrap_or_else(|| config.default_port.unwrap_or(9800));
            let harness = harness.unwrap_or_else(|| {
                config
                    .default_harness
                    .clone()
                    .unwrap_or_else(|| "echo".into())
            });

            if let Err(msg) = swarm::harness::preflight_check(&harness) {
                eprintln!("{msg}");
                std::process::exit(1);
            }

            if let Err(e) =
                run_orchestrator(project_dir, port, harness, prompt, role, no_gitignore).await
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Peers { all } => {
            if let Err(e) = cmd_peers(all).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Send { target, message } => {
            if let Err(e) = cmd_send(&target, &message).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Spawn {
            role,
            harness,
            prompt,
            comms,
            model,
            worktree,
        } => {
            let config = SwarmConfig::load(None);
            let harness = harness.unwrap_or_else(|| {
                config
                    .default_harness
                    .clone()
                    .unwrap_or_else(|| "echo".into())
            });
            let comms = comms.unwrap_or_else(|| {
                config
                    .default_comms
                    .clone()
                    .unwrap_or_else(|| "mesh".into())
            });

            if let Err(msg) = swarm::harness::preflight_check(&harness) {
                eprintln!("{msg}");
                std::process::exit(1);
            }

            if let Err(e) =
                cmd_spawn(&role, &harness, &prompt, &comms, model.as_deref(), worktree).await
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Models => {
            cmd_models_offline();
        }
        Commands::Status => {
            if let Err(e) = cmd_status().await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Log {
            target,
            last,
            messages,
            output,
        } => {
            let filter = if messages {
                "messages"
            } else if output {
                "output"
            } else {
                "all"
            };
            if let Err(e) = cmd_log(&target, last, filter).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Cleanup {
            target,
            delete_branch,
        } => {
            if let Err(e) = cmd_cleanup(&target, delete_branch).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Done { message } => {
            if let Err(e) = cmd_done(message.as_deref()).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Kill { target } => {
            if let Err(e) = cmd_kill(&target).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Doctor => {
            cmd_doctor();
        }
        Commands::Completions { shell } => {
            cmd_completions(shell);
        }
        Commands::Manpage => {
            cmd_manpage();
        }
    }
}

async fn run_orchestrator(
    project_dir: PathBuf,
    port: u16,
    harness: String,
    prompt: String,
    role: String,
    no_gitignore: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("swarm=info")),
        )
        .init();

    let project_dir = std::fs::canonicalize(&project_dir)?;
    let data_dir = project_dir.join(".swarm");
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(data_dir.join("agents"))?;

    if !no_gitignore {
        let gitignore = project_dir.join(".gitignore");
        let needs_entry = if gitignore.exists() {
            let content = std::fs::read_to_string(&gitignore)?;
            !content
                .lines()
                .any(|l| l.trim() == ".swarm" || l.trim() == ".swarm/")
        } else {
            true
        };
        if needs_entry {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&gitignore)?;
            writeln!(f, "\n.swarm/")?;
            tracing::info!("added .swarm/ to .gitignore");
        }
    }

    // Port conflict detection
    match std::net::TcpListener::bind(format!("127.0.0.1:{port}")) {
        Ok(listener) => drop(listener),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            eprintln!(
                "error: port {} is in use. Pass --port <other> or kill the existing process (lsof -i :{}).",
                port, port
            );
            std::process::exit(1);
        }
        Err(e) => return Err(e.into()),
    }

    let db = Arc::new(Db::open(&data_dir.join("swarm.db"))?);
    let registry = HarnessRegistry::new();
    let addr = format!("http://127.0.0.1:{port}");
    let orch = Arc::new(Orchestrator::new(
        db,
        registry,
        addr.clone(),
        project_dir,
        data_dir,
    ));
    let resumed = orch.resume_existing_workers()?;
    if resumed > 0 {
        tracing::info!("resumed {resumed} existing agent worker(s)");
    }

    // Start HTTP server
    let router = swarm::server::router(orch.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    tracing::info!("swarm orchestrator listening on {addr}");

    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!("server error: {e}");
        }
    });

    // Spawn parent agent (prompt auto-sent if non-empty)
    let parent = orch.spawn_agent(&role, &harness, &prompt, None, "mesh")?;
    tracing::info!("parent agent: {} ({})", parent.id, parent.harness);

    // Stream events to stdout
    let mut rx = orch.subscribe();
    let event_loop = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => match &event {
                    SwarmEvent::AgentOutput { agent_id, text } => {
                        println!("[{agent_id}] {text}");
                    }
                    SwarmEvent::AgentError { agent_id, error } => {
                        eprintln!("[{agent_id}] ERROR: {error}");
                    }
                    SwarmEvent::AgentSpawned { agent } => {
                        println!(
                            "[swarm] spawned: {} ({}, {})",
                            agent.id, agent.harness, agent.role
                        );
                    }
                    SwarmEvent::AgentDone { agent_id, message } => {
                        if let Some(msg) = message {
                            println!("[swarm] done: {agent_id} - {msg}");
                        } else {
                            println!("[swarm] done: {agent_id}");
                        }
                    }
                    SwarmEvent::AgentKilled { agent_id } => {
                        println!("[swarm] killed: {agent_id}");
                    }
                    SwarmEvent::AgentStatus { agent_id, status } => {
                        println!("[swarm] {agent_id} -> {status}");
                    }
                    SwarmEvent::MessageRouted { from, to } => {
                        println!("[swarm] message: {from} -> {to}");
                    }
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    eprintln!("[swarm] warning: skipped {skipped} lagged event(s)");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });

    tokio::select! {
        _ = server_handle => {}
        _ = event_loop => {}
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
        }
    }

    orch.shutdown_all().await?;

    Ok(())
}

fn swarm_socket() -> String {
    std::env::var("SWARM_SOCKET").unwrap_or_else(|_| "http://127.0.0.1:9800".to_string())
}

fn swarm_agent_id() -> Option<String> {
    std::env::var("SWARM_AGENT_ID").ok()
}

async fn cmd_peers(include_all: bool) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let mut url = format!("{socket}/api/agents");
    if let Some(agent_id) = swarm_agent_id() {
        url.push_str(&format!("?perspective={agent_id}"));
    }

    let resp = reqwest::get(&url).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    let resp: Vec<serde_json::Value> = resp.json().await?;

    if resp.is_empty() {
        println!("no agents");
        return Ok(());
    }

    let has_perspective = swarm_agent_id().is_some();
    for agent in &resp {
        let status = agent["status"].as_str().unwrap_or("?");
        if !include_all && status == "dead" {
            continue;
        }
        let id = agent["id"].as_str().unwrap_or("?");
        let harness = agent["harness"].as_str().unwrap_or("?");
        let role = agent["role"].as_str().unwrap_or("?");
        let model = agent["model"].as_str().unwrap_or("");
        let model_display = if model.is_empty() { "(default)" } else { model };

        if has_perspective {
            let relation = agent["relation"].as_str().unwrap_or("?");
            println!(
                "{:<24} {:<10} {:<10} {:<16} {:<12} {}",
                id, harness, status, role, relation, model_display
            );
        } else {
            let parent = agent["parent_id"].as_str().unwrap_or("-");
            println!(
                "{:<24} {:<10} {:<10} {:<16} parent={:<20} {}",
                id, harness, status, role, parent, model_display
            );
        }
    }
    Ok(())
}

async fn cmd_send(
    target: &str,
    message: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let from = swarm_agent_id().unwrap_or_else(|| "user".to_string());
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/messages"))
        .json(&serde_json::json!({
            "from": from,
            "to": target,
            "content": message,
        }))
        .send()
        .await?;

    if resp.status().is_success() {
        println!("sent to {target}");
    } else {
        return Err(response_error(resp).await);
    }
    Ok(())
}

async fn cmd_spawn(
    role: &str,
    harness: &str,
    prompt: &str,
    comms: &str,
    model: Option<&str>,
    worktree: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let parent_id = swarm_agent_id();
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/agents"))
        .json(&serde_json::json!({
            "role": role,
            "harness": harness,
            "system_prompt": prompt,
            "parent_id": parent_id,
            "comms": comms,
            "model": model,
            "worktree": worktree,
        }))
        .send()
        .await?;

    if resp.status().is_success() {
        let agent: serde_json::Value = resp.json().await?;
        let id = agent["id"].as_str().unwrap_or("?");
        println!("{id}");
    } else {
        return Err(response_error(resp).await);
    }
    Ok(())
}

async fn cmd_status() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let agent_id = swarm_agent_id().ok_or("SWARM_AGENT_ID not set")?;
    let resp = reqwest::get(format!("{socket}/api/agents/{agent_id}")).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    let resp: serde_json::Value = resp.json().await?;

    let model = resp["model"].as_str().unwrap_or("");
    let model_display = if model.is_empty() { "(default)" } else { model };
    println!("id:      {}", resp["id"].as_str().unwrap_or("?"));
    println!("role:    {}", resp["role"].as_str().unwrap_or("?"));
    println!("harness: {}", resp["harness"].as_str().unwrap_or("?"));
    println!("model:   {}", model_display);
    println!("status:  {}", resp["status"].as_str().unwrap_or("?"));
    println!("parent:  {}", resp["parent_id"].as_str().unwrap_or("-"));
    println!("comms:   {}", resp["comms"].as_str().unwrap_or("?"));
    Ok(())
}

fn cmd_models_offline() {
    for kind in CliKind::all_kinds() {
        let name = kind.default_binary();
        let default = kind.default_model();
        let models = kind.known_models();
        println!("{}:", name);
        for m in models {
            if *m == default {
                println!("  {} (default)", m);
            } else {
                println!("  {}", m);
            }
        }
    }
}

async fn cmd_log(
    target: &str,
    limit: usize,
    filter: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let mut url = format!("{socket}/api/agents/{target}/log?n={limit}");
    if filter != "all" {
        url.push_str(&format!("&type={filter}"));
    }
    let resp = reqwest::get(&url).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    let resp: Vec<serde_json::Value> = resp.json().await?;

    if resp.is_empty() {
        println!("no log entries for {target}");
        return Ok(());
    }

    for entry in &resp {
        let ts = entry["timestamp"].as_str().unwrap_or("?");
        let short_ts = if ts.len() > 19 { &ts[..19] } else { ts };
        let kind = entry["kind"].as_str().unwrap_or("?");
        let peer = entry["peer"].as_str().unwrap_or("");
        let content = entry["content"].as_str().unwrap_or("");

        let display_content = if content.len() > 200 {
            format!("{}... ({} chars total)", &content[..200], content.len())
        } else {
            content.to_string()
        };

        match kind {
            "recv" => {
                println!("{} recv  from:{:<20} {}", short_ts, peer, display_content);
            }
            "sent" => {
                println!("{} sent  to:{:<22} {}", short_ts, peer, display_content);
            }
            _ => {
                println!("{} {:<5} {}", short_ts, kind, display_content);
            }
        }
    }

    Ok(())
}

async fn cmd_cleanup(
    target: &str,
    delete_branch: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let client = reqwest::Client::new();
    let mut url = format!("{socket}/api/agents/{target}/cleanup");
    if delete_branch {
        url.push_str("?delete_branch=true");
    }
    let resp = client.post(&url).send().await?;

    if resp.status().is_success() {
        println!("cleaned up {target}");
    } else {
        return Err(response_error(resp).await);
    }
    Ok(())
}

async fn cmd_done(message: Option<&str>) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let agent_id = swarm_agent_id().ok_or("SWARM_AGENT_ID not set")?;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/agents/{agent_id}/done"))
        .json(&serde_json::json!({
            "message": message,
        }))
        .send()
        .await?;

    if resp.status().is_success() {
        println!("done");
    } else {
        return Err(response_error(resp).await);
    }
    Ok(())
}

async fn cmd_kill(target: &str) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{socket}/api/agents/{target}"))
        .send()
        .await?;

    if resp.status().is_success() {
        println!("killed {target}");
    } else {
        return Err(response_error(resp).await);
    }
    Ok(())
}

async fn response_error(resp: reqwest::Response) -> Box<dyn std::error::Error> {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    let detail = if body.trim().is_empty() {
        status.to_string()
    } else if let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) {
        let mut detail = value
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or(body.trim())
            .to_string();
        if let Some(hint) = value.get("hint").and_then(|v| v.as_str()) {
            detail.push_str("; hint: ");
            detail.push_str(hint);
        }
        detail
    } else {
        body
    };
    format!("request failed ({status}): {detail}").into()
}

fn cmd_doctor() {
    println!("swarm doctor");
    println!("{:-<70}", "");

    let harnesses = CliKind::all_kinds();
    println!(
        "{:<10} {:<30} {:<8} {:<24} {:<8}",
        "Harness", "Binary", "Found", "Version", "API Key"
    );
    println!("{:-<70}", "");

    for kind in harnesses {
        let name = kind.default_binary();
        let env_var = kind.env_var_name();
        let binary = kind.resolved_binary();
        let bin_source = if std::env::var(env_var).is_ok() {
            format!("{} ({})", binary, env_var)
        } else {
            binary.clone()
        };

        let found = binary_on_path(&binary);
        let found_str = if found { "Y" } else { "N" };

        let version = if found {
            get_binary_version(&binary)
        } else {
            "-".to_string()
        };

        let api_key_present = kind
            .api_key_env_names()
            .iter()
            .any(|k| std::env::var(k).is_ok());
        let api_key_str = if api_key_present { "Y" } else { "N" };

        println!(
            "{:<10} {:<30} {:<8} {:<24} {:<8}",
            name, bin_source, found_str, version, api_key_str
        );
    }

    println!("{:-<70}", "");

    let git_found = binary_on_path("git");
    println!(
        "{:<10} {:<30} {:<8} {:<24}",
        "git",
        "git",
        if git_found { "Y" } else { "N" },
        if git_found {
            get_binary_version("git")
        } else {
            "-".to_string()
        }
    );

    println!("{:-<70}", "");

    let all_ok = harnesses
        .iter()
        .all(|k| binary_on_path(&k.resolved_binary()))
        && git_found;
    if all_ok {
        println!("PASS: all harnesses and git found");
    } else {
        println!("FAIL: some harnesses or git not found (see above)");
    }
}

fn binary_on_path(binary: &str) -> bool {
    let path = std::path::Path::new(binary);
    if path.is_absolute() {
        return path.exists();
    }
    std::process::Command::new("which")
        .arg(binary)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn get_binary_version(binary: &str) -> String {
    let result = std::process::Command::new(binary)
        .arg("--version")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output();

    match result {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout.lines().next().unwrap_or("-").trim().to_string()
        }
        _ => "-".to_string(),
    }
}

fn cmd_completions(shell: ShellArg) {
    let clap_shell = match shell {
        ShellArg::Bash => clap_complete::Shell::Bash,
        ShellArg::Zsh => clap_complete::Shell::Zsh,
        ShellArg::Fish => clap_complete::Shell::Fish,
        ShellArg::Powershell => clap_complete::Shell::PowerShell,
    };
    let mut cmd = Cli::command();
    clap_complete::generate(clap_shell, &mut cmd, "swarm", &mut std::io::stdout());
}

fn cmd_manpage() {
    let cmd = Cli::command();
    let man = clap_mangen::Man::new(cmd);
    man.render(&mut std::io::stdout())
        .expect("failed to write manpage");
}

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use swarm::db::Db;
use swarm::harness::HarnessRegistry;
use swarm::orchestrator::{Orchestrator, SwarmEvent};
use swarm::server;

#[derive(Parser)]
#[command(name = "swarm", about = "Multi-agent CLI orchestrator")]
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
        #[arg(long, default_value = "9800")]
        port: u16,

        /// Harness for the parent agent (claude, gemini, codex, grok, echo)
        #[arg(long, default_value = "echo")]
        harness: String,

        /// Initial prompt for the parent agent
        #[arg(long, default_value = "")]
        prompt: String,

        /// Role name for the parent agent
        #[arg(long, default_value = "coordinator")]
        role: String,
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
        #[arg(long, default_value = "echo")]
        harness: String,

        /// System prompt for the agent
        #[arg(long, default_value = "")]
        prompt: String,

        /// Communication mode: mesh or parent-only
        #[arg(long, default_value = "mesh")]
        comms: String,

        /// Model override (e.g. claude-sonnet-4-6, gemini-2.5-flash)
        #[arg(long)]
        model: Option<String>,
    },

    /// List available models for each harness
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

    /// Kill an agent
    Kill {
        /// Agent ID to terminate
        target: String,
    },
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
        } => {
            if let Err(e) = run_orchestrator(project_dir, port, harness, prompt, role).await {
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
        } => {
            if let Err(e) = cmd_spawn(&role, &harness, &prompt, &comms, model.as_deref()).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Models => {
            if let Err(e) = cmd_models().await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
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
        Commands::Kill { target } => {
            if let Err(e) = cmd_kill(&target).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
    }
}

async fn run_orchestrator(
    project_dir: PathBuf,
    port: u16,
    harness: String,
    prompt: String,
    role: String,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "swarm=info".parse().unwrap()),
        )
        .init();

    let project_dir = std::fs::canonicalize(&project_dir)?;
    let data_dir = project_dir.join(".swarm");
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(data_dir.join("agents"))?;

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

    // Start HTTP server
    let router = server::router(orch.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    tracing::info!("swarm orchestrator listening on {addr}");

    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!("server error: {e}");
        }
    });

    // Spawn parent agent
    let parent = orch.spawn_agent(&role, &harness, &prompt, None, "mesh")?;
    tracing::info!("parent agent: {} ({})", parent.id, parent.harness);

    // Send initial prompt if provided
    if !prompt.is_empty() {
        orch.send_message("user", &parent.id, &prompt).await?;
    }

    // Stream events to stdout
    let mut rx = orch.subscribe();
    let event_loop = tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            match &event {
                SwarmEvent::AgentOutput { agent_id, text } => {
                    println!("[{agent_id}] {text}");
                }
                SwarmEvent::AgentError { agent_id, error } => {
                    eprintln!("[{agent_id}] ERROR: {error}");
                }
                SwarmEvent::AgentSpawned { agent } => {
                    println!("[swarm] spawned: {} ({}, {})", agent.id, agent.harness, agent.role);
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

    let resp: Vec<serde_json::Value> = reqwest::get(&url).await?.json().await?;

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

async fn cmd_send(target: &str, message: &str) -> std::result::Result<(), Box<dyn std::error::Error>> {
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
        let body = resp.text().await?;
        eprintln!("failed: {body}");
    }
    Ok(())
}

async fn cmd_spawn(
    role: &str,
    harness: &str,
    prompt: &str,
    comms: &str,
    model: Option<&str>,
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
        }))
        .send()
        .await?;

    if resp.status().is_success() {
        let agent: serde_json::Value = resp.json().await?;
        let id = agent["id"].as_str().unwrap_or("?");
        println!("{id}");
    } else {
        let body = resp.text().await?;
        eprintln!("failed: {body}");
    }
    Ok(())
}

async fn cmd_status() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let agent_id = swarm_agent_id().ok_or("SWARM_AGENT_ID not set")?;
    let resp: serde_json::Value =
        reqwest::get(format!("{socket}/api/agents/{agent_id}"))
            .await?
            .json()
            .await?;

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

async fn cmd_models() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let resp: Vec<serde_json::Value> = reqwest::get(format!("{socket}/api/models"))
        .await?
        .json()
        .await?;

    for harness in &resp {
        let name = harness["harness"].as_str().unwrap_or("?");
        let default = harness["default_model"].as_str().unwrap_or("?");
        let models = harness["models"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .map(|v| v.as_str().unwrap_or("?"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        println!("{}:", name);
        for m in &models {
            if *m == default {
                println!("  {} (default)", m);
            } else {
                println!("  {}", m);
            }
        }
    }
    Ok(())
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
    let resp: Vec<serde_json::Value> = reqwest::get(&url).await?.json().await?;

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
        let body = resp.text().await?;
        eprintln!("failed: {body}");
    }
    Ok(())
}

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use swarm::config::SwarmConfig;
use swarm::db::Db;
use swarm::harness::{CliKind, HarnessRegistry};
use swarm::orchestrator::{Orchestrator, SwarmEvent};

const CLI_ENTRY_SEPARATOR: &str = "------------";

#[derive(Parser)]
#[command(
    name = "swarm",
    about = "Multi-agent CLI orchestrator",
    long_about = "Multi-agent CLI orchestrator that coordinates Claude, Codex, Gemini, and Grok CLIs.\n\n\
        Direct responses:\n  \
        swarm inbox <agent-id>          Read messages sent to the user/calling agent from one agent\n  \
        swarm inbox --all               Read all recent messages sent to the user/calling agent\n  \
        swarm log user --messages       Inspect the broader user message log\n\n\
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

        /// Override data directory (default: platform data dir)
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Path to the dashboard frontend dist directory (dev override)
        #[arg(long)]
        dashboard: Option<PathBuf>,

        /// Teammate harnesses to spawn for a task run, comma-separated or repeated
        #[arg(long, visible_alias = "teammates", value_delimiter = ',')]
        team: Vec<String>,

        /// Start the run and return immediately instead of watching direct responses
        #[arg(long)]
        detach: bool,

        /// Task to run. When provided, swarm uses a tracked run and auto-starts the daemon if needed.
        #[arg(value_name = "TASK", trailing_var_arg = true)]
        task: Vec<String>,
    },

    /// Start only the swarm daemon/API server
    Serve {
        /// Project directory (agents work here)
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,

        /// Server port
        #[arg(long)]
        port: Option<u16>,

        /// Skip automatic .gitignore update
        #[arg(long)]
        no_gitignore: bool,

        /// Override data directory (default: platform data dir)
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Path to the dashboard frontend dist directory (dev override)
        #[arg(long)]
        dashboard: Option<PathBuf>,
    },

    /// List all agents in the swarm
    Peers {
        /// Include done agents
        #[arg(long)]
        all: bool,

        /// Show a specific run id, or "current" (default)
        #[arg(long, default_value = "current")]
        run: String,

        /// Include agents from every run in this project
        #[arg(long, conflicts_with = "run")]
        all_runs: bool,

        /// Output JSON
        #[arg(long)]
        json: bool,
    },

    /// Send a direct message to an agent or user
    Send {
        /// Target agent ID, or "user" to notify the user
        target: String,
        /// Message content
        message: String,
    },

    /// Read direct messages sent to the user/calling agent from one agent
    Inbox {
        /// Source agent ID to read messages from; omit only with --all
        #[arg(conflicts_with = "all")]
        from: Option<String>,

        /// Recipient to inspect (default: current agent when SWARM_AGENT_ID is set, otherwise "user"; use "me" for the current agent)
        #[arg(long)]
        to: Option<String>,

        /// Show all recent direct messages sent to the recipient
        #[arg(long)]
        all: bool,

        /// Show only messages newer than the saved inbox cursor for this recipient/run
        #[arg(long)]
        new: bool,

        /// Show messages after an RFC3339 timestamp instead of using the saved cursor
        #[arg(long, conflicts_with = "new")]
        since: Option<String>,

        /// Scope user inbox reads to a run id, or "current" (default)
        #[arg(long, default_value = "current")]
        run: String,

        /// Include messages from every run
        #[arg(long, conflicts_with = "run")]
        all_runs: bool,

        /// Number of recent messages to show
        #[arg(
            short = 'n',
            long = "last",
            visible_alias = "tail",
            default_value = "20"
        )]
        last: usize,

        /// Output JSON
        #[arg(long)]
        json: bool,

        /// Search message content case-insensitively before applying the limit
        #[arg(long, alias = "grep")]
        search: Option<String>,

        /// Show exact full content in text output
        #[arg(long)]
        raw: bool,

        /// Maximum content characters to show in text output (default: full message; 0 also disables truncation)
        #[arg(long)]
        truncate: Option<usize>,
    },

    /// Spawn a new child agent
    Spawn {
        /// Agent role name
        #[arg(long)]
        role: String,

        /// Harness to use (claude, gemini, codex, grok, echo)
        #[arg(long)]
        harness: Option<String>,

        /// Task prompt for the agent; include how it should report back, e.g. `swarm send <your-id> "..."`
        #[arg(long, default_value = "")]
        prompt: String,

        /// Communication mode: mesh or parent-only
        #[arg(long)]
        comms: Option<String>,

        /// Model override supported by the selected harness CLI
        #[arg(long)]
        model: Option<String>,

        /// Give the agent its own git worktree (isolated branch)
        #[arg(long)]
        worktree: bool,

        /// Attach the agent to a run id; defaults to SWARM_RUN_ID when present
        #[arg(long)]
        run: Option<String>,
    },

    /// Watch new direct responses sent to the user/current agent
    Watch {
        /// Recipient to inspect (default: current agent when SWARM_AGENT_ID is set, otherwise "user")
        #[arg(long)]
        to: Option<String>,

        /// Source agent ID to watch; omit with --all
        #[arg(conflicts_with = "all")]
        from: Option<String>,

        /// Watch all source agents
        #[arg(long)]
        all: bool,

        /// Scope user inbox reads to a run id, or "current" (default)
        #[arg(long, default_value = "current")]
        run: String,

        /// Include messages from every run
        #[arg(long, conflicts_with = "run")]
        all_runs: bool,

        /// Number of messages to fetch per poll
        #[arg(
            short = 'n',
            long = "last",
            visible_alias = "tail",
            default_value = "20"
        )]
        last: usize,

        /// Poll interval in milliseconds
        #[arg(long, default_value = "2000")]
        interval_ms: u64,

        /// Output JSON
        #[arg(long)]
        json: bool,
    },

    /// Show harness model behavior (harness CLIs choose their own defaults)
    Models {
        /// Output JSON
        #[arg(long)]
        json: bool,
    },

    /// Show own agent status
    Status {
        /// Output JSON
        #[arg(long)]
        json: bool,
    },

    /// View recent activity for an agent, or the broader message log for "user"
    Log {
        /// Agent ID to inspect, or "user" to inspect the broader user message log
        target: String,

        /// Number of entries to show
        #[arg(
            short = 'n',
            long = "last",
            visible_alias = "tail",
            default_value = "20"
        )]
        last: usize,

        /// Show only messages (sent and received)
        #[arg(long, conflicts_with = "output")]
        messages: bool,

        /// Show only harness output
        #[arg(long, conflicts_with = "messages")]
        output: bool,

        /// Output JSON
        #[arg(long)]
        json: bool,

        /// Search log content case-insensitively before applying the limit
        #[arg(long, alias = "grep")]
        search: Option<String>,

        /// Show exact full content in text output
        #[arg(long)]
        raw: bool,

        /// Maximum content characters to show in text output (default: full with --messages, 500 otherwise; 0 disables truncation)
        #[arg(long)]
        truncate: Option<usize>,
    },

    /// Show a compact digest for a run or one agent
    Brief {
        /// Agent ID to inspect. Omit for run-level summary.
        target: Option<String>,

        /// Number of recent agents/log entries to show
        #[arg(short = 'n', long = "last", default_value = "20")]
        last: usize,

        /// Search compact run fields or agent log content
        #[arg(long, alias = "grep")]
        search: Option<String>,

        /// Output JSON
        #[arg(long)]
        json: bool,
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

        /// Structured outcome: done, partial, blocked, failed, etc.
        #[arg(long)]
        outcome: Option<String>,

        /// Deliverable produced, such as a branch, file, report, or none
        #[arg(long)]
        deliverable: Option<String>,

        /// Verification performed and result
        #[arg(long)]
        checks: Option<String>,

        /// Residual risk or unverified area
        #[arg(long)]
        risk: Option<String>,

        /// Recommended next action for the caller
        #[arg(long)]
        next_action: Option<String>,
    },

    /// Stop an agent and mark it done
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
            data_dir,
            dashboard,
            team,
            detach,
            task,
        } => {
            let config = SwarmConfig::load(Some(&project_dir));
            let port = port.unwrap_or_else(|| config.default_port.unwrap_or(9800));
            let harness = harness.unwrap_or_else(|| {
                config
                    .default_harness
                    .clone()
                    .unwrap_or_else(|| "echo".into())
            });

            let resolved_data_dir = SwarmConfig::resolve_data_dir(data_dir.as_deref(), &config);

            if let Err(msg) = swarm::harness::preflight_check(&harness) {
                eprintln!("{msg}");
                std::process::exit(1);
            }

            let task_text = task.join(" ").trim().to_string();
            let result = if task_text.is_empty() && team.is_empty() {
                run_orchestrator(
                    project_dir,
                    resolved_data_dir,
                    port,
                    Some(ParentSpec {
                        harness,
                        prompt,
                        role,
                    }),
                    no_gitignore,
                    dashboard,
                )
                .await
            } else {
                run_task_swarm(
                    project_dir,
                    resolved_data_dir,
                    port,
                    harness,
                    role,
                    prompt,
                    team,
                    task_text,
                    detach,
                    no_gitignore,
                    dashboard,
                )
                .await
            };

            if let Err(e) = result {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Serve {
            project_dir,
            port,
            no_gitignore,
            data_dir,
            dashboard,
        } => {
            let config = SwarmConfig::load(Some(&project_dir));
            let port = port.unwrap_or_else(|| config.default_port.unwrap_or(9800));
            let resolved_data_dir = SwarmConfig::resolve_data_dir(data_dir.as_deref(), &config);

            if let Err(e) = run_orchestrator(
                project_dir,
                resolved_data_dir,
                port,
                None,
                no_gitignore,
                dashboard,
            )
            .await
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Peers {
            all,
            run,
            all_runs,
            json,
        } => {
            let effective_run = effective_run_arg(&run);
            let run = (!all_runs).then_some(effective_run.as_str());
            if let Err(e) = cmd_peers(all, run, json).await {
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
        Commands::Inbox {
            from,
            to,
            all,
            new,
            since,
            run,
            all_runs,
            last,
            json,
            search,
            raw,
            truncate,
        } => {
            let truncate = if raw { 0 } else { truncate.unwrap_or(0) };
            let effective_run = effective_run_arg(&run);
            let run = (!all_runs).then_some(effective_run.as_str());
            if let Err(e) = cmd_inbox(
                from.as_deref(),
                all,
                to.as_deref(),
                run,
                new,
                since.as_deref(),
                last,
                json,
                truncate,
                search.as_deref(),
            )
            .await
            {
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
            run,
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

            if let Err(e) = cmd_spawn(
                &role,
                &harness,
                &prompt,
                &comms,
                model.as_deref(),
                worktree,
                run.as_deref(),
            )
            .await
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Watch {
            to,
            from,
            all,
            run,
            all_runs,
            last,
            interval_ms,
            json,
        } => {
            let effective_run = effective_run_arg(&run);
            if let Err(e) = cmd_watch(
                from.as_deref(),
                all,
                to.as_deref(),
                (!all_runs).then_some(effective_run.as_str()),
                last,
                interval_ms,
                json,
            )
            .await
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Models { json } => {
            if let Err(e) = cmd_models_offline(json) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Status { json } => {
            if let Err(e) = cmd_status(json).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Log {
            target,
            last,
            messages,
            output,
            json,
            search,
            raw,
            truncate,
        } => {
            let filter = if messages {
                "messages"
            } else if output {
                "output"
            } else {
                "all"
            };
            let truncate = if raw {
                0
            } else {
                truncate.unwrap_or(if messages { 0 } else { 500 })
            };
            if let Err(e) = cmd_log(&target, last, filter, json, truncate, search.as_deref()).await
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Brief {
            target,
            last,
            search,
            json,
        } => {
            if let Err(e) = cmd_brief(target.as_deref(), last, search.as_deref(), json).await {
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
        Commands::Done {
            message,
            outcome,
            deliverable,
            checks,
            risk,
            next_action,
        } => {
            if let Err(e) = cmd_done(
                message.as_deref(),
                outcome.as_deref(),
                deliverable.as_deref(),
                checks.as_deref(),
                risk.as_deref(),
                next_action.as_deref(),
            )
            .await
            {
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

struct ParentSpec {
    harness: String,
    prompt: String,
    role: String,
}

async fn run_orchestrator(
    project_dir: PathBuf,
    data_dir: PathBuf,
    port: u16,
    parent: Option<ParentSpec>,
    no_gitignore: bool,
    dashboard: Option<PathBuf>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("swarm=info")),
        )
        .init();

    let project_dir = std::fs::canonicalize(&project_dir)?;
    std::fs::create_dir_all(&data_dir)?;
    let agents_dir = project_dir.join(".swarm").join("agents");
    std::fs::create_dir_all(&agents_dir)?;

    SwarmConfig::write_breadcrumb(&data_dir);
    tracing::info!("data directory: {}", data_dir.display());

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
    let router = swarm::server::router_with_dashboard(orch.clone(), dashboard);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    tracing::info!("swarm orchestrator listening on {addr}");

    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!("server error: {e}");
        }
    });

    if let Some(parent_spec) = parent {
        let parent = orch.spawn_agent(
            &parent_spec.role,
            &parent_spec.harness,
            &parent_spec.prompt,
            None,
            "mesh",
        )?;
        tracing::info!("parent agent: {} ({})", parent.id, parent.harness);
    }

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
                        println!("[swarm] stopped: {agent_id}");
                    }
                    SwarmEvent::AgentStatus { agent_id, status } => {
                        println!("[swarm] {agent_id} -> {status}");
                    }
                    SwarmEvent::MessageRouted { from, to } => {
                        println!("[swarm] message: {from} -> {to}");
                    }
                    SwarmEvent::UserNotification { from, content } => {
                        println!("[NOTIFY {from}] {content}");
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

async fn run_task_swarm(
    project_dir: PathBuf,
    data_dir: PathBuf,
    port: u16,
    harness: String,
    role: String,
    prompt: String,
    team: Vec<String>,
    task: String,
    detach: bool,
    no_gitignore: bool,
    dashboard: Option<PathBuf>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let task = if task.trim().is_empty() {
        prompt.trim().to_string()
    } else if prompt.trim().is_empty() {
        task.trim().to_string()
    } else {
        format!("{}\n\n{}", task.trim(), prompt.trim())
    };
    if task.is_empty() {
        return Err("provide a task argument or --prompt for swarm run".into());
    }

    for spec in &team {
        let (_, teammate_harness) = parse_team_spec(spec)?;
        if let Err(msg) = swarm::harness::preflight_check(&teammate_harness) {
            return Err(msg.into());
        }
    }

    let socket = ensure_daemon(project_dir, data_dir, port, no_gitignore, dashboard).await?;
    let run = create_run_http(&socket, &task).await?;
    let run_id = run["id"].as_str().ok_or("create run returned no id")?;

    let coordinator = spawn_agent_http(
        &socket,
        SpawnHttpRequest {
            role: &role,
            harness: &harness,
            prompt: "",
            parent_id: None,
            comms: "mesh",
            model: None,
            worktree: false,
            run_id: Some(run_id),
            user_launched: true,
        },
    )
    .await?;
    let coordinator_id = coordinator["id"]
        .as_str()
        .ok_or("coordinator spawn returned no id")?
        .to_string();

    update_run_root_http(&socket, run_id, &coordinator_id).await?;

    let mut teammates = Vec::new();
    for (idx, spec) in team.iter().enumerate() {
        let (role_hint, teammate_harness) = parse_team_spec(spec)?;
        let teammate_role = sanitize_role(&format!("mate{}-{}", idx + 1, role_hint));
        let teammate_prompt = teammate_prompt(&task, run_id, &coordinator_id, &teammate_role);
        let agent = spawn_agent_http(
            &socket,
            SpawnHttpRequest {
                role: &teammate_role,
                harness: &teammate_harness,
                prompt: &teammate_prompt,
                parent_id: Some(&coordinator_id),
                comms: "mesh",
                model: None,
                worktree: false,
                run_id: Some(run_id),
                user_launched: false,
            },
        )
        .await?;
        teammates.push(agent);
    }

    let coordinator_prompt = coordinator_prompt(&task, run_id, &coordinator_id, &teammates);
    send_message_http(&socket, "user", &coordinator_id, &coordinator_prompt).await?;

    println!("run: {run_id}");
    println!("coordinator: {coordinator_id}");
    if !teammates.is_empty() {
        println!("teammates:");
        for teammate in &teammates {
            println!(
                "  {:<26} {:<10} {}",
                teammate["id"].as_str().unwrap_or("?"),
                teammate["harness"].as_str().unwrap_or("?"),
                teammate["role"].as_str().unwrap_or("?")
            );
        }
    }
    println!("dashboard: {socket}");
    println!("{CLI_ENTRY_SEPARATOR}");
    std::env::set_var("SWARM_SOCKET", &socket);

    if detach {
        println!("detached. watch responses with: swarm watch --run {run_id} --all");
        return Ok(());
    }

    println!("watching direct responses for run {run_id}; press Ctrl-C to stop");
    cmd_watch(None, true, Some("user"), Some(run_id), 20, 2_000, false).await
}

async fn ensure_daemon(
    project_dir: PathBuf,
    data_dir: PathBuf,
    port: u16,
    no_gitignore: bool,
    dashboard: Option<PathBuf>,
) -> std::result::Result<String, Box<dyn std::error::Error>> {
    let socket = format!("http://127.0.0.1:{port}");
    if daemon_healthy(&socket).await {
        return Ok(socket);
    }

    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("serve")
        .arg("--project-dir")
        .arg(&project_dir)
        .arg("--port")
        .arg(port.to_string())
        .arg("--data-dir")
        .arg(&data_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    if no_gitignore {
        cmd.arg("--no-gitignore");
    }
    if let Some(dashboard) = dashboard {
        cmd.arg("--dashboard").arg(dashboard);
    }
    cmd.spawn()?;

    for _ in 0..80 {
        if daemon_healthy(&socket).await {
            return Ok(socket);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!("daemon did not become healthy at {socket}").into())
}

async fn daemon_healthy(socket: &str) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(400))
        .build()
    {
        Ok(client) => client,
        Err(_) => return false,
    };
    client
        .get(format!("{socket}/api/health"))
        .send()
        .await
        .map(|resp| resp.status().is_success())
        .unwrap_or(false)
}

struct SpawnHttpRequest<'a> {
    role: &'a str,
    harness: &'a str,
    prompt: &'a str,
    parent_id: Option<&'a str>,
    comms: &'a str,
    model: Option<&'a str>,
    worktree: bool,
    run_id: Option<&'a str>,
    user_launched: bool,
}

async fn create_run_http(
    socket: &str,
    task: &str,
) -> std::result::Result<serde_json::Value, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/runs"))
        .json(&serde_json::json!({ "task": task }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    Ok(resp.json().await?)
}

async fn update_run_root_http(
    socket: &str,
    run_id: &str,
    root_agent_id: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/runs/{run_id}"))
        .json(&serde_json::json!({ "root_agent_id": root_agent_id }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    Ok(())
}

async fn spawn_agent_http(
    socket: &str,
    req: SpawnHttpRequest<'_>,
) -> std::result::Result<serde_json::Value, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/agents"))
        .json(&serde_json::json!({
            "role": req.role,
            "harness": req.harness,
            "system_prompt": req.prompt,
            "parent_id": req.parent_id,
            "comms": req.comms,
            "model": req.model,
            "worktree": req.worktree,
            "run_id": req.run_id,
            "user_launched": req.user_launched,
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    Ok(resp.json().await?)
}

async fn send_message_http(
    socket: &str,
    from: &str,
    to: &str,
    content: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/messages"))
        .json(&serde_json::json!({
            "from": from,
            "to": to,
            "content": content,
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    Ok(())
}

fn parse_team_spec(
    spec: &str,
) -> std::result::Result<(String, String), Box<dyn std::error::Error>> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err("empty teammate spec".into());
    }
    if let Some((role, harness)) = spec.split_once(':') {
        let harness = harness.trim().to_string();
        let role = role
            .trim()
            .is_empty()
            .then(|| harness.clone())
            .unwrap_or_else(|| role.trim().to_string());
        Ok((role, harness))
    } else {
        Ok((spec.to_string(), spec.to_string()))
    }
}

fn sanitize_role(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(b as char);
        } else {
            out.push('-');
        }
    }
    out.trim_matches('-').to_string()
}

fn teammate_prompt(task: &str, run_id: &str, coordinator_id: &str, role: &str) -> String {
    format!(
        "You are {role} in swarm run {run_id}.\n\nTask:\n{task}\n\nWork independently, send concise progress or conclusions to coordinator {coordinator_id} with `swarm send {coordinator_id} \"...\"`, then call `swarm done \"summary\"` when finished. Keep direct messages under 300 words unless asked for more."
    )
}

fn coordinator_prompt(
    task: &str,
    run_id: &str,
    coordinator_id: &str,
    teammates: &[serde_json::Value],
) -> String {
    let teammate_lines = if teammates.is_empty() {
        "No teammates were pre-spawned; do the task directly or spawn more if useful.".to_string()
    } else {
        teammates
            .iter()
            .map(|agent| {
                format!(
                    "- {} ({}, {})",
                    agent["id"].as_str().unwrap_or("?"),
                    agent["harness"].as_str().unwrap_or("?"),
                    agent["role"].as_str().unwrap_or("?")
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "You are the top-level orchestrator {coordinator_id} for swarm run {run_id}.\n\nTask:\n{task}\n\nTeammates:\n{teammate_lines}\n\nCoordinate the teammates, monitor their replies with `swarm inbox --new --all --run {run_id}`, and send conclusions, blockers, or questions to the calling user with `swarm send user \"...\"`. When the run is complete, send a concise final answer to the user and call `swarm done \"summary\"`."
    )
}

fn swarm_socket() -> String {
    std::env::var("SWARM_SOCKET").unwrap_or_else(|_| "http://127.0.0.1:9800".to_string())
}

fn swarm_agent_id() -> Option<String> {
    std::env::var("SWARM_AGENT_ID").ok()
}

fn effective_run_arg(run: &str) -> String {
    if run == "current" {
        std::env::var("SWARM_RUN_ID").unwrap_or_else(|_| "current".to_string())
    } else {
        run.to_string()
    }
}

fn wants_json(explicit: bool) -> bool {
    explicit || !std::io::stdout().is_terminal()
}

fn print_json<T: Serialize>(value: &T) -> std::result::Result<(), Box<dyn std::error::Error>> {
    serde_json::to_writer(std::io::stdout(), value)?;
    println!();
    Ok(())
}

async fn cmd_peers(
    include_all: bool,
    run: Option<&str>,
    json: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let mut url = format!("{socket}/api/agents");
    let mut params = Vec::new();
    if let Some(agent_id) = swarm_agent_id() {
        params.push(format!("perspective={agent_id}"));
    }
    if include_all {
        params.push("all=true".to_string());
    }
    if let Some(run) = run {
        params.push(format!("run={run}"));
    }
    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }

    let resp = reqwest::get(&url).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    let agents: Vec<serde_json::Value> = resp.json().await?;

    if wants_json(json) {
        return print_json(&agents);
    }

    if agents.is_empty() {
        println!("no agents");
        return Ok(());
    }

    let has_perspective = swarm_agent_id().is_some();
    let groups = grouped_peers(&agents);
    let mut printed_group = false;
    for (label, group_agents) in groups {
        if group_agents.is_empty() {
            continue;
        }
        if printed_group {
            println!("{CLI_ENTRY_SEPARATOR}");
        }
        printed_group = true;
        println!("{label}");
        for agent in group_agents {
            let status = agent["status"].as_str().unwrap_or("?");
            let id = agent["id"].as_str().unwrap_or("?");
            let harness = agent["harness"].as_str().unwrap_or("?");
            let role = agent["role"].as_str().unwrap_or("?");
            let run_id = agent["run_id"].as_str().unwrap_or("-");

            if has_perspective {
                let relation = agent["relation"].as_str().unwrap_or("?");
                println!(
                    "  {:<26} {:<10} {:<10} {:<16} {:<12} run={}",
                    id, harness, status, role, relation, run_id
                );
            } else {
                let parent = agent["parent_id"].as_str().unwrap_or("-");
                println!(
                    "  {:<26} {:<10} {:<10} {:<16} parent={:<20} run={}",
                    id, harness, status, role, parent, run_id
                );
            }
        }
    }
    Ok(())
}

fn grouped_peers<'a>(
    agents: &'a [serde_json::Value],
) -> Vec<(&'static str, Vec<&'a serde_json::Value>)> {
    let mut active_user = Vec::new();
    let mut active_children = Vec::new();
    let mut stale_active = Vec::new();
    let mut needs_attention = Vec::new();
    let mut recent_done = Vec::new();
    let mut older_done = Vec::new();
    let recent_cutoff = chrono::Utc::now() - chrono::Duration::hours(6);
    let stale_active_cutoff = chrono::Utc::now() - chrono::Duration::minutes(30);

    for agent in agents {
        let status = agent["status"].as_str().unwrap_or("");
        let user_launched = agent["user_launched"].as_bool().unwrap_or(false);
        let launched_at = agent["created_at"]
            .as_str()
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .map(|ts| ts.with_timezone(&chrono::Utc));
        if status == "error" {
            needs_attention.push(agent);
        } else if status != "done"
            && launched_at
                .map(|ts| ts < stale_active_cutoff)
                .unwrap_or(false)
        {
            stale_active.push(agent);
        } else if status != "done" && user_launched {
            active_user.push(agent);
        } else if status != "done" {
            active_children.push(agent);
        } else {
            let timestamp = agent["ended_at"]
                .as_str()
                .or_else(|| agent["created_at"].as_str());
            let recent = timestamp
                .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                .map(|ts| ts.with_timezone(&chrono::Utc) >= recent_cutoff)
                .unwrap_or(false);
            if recent {
                recent_done.push(agent);
            } else {
                older_done.push(agent);
            }
        }
    }

    vec![
        ("active user-launched", active_user),
        ("active delegated", active_children),
        ("stale active", stale_active),
        ("needs attention", needs_attention),
        ("recently done", recent_done),
        ("older done", older_done),
    ]
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

fn resolve_inbox_target(
    to: Option<&str>,
) -> std::result::Result<String, Box<dyn std::error::Error>> {
    match to {
        Some("me") => swarm_agent_id()
            .ok_or("SWARM_AGENT_ID is not set; use --to user or --to <agent-id>")
            .map_err(Into::into),
        Some(target) => Ok(target.to_string()),
        None => Ok(swarm_agent_id().unwrap_or_else(|| "user".to_string())),
    }
}

async fn cmd_inbox(
    from: Option<&str>,
    all: bool,
    to: Option<&str>,
    run: Option<&str>,
    only_new: bool,
    since: Option<&str>,
    limit: usize,
    json: bool,
    truncate: usize,
    search: Option<&str>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let from_agent = if all {
        None
    } else {
        Some(from.ok_or("pass a source agent id, or use --all to read all inbox messages")?)
    };
    let target = resolve_inbox_target(to)?;
    let resp =
        fetch_inbox_entries(&target, from_agent, run, only_new, since, limit, search).await?;

    if wants_json(json) {
        return print_json(&resp);
    }

    if resp.is_empty() {
        let source = from_agent.unwrap_or("any agent");
        println!("no inbox messages for {target} from {source}");
        return Ok(());
    }

    print_inbox_entries(&resp, truncate);

    Ok(())
}

async fn fetch_inbox_entries(
    target: &str,
    from_agent: Option<&str>,
    run: Option<&str>,
    only_new: bool,
    since: Option<&str>,
    limit: usize,
    search: Option<&str>,
) -> std::result::Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let mut url = reqwest::Url::parse(&format!("{socket}/api/agents/{target}/inbox"))?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("n", &limit.to_string());
        if let Some(from_agent) = from_agent {
            query.append_pair("from", from_agent);
        }
        if let Some(run) = run {
            query.append_pair("run", run);
        }
        if only_new {
            query.append_pair("new", "true");
        }
        if let Some(since) = since {
            query.append_pair("since", since);
        }
        if let Some(search) = search {
            query.append_pair("q", search);
        }
    }

    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    Ok(resp.json().await?)
}

fn print_inbox_entries(resp: &[serde_json::Value], truncate: usize) {
    for (idx, entry) in resp.iter().enumerate() {
        let ts = entry["timestamp"].as_str().unwrap_or("?");
        let short_ts = if ts.len() > 19 { &ts[..19] } else { ts };
        let peer = entry["peer"].as_str().unwrap_or("");
        let content = entry["content"].as_str().unwrap_or("");
        let display_content = truncate_for_display(content, truncate);

        if idx > 0 {
            println!("{CLI_ENTRY_SEPARATOR}");
        }
        println!("{short_ts} from:{peer}");
        println!("{display_content}");
    }
}

async fn cmd_watch(
    from: Option<&str>,
    all: bool,
    to: Option<&str>,
    run: Option<&str>,
    limit: usize,
    interval_ms: u64,
    json: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let from_agent = if all {
        None
    } else {
        Some(from.ok_or("pass a source agent id, or use --all to watch all inbox messages")?)
    };
    let target = resolve_inbox_target(to)?;
    let interval_ms = interval_ms.max(250);

    loop {
        let entries =
            fetch_inbox_entries(&target, from_agent, run, true, None, limit, None).await?;
        if !entries.is_empty() {
            if wants_json(json) {
                print_json(&entries)?;
            } else {
                print_inbox_entries(&entries, 0);
                println!("{CLI_ENTRY_SEPARATOR}");
            }
        }
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }
}

async fn cmd_spawn(
    role: &str,
    harness: &str,
    prompt: &str,
    comms: &str,
    model: Option<&str>,
    worktree: bool,
    run: Option<&str>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let parent_id = swarm_agent_id();
    let run_id = run
        .map(effective_run_arg)
        .or_else(|| std::env::var("SWARM_RUN_ID").ok());
    let user_launched = parent_id.is_none();
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
            "run_id": run_id,
            "user_launched": user_launched,
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

async fn cmd_status(json: bool) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let agent_id = swarm_agent_id().ok_or("SWARM_AGENT_ID not set")?;
    let resp = reqwest::get(format!("{socket}/api/agents/{agent_id}")).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    let resp: serde_json::Value = resp.json().await?;

    if wants_json(json) {
        return print_json(&resp);
    }

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

#[derive(Serialize)]
struct ModelsCatalogEntry {
    harness: String,
    default_model: String,
    models: Vec<String>,
}

fn models_catalog() -> Vec<ModelsCatalogEntry> {
    CliKind::all_kinds()
        .iter()
        .map(|kind| ModelsCatalogEntry {
            harness: kind.default_binary().to_string(),
            default_model: "CLI default".to_string(),
            models: Vec::new(),
        })
        .collect()
}

fn cmd_models_offline(json: bool) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let catalog = models_catalog();
    if wants_json(json) {
        return print_json(&catalog);
    }

    for entry in &catalog {
        let name = &entry.harness;
        let default = &entry.default_model;
        println!("{}:", name);
        println!("  default: {default}");
        println!("  pass --model <model> to use an explicit CLI-supported override");
    }
    Ok(())
}

fn truncate_for_display(content: &str, limit: usize) -> String {
    if limit == 0 {
        return content.to_string();
    }

    let mut indices = content.char_indices();
    if let Some((byte_idx, _)) = indices.nth(limit) {
        format!(
            "{}... ({} chars total)",
            &content[..byte_idx],
            content.chars().count()
        )
    } else {
        content.to_string()
    }
}

async fn cmd_log(
    target: &str,
    limit: usize,
    filter: &str,
    json: bool,
    truncate: usize,
    search: Option<&str>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let mut url = reqwest::Url::parse(&format!("{socket}/api/agents/{target}/log"))?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("n", &limit.to_string());
        if filter != "all" {
            query.append_pair("type", filter);
        }
        if let Some(search) = search {
            query.append_pair("q", search);
        }
    }
    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    let resp: Vec<serde_json::Value> = resp.json().await?;

    if wants_json(json) {
        return print_json(&resp);
    }

    if resp.is_empty() {
        println!("no log entries for {target}");
        return Ok(());
    }

    for (idx, entry) in resp.iter().enumerate() {
        let ts = entry["timestamp"].as_str().unwrap_or("?");
        let short_ts = if ts.len() > 19 { &ts[..19] } else { ts };
        let kind = entry["kind"].as_str().unwrap_or("?");
        let peer = entry["peer"].as_str().unwrap_or("");
        let content = entry["content"].as_str().unwrap_or("");
        let display_content = truncate_for_display(content, truncate);

        if idx > 0 {
            println!("{CLI_ENTRY_SEPARATOR}");
        }
        match kind {
            "recv" => {
                println!("{short_ts} recv from:{peer}");
                println!("{display_content}");
            }
            "sent" => {
                println!("{short_ts} sent to:{peer}");
                println!("{display_content}");
            }
            _ => {
                println!("{short_ts} {kind}");
                println!("{display_content}");
            }
        }
    }

    Ok(())
}

async fn cmd_brief(
    target: Option<&str>,
    limit: usize,
    search: Option<&str>,
    json: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let endpoint = if let Some(target) = target {
        format!("{socket}/api/agents/{target}/brief")
    } else {
        format!("{socket}/api/brief")
    };
    let mut url = reqwest::Url::parse(&endpoint)?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("limit", &limit.to_string());
        if let Some(search) = search {
            query.append_pair("q", search);
        }
    }

    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    let brief: serde_json::Value = resp.json().await?;

    if wants_json(json) {
        return print_json(&brief);
    }

    if target.is_some() {
        print_agent_brief(&brief);
    } else {
        print_run_brief(&brief);
    }
    Ok(())
}

fn print_agent_brief(brief: &serde_json::Value) {
    println!(
        "{} ({}) [{}]",
        brief["id"].as_str().unwrap_or("?"),
        brief["role"].as_str().unwrap_or("?"),
        brief["status"].as_str().unwrap_or("?")
    );
    println!(
        "harness: {}  model: {}  parent: {}",
        brief["harness"].as_str().unwrap_or("?"),
        brief["model"].as_str().unwrap_or("").trim(),
        brief["parent_id"].as_str().unwrap_or("-")
    );
    println!(
        "created: {}  ended: {}  prompt: {} chars",
        brief["created_at"].as_str().unwrap_or("?"),
        brief["ended_at"].as_str().unwrap_or("-"),
        brief["prompt_chars"].as_u64().unwrap_or(0)
    );
    if let Some(branch) = brief["worktree_branch"].as_str() {
        println!("worktree: {branch}");
    }
    print_handover(brief.get("latest_handover"));

    if let Some(entries) = brief["recent_log"].as_array() {
        if !entries.is_empty() {
            println!("recent:");
            for entry in entries {
                println!(
                    "  {} {:<11} {:<20} {:>5} chars  {}",
                    entry["timestamp"].as_str().unwrap_or("?"),
                    entry["kind"].as_str().unwrap_or("?"),
                    entry["peer"].as_str().unwrap_or(""),
                    entry["content_chars"].as_u64().unwrap_or(0),
                    entry["preview"].as_str().unwrap_or("")
                );
            }
        }
    }
}

fn print_run_brief(brief: &serde_json::Value) {
    let stats = &brief["stats"];
    println!(
        "agents: total={} alive={} done={} messages={} errors={}",
        stats["total"].as_u64().unwrap_or(0),
        stats["alive"].as_u64().unwrap_or(0),
        stats["done"].as_u64().unwrap_or(0),
        stats["messages"].as_u64().unwrap_or(0),
        stats["errors"].as_u64().unwrap_or(0)
    );

    if let Some(agents) = brief["agents"].as_array() {
        if !agents.is_empty() {
            println!("recent agents:");
            for agent in agents {
                let summary = agent
                    .get("latest_handover")
                    .and_then(|h| h.get("summary"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                println!(
                    "  {:<26} {:<18} {:<8} prompt={:>5} {}",
                    agent["id"].as_str().unwrap_or("?"),
                    agent["role"].as_str().unwrap_or("?"),
                    agent["status"].as_str().unwrap_or("?"),
                    agent["prompt_chars"].as_u64().unwrap_or(0),
                    summary
                );
            }
        }
    }

    if let Some(handovers) = brief["recent_handovers"].as_array() {
        if !handovers.is_empty() {
            println!("recent handovers:");
            for handover in handovers {
                println!(
                    "  {:<26} {}",
                    handover["agent_id"].as_str().unwrap_or("?"),
                    handover["summary"].as_str().unwrap_or("")
                );
            }
        }
    }
}

fn print_handover(handover: Option<&serde_json::Value>) {
    let Some(handover) = handover.filter(|h| !h.is_null()) else {
        return;
    };
    println!("handover:");
    for (label, key) in [
        ("summary", "summary"),
        ("outcome", "outcome"),
        ("deliverable", "deliverable"),
        ("checks", "checks"),
        ("risk", "risk"),
        ("next", "next_action"),
    ] {
        if let Some(value) = handover[key].as_str().filter(|value| !value.is_empty()) {
            println!("  {label}: {value}");
        }
    }
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

async fn cmd_done(
    message: Option<&str>,
    outcome: Option<&str>,
    deliverable: Option<&str>,
    checks: Option<&str>,
    risk: Option<&str>,
    next_action: Option<&str>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let socket = swarm_socket();
    let agent_id = swarm_agent_id().ok_or("SWARM_AGENT_ID not set")?;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/agents/{agent_id}/done"))
        .json(&serde_json::json!({
            "message": message,
            "outcome": outcome,
            "deliverable": deliverable,
            "checks": checks,
            "risk": risk,
            "next_action": next_action,
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
        println!("stopped {target}");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_for_display_uses_requested_limit() {
        assert_eq!(
            truncate_for_display("abcdefghijklmnopqrstuvwxyz", 5),
            "abcde... (26 chars total)"
        );
    }

    #[test]
    fn truncate_for_display_zero_disables_truncation() {
        assert_eq!(
            truncate_for_display("abcdefghijklmnopqrstuvwxyz", 0),
            "abcdefghijklmnopqrstuvwxyz"
        );
    }

    #[test]
    fn truncate_for_display_handles_char_boundaries() {
        assert_eq!(truncate_for_display("éclair", 1), "é... (6 chars total)");
    }
}

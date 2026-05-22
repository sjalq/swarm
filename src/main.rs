use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::collections::HashSet;
use std::io::IsTerminal;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use swarm::config::SwarmConfig;
use swarm::db::Db;
use swarm::guidance::{HELP_DISCOVERY_HINT, PARENT_REPLY_HINT};
use swarm::harness::{CliKind, HarnessRegistry};
use swarm::orchestrator::{Orchestrator, SwarmEvent};
use swarm::types::{SWARM_PROTOCOL_VERSION, USER_TOPIC_ID};

const CLI_ENTRY_SEPARATOR: &str = "------------";

#[derive(Parser)]
#[command(
    name = "swarm",
    about = "Coordinate durable LLM topic streams across harness CLIs",
    long_about = "Swarm coordinates durable topic streams backed by Claude, Codex, Gemini, Grok, or echo workers.\n\
        A topic stream has an ID, label, parent, mailbox, status, log, and optional child topics.\n\n\
        Mental model:\n  \
        - `swarm run \"task\"` starts one topic, sends the task as its first direct message, then watches direct replies by default.\n  \
        - Outside swarm, a new topic's parent is `user`; inside swarm, its parent is the current topic.\n  \
        - Topics reply with `swarm send parent \"message\"`; harness stdout is process output, not the message path.\n  \
        - Pressing Ctrl-C in the default `run` watcher stops only your local watch; the topic and daemon keep running.\n  \
        - Topics can be paused with `swarm done` and resumed by sending them another message.\n\n\
        Common LLM loop:\n  \
        1. `swarm run --label worker --harness codex \"task\"`\n  \
        2. Keep the default `run` watch open, or use `--detach` and later `swarm watch --all`\n  \
        3. `swarm brief` before opening full logs\n  \
        4. `swarm log <topic-id> --messages` when exact message history is needed\n\n\
        Useful commands:\n  \
        swarm run \"task\"              Start a topic in this context\n  \
        swarm send parent \"message\"   Reply to whoever started this topic\n  \
        swarm inbox --all             Read direct messages sent to you/current topic\n  \
        swarm watch --all             Stream new direct messages without changing global read state\n  \
        swarm peers --all             List visible topics, including paused/done topics\n  \
        swarm brief [topic-id]        Read compact project/topic status\n  \
        swarm log <topic-id>          Read historical topic activity\n  \
        swarm done \"summary\"          Pause current topic and optionally report completion\n\n\
        Use `swarm serve` only when you want the daemon/API without starting a topic.\n\n\
        Environment variables:\n  \
        SWARM_SOCKET      Daemon HTTP URL, default http://127.0.0.1:9800\n  \
        SWARM_AGENT_ID    Current topic ID, set inside harness processes\n  \
        SWARM_PROJECT_DIR Project root, set inside harness processes\n  \
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
    /// Start a topic in the current context
    #[command(
        about = "Start a topic in the current context",
        long_about = "Start one durable topic and send TASK as its first direct message.\n\n\
            What happens:\n  \
            - Starts the background daemon if needed.\n  \
            - Creates one topic with an ID like <label>-<short-id>.\n  \
            - Sets parent=user outside swarm, or parent=current topic inside swarm.\n  \
            - Sends TASK plus any --prompt text to the topic.\n  \
            - Prints topic ID, parent ID, and dashboard URL.\n  \
            - By default, stays in watch mode and prints direct replies from the new topic to its parent.\n\n\
            Default watch mode:\n  \
            - `swarm run \"task\"` does not return after creating the topic; it keeps polling for direct replies.\n  \
            - Press Ctrl-C to stop only the local watcher. The topic and daemon keep running.\n  \
            - Use --detach to return immediately. Later monitor with `swarm watch --to <parent> <topic-id>`, `swarm inbox`, `swarm brief`, or `swarm log`.\n\n\
            Key options and accepted values:\n  \
            --label <LABEL>       Human-readable prefix for the topic ID. Default: coordinator.\n  \
            --harness <HARNESS>   One of: claude, codex, gemini, grok, echo. Default: config default_harness, else echo.\n  \
            --model <MODEL>       Free-form model string passed through to harnesses that support model selection.\n  \
            --comms <MODE>        One of: mesh, parent-only. Default: config default_comms, else mesh.\n  \
            --worktree            Give the topic an isolated git worktree and branch.\n  \
            --detach              Return after creating the topic; use watch/inbox/brief/log later.\n  \
            --prompt <TEXT>       Extra task text appended after positional TASK, or used as TASK if no positional TASK is given.\n  \
            --project-dir <PATH>  Project root for daemon and topic work. Default: current directory.\n  \
            --data-dir <PATH>     SQLite/log/worktree storage. Default: platform data directory.\n  \
            --port <PORT>         Daemon port. Default: config port, else 9800.\n\n\
            Examples:\n  \
            swarm run \"say hi\"\n  \
            swarm run --label reviewer --harness codex \"Review the current branch\"\n  \
            swarm run --label editor --harness claude --worktree \"Implement the parser cleanup\"\n  \
            swarm run --detach --label monitor \"Watch for messages and summarize blockers\""
    )]
    Run {
        /// Project directory where topics work
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,

        /// Daemon port. Default comes from config, else 9800.
        #[arg(long)]
        port: Option<u16>,

        /// Harness for the topic worker
        #[arg(long, value_parser = ["claude", "gemini", "codex", "grok", "echo"])]
        harness: Option<String>,

        /// Extra task text appended to TASK; if TASK is omitted, this is the task
        #[arg(long, default_value = "")]
        prompt: String,

        /// Readable label for the topic
        #[arg(long, default_value = "coordinator")]
        label: String,

        /// Skip automatic .gitignore update
        #[arg(long)]
        no_gitignore: bool,

        /// Override data directory (default: platform data dir)
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Path to the dashboard frontend dist directory (dev override)
        #[arg(long)]
        dashboard: Option<PathBuf>,

        /// Communication mode
        #[arg(long, value_parser = ["mesh", "parent-only"])]
        comms: Option<String>,

        /// Free-form model override passed to the selected harness CLI
        #[arg(long)]
        model: Option<String>,

        /// Give the topic its own git worktree (isolated branch)
        #[arg(long)]
        worktree: bool,

        /// Return immediately instead of staying in default watch mode
        #[arg(long)]
        detach: bool,

        /// Task to run. Uses parent=user outside swarm, or parent=current topic inside swarm.
        #[arg(value_name = "TASK", trailing_var_arg = true)]
        task: Vec<String>,
    },

    /// Start only the background daemon/API server
    #[command(
        about = "Start only the background daemon/API server",
        long_about = "Start the daemon, HTTP API, dashboard, and worker resume loop without creating a topic.\n\n\
            Use this when you want the UI/API available before starting topics, or when another process will call the API directly.\n\n\
            Most users and LLM callers should prefer `swarm run \"task\"`, because it starts the daemon automatically when needed.\n\n\
            Options:\n  \
            --project-dir <PATH>  Project root served by this daemon. Default: current directory.\n  \
            --port <PORT>         Daemon port. Default: config port, else 9800.\n  \
            --data-dir <PATH>     SQLite/log/worktree storage. Default: platform data directory.\n  \
            --dashboard <PATH>    Override dashboard dist directory for development.\n  \
            --no-gitignore        Do not add .swarm/ to the project .gitignore."
    )]
    Serve {
        /// Project directory served by this daemon
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,

        /// Daemon port. Default comes from config, else 9800.
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

    /// List topic streams
    #[command(
        about = "List topic streams",
        long_about = "List topics visible from the current context.\n\n\
            Outside swarm, this lists project topics. Inside a topic, it lists the visible family tree: parent, siblings, and children. By default paused/done topics are hidden.\n\n\
            Options:\n  \
            --all   Include paused/done topics.\n  \
            --json  Emit machine-readable JSON. Commands also emit JSON automatically when stdout is piped.\n\n\
            Examples:\n  \
            swarm peers\n  \
            swarm peers --all\n  \
            swarm peers --all --json"
    )]
    Peers {
        /// Include paused/done topics
        #[arg(long)]
        all: bool,

        /// Output JSON; also automatic when stdout is piped
        #[arg(long)]
        json: bool,
    },

    /// Send a direct message to a topic, parent, or user
    #[command(
        about = "Send a direct message to a topic, parent, or user",
        long_about = "Send one direct message through the swarm mailbox.\n\n\
            Sender behavior:\n  \
            - Outside swarm, the sender is `user`.\n  \
            - Inside a topic, the sender is SWARM_AGENT_ID.\n\n\
            Targets:\n  \
            <topic-id>  Send to a specific topic.\n  \
            parent      Inside a topic, resolves to whoever started the current topic.\n  \
            user        Send to the root user mailbox.\n\n\
            This command only confirms that the message was queued. Use `watch`, `inbox`, `brief`, or `log` to observe replies.\n\n\
            Examples:\n  \
            swarm send parent \"I found the issue\"\n  \
            swarm send reviewer-1234abcd \"Please review this branch\"\n  \
            swarm send user \"Blocked: missing credentials\""
    )]
    Send {
        /// Target topic ID, "parent" for the current topic's parent, or "user"
        target: String,
        /// Message content
        message: String,
    },

    /// Read direct messages sent to the user/current topic
    #[command(
        about = "Read direct messages sent to the user/current topic",
        long_about = "Read a snapshot of mailbox messages for a recipient.\n\n\
            Default recipient:\n  \
            - Outside swarm: user.\n  \
            - Inside a topic: current topic (SWARM_AGENT_ID).\n\n\
            Source selection:\n  \
            - Pass a FROM topic ID to read messages from one source.\n  \
            - Use --all to read messages from any source.\n\n\
            Cursor behavior:\n  \
            --new reads only messages newer than the saved cursor and then advances that cursor for the recipient.\n  \
            --since reads messages after a timestamp without using the saved cursor.\n\n\
            Output behavior:\n  \
            Text output shows full message bodies by default. Use --truncate <N> for shorter text. Use --json for tools. JSON is also automatic when stdout is piped.\n\n\
            Accepted values:\n  \
            --to <TO>       `user`, `me` inside a topic, or a topic ID.\n  \
            --last <N>      Positive integer count. Default: 20. Alias: --tail.\n  \
            --truncate <N>  Character count. 0 disables truncation.\n\n\
            Examples:\n  \
            swarm inbox --all\n  \
            swarm inbox reviewer-1234abcd\n  \
            swarm inbox --new --all\n  \
            swarm inbox --all --to user\n  \
            swarm inbox --all --search blocker --json"
    )]
    Inbox {
        /// Source topic ID to read messages from; omit only with --all
        #[arg(conflicts_with = "all")]
        from: Option<String>,

        /// Recipient: user, me, or topic ID. Default: current topic inside swarm, else user.
        #[arg(long)]
        to: Option<String>,

        /// Show all recent direct messages sent to the recipient
        #[arg(long)]
        all: bool,

        /// Show only messages newer than the saved inbox cursor for this recipient
        #[arg(long)]
        new: bool,

        /// Show messages after an RFC3339 timestamp instead of using the saved cursor
        #[arg(long, conflicts_with = "new")]
        since: Option<String>,

        /// Number of recent messages to show
        #[arg(
            short = 'n',
            long = "last",
            visible_alias = "tail",
            default_value = "20"
        )]
        last: usize,

        /// Output JSON; also automatic when stdout is piped
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

    /// Watch new direct messages sent to the user/current topic without marking them read globally
    #[command(
        about = "Watch new direct messages sent to the user/current topic",
        long_about = "Poll for new mailbox messages and print them as they arrive.\n\n\
            `watch` is session-local: it remembers what this invocation has already printed, but it does not advance the saved inbox cursor used by `inbox --new`.\n\n\
            Default recipient:\n  \
            - Outside swarm: user.\n  \
            - Inside a topic: current topic (SWARM_AGENT_ID).\n\n\
            Source selection:\n  \
            - Pass a FROM topic ID to watch one source.\n  \
            - Use --all to watch messages from any source.\n\n\
            Stop with Ctrl-C.\n\n\
            Accepted values:\n  \
            --to <TO>           `user`, `me` inside a topic, or a topic ID.\n  \
            --last <N>          Positive integer count fetched per poll. Default: 20. Alias: --tail.\n  \
            --interval-ms <MS>  Poll interval in milliseconds. Default: 2000. Minimum effective value: 250.\n\n\
            Examples:\n  \
            swarm watch --all\n  \
            swarm watch reviewer-1234abcd --to user\n  \
            swarm watch --all --interval-ms 500\n  \
            swarm watch --all --json"
    )]
    Watch {
        /// Recipient: user, me, or topic ID. Default: current topic inside swarm, else user.
        #[arg(long)]
        to: Option<String>,

        /// Source topic ID to watch; omit with --all
        #[arg(conflicts_with = "all")]
        from: Option<String>,

        /// Watch all source topics
        #[arg(long)]
        all: bool,

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

        /// Output JSON; also automatic when stdout is piped
        #[arg(long)]
        json: bool,
    },

    /// Show harness model behavior (harness CLIs choose their own defaults)
    #[command(
        about = "Show harness model behavior",
        long_about = "List available harness kinds and their default model behavior.\n\n\
            Swarm usually lets each harness CLI choose its own default model. Use `swarm run --model <MODEL>` when you need an explicit model and the selected harness supports it.\n\n\
            Accepted values:\n  \
            --model values are not listed by swarm; each harness CLI owns its supported model names.\n\n\
            Examples:\n  \
            swarm models\n  \
            swarm models --json"
    )]
    Models {
        /// Output JSON; also automatic when stdout is piped
        #[arg(long)]
        json: bool,
    },

    /// Show current topic status
    #[command(
        about = "Show current topic status",
        long_about = "Show identity and runtime state for the current topic.\n\n\
            This command requires SWARM_AGENT_ID, so it is mainly useful inside a running harness/topic process. It does not show children; use `swarm peers` for visible parent, sibling, and child topics.\n\n\
            Output includes topic ID, label, harness, model, status, parent, and comms mode.\n\n\
            Examples:\n  \
            swarm status\n  \
            swarm status --json"
    )]
    Status {
        /// Output JSON; also automatic when stdout is piped
        #[arg(long)]
        json: bool,
    },

    /// View recent topic activity, or the broader user message log
    #[command(
        about = "View recent topic activity, or the broader user message log",
        long_about = "Read historical activity for one topic, or the broader user message log with `swarm log user`.\n\n\
            Use `brief` first for compact context. Use `log` when you need recent transcript details.\n\n\
            Filters and values:\n  \
            --messages  Show only sent/received messages.\n  \
            --output    Show only harness stdout/stderr style output.\n  \
            --search    Case-insensitive content search before applying the limit.\n  \
            --raw       Disable default truncation.\n  \
            --last <N>  Positive integer count. Default: 20. Alias: --tail.\n  \
            --truncate <N>  Character count; 0 disables truncation.\n\n\
            Examples:\n  \
            swarm log reviewer-1234abcd\n  \
            swarm log reviewer-1234abcd --messages\n  \
            swarm log user --messages --search blocker\n  \
            swarm log reviewer-1234abcd --raw -n 100\n  \
            swarm log reviewer-1234abcd --json"
    )]
    Log {
        /// Topic ID to inspect, or "user" to inspect the broader user message log
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

        /// Output JSON; also automatic when stdout is piped
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

    /// Show a compact digest for the project or one topic
    #[command(
        about = "Show a compact digest for the project or one topic",
        long_about = "Show deterministic, low-noise status without dumping full transcripts.\n\n\
            Project brief (`swarm brief`) includes topic counts, recent topics, prompt sizes, statuses, and recent structured handovers.\n\n\
            Topic brief (`swarm brief <topic-id>`) includes metadata, latest handover, and compact recent log previews. Previews are not LLM summaries; they are deterministic truncations of stored log entries.\n\n\
            Use `swarm done --outcome ... --deliverable ... --checks ...` to make future briefs more useful.\n\n\
            Options and values:\n  \
            --last <N>      Positive integer count for recent topics/log entries. Default: 20.\n  \
            --search <TEXT> Case-insensitive search over compact fields or topic log content.\n  \
            --json          Machine-readable output; also automatic when stdout is piped.\n\n\
            Examples:\n  \
            swarm brief\n  \
            swarm brief reviewer-1234abcd\n  \
            swarm brief --search blocked\n  \
            swarm brief reviewer-1234abcd --json"
    )]
    Brief {
        /// Topic ID to inspect. Omit for project summary.
        target: Option<String>,

        /// Number of recent topics/log entries to show
        #[arg(short = 'n', long = "last", default_value = "20")]
        last: usize,

        /// Search compact topic fields or topic log content
        #[arg(long, alias = "grep")]
        search: Option<String>,

        /// Output JSON; also automatic when stdout is piped
        #[arg(long)]
        json: bool,
    },

    /// Clean up a topic worktree and optionally its branch
    #[command(
        about = "Clean up a topic worktree and optionally its branch",
        long_about = "Remove the git worktree created for a topic started with --worktree.\n\n\
            This does not delete the topic record, mailbox, or logs. Use it after reviewing or merging worktree changes.\n\n\
            Options:\n  \
            --delete-branch  Also delete the topic branch after removing the worktree.\n\n\
            Examples:\n  \
            swarm cleanup editor-1234abcd\n  \
            swarm cleanup editor-1234abcd --delete-branch"
    )]
    Cleanup {
        /// Topic ID to clean up
        target: String,

        /// Also delete the git branch
        #[arg(long)]
        delete_branch: bool,
    },

    /// Pause this topic and optionally report to parent
    #[command(
        about = "Pause this topic and optionally report to parent",
        long_about = "Mark the current topic paused/done, optionally send a final message to the parent, and store structured handover fields for future briefs.\n\n\
            This command requires SWARM_AGENT_ID, so it is for use inside a running topic. A paused topic can be resumed later by sending it another message.\n\n\
            Good LLM usage:\n  \
            - Put the concise final answer in MESSAGE.\n  \
            - Use --outcome, --deliverable, --checks, --risk, and --next-action to make `swarm brief` useful.\n\n\
            Suggested outcome values are free-form but should usually be: done, partial, blocked, failed.\n\n\
            Examples:\n  \
            swarm done \"Implemented parser cleanup\"\n  \
            swarm done \"Blocked on credentials\" --outcome blocked --risk \"Cannot verify API call\" --next-action \"Provide API token\"\n  \
            swarm done \"Review complete\" --outcome done --checks \"cargo test passed\" --deliverable \"review notes in parent message\""
    )]
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

    /// Stop a topic worker and mark the topic paused/done
    #[command(
        about = "Stop a topic worker and mark the topic paused/done",
        long_about = "Stop a running topic worker from outside or inside swarm.\n\n\
            This marks the topic paused/done. It does not delete logs, messages, or worktrees. If a topic has a worktree, use `swarm cleanup <topic-id>` separately after reviewing any work.\n\n\
            Example:\n  \
            swarm kill reviewer-1234abcd"
    )]
    Kill {
        /// Topic ID to stop
        target: String,
    },

    /// Check harness availability, versions, and API keys
    #[command(
        about = "Check harness availability, versions, and API keys",
        long_about = "Check whether configured harness CLIs are available and whether expected API key environment variables are present.\n\n\
            This is a local diagnostic. It does not start a topic.\n\n\
            Example:\n  \
            swarm doctor"
    )]
    Doctor,

    /// Print shell completion script to stdout
    #[command(
        about = "Print shell completion script to stdout",
        long_about = "Generate shell autocomplete code for swarm commands and flags.\n\n\
            This prints the script to stdout. Redirect it into the completion location for your shell.\n\n\
            Examples:\n  \
            swarm completions zsh > ~/.zfunc/_swarm\n  \
            swarm completions bash > ~/.local/share/bash-completion/completions/swarm\n  \
            swarm completions fish > ~/.config/fish/completions/swarm.fish"
    )]
    Completions {
        /// Target shell
        shell: ShellArg,
    },

    /// Print roff manpage to stdout
    #[command(
        about = "Print roff manpage to stdout",
        long_about = "Generate a roff manpage for swarm and print it to stdout.\n\n\
            Example:\n  \
            swarm manpage > swarm.1"
    )]
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
            label,
            no_gitignore,
            data_dir,
            dashboard,
            comms,
            model,
            worktree,
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
            let comms = comms.unwrap_or_else(|| {
                config
                    .default_comms
                    .clone()
                    .unwrap_or_else(|| "mesh".into())
            });

            let resolved_data_dir = SwarmConfig::resolve_data_dir(data_dir.as_deref(), &config);

            if let Err(msg) = swarm::harness::preflight_check(&harness) {
                eprintln!("{msg}");
                std::process::exit(1);
            }

            let task_text = task.join(" ").trim().to_string();
            let result = run_task_swarm(
                project_dir,
                resolved_data_dir,
                port,
                harness,
                label,
                prompt,
                comms,
                model,
                worktree,
                task_text,
                detach,
                no_gitignore,
                dashboard,
            )
            .await;

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
                no_gitignore,
                dashboard,
            )
            .await
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Peers { all, json } => {
            if let Err(e) = cmd_peers(all, json).await {
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
            last,
            json,
            search,
            raw,
            truncate,
        } => {
            let truncate = if raw { 0 } else { truncate.unwrap_or(0) };
            if let Err(e) = cmd_inbox(
                from.as_deref(),
                all,
                to.as_deref(),
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
        Commands::Watch {
            to,
            from,
            all,
            last,
            interval_ms,
            json,
        } => {
            if let Err(e) =
                cmd_watch(from.as_deref(), all, to.as_deref(), last, interval_ms, json).await
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

async fn run_orchestrator(
    project_dir: PathBuf,
    data_dir: PathBuf,
    port: u16,
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
        tracing::info!("resumed {resumed} existing topic worker(s)");
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
                    SwarmEvent::TopicStarted { agent } => {
                        println!(
                            "[swarm] topic: {} ({}, {})",
                            agent.id, agent.harness, agent.label
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
    label: String,
    prompt: String,
    comms: String,
    model: Option<String>,
    worktree: bool,
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

    let parent_id = runtime_parent_id();
    let user_launched = parent_id == USER_TOPIC_ID;
    let socket = match std::env::var("SWARM_SOCKET") {
        Ok(socket) if !socket.trim().is_empty() => {
            ensure_socket_protocol(&socket).await?;
            socket
        }
        _ => ensure_daemon(project_dir, data_dir, port, no_gitignore, dashboard).await?,
    };
    let topic = create_topic_http(
        &socket,
        CreateTopicHttpRequest {
            label: &label,
            harness: &harness,
            prompt: "",
            parent_id: Some(&parent_id),
            comms: &comms,
            model: model.as_deref(),
            worktree,
            user_launched,
        },
    )
    .await?;
    let topic_id = topic["id"]
        .as_str()
        .ok_or("topic create returned no id")?
        .to_string();

    let topic_prompt = topic_prompt(&task, &topic_id, &parent_id);
    send_message_http(&socket, &parent_id, &topic_id, &topic_prompt).await?;

    println!("topic: {topic_id}");
    println!("parent: {parent_id}");
    println!("dashboard: {socket}");
    println!("{CLI_ENTRY_SEPARATOR}");
    std::env::set_var("SWARM_SOCKET", &socket);

    if detach {
        println!("detached. watch responses with: swarm watch --to {parent_id} {topic_id}");
        return Ok(());
    }

    println!(
        "watching direct responses from {}; press Ctrl-C to stop",
        topic_id
    );
    watch_started_topic_responses(&parent_id, &topic_id, 20, 2_000).await
}

async fn watch_started_topic_responses(
    target: &str,
    from_agent: &str,
    limit: usize,
    interval_ms: u64,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let interval_ms = interval_ms.max(250);
    let mut seen = HashSet::new();

    loop {
        let entries =
            fetch_inbox_entries(target, Some(from_agent), false, None, limit, None).await?;
        let mut new_entries = entries
            .into_iter()
            .filter(|entry| seen.insert(inbox_entry_key(entry)))
            .collect::<Vec<_>>();

        if !new_entries.is_empty() {
            new_entries.reverse();
            print_inbox_entries(&new_entries, 0);
            println!("{CLI_ENTRY_SEPARATOR}");
            std::io::stdout().flush()?;
        }

        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }
}

fn inbox_entry_key(entry: &serde_json::Value) -> String {
    format!(
        "{}\u{0}{}\u{0}{}",
        entry["timestamp"].as_str().unwrap_or(""),
        entry["peer"].as_str().unwrap_or(""),
        entry["content"].as_str().unwrap_or("")
    )
}

async fn ensure_daemon(
    project_dir: PathBuf,
    data_dir: PathBuf,
    port: u16,
    no_gitignore: bool,
    dashboard: Option<PathBuf>,
) -> std::result::Result<String, Box<dyn std::error::Error>> {
    let socket = format!("http://127.0.0.1:{port}");
    match daemon_health(&socket).await {
        Some(health) if daemon_matches(&health, &project_dir, &data_dir) => return Ok(socket),
        Some(health) => {
            return Err(format!(
                "daemon already running at {socket} for project {} with data dir {}. Stop it or pass --port <other>.",
                health["project_dir"].as_str().unwrap_or("?"),
                health["data_dir"].as_str().unwrap_or("?")
            )
            .into());
        }
        None => {}
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
        if daemon_health(&socket).await.is_some() {
            return Ok(socket);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!("daemon did not become healthy at {socket}").into())
}

async fn ensure_socket_protocol(
    socket: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    match daemon_health(socket).await {
        Some(health) if health["protocol"].as_str() == Some(SWARM_PROTOCOL_VERSION) => Ok(()),
        Some(health) => Err(format!(
            "daemon at {socket} uses protocol {} but this CLI expects {SWARM_PROTOCOL_VERSION}. Restart the daemon.",
            health["protocol"].as_str().unwrap_or("unknown")
        )
        .into()),
        None => Err(format!("no swarm daemon responded at {socket}").into()),
    }
}

async fn daemon_health(socket: &str) -> Option<serde_json::Value> {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(400))
        .build()
    {
        Ok(client) => client,
        Err(_) => return None,
    };
    let resp = client
        .get(format!("{socket}/api/health"))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json().await.ok()
}

fn daemon_matches(
    health: &serde_json::Value,
    project_dir: &std::path::Path,
    data_dir: &std::path::Path,
) -> bool {
    if health["protocol"].as_str() != Some(SWARM_PROTOCOL_VERSION) {
        return false;
    }
    let Some(daemon_project) = health["project_dir"].as_str() else {
        return false;
    };
    let Some(daemon_data) = health["data_dir"].as_str() else {
        return false;
    };

    paths_equivalent(daemon_project, project_dir) && paths_equivalent(daemon_data, data_dir)
}

fn paths_equivalent(left: impl AsRef<std::path::Path>, right: impl AsRef<std::path::Path>) -> bool {
    let left = left.as_ref();
    let right = right.as_ref();
    let left = std::fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = std::fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    left == right
}

struct CreateTopicHttpRequest<'a> {
    label: &'a str,
    harness: &'a str,
    prompt: &'a str,
    parent_id: Option<&'a str>,
    comms: &'a str,
    model: Option<&'a str>,
    worktree: bool,
    user_launched: bool,
}

async fn create_topic_http(
    socket: &str,
    req: CreateTopicHttpRequest<'_>,
) -> std::result::Result<serde_json::Value, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{socket}/api/agents"))
        .json(&serde_json::json!({
            "label": req.label,
            "harness": req.harness,
            "system_prompt": req.prompt,
            "parent_id": req.parent_id,
            "comms": req.comms,
            "model": req.model,
            "worktree": req.worktree,
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

fn topic_prompt(task: &str, topic_id: &str, parent_id: &str) -> String {
    format!(
        "Topic: {topic_id}\nParent: {parent_id}\n\nTask:\n{task}\n\nWork independently. {PARENT_REPLY_HINT} When complete, call `swarm done \"summary\"`. Start child topics only when useful; {HELP_DISCOVERY_HINT}"
    )
}

fn swarm_socket() -> String {
    std::env::var("SWARM_SOCKET").unwrap_or_else(|_| "http://127.0.0.1:9800".to_string())
}

fn swarm_agent_id() -> Option<String> {
    std::env::var("SWARM_AGENT_ID").ok()
}

fn parent_id_from_context(agent_id: Option<String>) -> String {
    agent_id.unwrap_or_else(|| USER_TOPIC_ID.to_string())
}

fn runtime_parent_id() -> String {
    parent_id_from_context(swarm_agent_id())
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
        println!("no topics");
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
            let label = agent["label"].as_str().unwrap_or("?");
            if has_perspective {
                let relation = agent["relation"].as_str().unwrap_or("?");
                println!(
                    "  {:<26} {:<10} {:<10} {:<16} {:<12}",
                    id, harness, status, label, relation
                );
            } else {
                let parent = agent["parent_id"].as_str().unwrap_or("-");
                println!(
                    "  {:<26} {:<10} {:<10} {:<16} parent={:<20}",
                    id, harness, status, label, parent
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
    let target = resolve_send_target(&socket, target, &from).await?;
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

async fn resolve_send_target(
    socket: &str,
    target: &str,
    from: &str,
) -> std::result::Result<String, Box<dyn std::error::Error>> {
    if target != "parent" {
        return Ok(target.to_string());
    }
    if from == "user" {
        return Err("`parent` is only available inside a swarm topic".into());
    }

    let resp = reqwest::get(format!("{socket}/api/agents/{from}")).await?;
    if !resp.status().is_success() {
        return Err(response_error(resp).await);
    }
    let agent: serde_json::Value = resp.json().await?;
    Ok(agent["parent_id"].as_str().unwrap_or("user").to_string())
}

fn resolve_inbox_target(
    to: Option<&str>,
) -> std::result::Result<String, Box<dyn std::error::Error>> {
    match to {
        Some("me") => swarm_agent_id()
            .ok_or("SWARM_AGENT_ID is not set; use --to user or --to <topic-id>")
            .map_err(Into::into),
        Some(target) => Ok(target.to_string()),
        None => Ok(swarm_agent_id().unwrap_or_else(|| "user".to_string())),
    }
}

async fn cmd_inbox(
    from: Option<&str>,
    all: bool,
    to: Option<&str>,
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
        Some(from.ok_or("pass a source topic id, or use --all to read all inbox messages")?)
    };
    let target = resolve_inbox_target(to)?;
    let resp = fetch_inbox_entries(&target, from_agent, only_new, since, limit, search).await?;

    if wants_json(json) {
        return print_json(&resp);
    }

    if resp.is_empty() {
        let source = from_agent.unwrap_or("any topic");
        println!("no inbox messages for {target} from {source}");
        return Ok(());
    }

    print_inbox_entries(&resp, truncate);

    Ok(())
}

async fn fetch_inbox_entries(
    target: &str,
    from_agent: Option<&str>,
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

async fn fetch_log_entries(
    target: &str,
    limit: usize,
    filter: &str,
    search: Option<&str>,
) -> std::result::Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
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
    limit: usize,
    interval_ms: u64,
    json: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let from_agent = if all {
        None
    } else {
        Some(from.ok_or("pass a source topic id, or use --all to watch all inbox messages")?)
    };
    let target = resolve_inbox_target(to)?;
    let interval_ms = interval_ms.max(250);
    let mut since = chrono::Utc::now().to_rfc3339();
    let mut seen = HashSet::new();

    loop {
        let entries =
            fetch_inbox_entries(&target, from_agent, false, Some(&since), limit, None).await?;
        let mut new_entries = entries
            .into_iter()
            .filter(|entry| seen.insert(inbox_entry_key(entry)))
            .collect::<Vec<_>>();

        if !new_entries.is_empty() {
            new_entries.reverse();
            if let Some(last) = new_entries.last() {
                if let Some(timestamp) = last["timestamp"].as_str() {
                    since = timestamp.to_string();
                }
            }
            if wants_json(json) {
                print_json(&new_entries)?;
            } else {
                print_inbox_entries(&new_entries, 0);
                println!("{CLI_ENTRY_SEPARATOR}");
            }
            std::io::stdout().flush()?;
        }
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }
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
    println!("label:    {}", resp["label"].as_str().unwrap_or("?"));
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
    let resp = fetch_log_entries(target, limit, filter, search).await?;

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
        print_swarm_brief(&brief);
    }
    Ok(())
}

fn print_agent_brief(brief: &serde_json::Value) {
    println!(
        "{} ({}) [{}]",
        brief["id"].as_str().unwrap_or("?"),
        brief["label"].as_str().unwrap_or("?"),
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

fn print_swarm_brief(brief: &serde_json::Value) {
    let stats = &brief["stats"];
    println!(
        "topics: total={} alive={} done={} messages={} errors={}",
        stats["total"].as_u64().unwrap_or(0),
        stats["alive"].as_u64().unwrap_or(0),
        stats["done"].as_u64().unwrap_or(0),
        stats["messages"].as_u64().unwrap_or(0),
        stats["errors"].as_u64().unwrap_or(0)
    );

    if let Some(agents) = brief["agents"].as_array() {
        if !agents.is_empty() {
            println!("recent topics:");
            for agent in agents {
                let summary = agent
                    .get("latest_handover")
                    .and_then(|h| h.get("summary"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                println!(
                    "  {:<26} {:<18} {:<8} prompt={:>5} {}",
                    agent["id"].as_str().unwrap_or("?"),
                    agent["label"].as_str().unwrap_or("?"),
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

    fn removed_command_name() -> String {
        ["sp", "awn"].concat()
    }

    fn removed_team_flag() -> String {
        ["--te", "am"].concat()
    }

    fn removed_label_predecessor_flag() -> String {
        ["--ro", "le"].concat()
    }

    #[test]
    fn top_level_help_points_to_run_only() {
        let mut cmd = Cli::command();
        let mut help = Vec::new();
        cmd.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();
        let removed = removed_command_name();
        let removed_team = removed_team_flag();

        assert!(help.contains("swarm run \"task\""));
        assert!(!help.contains(&removed));
        assert!(!help.contains(&removed_team));
    }

    #[test]
    fn run_help_describes_contextual_topic_start() {
        let mut cmd = Cli::command();
        let run = cmd.find_subcommand_mut("run").unwrap();
        let mut help = Vec::new();
        run.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();
        let removed = removed_command_name();
        let removed_team = removed_team_flag();
        let removed_label_predecessor = removed_label_predecessor_flag();

        assert!(help.contains("Start one durable topic"));
        assert!(help.contains("parent=user outside swarm"));
        assert!(help.contains("--label"));
        assert!(help.contains("--worktree"));
        assert!(!help.contains(&removed));
        assert!(!help.contains(&removed_team));
        assert!(!help.contains(&removed_label_predecessor));
    }

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

    #[test]
    fn parent_id_defaults_to_user_outside_swarm() {
        assert_eq!(parent_id_from_context(None), "user");
    }

    #[test]
    fn parent_id_uses_current_topic_inside_swarm() {
        assert_eq!(
            parent_id_from_context(Some("coordinator-12345678".to_string())),
            "coordinator-12345678"
        );
    }
}

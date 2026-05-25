# swarm

A topic-stream CLI for coordinating LLM coding assistants across harnesses.

<!-- Badges -->
[![CI](https://github.com/sjalq/swarm/actions/workflows/ci.yml/badge.svg)](https://github.com/sjalq/swarm/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/swarm-cli.svg)](https://crates.io/crates/swarm-cli)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

## Overview

Swarm lets you run multiple durable topic streams on the same project. A topic has an ID, label, mailbox, log, parent, and optional child topics. A worker process backed by a real LLM CLI (Claude, Codex, Gemini, or Grok) advances that topic when messages arrive, and the topic can be resumed later.

Use cases:

- Split a large feature across multiple topics working in parallel worktrees.
- Have one topic implement and another review, with the coordinator merging results.
- Run a heterogeneous swarm (Claude for planning, Codex for implementation, Gemini for testing).
- Automate multi-step workflows that would be tedious to drive by hand.

## Quickstart

```bash
# Install on Linux or macOS
curl -fsSL https://raw.githubusercontent.com/sjalq/swarm/main/install.sh | bash
```

The installer uses a published GitHub release when available; if none exists yet, use the source install method below.

1. Run a no-API smoke test:

```bash
swarm run --harness echo 'hello'
```

2. Check your real harness setup:

```bash
swarm doctor
```

3. Start a topic with a real harness after installing that harness and signing in:

```bash
swarm run --harness claude "Refactor the auth module into smaller files."
```

In another terminal, stream direct replies from the printed topic:

```bash
swarm watch-inbox user --from <topic-id>
```

View a compact digest before opening raw logs:

```bash
swarm brief <topic-id>
```

## Install

### From prebuilt binary

```bash
curl -fsSL https://raw.githubusercontent.com/sjalq/swarm/main/install.sh | bash
```

Requires a published GitHub release for the prebuilt path. If none exists yet, use the source install method below; the installer also falls back to that method when `cargo` is available.

Set `SWARM_VERSION=v0.1.0` to pin a specific version. Set `BIN_DIR=~/.local/bin` to change the install location.

### From source

```bash
cargo install --git https://github.com/sjalq/swarm swarm-cli
```

### From Homebrew (coming soon)

The Homebrew tap is not published yet. This will become the preferred macOS install path after release checksums are wired into the formula.

```bash
# Coming soon:
brew install sjalq/swarm/swarm
```

### Shell completions

```bash
mkdir -p ~/.zfunc
swarm completions zsh >> ~/.zfunc/_swarm
```

See `swarm completions --help` for other shells.

## Harnesses

Swarm delegates actual AI work to external CLI tools called harnesses. Each harness wraps a specific LLM provider's CLI.

```
Harness  | Install                              | API Key Env Var              | Docs
---------|--------------------------------------|------------------------------|-----------------------------------------------
claude   | npm install -g @anthropic-ai/claude-code | ANTHROPIC_API_KEY         | https://docs.anthropic.com/en/docs/claude-code
codex    | npm install -g @openai/codex         | OPENAI_API_KEY               | https://github.com/openai/codex
gemini   | npm install -g @google/gemini-cli    | GEMINI_API_KEY               | https://github.com/google-gemini/gemini-cli
grok     | npm install -g @xai-official/grok    | XAI_API_KEY                  | https://docs.x.ai/
```

Swarm also includes an `echo` harness for testing that mirrors prompts back without calling any API.

## Configuration

### Global config

`~/.config/swarm/config.toml` applies to all projects.

### Project config

`.swarm/config.toml` in any project directory overrides global settings for that project.

### Sample config

```toml
# ~/.config/swarm/config.toml

# Default local server port
# default_port = 9800

# Default harness for new topics
# default_harness = "claude"

# Default communication mode: "mesh" or "parent-only"
# default_comms = "mesh"

# Runtime storage directory. Defaults to the latest active data dir, then
# the platform data directory (for example ~/Library/Application Support/swarm on macOS).
# data_dir = "/path/to/swarm-data"

# Harness binary overrides
# claude_bin = "/usr/local/bin/claude"
# codex_bin = "/opt/codex/bin/codex"
# gemini_bin = "gemini"
# grok_bin = "grok"
```

### Environment variables

```
Variable            | Description
--------------------|------------------------------------------------------------
SWARM_SOCKET        | HTTP URL for topic-to-daemon communication; local HTTP sockets auto-start when quiet
SWARM_AGENT_ID      | Current topic identifier (set automatically)
SWARM_PROJECT_DIR   | Project root directory (set automatically)
SWARM_CLAUDE_BIN    | Override the Claude CLI binary path
SWARM_CODEX_BIN     | Override the Codex CLI binary path
SWARM_GEMINI_BIN    | Override the Gemini CLI binary path
SWARM_GROK_BIN      | Override the Grok CLI binary path
RUST_LOG            | Control log verbosity (e.g. RUST_LOG=swarm=debug)
```

## Daemon and data layout

Daemon-backed commands use `SWARM_SOCKET` when it is set. Otherwise they use the configured local port, defaulting to `http://127.0.0.1:9800`. If that local socket is not running, commands such as `run`, `send`, `inbox`, `watch-inbox`, `peers`, `brief`, `log`, `cleanup`, and `kill` start the daemon automatically.

If a daemon is already running on the selected socket, swarm uses it. Running a command from another folder does not make swarm reject the daemon just because its project root differs; the socket is the source of truth unless you explicitly start a separate daemon on another port/socket.

Runtime state lives in the configured data directory:

```
<data-dir>/
  swarm.db          SQLite database (topic state, messages, logs)
  agents/           Per-topic working directories
    <topic-id>/     Topic home
  worktrees/        Git worktrees for isolated topic branches
    <topic-id>/     Separate checkout on branch swarm/<topic-id>
```

The data directory comes from `--data-dir`, then project/global `data_dir`, then the latest active data-dir breadcrumb, then the platform data directory. Project-local `.swarm/config.toml` is still used for project config.

## Command reference

### `swarm run`

Start a topic in the current context. Outside swarm the parent is `user`; inside swarm the parent is the current topic.

```bash
swarm run "Investigate the failing checkout flow"
swarm run --label reviewer --harness codex "Review the current branch"
swarm run --label editor --harness codex --worktree "Implement the parser cleanup"
```

Options:
- `--project-dir <PATH>` : Project directory (default: `.`)
- `--port <PORT>` : Local server port when `SWARM_SOCKET` is not set (default: `9800`)
- `--harness <NAME>` : Harness for the topic worker (default: `echo`)
- `--prompt <TEXT>` : Extra prompt text, or the task text when no positional task is provided
- `--label <NAME>` : Readable label for the topic (default: `coordinator`)
- `--comms <MODE>` : Communication mode: `mesh` or `parent-only` (default: `mesh`)
- `--model <MODEL>` : Model override supported by the selected harness CLI.
- `--worktree` : Give the topic its own git worktree (isolated branch)
- `--detach` : Deprecated compatibility flag; `swarm run` now returns immediately.

`swarm run` uses `SWARM_SOCKET` when set, otherwise the configured/default local socket. If that local socket is not running, it starts the daemon, starts one topic, sends the task to it, then prints the topic ID and a `swarm watch-inbox ...` command for replies.

### `swarm serve`

Start only the daemon/API server without starting a topic.

```bash
swarm serve [OPTIONS]
```

Most workflows do not need `swarm serve`; daemon-backed commands auto-start a local server when their socket is quiet. Use `serve` when you want to pin the daemon process yourself, choose a specific project/data directory up front, or run the dashboard/API before issuing topic commands.

### `swarm peers`

List topic streams visible to you (parent, siblings, descendants).

```bash
swarm peers [--all]
```

- `--all` : Include paused/done topics.

### `swarm send`

Send a message to another topic stream, the current parent, or the user.

```bash
swarm send <TOPIC_ID> "<MESSAGE>"
swarm send parent "<MESSAGE>"
swarm send user "<MESSAGE>"
```

Inside a topic, `parent` means the user or topic stream that started the current topic.

### `swarm inbox`

Read direct messages sent to the user/current topic from one source topic. Outside swarm, the default recipient is `user`; inside a topic, the default recipient is `SWARM_AGENT_ID`.

Running topics do not need to poll their inbox to wait for children or peers. New direct messages automatically wake the topic and resume the harness with those messages included. Use `inbox` inside a topic for occasional snapshots or debugging, not as a long-running wait loop.

```bash
swarm inbox <FROM_TOPIC_ID> [-n <COUNT>]
swarm inbox <FROM_TOPIC_ID> --to user
swarm inbox --all [-n <COUNT>]
swarm inbox --new --all
```

Inbox output shows full direct message bodies by default. Use `--truncate <COUNT>` if you want a shorter terminal view.

- `--new` : Read only messages newer than the saved SQLite cursor for this recipient.
- `--since <TIMESTAMP>` : Read messages after an RFC3339 timestamp.

### `swarm watch-inbox`

Poll and print new direct messages sent to an inbox. Without a topic ID, it watches the current topic inbox inside swarm, or the user inbox outside swarm. By default it shows messages from all senders.

```bash
swarm watch-inbox
swarm watch-inbox user --from <FROM_TOPIC_ID>
swarm watch-inbox <TOPIC_ID>
```

### `swarm status`

Show the current topic's status, including model and harness info.

```bash
swarm status
```

### `swarm models`

Show harnesses and note that their CLIs choose default models.

```bash
swarm models
```

### `swarm log`

View recent activity for a topic, or inspect the broader user message log.

```bash
swarm log <TOPIC_ID> [-n <COUNT>] [--messages] [--output] [--search <TEXT>] [--raw]
swarm log user --messages
```

Options:
- `-n <COUNT>` / `--tail <COUNT>` : Number of entries to show (default: `20`)
- `--messages` : Show only messages (sent and received). Use `swarm inbox <FROM_TOPIC_ID>` when you only want messages sent to you from one topic.
- `--output` : Show only harness output
- `--search <TEXT>` : Search log content case-insensitively before applying the limit
- `--raw` : Disable text truncation and show exact full log entries
- `--truncate <COUNT>` : Limit text output length. Message-only logs default to full content; mixed logs and output logs default to 500 characters.

### `swarm brief`

Show a compact digest that is safe to use as working context before reaching for raw logs.

```bash
swarm brief                 # project/topic overview
swarm brief <TOPIC_ID>      # one topic summary and compact recent log
swarm brief <TOPIC_ID> --search "timeout"
```

Brief output includes status, prompt size, latest structured handover, and short log previews. Use `swarm log --raw` when you need the exact transcript.

### `swarm cleanup`

Remove a finished topic's worktree.

```bash
swarm cleanup <TOPIC_ID> [--delete-branch]
```

- `--delete-branch` : Also delete the git branch.

### `swarm done`

Signal that you have finished your task. Sends an optional final message to your parent and terminates gracefully.

```bash
swarm done ["optional message"]
swarm done "Implemented auth" \
  --outcome done \
  --deliverable "branch swarm/auth-worker" \
  --checks "cargo test" \
  --risk "browser flow not checked" \
  --next-action "review and merge"
```

The optional structured fields are stored separately from the raw transcript and appear in `swarm brief`, keeping handoffs concise for coordinators and follow-on topics.

### `swarm kill`

Stop a topic and mark it paused/done.

```bash
swarm kill <TOPIC_ID>
```

### `swarm doctor`

Check that required harness CLIs are installed and reachable, API keys are set, and the swarm environment is healthy.

```bash
swarm doctor
```

Reports a checklist of passed/failed checks for each configured harness.

### `swarm completions`

Generate shell completions for your shell.

```bash
swarm completions <SHELL>
```

Supported shells: `bash`, `zsh`, `fish`, `elvish`, `powershell`. Pipe the output into your shell's completions directory, e.g.:

```bash
swarm completions zsh > ~/.zfunc/_swarm
```

### `swarm manpage`

Generate a man page.

```bash
swarm manpage > swarm.1
```

## Worktrees

When you pass `--worktree` to `swarm run`, the topic gets its own git branch and file checkout under `<data-dir>/worktrees/<topic-id>/`. This prevents file conflicts when multiple topics edit the same project concurrently.

**When to use worktrees:**

- Multiple topics editing files in the same compiled project (Rust, TypeScript, etc.) where concurrent edits would break the build.
- Parallel feature branches that will be merged by the coordinator.

**When not to use worktrees:**

- Read-only tasks: reviewing, searching, analyzing code. These topics should see the real source tree.
- A single editing topic when no other topic is modifying the same files.

**Important:** Topics must `git add` and `git commit` their changes before calling `swarm done`. Uncommitted work in a worktree is invisible to other topics and the coordinator. After merging a worktree branch, clean it up with `swarm cleanup <id>`.

## Troubleshooting

### Port conflict

If port 9800 is already in use by something that is not a swarm daemon:

```bash
swarm run --port 9801 --harness claude --prompt "..."
```

If it is already a swarm daemon, swarm will reuse it.

### Missing harness CLI

If swarm reports "failed to start claude", the harness binary is not on your PATH. Install it (see the Harnesses table above) or set the override env var:

```bash
export SWARM_CLAUDE_BIN=/path/to/claude
```

### Stuck Topic

Check the topic's log for errors:

```bash
swarm log <topic-id> --output
```

If a topic is unresponsive, kill it:

```bash
swarm kill <topic-id>
```

### Done worktree cleanup

If worktrees are left behind after topics finish, clean them up:

```bash
swarm peers --all                          # find the topic ID
swarm cleanup <topic-id> --delete-branch   # remove worktree + branch
```

### Debug logging

Enable detailed logging with:

```bash
RUST_LOG=swarm=debug swarm run --harness claude --prompt "..."
```

## Contributing / Development

```bash
# Clone
git clone https://github.com/sjalq/swarm.git
cd swarm

# Build
cargo build

# Run tests
cargo test --all-features

# Format and lint
cargo fmt
cargo clippy -- -D warnings
```

### Project layout

```
src/
  main.rs          CLI entry point, subcommand dispatch
  lib.rs           Public module re-exports
  orchestrator.rs  Topic lifecycle and message routing
  server.rs        HTTP API and dashboard server setup
  harness.rs       Harness trait + CLI harness implementations
  db.rs            SQLite persistence layer
  error.rs         Error types
tests/             Integration tests
```

## License

Licensed under either of

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

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
# Install
curl -fsSL https://raw.githubusercontent.com/sjalq/swarm/main/install.sh | bash

# Start a topic with Claude
swarm run --harness claude "Refactor the auth module into smaller files."

# In another terminal, check on topics
swarm peers

# View a compact digest before opening raw logs
swarm brief <topic-id>
```

## Install

### From prebuilt binary

```bash
curl -fsSL https://raw.githubusercontent.com/sjalq/swarm/main/install.sh | bash
```

Set `SWARM_VERSION=v0.1.0` to pin a specific version. Set `BIN_DIR=~/.local/bin` to change the install location.

### From source

```bash
cargo install --git https://github.com/sjalq/swarm.git
```

### From crates.io

```bash
cargo install swarm-cli
```

The crate is named `swarm-cli`; the installed binary is `swarm`.

### From Homebrew

```bash
brew install sjalq/swarm/swarm
```

Available once the tap is published.

## Harnesses

Swarm delegates actual AI work to external CLI tools called harnesses. Each harness wraps a specific LLM provider's CLI.

```
Harness  | Install                              | API Key Env Var              | Docs
---------|--------------------------------------|------------------------------|-----------------------------------------------
claude   | npm install -g @anthropic-ai/claude  | ANTHROPIC_API_KEY            | https://docs.anthropic.com/en/docs/claude-code
codex    | npm install -g @openai/codex         | OPENAI_API_KEY               | https://github.com/openai/codex
gemini   | npm install -g @anthropic-ai/claude  | GEMINI_API_KEY               | https://github.com/google-gemini/gemini-cli
grok     | npm install -g grok-cli              | XAI_API_KEY                  | https://docs.x.ai/docs/grok-cli
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

# Default server port
# port = 9800

# Default harness for new topics
# default_harness = "claude"

# Default communication mode: "mesh" or "parent-only"
# default_comms = "mesh"

# Topic worker timeout in milliseconds (default: 6 hours)
# agent_timeout_ms = 21600000

# Harness binary overrides
# [harness.claude]
# binary = "/usr/local/bin/claude"

# [harness.codex]
# binary = "/opt/codex/bin/codex"

# [harness.gemini]
# binary = "gemini"

# [harness.grok]
# binary = "grok"
```

### Environment variables

```
Variable            | Description
--------------------|------------------------------------------------------------
SWARM_SOCKET        | HTTP URL for topic-to-daemon communication
SWARM_AGENT_ID      | Current topic identifier (set automatically)
SWARM_PROJECT_DIR   | Project root directory (set automatically)
SWARM_CLAUDE_BIN    | Override the Claude CLI binary path
SWARM_CODEX_BIN     | Override the Codex CLI binary path
SWARM_GEMINI_BIN    | Override the Gemini CLI binary path
SWARM_GROK_BIN      | Override the Grok CLI binary path
RUST_LOG            | Control log verbosity (e.g. RUST_LOG=swarm=debug)
```

## Data layout

Swarm stores all runtime data under `.swarm/` in the project directory:

```
.swarm/
  swarm.db          SQLite database (topic state, messages, logs)
  agents/           Per-topic working directories (legacy path name)
    <topic-id>/     Topic home (env file, harness config)
  worktrees/        Git worktrees for isolated topic branches
    <topic-id>/     Separate checkout on branch swarm/<topic-id>
```

On first run, swarm automatically appends `.swarm/` to the project's `.gitignore` if it is not already present. To suppress this behavior, pass `--no-gitignore` to `swarm run`.

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
- `--port <PORT>` : Server port (default: `9800`)
- `--harness <NAME>` : Harness for the topic worker (default: `echo`)
- `--prompt <TEXT>` : Extra prompt text, or the task text when no positional task is provided
- `--label <NAME>` : Readable label for the topic (default: `coordinator`)
- `--comms <MODE>` : Communication mode: `mesh` or `parent-only` (default: `mesh`)
- `--model <MODEL>` : Model override supported by the selected harness CLI.
- `--worktree` : Give the topic its own git worktree (isolated branch)
- `--detach` : Return immediately instead of watching direct messages to the parent.

`swarm run` starts the daemon if needed, starts one topic, and sends the task to it.

### `swarm serve`

Start only the daemon/API server without starting a topic.

```bash
swarm serve [OPTIONS]
```

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

```bash
swarm inbox <FROM_TOPIC_ID> [-n <COUNT>]
swarm inbox <FROM_TOPIC_ID> --to user
swarm inbox --all [-n <COUNT>]
swarm inbox --new --all
```

Inbox output shows full direct message bodies by default. Use `--truncate <COUNT>` if you want a shorter terminal view.

- `--new` : Read only messages newer than the saved SQLite cursor for this recipient.
- `--since <TIMESTAMP>` : Read messages after an RFC3339 timestamp.

### `swarm watch`

Poll and print new direct responses sent to the user/current topic.

```bash
swarm watch --all
swarm watch <FROM_TOPIC_ID> --to user
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

When you pass `--worktree` to `swarm run`, the topic gets its own git branch and file checkout under `.swarm/worktrees/<topic-id>/`. This prevents file conflicts when multiple topics edit the same project concurrently.

**When to use worktrees:**

- Multiple topics editing files in the same compiled project (Rust, TypeScript, etc.) where concurrent edits would break the build.
- Parallel feature branches that will be merged by the coordinator.

**When not to use worktrees:**

- Read-only tasks: reviewing, searching, analyzing code. These topics should see the real source tree.
- A single editing topic when no other topic is modifying the same files.

**Important:** Topics must `git add` and `git commit` their changes before calling `swarm done`. Uncommitted work in a worktree is invisible to other topics and the coordinator. After merging a worktree branch, clean it up with `swarm cleanup <id>`.

## Troubleshooting

### Port conflict

If port 9800 is already in use:

```bash
swarm run --port 9801 --harness claude --prompt "..."
```

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

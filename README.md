# swarm

A multi-agent CLI orchestrator that coordinates LLM coding assistants working together on your codebase.

<!-- Badges -->
[![CI](https://github.com/sjalq/swarm/actions/workflows/ci.yml/badge.svg)](https://github.com/sjalq/swarm/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/swarm-cli.svg)](https://crates.io/crates/swarm-cli)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

## Overview

Swarm lets you run multiple AI coding agents in parallel on the same project. A coordinator agent can spawn children, assign them tasks, communicate via messages, and merge their work. Each agent runs in its own process backed by a real LLM CLI (Claude, Codex, Gemini, or Grok), and swarm handles the orchestration: process lifecycle, message routing, git worktree isolation, and persistent state.

Use cases:

- Split a large feature across multiple agents working in parallel worktrees.
- Have one agent implement and another review, with the coordinator merging results.
- Run a heterogeneous swarm (Claude for planning, Codex for implementation, Gemini for testing).
- Automate multi-step workflows that would be tedious to drive by hand.

## Quickstart

```bash
# Install
curl -fsSL https://raw.githubusercontent.com/sjalq/swarm/main/install.sh | bash

# Start a swarm with a Claude coordinator
swarm run --harness claude --prompt "Refactor the auth module into smaller files."

# In another terminal, check on agents
swarm peers

# View a compact digest before opening raw logs
swarm brief <agent-id>
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

# Default harness for the root agent
# default_harness = "claude"

# Default communication mode: "mesh" or "parent-only"
# default_comms = "mesh"

# Agent timeout in milliseconds (default: 6 hours)
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
SWARM_SOCKET        | WebSocket URL for agent-to-orchestrator communication
SWARM_AGENT_ID      | Current agent's unique identifier (set automatically)
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
  swarm.db          SQLite database (agent state, messages, logs)
  agents/           Per-agent working directories
    <agent-id>/     Agent's home (env file, harness config)
  worktrees/        Git worktrees for isolated agent branches
    <agent-id>/     Separate checkout on branch swarm/<agent-id>
```

On first run, swarm automatically appends `.swarm/` to the project's `.gitignore` if it is not already present. To suppress this behavior, pass `--no-gitignore` to `swarm run`.

## Command reference

### `swarm run`

Start the orchestrator and root agent.

```bash
swarm run [OPTIONS]
```

Options:
- `--project-dir <PATH>` : Project directory (default: `.`)
- `--port <PORT>` : Server port (default: `9800`)
- `--harness <NAME>` : Harness for the root agent (default: `echo`)
- `--prompt <TEXT>` : Initial prompt for the root agent
- `--role <NAME>` : Role name for the root agent (default: `coordinator`)

### `swarm peers`

List all agents in the swarm visible to you (parent, siblings, descendants).

```bash
swarm peers [--all]
```

- `--all` : Include done agents.

### `swarm send`

Send a message to another agent.

```bash
swarm send <AGENT_ID> "<MESSAGE>"
```

### `swarm spawn`

Create a new child agent.

```bash
swarm spawn --role <NAME> --harness <HARNESS> [OPTIONS]
```

Options:
- `--role <NAME>` : Agent role name (required)
- `--harness <NAME>` : Harness to use (default: `echo`)
- `--prompt <TEXT>` : System prompt for the agent
- `--comms <MODE>` : Communication mode: `mesh` or `parent-only` (default: `mesh`)
- `--model <MODEL>` : Model override (e.g. `claude-sonnet-4-6`, `o3`)
- `--worktree` : Give the agent its own git worktree (isolated branch)

### `swarm status`

Show your own agent's status, including model and harness info.

```bash
swarm status
```

### `swarm models`

List available models for each harness.

```bash
swarm models
```

### `swarm log`

View an agent's recent activity.

```bash
swarm log <AGENT_ID> [-n <COUNT>] [--messages] [--output] [--search <TEXT>] [--raw]
```

Options:
- `-n <COUNT>` : Number of entries to show (default: `20`)
- `--messages` : Show only messages (sent and received)
- `--output` : Show only harness output
- `--search <TEXT>` : Search log content case-insensitively before applying the limit
- `--raw` : Disable text truncation and show exact full log entries

### `swarm brief`

Show a compact digest that is safe to use as working context before reaching for raw logs.

```bash
swarm brief                 # run-level summary
swarm brief <AGENT_ID>      # one agent summary and compact recent log
swarm brief <AGENT_ID> --search "timeout"
```

Brief output includes status, prompt size, latest structured handover, and short log previews. Use `swarm log --raw` when you need the exact transcript.

### `swarm cleanup`

Remove a finished agent's worktree.

```bash
swarm cleanup <AGENT_ID> [--delete-branch]
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

The optional structured fields are stored separately from the raw transcript and appear in `swarm brief`, keeping handoffs concise for coordinators and follow-on agents.

### `swarm kill`

Stop an agent and mark it done.

```bash
swarm kill <AGENT_ID>
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

When you pass `--worktree` to `swarm spawn`, the agent gets its own git branch and file checkout under `.swarm/worktrees/<agent-id>/`. This prevents file conflicts when multiple agents edit the same project concurrently.

**When to use worktrees:**

- Multiple agents editing files in the same compiled project (Rust, TypeScript, etc.) where concurrent edits would break the build.
- Parallel feature branches that will be merged by the coordinator.

**When not to use worktrees:**

- Read-only tasks: reviewing, searching, analyzing code. These agents should see the real source tree.
- A single editing agent when no other agent is modifying the same files.

**Important:** Agents must `git add` and `git commit` their changes before calling `swarm done`. Uncommitted work in a worktree is invisible to other agents and the coordinator. After merging a worktree branch, clean it up with `swarm cleanup <id>`.

## Troubleshooting

### Port conflict

If port 9800 is already in use:

```bash
swarm run --port 9801 --harness claude --prompt "..."
```

### Missing harness CLI

If swarm reports "failed to spawn claude", the harness binary is not on your PATH. Install it (see the Harnesses table above) or set the override env var:

```bash
export SWARM_CLAUDE_BIN=/path/to/claude
```

### Stuck agent

Check the agent's log for errors:

```bash
swarm log <agent-id> --output
```

If an agent is unresponsive, kill it:

```bash
swarm kill <agent-id>
```

### Done worktree cleanup

If worktrees are left behind after agents finish, clean them up:

```bash
swarm peers --all                          # find the agent ID
swarm cleanup <agent-id> --delete-branch   # remove worktree + branch
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
  orchestrator.rs  Agent lifecycle, message routing, WebSocket server
  server.rs        HTTP/WebSocket server setup
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

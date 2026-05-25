# swarm

## TL;DR

Swarm is a local CLI + daemon for coordinating durable LLM coding topics. Each topic has an ID, parent, children, mailbox, status, log, optional worktree, and a worker backed by a real harness CLI such as Claude, Codex, Gemini, Grok, or the built-in `echo` test harness.

Unlike pane/session launchers, swarm makes message delivery and topic state explicit. That is more reliable for coding harnesses because these tools are CLI processes resumed with new prompts and filesystem state, not long-lived in-memory agents with perfect shared context.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/sjalq/swarm/main/install.sh | bash
```

The installer uses a checksum-verified GitHub release asset when available. If no matching release exists, it falls back to building from GitHub source with Cargo.

Useful install modes:

```bash
# Pin a release and choose the install directory
curl -fsSL https://raw.githubusercontent.com/sjalq/swarm/main/install.sh | bash -s -- --version v0.1.0 --bin-dir ~/.local/bin

# Force a source build from GitHub
curl -fsSL https://raw.githubusercontent.com/sjalq/swarm/main/install.sh | bash -s -- --source

# Build and install this local checkout
git clone https://github.com/sjalq/swarm.git
cd swarm
./install.sh --local
```

Source installs require Rust/Cargo. The installer adds `wasm32-unknown-unknown` and installs `trunk` when needed so the dashboard is embedded in the binary.

## Quick Start

```bash
# No external API required
swarm run --harness echo "hello"

# Check configured harness CLIs and API keys
swarm doctor

# Start a real coding topic
swarm run --harness claude "Investigate the checkout failures"

# Watch replies from the printed topic ID
swarm watch-inbox user --from <topic-id>
```

Any daemon-backed command auto-starts the local API/dashboard if it is not already running. The printed dashboard URL is usually `http://127.0.0.1:9800`.

## Why This Exists

LLM coding tools already know how to edit files, run commands, and resume from CLI prompts. Swarm does not replace those harnesses; it gives them durable coordination:

- topic tree: parent, siblings, and children
- SQLite-backed mailboxes and logs
- wake-on-message execution instead of polling loops
- dashboard and HTTP API over the same state
- optional git worktrees for parallel editing

Compared with using agents in cmux, swarm is less about arranging terminals and more about preserving the coordination contract. cmux is useful when you want to supervise multiple interactive panes. Swarm is better when you need tasks to survive reconnects, keep explicit parent/child routing, and resume harness CLIs from persisted messages. That matches how LLM coding harnesses actually work: prompt in, filesystem and process output out, next prompt later.

## Harnesses

```
Harness  Install                               Key
claude   npm install -g @anthropic-ai/claude-code  ANTHROPIC_API_KEY
codex    npm install -g @openai/codex              OPENAI_API_KEY
gemini   npm install -g @google/gemini-cli         GEMINI_API_KEY
grok     npm install -g @xai-official/grok         XAI_API_KEY
echo     built in                                  none
```

Set binary overrides with `SWARM_CLAUDE_BIN`, `SWARM_CODEX_BIN`, `SWARM_GEMINI_BIN`, or `SWARM_GROK_BIN`.

## Core Commands

```bash
swarm run "task"                         # start a root topic, or child topic inside swarm
swarm peers --all                         # list topics
swarm send <topic-id> "message"           # direct message
swarm send parent "message"               # reply from inside a topic
swarm send-family "message"               # broadcast to parent, siblings, children
swarm inbox --all                         # read direct messages
swarm watch-inbox                         # stream new direct messages
swarm log <topic-id> --messages --raw     # inspect exact message history
swarm brief [topic-id]                    # compact deterministic status
swarm done "summary"                      # finish and optionally report to parent
swarm cleanup <topic-id> --delete-branch  # remove a worktree
```

Inside a topic, `swarm run` automatically creates a child of the current topic because `SWARM_AGENT_ID` and `SWARM_SOCKET` are set for the harness process.

## Worktrees

Use `--worktree` when multiple topics may edit the same repository:

```bash
swarm run --label parser --harness codex --worktree "Refactor the parser"
```

The checkout lives under `<data-dir>/worktrees/<topic-id>` on branch `swarm/<topic-id>`. Topics should commit their work before `swarm done`; the coordinator can then review or merge the branch and run `swarm cleanup`.

## Configuration

Global config: `~/.config/swarm/config.toml`

Project config: `.swarm/config.toml`

```toml
default_port = 9800
default_harness = "claude"
default_comms = "mesh" # or "parent-only"
data_dir = "/path/to/swarm-data"
```

Important environment variables:

```
SWARM_SOCKET       daemon URL, default http://127.0.0.1:9800
SWARM_AGENT_ID     current topic ID, set inside harnesses
SWARM_PROJECT_DIR  project root, set inside harnesses
RUST_LOG           e.g. swarm=debug
```

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features

# Build the embedded dashboard locally
rustup target add wasm32-unknown-unknown
cargo install trunk --locked
cargo build
```

GitHub CI keeps this concise: Ubuntu runs fmt, clippy, tests, and the dashboard build path; macOS runs the test suite as a portability check. Release builds produce Linux and macOS archives plus `SHA256SUMS` for the installer.

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

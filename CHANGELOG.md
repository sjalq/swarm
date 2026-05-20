# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-20

### Added

- Multi-agent CLI orchestrator with WebSocket-based communication.
- Harness support for Claude, Codex, Gemini, and Grok LLM backends.
- Agent lifecycle management: spawn, kill, done, cleanup.
- Git worktree isolation for concurrent agent editing.
- Peer discovery and mesh/parent-only communication modes.
- Agent activity logging with message and output filtering.
- Model listing and status introspection commands.
- SQLite-backed agent state persistence.
- Automatic `.gitignore` management for `.swarm/` directory.
- curl-pipe-sh installer script with checksum verification.
- CI workflow (fmt, clippy, test) on Ubuntu and macOS matrix.
- Release workflow with cross-compilation for Linux and macOS (x86_64 + aarch64).
- Homebrew formula for macOS installation.
- Comprehensive README with quickstart, configuration, and troubleshooting.

[Unreleased]: https://github.com/sjalq/swarm/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/sjalq/swarm/releases/tag/v0.1.0

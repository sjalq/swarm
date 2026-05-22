# Multi-Topic Walkthrough: Feature Implementation with Review

This example walks through a realistic workflow: a Claude coordinator starts two
Codex topics to implement and review a feature, then merges the results.

## Scenario

You want to add a `/health` endpoint to a web service. One topic writes the code,
another reviews it. A Claude coordinator topic orchestrates the whole thing.

## Step 1: Start the coordinator topic

```bash
swarm run \
  --project-dir ./my-project \
  --harness claude \
  --label coordinator \
  "Add a /health endpoint. Start one codex topic to implement it in a worktree, and another codex topic to review the implementation once it is done. Merge both branches when finished."
```

This starts the daemon on port 9800 if needed, creates the `.swarm/` directory,
and starts a Claude coordinator topic.

## Step 2: What the coordinator does

The coordinator receives the prompt and autonomously:

### Starts the implementation topic

```bash
swarm run \
  --label implementer \
  --harness codex \
  --worktree \
  "Add a GET /health endpoint to src/server.rs that returns 200 OK with a JSON body {\"status\": \"healthy\"}. Write a test for it. Commit your changes when done."
```

The `--worktree` flag gives this topic its own git branch
(`swarm/implementer-<id>`) so it can edit files without conflicting with other topics.

### Starts the review topic (no worktree, read-only)

```bash
swarm run \
  --label reviewer \
  --harness codex \
  "Wait for a message from the coordinator with a branch name. Then review the code changes on that branch. Check for correctness, error handling, and test coverage. Send your review back to the coordinator."
```

No `--worktree` here since the reviewer only reads code; it does not need an
isolated checkout.

## Step 3: Implementation

The implementer topic works in its worktree:

1. Edits `src/server.rs` to add the `/health` route.
2. Adds a test in `tests/health_test.rs`.
3. Runs `cargo test` to verify.
4. Commits: `git add -A && git commit -m "feat: add /health endpoint"`
5. Signals completion: `swarm done "Implemented /health endpoint on branch swarm/implementer-abc123"`

The coordinator receives this message and sends the branch name to the reviewer.

## Step 4: Review

The coordinator sends the branch info to the reviewer:

```bash
swarm send reviewer-def456 "Please review branch swarm/implementer-abc123. Check the /health endpoint implementation and tests."
```

The reviewer topic:

1. Runs `git log main..swarm/implementer-abc123` and `git diff main...swarm/implementer-abc123`.
2. Reads the changed files.
3. Sends feedback: `swarm send coordinator-789 "Review: LGTM. The endpoint returns correct JSON, test covers the happy path. Suggestion: consider adding a test for the response content-type header."`
4. Signals done: `swarm done "Review complete"`

## Step 5: Merge

The coordinator merges the implementation branch:

```bash
git merge swarm/implementer-abc123
```

Then cleans up the worktree:

```bash
swarm cleanup implementer-abc123
```

Finally, the coordinator signals completion:

```bash
swarm done "Feature complete: /health endpoint implemented, reviewed, and merged."
```

## Monitoring the swarm

While topics are working, you can check on them from any terminal that can reach
the swarm socket:

```bash
# List all topics and their status
swarm peers

# Check what the implementer is doing
swarm log implementer-abc123

# See only messages exchanged
swarm log implementer-abc123 --messages

# View available models
swarm models

# Check your own status
swarm status
```

## Key takeaways

- Use `--worktree` when topics will edit files, skip it for read-only tasks.
- Topics communicate via `swarm send` and receive messages through their harness.
- The coordinator is just another topic; it orchestrates by sending messages and
  starting child topics with `swarm run`.
- Always commit changes in a worktree before calling `swarm done`, otherwise the
  work is invisible to other topics.
- Use `swarm cleanup <id>` after merging a worktree branch to remove the temporary
  checkout.

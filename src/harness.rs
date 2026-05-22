use crate::error::{Result, SwarmError};
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum HarnessOutput {
    Text(String),
    Complete(String),
    Error(String),
    Timeout(String),
}

pub trait Harness: Send + Sync {
    fn name(&self) -> &str;
    fn run(
        &self,
        prompt: &str,
        model: Option<&str>,
        continue_conversation: bool,
        work_dir: &Path,
        env_extra: HashMap<String, String>,
        tx: mpsc::Sender<HarnessOutput>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
}

// -- Echo harness (testing) --------------------------------------------------

pub struct EchoHarness;

impl Harness for EchoHarness {
    fn name(&self) -> &str {
        "echo"
    }

    fn run(
        &self,
        prompt: &str,
        _model: Option<&str>,
        _continue: bool,
        _work_dir: &Path,
        env: HashMap<String, String>,
        tx: mpsc::Sender<HarnessOutput>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let messages = parse_echo_messages(prompt);
        Box::pin(async move {
            let Some(socket) = env.get("SWARM_SOCKET").cloned() else {
                return Ok(());
            };
            let Some(agent_id) = env.get("SWARM_AGENT_ID").cloned() else {
                return Ok(());
            };
            let client = reqwest::Client::new();
            for (sender, content) in messages {
                let response = format!("(echo) {}", echo_payload(&content));
                let resp = client
                    .post(format!("{socket}/api/messages"))
                    .json(&serde_json::json!({
                        "from": agent_id,
                        "to": sender,
                        "content": response,
                    }))
                    .send()
                    .await
                    .map_err(|e| SwarmError::Internal(format!("echo send failed: {e}")))?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(SwarmError::Internal(format!(
                        "echo send failed: {status} {}",
                        body.trim()
                    )));
                }
            }
            drop(tx);
            Ok(())
        })
    }
}

fn parse_echo_messages(prompt: &str) -> Vec<(String, String)> {
    let mut messages = Vec::new();
    let mut sender: Option<String> = None;
    let mut content = String::new();

    for line in prompt.lines() {
        if let Some(from) = line
            .strip_prefix("[from: ")
            .and_then(|line| line.strip_suffix(']'))
        {
            if let Some(sender) = sender.replace(from.to_string()) {
                messages.push((sender, content.trim().to_string()));
                content.clear();
            }
            continue;
        }

        if sender.is_some() {
            content.push_str(line);
            content.push('\n');
        }
    }

    if let Some(sender) = sender {
        messages.push((sender, content.trim().to_string()));
    }

    messages
}

fn echo_payload(message: &str) -> &str {
    let Some((_, after_task)) = message.rsplit_once("\nTask:\n") else {
        return message.trim();
    };
    after_task
        .split_once("\n\nWork independently.")
        .map(|(task, _)| task)
        .unwrap_or(after_task)
        .trim()
}

// -- Generic CLI harness -----------------------------------------------------

#[derive(Debug, Clone)]
pub enum CliKind {
    Claude,
    Gemini,
    Codex,
    Grok,
}

impl CliKind {
    pub fn default_binary(&self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Gemini => "gemini",
            Self::Codex => "codex",
            Self::Grok => "grok",
        }
    }

    pub fn env_var_name(&self) -> &'static str {
        match self {
            Self::Claude => "SWARM_CLAUDE_BIN",
            Self::Gemini => "SWARM_GEMINI_BIN",
            Self::Codex => "SWARM_CODEX_BIN",
            Self::Grok => "SWARM_GROK_BIN",
        }
    }

    pub fn resolved_binary(&self) -> String {
        std::env::var(self.env_var_name()).unwrap_or_else(|_| self.default_binary().to_string())
    }

    pub fn api_key_env_names(&self) -> &[&'static str] {
        match self {
            Self::Claude => &["ANTHROPIC_API_KEY"],
            Self::Codex => &["OPENAI_API_KEY", "CODEX_API_KEY"],
            Self::Gemini => &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
            Self::Grok => &["XAI_API_KEY"],
        }
    }

    pub fn default_model(&self) -> &'static str {
        ""
    }

    pub fn known_models(&self) -> &[&str] {
        &[]
    }

    pub fn all_kinds() -> &'static [CliKind] {
        &[Self::Claude, Self::Gemini, Self::Codex, Self::Grok]
    }

    pub fn from_harness_name(name: &str) -> Option<Self> {
        match name {
            "claude" => Some(Self::Claude),
            "gemini" => Some(Self::Gemini),
            "codex" => Some(Self::Codex),
            "grok" => Some(Self::Grok),
            _ => None,
        }
    }

    fn needs_stdin_prompt(&self) -> bool {
        matches!(self, Self::Codex)
    }

    fn build_args(
        &self,
        prompt: &str,
        model: Option<&str>,
        continue_conversation: bool,
        work_dir: &Path,
        env: &HashMap<String, String>,
    ) -> Vec<String> {
        match self {
            Self::Claude => {
                let mut args = vec!["-p".into(), prompt.into()];
                if continue_conversation {
                    args.push("-c".into());
                }
                if let Some(m) = model {
                    args.extend_from_slice(&["--model".into(), m.into()]);
                }
                args.extend_from_slice(&[
                    "--output-format".into(),
                    "stream-json".into(),
                    "--verbose".into(),
                    "--dangerously-skip-permissions".into(),
                ]);
                args
            }
            Self::Gemini => {
                let mut args = vec!["-p".into(), prompt.into()];
                if continue_conversation {
                    args.extend_from_slice(&["--resume".into(), "latest".into()]);
                }
                if let Some(m) = model {
                    args.extend_from_slice(&["-m".into(), m.into()]);
                }
                args.extend_from_slice(&[
                    "-o".into(),
                    "stream-json".into(),
                    "-y".into(),
                    "--skip-trust".into(),
                    "--no-sandbox".into(),
                ]);
                if let Some(project_dir) = env.get("SWARM_PROJECT_DIR") {
                    args.extend_from_slice(&["--include-directories".into(), project_dir.clone()]);
                }
                args
            }
            Self::Codex => {
                let mut args = if continue_conversation {
                    vec![
                        "exec".into(),
                        "resume".into(),
                        "--last".into(),
                        "--skip-git-repo-check".into(),
                        "--dangerously-bypass-approvals-and-sandbox".into(),
                        "--json".into(),
                    ]
                } else {
                    vec![
                        "exec".into(),
                        "-C".into(),
                        work_dir.to_string_lossy().into(),
                        "--skip-git-repo-check".into(),
                        "--dangerously-bypass-approvals-and-sandbox".into(),
                        "--json".into(),
                    ]
                };
                if let Some(m) = model {
                    args.extend_from_slice(&["-m".into(), m.into()]);
                }
                args.push("-".into());
                args
            }
            Self::Grok => {
                let mut args = vec!["-p".into(), prompt.into()];
                if continue_conversation {
                    args.push("-c".into());
                }
                if let Some(m) = model {
                    args.extend_from_slice(&["-m".into(), m.into()]);
                }
                args.extend_from_slice(&[
                    "--output-format".into(),
                    "streaming-json".into(),
                    "--always-approve".into(),
                ]);
                args
            }
        }
    }
}

pub struct CliHarness {
    kind: CliKind,
    binary: String,
    #[allow(dead_code)]
    model: String,
    timeout_ms: u64,
}

impl CliHarness {
    pub fn new(kind: CliKind) -> Self {
        let binary = kind.resolved_binary();
        Self {
            kind,
            binary,
            model: String::new(),
            timeout_ms: 21_600_000,
        }
    }

    #[allow(dead_code)]
    pub fn with_binary(mut self, binary: String) -> Self {
        self.binary = binary;
        self
    }

    #[allow(dead_code)]
    pub fn with_model(mut self, model: String) -> Self {
        self.model = model;
        self
    }

    #[allow(dead_code)]
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

impl Harness for CliHarness {
    fn name(&self) -> &str {
        self.kind.default_binary()
    }

    fn run(
        &self,
        prompt: &str,
        model: Option<&str>,
        continue_conversation: bool,
        work_dir: &Path,
        env_extra: HashMap<String, String>,
        tx: mpsc::Sender<HarnessOutput>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let args = self
            .kind
            .build_args(prompt, model, continue_conversation, work_dir, &env_extra);
        let binary = self.binary.clone();
        let work_dir = work_dir.to_path_buf();
        let timeout_ms = self.timeout_ms;
        let needs_stdin = self.kind.needs_stdin_prompt();
        let prompt_owned = prompt.to_string();

        Box::pin(async move {
            let mut cmd = tokio::process::Command::new(&binary);
            cmd.args(&args)
                .current_dir(&work_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);

            if needs_stdin {
                cmd.stdin(Stdio::piped());
            } else {
                cmd.stdin(Stdio::null());
            }

            for (k, v) in &env_extra {
                cmd.env(k, v);
            }

            let mut child = cmd
                .spawn()
                .map_err(|e| SwarmError::Process(format!("failed to start {binary}: {e}")))?;

            if needs_stdin {
                if let Some(mut stdin) = child.stdin.take() {
                    stdin
                        .write_all(prompt_owned.as_bytes())
                        .await
                        .map_err(|e| SwarmError::Process(format!("stdin write failed: {e}")))?;
                    drop(stdin);
                }
            }

            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| SwarmError::Process("failed to capture stdout".into()))?;
            let stderr = child
                .stderr
                .take()
                .ok_or_else(|| SwarmError::Process("failed to capture stderr".into()))?;

            let stderr_buf = Arc::new(tokio::sync::Mutex::new(String::new()));
            let stderr_buf_clone = stderr_buf.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    if !line.trim().is_empty() {
                        tracing::debug!("stderr: {}", line);
                        let mut buf = stderr_buf_clone.lock().await;
                        buf.push_str(&line);
                        buf.push('\n');
                    }
                }
            });

            let mut reader = BufReader::new(stdout).lines();
            let tx_clone = tx.clone();

            let process_fut = async {
                let mut accumulated = String::new();
                let mut exit_code: Option<i32> = None;
                loop {
                    tokio::select! {
                        line_result = reader.next_line() => {
                            match line_result {
                                Ok(Some(line)) => {
                                    if !line.trim().is_empty() {
                                        accumulated.push_str(&line);
                                        accumulated.push('\n');
                                        let _ = tx_clone.send(HarnessOutput::Text(line)).await;
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    tracing::error!("stdout read error: {e}");
                                    break;
                                }
                            }
                        }
                        status = child.wait() => {
                            match status {
                                Ok(exit) => {
                                    tracing::debug!("process exited: {exit}");
                                    exit_code = exit.code();
                                    while let Ok(Some(line)) = reader.next_line().await {
                                        if !line.trim().is_empty() {
                                            accumulated.push_str(&line);
                                            accumulated.push('\n');
                                            let _ = tx_clone.send(HarnessOutput::Text(line)).await;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("wait error: {e}");
                                    exit_code = Some(-1);
                                }
                            }
                            break;
                        }
                    }
                }
                (accumulated, exit_code)
            };

            match tokio::time::timeout(Duration::from_millis(timeout_ms), process_fut).await {
                Ok((text, exit_code)) => {
                    let failed = exit_code.is_some_and(|c| c != 0);
                    if failed {
                        let stderr_text = stderr_buf.lock().await;
                        let err_detail = if stderr_text.is_empty() {
                            format!("process exited with code {}", exit_code.unwrap_or(-1))
                        } else {
                            let truncated = if stderr_text.len() > 500 {
                                format!("{}... ({} chars)", &stderr_text[..500], stderr_text.len())
                            } else {
                                stderr_text.to_string()
                            };
                            format!(
                                "process exited with code {}: {}",
                                exit_code.unwrap_or(-1),
                                truncated.trim()
                            )
                        };
                        if !text.is_empty() {
                            let _ = tx.send(HarnessOutput::Complete(text)).await;
                        }
                        let _ = tx.send(HarnessOutput::Error(err_detail.clone())).await;
                        Err(SwarmError::Process(err_detail))
                    } else {
                        let _ = tx.send(HarnessOutput::Complete(text)).await;
                        Ok(())
                    }
                }
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = tx
                        .send(HarnessOutput::Timeout("timed out".to_string()))
                        .await;
                    Err(SwarmError::Timeout(format!("timeout after {timeout_ms}ms")))
                }
            }
        })
    }
}

// -- Pre-flight check --------------------------------------------------------

pub fn preflight_check(harness_name: &str) -> std::result::Result<(), String> {
    if harness_name == "echo" {
        return Ok(());
    }
    let kind = CliKind::from_harness_name(harness_name)
        .ok_or_else(|| format!("unknown harness: {harness_name}"))?;
    let binary = kind.resolved_binary();
    let found = which_binary(&binary);
    if !found {
        let env_var = kind.env_var_name();
        let is_override = std::env::var(env_var).is_ok();
        let detail = if is_override {
            format!(
                "error: harness '{}' binary '{}' (from {}) not found on PATH or as absolute path.",
                harness_name, binary, env_var
            )
        } else {
            format!(
                "error: harness '{}' requires the `{}` CLI on PATH. \
                 Install it (see https://github.com/sjalq/swarm#harnesses) \
                 or set {} to its path. Run `swarm doctor` to diagnose.",
                harness_name, binary, env_var
            )
        };
        return Err(detail);
    }
    Ok(())
}

fn which_binary(binary: &str) -> bool {
    let path = std::path::Path::new(binary);
    if path.is_absolute() {
        return path.exists();
    }
    std::process::Command::new("which")
        .arg(binary)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// -- Registry ----------------------------------------------------------------

pub struct HarnessRegistry {
    harnesses: HashMap<String, Arc<dyn Harness>>,
}

impl HarnessRegistry {
    pub fn new() -> Self {
        let mut reg = Self {
            harnesses: HashMap::new(),
        };
        reg.register(EchoHarness);
        reg.register(CliHarness::new(CliKind::Claude));
        reg.register(CliHarness::new(CliKind::Gemini));
        reg.register(CliHarness::new(CliKind::Codex));
        reg.register(CliHarness::new(CliKind::Grok));
        reg
    }

    pub fn register<H: Harness + 'static>(&mut self, harness: H) {
        self.harnesses
            .insert(harness.name().to_string(), Arc::new(harness));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Harness>> {
        self.harnesses.get(name).cloned()
    }
}

impl Default for HarnessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_defaults() {
        let reg = HarnessRegistry::new();
        assert!(reg.get("echo").is_some());
        assert!(reg.get("claude").is_some());
        assert!(reg.get("gemini").is_some());
        assert!(reg.get("codex").is_some());
        assert!(reg.get("grok").is_some());
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn echo_parses_received_messages() {
        let messages = parse_echo_messages("[from: user]\nhello world");
        assert_eq!(
            messages,
            vec![("user".to_string(), "hello world".to_string())]
        );
    }

    #[test]
    fn echo_extracts_task_from_swarm_task_message() {
        let message =
            "You are topic t.\n\nTask:\nhello from task\n\nWork independently. report back";
        assert_eq!(echo_payload(message), "hello from task");
    }

    #[test]
    fn echo_preflight_always_passes() {
        assert!(preflight_check("echo").is_ok());
    }

    #[test]
    fn unknown_harness_preflight_fails() {
        assert!(preflight_check("nonexistent").is_err());
    }

    #[test]
    fn cli_kind_env_vars() {
        assert_eq!(CliKind::Claude.env_var_name(), "SWARM_CLAUDE_BIN");
        assert_eq!(CliKind::Codex.env_var_name(), "SWARM_CODEX_BIN");
        assert_eq!(CliKind::Gemini.env_var_name(), "SWARM_GEMINI_BIN");
        assert_eq!(CliKind::Grok.env_var_name(), "SWARM_GROK_BIN");
    }

    #[test]
    fn cli_kind_all_kinds() {
        let kinds = CliKind::all_kinds();
        assert_eq!(kinds.len(), 4);
    }

    #[test]
    fn gemini_resume_uses_resume_latest_not_c_flag() {
        let args = CliKind::Gemini.build_args(
            "follow up",
            None,
            true,
            Path::new("/tmp/swarm-test"),
            &HashMap::new(),
        );

        assert!(
            args.windows(2)
                .any(|window| window == ["--resume", "latest"]),
            "Gemini resume should use --resume latest; args: {args:?}"
        );
        assert!(
            !args.iter().any(|arg| arg == "-c"),
            "Gemini CLI 0.42 rejects -c; args: {args:?}"
        );
    }

    #[test]
    fn gemini_first_turn_does_not_resume() {
        let args = CliKind::Gemini.build_args(
            "first turn",
            None,
            false,
            Path::new("/tmp/swarm-test"),
            &HashMap::new(),
        );

        assert!(
            !args.iter().any(|arg| arg == "--resume" || arg == "-c"),
            "Gemini first turn should not request resume; args: {args:?}"
        );
    }

    #[test]
    fn api_key_env_names() {
        assert_eq!(CliKind::Claude.api_key_env_names(), &["ANTHROPIC_API_KEY"]);
        assert!(CliKind::Codex
            .api_key_env_names()
            .contains(&"OPENAI_API_KEY"));
        assert!(CliKind::Gemini
            .api_key_env_names()
            .contains(&"GEMINI_API_KEY"));
        assert_eq!(CliKind::Grok.api_key_env_names(), &["XAI_API_KEY"]);
    }
}

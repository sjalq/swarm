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
        _env: HashMap<String, String>,
        tx: mpsc::Sender<HarnessOutput>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let response = format!("(echo) {}", prompt.trim());
        Box::pin(async move {
            tx.send(HarnessOutput::Complete(response))
                .await
                .map_err(|e| SwarmError::Internal(e.to_string()))?;
            Ok(())
        })
    }
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

    pub fn default_model(&self) -> &'static str {
        match self {
            Self::Claude => "claude-opus-4-6",
            Self::Gemini => "gemini-3.1-pro-preview",
            Self::Codex => "gpt-5.5",
            Self::Grok => "grok-3",
        }
    }

    pub fn known_models(&self) -> &[&str] {
        match self {
            Self::Claude => &[
                "claude-opus-4-6",
                "claude-sonnet-4-6",
                "claude-haiku-4-5-20251001",
            ],
            Self::Gemini => &[
                "gemini-3.1-pro-preview",
                "gemini-2.5-pro",
                "gemini-2.5-flash",
            ],
            Self::Codex => &["gpt-5.5", "o3", "o4-mini"],
            Self::Grok => &["grok-3", "grok-build"],
        }
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
    ) -> Vec<String> {
        match self {
            Self::Claude => {
                let mut args = vec![
                    "-p".into(),
                    prompt.into(),
                ];
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
                let mut args = vec![
                    "-p".into(),
                    prompt.into(),
                ];
                if continue_conversation {
                    args.push("-c".into());
                }
                if let Some(m) = model {
                    args.extend_from_slice(&["-m".into(), m.into()]);
                }
                args.extend_from_slice(&[
                    "-o".into(),
                    "stream-json".into(),
                    "-y".into(),
                    "--skip-trust".into(),
                    "--sandbox".into(),
                    "false".into(),
                ]);
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
                let mut args = vec![
                    "-p".into(),
                    prompt.into(),
                ];
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
        let binary = kind.default_binary().to_string();
        let model = kind.default_model().to_string();
        Self {
            kind,
            binary,
            model,
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
        let args = self.kind.build_args(prompt, model, continue_conversation, work_dir);
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
                .map_err(|e| SwarmError::Process(format!("failed to spawn {binary}: {e}")))?;

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
                    let failed = exit_code.map_or(false, |c| c != 0);
                    if failed {
                        let stderr_text = stderr_buf.lock().await;
                        let err_detail = if stderr_text.is_empty() {
                            format!(
                                "process exited with code {}",
                                exit_code.unwrap_or(-1)
                            )
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
                    Err(SwarmError::Timeout(format!(
                        "timeout after {timeout_ms}ms"
                    )))
                }
            }
        })
    }
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

    #[tokio::test]
    async fn echo_harness_returns_prompt() {
        let harness = EchoHarness;
        let (tx, mut rx) = mpsc::channel(10);
        let dir = std::env::temp_dir();
        harness
            .run("hello world", None, false, &dir, HashMap::new(), tx)
            .await
            .unwrap();
        let output = rx.recv().await.unwrap();
        match output {
            HarnessOutput::Complete(text) => assert_eq!(text, "(echo) hello world"),
            other => panic!("expected Complete, got {:?}", other),
        }
    }
}

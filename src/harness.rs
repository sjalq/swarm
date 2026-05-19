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

    fn needs_stdin_prompt(&self) -> bool {
        matches!(self, Self::Codex)
    }

    fn build_args(&self, prompt: &str, model: &str, work_dir: &Path) -> Vec<String> {
        match self {
            Self::Claude => vec![
                "-p".into(),
                prompt.into(),
                "--model".into(),
                model.into(),
                "--output-format".into(),
                "stream-json".into(),
                "--verbose".into(),
                "--dangerously-skip-permissions".into(),
            ],
            Self::Gemini => vec![
                "-p".into(),
                prompt.into(),
                "-m".into(),
                model.into(),
                "-o".into(),
                "stream-json".into(),
                "-y".into(),
                "--skip-trust".into(),
            ],
            Self::Codex => {
                let mut args = vec![
                    "exec".into(),
                    "--cd".into(),
                    work_dir.to_string_lossy().into(),
                    "--skip-git-repo-check".into(),
                    "--dangerously-bypass-approvals-and-sandbox".into(),
                    "--color".into(),
                    "never".into(),
                ];
                args.push("-".into());
                args
            }
            Self::Grok => vec![
                "-p".into(),
                prompt.into(),
                "-m".into(),
                model.into(),
                "--output-format".into(),
                "streaming-json".into(),
                "--always-approve".into(),
            ],
        }
    }
}

pub struct CliHarness {
    kind: CliKind,
    binary: String,
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
        work_dir: &Path,
        env_extra: HashMap<String, String>,
        tx: mpsc::Sender<HarnessOutput>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let args = self.kind.build_args(prompt, &self.model, work_dir);
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

            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    if !line.trim().is_empty() {
                        tracing::debug!("stderr: {}", line);
                    }
                }
            });

            let mut reader = BufReader::new(stdout).lines();
            let tx_clone = tx.clone();

            let process_fut = async {
                let mut accumulated = String::new();
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
                            if let Ok(exit) = status {
                                tracing::debug!("process exited: {exit}");
                                while let Ok(Some(line)) = reader.next_line().await {
                                    if !line.trim().is_empty() {
                                        accumulated.push_str(&line);
                                        accumulated.push('\n');
                                        let _ = tx_clone.send(HarnessOutput::Text(line)).await;
                                    }
                                }
                            }
                            break;
                        }
                    }
                }
                accumulated
            };

            match tokio::time::timeout(Duration::from_millis(timeout_ms), process_fut).await {
                Ok(text) => {
                    let _ = tx.send(HarnessOutput::Complete(text)).await;
                    Ok(())
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
            .run("hello world", &dir, HashMap::new(), tx)
            .await
            .unwrap();
        let output = rx.recv().await.unwrap();
        match output {
            HarnessOutput::Complete(text) => assert_eq!(text, "(echo) hello world"),
            other => panic!("expected Complete, got {:?}", other),
        }
    }
}

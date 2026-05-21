use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize, Clone)]
pub struct SwarmConfig {
    pub default_harness: Option<String>,
    pub default_port: Option<u16>,
    pub default_model: Option<String>,
    pub default_comms: Option<String>,
    pub data_dir: Option<String>,
    pub claude_bin: Option<String>,
    pub codex_bin: Option<String>,
    pub gemini_bin: Option<String>,
    pub grok_bin: Option<String>,
}

impl SwarmConfig {
    pub fn load(project_dir: Option<&Path>) -> Self {
        let global = Self::load_global();
        let project = project_dir.and_then(Self::load_project);
        Self::merge(global, project)
    }

    pub fn default_data_dir() -> PathBuf {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("swarm")
    }

    fn breadcrumb_path() -> Option<PathBuf> {
        dirs::config_dir().map(|d| d.join("swarm").join("active-data-dir"))
    }

    pub fn write_breadcrumb(data_dir: &Path) {
        if let Some(path) = Self::breadcrumb_path() {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(&path, data_dir.to_string_lossy().as_bytes());
        }
    }

    pub fn read_breadcrumb() -> Option<PathBuf> {
        let path = Self::breadcrumb_path()?;
        let content = std::fs::read_to_string(path).ok()?;
        let p = PathBuf::from(content.trim());
        p.exists().then_some(p)
    }

    pub fn resolve_data_dir(cli_override: Option<&Path>, config: &SwarmConfig) -> PathBuf {
        if let Some(dir) = cli_override {
            return dir.to_path_buf();
        }
        if let Some(ref dir) = config.data_dir {
            return PathBuf::from(dir);
        }
        if let Some(dir) = Self::read_breadcrumb() {
            return dir;
        }
        Self::default_data_dir()
    }

    fn global_path() -> Option<PathBuf> {
        dirs::config_dir().map(|d| d.join("swarm").join("config.toml"))
    }

    fn load_global() -> Option<SwarmConfig> {
        let path = Self::global_path()?;
        Self::load_file(&path)
    }

    fn load_project(project_dir: &Path) -> Option<SwarmConfig> {
        let path = project_dir.join(".swarm").join("config.toml");
        Self::load_file(&path)
    }

    fn load_file(path: &Path) -> Option<SwarmConfig> {
        let content = std::fs::read_to_string(path).ok()?;
        toml::from_str(&content).ok()
    }

    fn merge(global: Option<SwarmConfig>, project: Option<SwarmConfig>) -> Self {
        let base = global.unwrap_or_default();
        let over = match project {
            Some(p) => p,
            None => return base,
        };
        SwarmConfig {
            default_harness: over.default_harness.or(base.default_harness),
            default_port: over.default_port.or(base.default_port),
            default_model: over.default_model.or(base.default_model),
            default_comms: over.default_comms.or(base.default_comms),
            data_dir: over.data_dir.or(base.data_dir),
            claude_bin: over.claude_bin.or(base.claude_bin),
            codex_bin: over.codex_bin.or(base.codex_bin),
            gemini_bin: over.gemini_bin.or(base.gemini_bin),
            grok_bin: over.grok_bin.or(base.grok_bin),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_all_none() {
        let cfg = SwarmConfig::default();
        assert!(cfg.default_harness.is_none());
        assert!(cfg.default_port.is_none());
    }

    #[test]
    fn merge_project_wins() {
        let global = SwarmConfig {
            default_port: Some(9800),
            default_harness: Some("echo".into()),
            ..Default::default()
        };
        let project = SwarmConfig {
            default_port: Some(9999),
            ..Default::default()
        };
        let merged = SwarmConfig::merge(Some(global), Some(project));
        assert_eq!(merged.default_port, Some(9999));
        assert_eq!(merged.default_harness, Some("echo".into()));
    }

    #[test]
    fn merge_with_no_project() {
        let global = SwarmConfig {
            default_port: Some(9800),
            ..Default::default()
        };
        let merged = SwarmConfig::merge(Some(global), None);
        assert_eq!(merged.default_port, Some(9800));
    }

    #[test]
    fn load_missing_file_returns_defaults() {
        let cfg = SwarmConfig::load(Some(Path::new("/nonexistent/path")));
        assert!(cfg.default_port.is_none());
    }

    #[test]
    fn parse_toml_config() {
        let toml_str = r#"
            default_harness = "claude"
            default_port = 9801
            default_model = "harness-supported-model"
            claude_bin = "/usr/local/bin/claude"
        "#;
        let cfg: SwarmConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.default_harness.as_deref(), Some("claude"));
        assert_eq!(cfg.default_port, Some(9801));
        assert_eq!(cfg.claude_bin.as_deref(), Some("/usr/local/bin/claude"));
    }
}

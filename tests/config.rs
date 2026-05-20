use std::io::Write;

#[test]
fn project_config_overrides_global() {
    let dir = tempfile::tempdir().unwrap();

    let global_dir = dir.path().join("global_config").join("swarm");
    std::fs::create_dir_all(&global_dir).unwrap();
    let mut global_file = std::fs::File::create(global_dir.join("config.toml")).unwrap();
    writeln!(
        global_file,
        "default_port = 9800\ndefault_harness = \"echo\""
    )
    .unwrap();

    let project_dir = dir.path().join("project");
    let project_swarm = project_dir.join(".swarm");
    std::fs::create_dir_all(&project_swarm).unwrap();
    let mut project_file = std::fs::File::create(project_swarm.join("config.toml")).unwrap();
    writeln!(project_file, "default_port = 9999").unwrap();

    let global_cfg: swarm::config::SwarmConfig =
        toml::from_str(&std::fs::read_to_string(global_dir.join("config.toml")).unwrap()).unwrap();
    let project_cfg: swarm::config::SwarmConfig =
        toml::from_str(&std::fs::read_to_string(project_swarm.join("config.toml")).unwrap())
            .unwrap();

    assert_eq!(global_cfg.default_port, Some(9800));
    assert_eq!(project_cfg.default_port, Some(9999));

    let merged = swarm::config::SwarmConfig::load(Some(&project_dir));

    assert_eq!(
        merged.default_port,
        Some(9999),
        "project config port should win over global"
    );
}

#[test]
fn missing_config_returns_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = swarm::config::SwarmConfig::load(Some(dir.path()));
    assert!(cfg.default_port.is_none());
    assert!(cfg.default_harness.is_none());
}

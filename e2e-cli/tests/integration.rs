use std::process::Command;

#[test]
fn cli_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_ez-cdc"))
        .arg("--version")
        .output()
        .expect("failed to run ez-cdc");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ez-cdc"));
}

#[test]
fn cli_help() {
    let output = Command::new(env!("CARGO_BIN_EXE_ez-cdc"))
        .arg("--help")
        .output()
        .expect("failed to run ez-cdc");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("verify"));
    assert!(stdout.contains("quickstart"));
    assert!(stdout.contains("datasource"));
}

#[test]
fn cli_ds_help() {
    let output = Command::new(env!("CARGO_BIN_EXE_ez-cdc"))
        .args(["datasource", "--help"])
        .output()
        .expect("failed to run ez-cdc");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("list"));
    assert!(stdout.contains("init"));
}

#[test]
fn cli_ds_init_blank_template() {
    // Blank template: writes a starter config with every dbmazz variable
    // commented inline, no demo datasources. The file must be parseable
    // by the CLI afterwards (ds list should succeed and report empty).
    let dir = std::env::temp_dir().join("ez-cdc-integration-test-blank");
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("config.yaml");

    let output = Command::new(env!("CARGO_BIN_EXE_ez-cdc"))
        .args([
            "ds",
            "init",
            "--template",
            "blank",
            "--config",
            &config.to_string_lossy(),
        ])
        .output()
        .expect("failed to run ds init");
    assert!(
        output.status.success(),
        "ds init --template blank failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(config.exists(), "blank template was not written to disk");
    let yaml = std::fs::read_to_string(&config).unwrap();
    assert!(yaml.contains("flush_size: 10000"), "blank template missing settings");
    assert!(yaml.contains("Maps to SOURCE_URL"), "blank template missing source docs");

    let output = Command::new(env!("CARGO_BIN_EXE_ez-cdc"))
        .args(["ds", "list", "--config", &config.to_string_lossy()])
        .output()
        .expect("failed to run ds list");
    assert!(
        output.status.success(),
        "ds list failed on blank template: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No datasources configured"),
        "ds list did not report empty store on blank template: {stdout}"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn cli_ds_init_demo_template() {
    // Demo template: preserves the pre-existing in-repo behavior of
    // seeding demo-pg + demo-starrocks. Backwards compat check.
    let dir = std::env::temp_dir().join("ez-cdc-integration-test-demo");
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("ez-cdc.yaml");

    let output = Command::new(env!("CARGO_BIN_EXE_ez-cdc"))
        .args([
            "ds",
            "init",
            "--template",
            "demo",
            "--config",
            &config.to_string_lossy(),
        ])
        .output()
        .expect("failed to run ds init");
    assert!(
        output.status.success(),
        "ds init --template demo failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = Command::new(env!("CARGO_BIN_EXE_ez-cdc"))
        .args(["ds", "list", "--config", &config.to_string_lossy()])
        .output()
        .expect("failed to run ds list");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("demo-pg"));
    assert!(stdout.contains("demo-starrocks"));

    std::fs::remove_dir_all(&dir).ok();
}

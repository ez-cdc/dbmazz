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
fn cli_ds_init_and_list() {
    let dir = std::env::temp_dir().join("ez-cdc-integration-test");
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("ez-cdc.yaml");

    // Init demos
    let output = Command::new(env!("CARGO_BIN_EXE_ez-cdc"))
        .args(["ds", "init", "--config", &config.to_string_lossy()])
        .output()
        .expect("failed to run ds init");
    assert!(
        output.status.success(),
        "ds init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // List
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

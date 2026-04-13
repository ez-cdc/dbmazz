//! Docker Compose CLI wrapper.
//!
//! Shells out to `docker compose` to manage stacks.

use std::path::Path;
use std::process::Command;

use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ComposeError(pub String);

/// Verify that `docker compose` is available.
pub fn check_docker_compose() -> Result<(), ComposeError> {
    let result = Command::new("docker")
        .args(["compose", "version"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    match result {
        Ok(status) if status.success() => Ok(()),
        Ok(_) => Err(ComposeError(
            "'docker compose' is not available. Install Docker Desktop or the compose plugin: \
             https://docs.docker.com/compose/install/"
                .into(),
        )),
        Err(e) => Err(ComposeError(format!(
            "docker is not installed or not in PATH: {e}"
        ))),
    }
}

fn base_cmd(compose_file: &Path) -> Command {
    let mut cmd = Command::new("docker");
    cmd.args(["compose", "-f", &compose_file.to_string_lossy()]);
    cmd
}

/// Start the stack.
pub fn up(
    compose_file: &Path,
    wait: bool,
    build: bool,
    force_recreate: bool,
    services: &[&str],
) -> Result<(), ComposeError> {
    check_docker_compose()?;

    let mut cmd = base_cmd(compose_file);
    cmd.arg("up").arg("-d");
    if build {
        cmd.arg("--build");
    }
    if force_recreate {
        cmd.arg("--force-recreate");
    }
    if wait {
        cmd.arg("--wait");
    }
    for svc in services {
        cmd.arg(svc);
    }

    let status = cmd
        .status()
        .map_err(|e| ComposeError(format!("failed to run docker compose up: {e}")))?;

    // `docker compose up --wait` returns exit 1 when one-shot init containers
    // (like sink-starrocks-init) exit successfully. This is expected behavior —
    // the init container's job is to run once and exit. We only fail if the
    // exit code indicates a real problem (and --wait was not used).
    if !status.success() && !wait {
        return Err(ComposeError(format!(
            "docker compose up failed for {} (exit {:?})",
            compose_file.display(),
            status.code()
        )));
    }
    Ok(())
}

/// Build images without starting containers.
#[allow(dead_code)]
pub fn build_images(compose_file: &Path, services: &[&str]) -> Result<(), ComposeError> {
    check_docker_compose()?;

    let mut cmd = base_cmd(compose_file);
    cmd.arg("build");
    for svc in services {
        cmd.arg(svc);
    }

    let status = cmd
        .status()
        .map_err(|e| ComposeError(format!("failed to run docker compose build: {e}")))?;

    if !status.success() {
        return Err(ComposeError(format!(
            "docker compose build failed for {} (exit {:?})",
            compose_file.display(),
            status.code()
        )));
    }
    Ok(())
}

/// Stop and remove the stack.
pub fn down(
    compose_file: &Path,
    remove_volumes: bool,
    services: &[&str],
) -> Result<(), ComposeError> {
    check_docker_compose()?;

    if !services.is_empty() {
        return stop_services(compose_file, services);
    }

    let mut cmd = base_cmd(compose_file);
    cmd.arg("down");
    if remove_volumes {
        cmd.arg("-v");
    }

    let status = cmd
        .status()
        .map_err(|e| ComposeError(format!("failed to run docker compose down: {e}")))?;

    if !status.success() {
        return Err(ComposeError(format!(
            "docker compose down failed for {} (exit {:?})",
            compose_file.display(),
            status.code()
        )));
    }
    Ok(())
}

/// Stream logs (inherits stdio so the user sees output in real time).
pub fn logs(
    compose_file: &Path,
    service: Option<&str>,
    follow: bool,
    tail: u32,
) -> Result<(), ComposeError> {
    check_docker_compose()?;

    let mut cmd = base_cmd(compose_file);
    cmd.args(["logs", &format!("--tail={tail}")]);
    if follow {
        cmd.arg("-f");
    }
    if let Some(svc) = service {
        cmd.arg(svc);
    }

    // Inherit stdio — user sees logs in real time, Ctrl+C stops.
    let _ = cmd.status();
    Ok(())
}

/// Return the output of `docker compose ps`.
#[allow(dead_code)]
pub fn ps(compose_file: &Path) -> Result<String, ComposeError> {
    check_docker_compose()?;

    let output = base_cmd(compose_file)
        .arg("ps")
        .output()
        .map_err(|e| ComposeError(format!("failed to run docker compose ps: {e}")))?;

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Stop and remove specific services (not the whole stack).
pub fn stop_services(compose_file: &Path, services: &[&str]) -> Result<(), ComposeError> {
    check_docker_compose()?;

    // Stop.
    let mut cmd = base_cmd(compose_file);
    cmd.arg("stop");
    for svc in services {
        cmd.arg(svc);
    }
    let status = cmd
        .status()
        .map_err(|e| ComposeError(format!("docker compose stop failed: {e}")))?;
    if !status.success() {
        return Err(ComposeError(format!(
            "docker compose stop failed for services {services:?}"
        )));
    }

    // Remove.
    let mut cmd = base_cmd(compose_file);
    cmd.args(["rm", "-f"]);
    for svc in services {
        cmd.arg(svc);
    }
    let status = cmd
        .status()
        .map_err(|e| ComposeError(format!("docker compose rm failed: {e}")))?;
    if !status.success() {
        return Err(ComposeError(format!(
            "docker compose rm failed for services {services:?}"
        )));
    }

    Ok(())
}

/// Check if a specific service has a running container.
pub fn is_service_running(compose_file: &Path, service: &str) -> bool {
    if !compose_file.exists() {
        return false;
    }
    if check_docker_compose().is_err() {
        return false;
    }

    let output = base_cmd(compose_file)
        .args(["ps", "-q", service])
        .output();

    match output {
        Ok(o) => o.status.success() && !o.stdout.is_empty(),
        Err(_) => false,
    }
}

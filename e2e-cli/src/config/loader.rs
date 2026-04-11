//! YAML loader and `${VAR}` interpolation for datasources files.
//!
//! Pipeline:
//!   1. Read raw YAML text from disk.
//!   2. Optionally load `.env` files into the process environment.
//!   3. Walk every string value and substitute `${VAR}` with the env var.
//!   4. Deserialize the interpolated tree into `DatasourcesFile` via serde.
//!
//! Interpolation syntax:
//!   `${VAR}`            — required (error if not set)
//!   `${VAR:-default}`   — optional (uses default if unset/empty)
//!   `$$VAR` / `$${VAR}` — escape (literal `$`)
//!   `$VAR`              — bare reference (also supported)

use std::path::Path;

use regex::Regex;
use thiserror::Error;

use super::schema::DatasourcesFile;

// ── Errors ─────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DatasourceError {
    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    Validation(String),

    #[error("{0}")]
    Interpolation(String),

    #[error("{0}")]
    Io(String),

    #[error("{0}")]
    Yaml(String),
}

// ── Variable interpolation ─────────────────────────────────────────────────

/// Substitute `${VAR}` placeholders in a single string.
pub fn interpolate_string(value: &str, source_path: Option<&Path>) -> Result<String, DatasourceError> {
    if !value.contains('$') {
        return Ok(value.to_string());
    }

    let re = Regex::new(
        r"(?x)
        \$\$ (?: \{[^}]*\} | [A-Za-z_][A-Za-z0-9_]* )    # escape: $${VAR} or $$VAR
        |
        \$\{ (?P<name>[A-Za-z_][A-Za-z0-9_]*) (?: :- (?P<default>[^}]*) )? \}
        |
        \$ (?P<bare> [A-Za-z_][A-Za-z0-9_]* )              # bare $VAR
        "
    ).expect("valid regex");

    let mut result = String::with_capacity(value.len());
    let mut last_end = 0;

    for caps in re.captures_iter(value) {
        let m = caps.get(0).unwrap();
        result.push_str(&value[last_end..m.start()]);
        last_end = m.end();

        let full = m.as_str();

        // Escape: $${...} or $$VAR → strip one $
        if full.starts_with("$$") {
            result.push_str(&full[1..]);
            continue;
        }

        let var_name = caps.name("name")
            .or_else(|| caps.name("bare"))
            .map(|m| m.as_str());

        let Some(var_name) = var_name else {
            result.push_str(full);
            continue;
        };

        let default = caps.name("default").map(|m| m.as_str());

        match std::env::var(var_name) {
            Ok(v) if !v.is_empty() => result.push_str(&v),
            _ => {
                if let Some(default) = default {
                    result.push_str(default);
                } else {
                    let location = source_path
                        .map(|p| format!(" in {}", p.display()))
                        .unwrap_or_default();
                    return Err(DatasourceError::Interpolation(format!(
                        "environment variable ${{{var_name}}} is not set{location}. \
                         Either export it before running ez-cdc, add it to a .env file, \
                         or provide a default with ${{{var_name}:-default}}."
                    )));
                }
            }
        }
    }
    result.push_str(&value[last_end..]);
    Ok(result)
}

/// Recursively walk a `serde_json::Value` tree, interpolating all strings.
fn interpolate_recursive(
    value: serde_json::Value,
    source_path: Option<&Path>,
) -> Result<serde_json::Value, DatasourceError> {
    match value {
        serde_json::Value::String(s) => {
            let interpolated = interpolate_string(&s, source_path)?;
            Ok(serde_json::Value::String(interpolated))
        }
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k, interpolate_recursive(v, source_path)?);
            }
            Ok(serde_json::Value::Object(out))
        }
        serde_json::Value::Array(arr) => {
            let out: Result<Vec<_>, _> = arr
                .into_iter()
                .map(|v| interpolate_recursive(v, source_path))
                .collect();
            Ok(serde_json::Value::Array(out?))
        }
        other => Ok(other),
    }
}

// ── .env file loading ──────────────────────────────────────────────────────

/// Load `.env` files into the process environment.
///
/// Existing variables are NOT overwritten. Missing files are silently ignored.
pub fn load_env_files(paths: &[&Path]) {
    for path in paths {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(_) => continue,
        };

        for raw_line in text.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let key = key.trim();
            let mut value = value.trim().to_string();

            // Strip balanced surrounding quotes.
            if value.len() >= 2 {
                let first = value.as_bytes()[0];
                let last = value.as_bytes()[value.len() - 1];
                if first == last && (first == b'"' || first == b'\'') {
                    value = value[1..value.len() - 1].to_string();
                }
            }

            // setdefault semantics: only set if not already present.
            if std::env::var(key).is_err() {
                std::env::set_var(key, &value);
            }
        }
    }
}

// ── Top-level loader ───────────────────────────────────────────────────────

/// Load and validate a datasources YAML file.
///
/// With `allow_missing=true`, a non-existent file returns an empty
/// `DatasourcesFile` instead of an error.
pub fn load_datasources(
    path: &Path,
    env_files: &[&Path],
    allow_missing: bool,
) -> Result<DatasourcesFile, DatasourceError> {
    if !env_files.is_empty() {
        load_env_files(env_files);
    }

    if !path.exists() {
        if allow_missing {
            return Ok(DatasourcesFile::default());
        }
        return Err(DatasourceError::NotFound(format!(
            "datasources file not found: {}\n  \
             Run `ez-cdc datasource init` to create the bundled demo datasources, \
             or `ez-cdc datasource add` to configure your own.",
            path.display()
        )));
    }

    let raw_text = std::fs::read_to_string(path)
        .map_err(|e| DatasourceError::Io(format!("failed to read {}: {e}", path.display())))?;

    // Parse YAML into a generic JSON value for interpolation.
    let parsed: serde_json::Value = serde_yml::from_str(&raw_text)
        .map_err(|e| DatasourceError::Yaml(format!("failed to parse {} as YAML: {e}", path.display())))?;

    if !parsed.is_object() {
        return Err(DatasourceError::Yaml(format!(
            "top-level YAML in {} must be a mapping, got {}",
            path.display(),
            match &parsed {
                serde_json::Value::Array(_) => "array",
                serde_json::Value::String(_) => "string",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Null => "null",
                _ => "unknown",
            }
        )));
    }

    // Interpolate ${VAR} across the whole tree.
    let interpolated = interpolate_recursive(parsed, Some(path))?;

    // Deserialize into the typed schema.
    let file: DatasourcesFile = serde_json::from_value(interpolated)
        .map_err(|e| DatasourceError::Validation(format!(
            "datasources file {} failed schema validation: {e}",
            path.display()
        )))?;

    // Run additional validation (pg ident patterns, table names, etc.).
    file.validate()
        .map_err(|e| DatasourceError::Validation(format!(
            "datasources file {} failed validation: {e}",
            path.display()
        )))?;

    Ok(file)
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn interpolate_simple_var() {
        std::env::set_var("EZ_TEST_VAR", "hello");
        let result = interpolate_string("prefix_${EZ_TEST_VAR}_suffix", None).unwrap();
        assert_eq!(result, "prefix_hello_suffix");
        std::env::remove_var("EZ_TEST_VAR");
    }

    #[test]
    fn interpolate_default_value() {
        std::env::remove_var("EZ_MISSING_VAR");
        let result = interpolate_string("val=${EZ_MISSING_VAR:-fallback}", None).unwrap();
        assert_eq!(result, "val=fallback");
    }

    #[test]
    fn interpolate_missing_var_error() {
        std::env::remove_var("EZ_REALLY_MISSING");
        let err = interpolate_string("${EZ_REALLY_MISSING}", None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("EZ_REALLY_MISSING"));
        assert!(msg.contains("is not set"));
    }

    #[test]
    fn interpolate_escape() {
        let result = interpolate_string("$${NOT_A_VAR}", None).unwrap();
        assert_eq!(result, "${NOT_A_VAR}");
    }

    #[test]
    fn interpolate_bare_var() {
        std::env::set_var("EZ_BARE", "works");
        let result = interpolate_string("$EZ_BARE!", None).unwrap();
        assert_eq!(result, "works!");
        std::env::remove_var("EZ_BARE");
    }

    #[test]
    fn interpolate_no_dollar_passthrough() {
        let result = interpolate_string("no vars here", None).unwrap();
        assert_eq!(result, "no vars here");
    }

    #[test]
    fn load_real_yaml() {
        let yaml_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("e2e/ez-cdc.yaml");
        if yaml_path.exists() {
            let file = load_datasources(&yaml_path, &[], false).unwrap();
            assert!(file.has_any());
            assert!(file.sources.contains_key("demo-pg"));
        }
    }

    #[test]
    fn load_missing_allow() {
        let file = load_datasources(Path::new("/tmp/nonexistent-ez-cdc.yaml"), &[], true).unwrap();
        assert!(file.is_empty());
    }

    #[test]
    fn load_missing_deny() {
        let err = load_datasources(Path::new("/tmp/nonexistent-ez-cdc.yaml"), &[], false).unwrap_err();
        assert!(matches!(err, DatasourceError::NotFound(_)));
    }

    #[test]
    fn load_env_file() {
        let dir = std::env::temp_dir().join("ez-cdc-test-env");
        std::fs::create_dir_all(&dir).unwrap();
        let env_path = dir.join(".env.test");
        {
            let mut f = std::fs::File::create(&env_path).unwrap();
            writeln!(f, "EZ_ENV_TEST_KEY=from_env_file").unwrap();
            writeln!(f, "# comment line").unwrap();
            writeln!(f, "EZ_ENV_QUOTED=\"quoted_val\"").unwrap();
        }

        std::env::remove_var("EZ_ENV_TEST_KEY");
        std::env::remove_var("EZ_ENV_QUOTED");

        load_env_files(&[env_path.as_path()]);

        assert_eq!(std::env::var("EZ_ENV_TEST_KEY").unwrap(), "from_env_file");
        assert_eq!(std::env::var("EZ_ENV_QUOTED").unwrap(), "quoted_val");

        std::env::remove_var("EZ_ENV_TEST_KEY");
        std::env::remove_var("EZ_ENV_QUOTED");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

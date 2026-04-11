//! Preflight checks — connectivity and schema validation.
//!
//! Called before running the full verify suite to fast-fail on obviously
//! broken configurations (wrong host, bad credentials, schema mismatch).

use crate::config::schema::{SinkSpec, SourceSpec};
use crate::instantiate::{instantiate_backend, instantiate_source};

/// Check that source and target are reachable.
///
/// Returns `true` if both connections succeed.
pub async fn check_connectivity(
    src_spec: &SourceSpec,
    sk_spec: &SinkSpec,
    src_name: &str,
    sk_name: &str,
) -> bool {
    let mut ok = true;

    // Source
    match instantiate_source(src_spec) {
        Ok(mut source) => {
            match source.connect().await {
                Ok(()) => {
                    println!("  [ok] source '{}' ({}) reachable", src_name, source.name());
                    let _ = source.close().await;
                }
                Err(e) => {
                    eprintln!("  [FAIL] source '{}' unreachable: {}", src_name, e);
                    ok = false;
                }
            }
        }
        Err(e) => {
            eprintln!(
                "  [FAIL] cannot instantiate source '{}': {}",
                src_name, e
            );
            ok = false;
        }
    }

    // Target
    match instantiate_backend(sk_spec) {
        Ok(mut target) => {
            match target.connect().await {
                Ok(()) => {
                    println!("  [ok] target '{}' ({}) reachable", sk_name, target.name());
                    let _ = target.close().await;
                }
                Err(e) => {
                    eprintln!("  [FAIL] target '{}' unreachable: {}", sk_name, e);
                    ok = false;
                }
            }
        }
        Err(e) => {
            eprintln!(
                "  [FAIL] cannot instantiate target '{}': {}",
                sk_name, e
            );
            ok = false;
        }
    }

    ok
}

/// Check that source tables exist and target schema is accessible.
///
/// Returns `true` if schema checks pass.
pub async fn check_schema(src_spec: &SourceSpec, sk_spec: &SinkSpec) -> bool {
    let mut ok = true;

    // Source: verify at least one table has rows
    match instantiate_source(src_spec) {
        Ok(mut source) => {
            if source.connect().await.is_ok() {
                for table in src_spec.tables() {
                    match source.count_rows(table).await {
                        Ok(count) => {
                            println!("  [ok] source table '{}': {} rows", table, count);
                        }
                        Err(e) => {
                            eprintln!(
                                "  [FAIL] source table '{}' not accessible: {}",
                                table, e
                            );
                            ok = false;
                        }
                    }
                }
                let _ = source.close().await;
            } else {
                eprintln!("  [FAIL] source connect failed during schema check");
                ok = false;
            }
        }
        Err(e) => {
            eprintln!("  [FAIL] cannot instantiate source: {}", e);
            ok = false;
        }
    }

    // Target: verify list_tables works
    match instantiate_backend(sk_spec) {
        Ok(mut target) => {
            if target.connect().await.is_ok() {
                match target.list_tables().await {
                    Ok(tables) => {
                        println!(
                            "  [ok] target has {} tables: {}",
                            tables.len(),
                            tables.join(", ")
                        );
                    }
                    Err(e) => {
                        eprintln!("  [FAIL] target list_tables failed: {}", e);
                        ok = false;
                    }
                }
                let _ = target.close().await;
            } else {
                eprintln!("  [FAIL] target connect failed during schema check");
                ok = false;
            }
        }
        Err(e) => {
            eprintln!("  [FAIL] cannot instantiate target: {}", e);
            ok = false;
        }
    }

    ok
}

/// Run all preflight checks (connectivity + schema).
///
/// Returns `true` if all checks pass.
pub async fn run_preflight(
    src_spec: &SourceSpec,
    sk_spec: &SinkSpec,
    src_name: &str,
    sk_name: &str,
) -> bool {
    println!("Preflight: checking connectivity...");
    let conn_ok = check_connectivity(src_spec, sk_spec, src_name, sk_name).await;
    if !conn_ok {
        eprintln!("Preflight FAILED: connectivity checks did not pass");
        return false;
    }

    println!("Preflight: checking schema...");
    let schema_ok = check_schema(src_spec, sk_spec).await;
    if !schema_ok {
        eprintln!("Preflight FAILED: schema checks did not pass");
        return false;
    }

    println!("Preflight: all checks passed");
    true
}

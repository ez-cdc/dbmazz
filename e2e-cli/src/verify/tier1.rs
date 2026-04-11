//! Tier 1 — Correctness baseline checks.
//!
//! All checks in this module take `&mut TestContext` and return
//! `anyhow::Result<CheckResult>`. They are async, called by the runner.
//!
//! Check series:
//!   A — schema validation (tables, columns, audit columns, metadata)
//!   B — snapshot integrity (row counts, content, duplicates, post-CDC delta)
//!   C — type fidelity (NULL roundtrip)
//!   D — CDC operations (insert, update, delete, multi-row, TOAST)
//!   E — sequential updates
//!   H — idempotency

use std::collections::HashMap;
use std::time::Duration;

use crate::clients::source_pg::SqlValue;
use crate::clients::targets;
use crate::tui::report::{CheckResult, CheckStatus};
use crate::verify::polling::wait_until;

use super::runner::TestContext;

// ── Constants ───────────────────────────────────────────────────────────────

const TEST_CUSTOMER_ID: i64 = 99_999;
const STATUS_D1: &str = "e2e_d1_ins";
const STATUS_D2: &str = "e2e_d2_upd";
const STATUS_E1_MID: &str = "e2e_e1_mid";
const STATUS_E1_END: &str = "e2e_e1_end";
const STATUS_D4_INI: &str = "e2e_d4_ini";
const STATUS_D4_UPD: &str = "e2e_d4_upd";
const STATUS_C10: &str = "e2e_c10";
const TOAST_COLUMN: &str = "description";
const TOAST_COLUMN_TYPE: &str = "TEXT";
const TOAST_BIG_TEXT_MARKER: &str = "DBMAZZ_TOAST_TEST_MARKER";

/// 9 KB of 'X' — large enough to trigger PG TOAST storage.
fn toast_big_text() -> String {
    "X".repeat(9216)
}

/// Scratch key for B1 baselines.
const SCRATCH_B1_BASELINES: &str = "b1_baselines";

/// Normalize a source PK to a comparable string.
fn source_pk_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".into(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => format!("{f}"),
        SqlValue::Text(s) => s.clone(),
    }
}

/// Normalize a target PK to a comparable string.
fn target_pk_to_string(v: &targets::SqlValue) -> String {
    match v {
        targets::SqlValue::Null => "NULL".into(),
        targets::SqlValue::Int(i) => i.to_string(),
        targets::SqlValue::Float(f) => format!("{f}"),
        targets::SqlValue::Bool(b) => b.to_string(),
        targets::SqlValue::Text(s) => s.clone(),
    }
}

// ── Settle / poll helpers ───────────────────────────────────────────────────

/// Wait for sink settle time after a CDC operation.
async fn settle(ctx: &TestContext) {
    let caps = ctx.target.capabilities();
    let secs = caps.post_cdc_settle_seconds;
    if secs > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(secs)).await;
    }
}

/// Wait for snapshot settle time.
async fn settle_snapshot(ctx: &TestContext) {
    let caps = ctx.target.capabilities();
    let secs = caps.post_snapshot_settle_seconds;
    if secs > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(secs)).await;
    }
}

/// Convert a source SqlValue to a target SqlValue for comparison.
fn source_to_target_value(v: &SqlValue) -> targets::SqlValue {
    match v {
        SqlValue::Null => targets::SqlValue::Null,
        SqlValue::Int(i) => targets::SqlValue::Int(*i),
        SqlValue::Float(f) => targets::SqlValue::Float(*f),
        SqlValue::Text(s) => targets::SqlValue::Text(s.clone()),
    }
}

// ── A-series: Schema checks ─────────────────────────────────────────────────

/// A1: Verify that all expected source tables exist in the target.
pub async fn check_a1_target_tables_present(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let target_tables = ctx.target.list_tables().await?;
    let target_set: std::collections::HashSet<String> = target_tables
        .iter()
        .map(|t| t.to_lowercase())
        .collect();

    let mut missing = Vec::new();
    for table in &ctx.tables {
        if !target_set.contains(&table.to_lowercase()) {
            missing.push(table.clone());
        }
    }

    if missing.is_empty() {
        Ok(CheckResult {
            id: "A1".into(),
            description: "Target tables present".into(),
            status: CheckStatus::Pass,
            detail: format!("{}/{}", ctx.tables.len(), ctx.tables.len()),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "A1".into(),
            description: "Target tables present".into(),
            status: CheckStatus::Fail,
            detail: format!(
                "{}/{}",
                ctx.tables.len() - missing.len(),
                ctx.tables.len()
            ),
            error: Some(format!("missing tables: {}", missing.join(", "))),
            duration_ms: None,
        })
    }
}

/// A2: Verify that all source columns exist in the target (case-insensitive).
pub async fn check_a2_source_columns_in_target(
    ctx: &mut TestContext,
) -> anyhow::Result<CheckResult> {
    let mut all_missing: Vec<String> = Vec::new();
    let mut total_checked = 0_usize;

    for table in &ctx.tables.clone() {
        // Get source columns via list_columns
        let source_cols = ctx.source.list_columns(table).await?;
        let target_cols = ctx.target.get_columns(table).await?;
        let target_set: std::collections::HashSet<String> = target_cols
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect();

        for col in &source_cols {
            total_checked += 1;
            if !target_set.contains(&col.to_lowercase()) {
                all_missing.push(format!("{table}.{col}"));
            }
        }
    }

    if all_missing.is_empty() {
        Ok(CheckResult {
            id: "A2".into(),
            description: "Source columns in target".into(),
            status: CheckStatus::Pass,
            detail: format!("{total_checked} columns checked"),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "A2".into(),
            description: "Source columns in target".into(),
            status: CheckStatus::Fail,
            detail: format!("{} missing", all_missing.len()),
            error: Some(format!("missing: {}", all_missing.join(", "))),
            duration_ms: None,
        })
    }
}

/// A3: Verify that audit columns are present in target tables.
pub async fn check_a3_audit_columns_present(
    ctx: &mut TestContext,
) -> anyhow::Result<CheckResult> {
    let expected_audit = ctx.target.expected_audit_columns();
    let mut missing_list: Vec<String> = Vec::new();

    for table in &ctx.tables.clone() {
        let columns = ctx.target.get_columns(table).await?;
        let col_set: std::collections::HashSet<String> =
            columns.iter().map(|c| c.name.to_lowercase()).collect();

        for audit_col in &expected_audit {
            if !col_set.contains(&audit_col.to_lowercase()) {
                missing_list.push(format!("{table}.{audit_col}"));
            }
        }
    }

    if missing_list.is_empty() {
        Ok(CheckResult {
            id: "A3".into(),
            description: "Audit columns present".into(),
            status: CheckStatus::Pass,
            detail: format!(
                "{} audit cols x {} tables",
                expected_audit.len(),
                ctx.tables.len()
            ),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "A3".into(),
            description: "Audit columns present".into(),
            status: CheckStatus::Fail,
            detail: format!("{} missing", missing_list.len()),
            error: Some(format!("missing: {}", missing_list.join(", "))),
            duration_ms: None,
        })
    }
}

/// A4: Verify metadata table rows (if backend supports it).
pub async fn check_a4_metadata_table(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let caps = ctx.target.capabilities();
    if !caps.has_metadata_table {
        return Ok(CheckResult {
            id: "A4".into(),
            description: "Metadata table".into(),
            status: CheckStatus::Skip,
            detail: format!("{} has no metadata table", ctx.target.name()),
            error: None,
            duration_ms: None,
        });
    }

    let count = ctx.target.metadata_row_count().await?;
    if count > 0 {
        Ok(CheckResult {
            id: "A4".into(),
            description: "Metadata table".into(),
            status: CheckStatus::Pass,
            detail: format!("{count} rows"),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "A4".into(),
            description: "Metadata table".into(),
            status: CheckStatus::Fail,
            detail: "0 rows".into(),
            error: Some("metadata table is empty".into()),
            duration_ms: None,
        })
    }
}

// ── B-series: Snapshot checks ───────────────────────────────────────────────

/// B1: Compare row counts between source and target after snapshot.
/// Stores baselines in scratch for B2.
pub async fn check_b1_snapshot_counts(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    settle_snapshot(ctx).await;

    let mut baselines: HashMap<String, i64> = HashMap::new();
    let mut mismatches: Vec<String> = Vec::new();

    for table in &ctx.tables.clone() {
        let src_count = ctx.source.count_rows(table).await?;
        let tgt_count = ctx.target.count_rows(table, true).await?;
        baselines.insert(table.clone(), src_count);

        if src_count != tgt_count {
            mismatches.push(format!("{table}: src={src_count}, tgt={tgt_count}"));
        }
    }

    // Store baselines for B2
    ctx.scratch.insert(
        SCRATCH_B1_BASELINES.into(),
        Box::new(baselines.clone()),
    );

    if mismatches.is_empty() {
        let total: i64 = baselines.values().sum();
        Ok(CheckResult {
            id: "B1".into(),
            description: "Snapshot row counts".into(),
            status: CheckStatus::Pass,
            detail: format!("{total} rows across {} tables", ctx.tables.len()),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "B1".into(),
            description: "Snapshot row counts".into(),
            status: CheckStatus::Fail,
            detail: format!("{} mismatches", mismatches.len()),
            error: Some(mismatches.join("; ")),
            duration_ms: None,
        })
    }
}

/// B1b: Spot-check snapshot content — compare PK sets between source and target.
pub async fn check_b1b_snapshot_content(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let mut issues: Vec<String> = Vec::new();

    for table in &ctx.tables.clone() {
        // Get PKs from source and target. Use first column as PK heuristic.
        let src_pks = ctx.source.list_primary_keys(table, "id").await?;
        let tgt_pks = ctx.target.list_primary_keys(table, "id").await?;

        // Normalize PKs to string for comparison — source returns source_pg::SqlValue
        // while target returns targets::SqlValue. Normalize both to plain strings.
        let src_set: std::collections::HashSet<String> =
            src_pks.iter().map(|v| source_pk_to_string(v)).collect();
        let tgt_set: std::collections::HashSet<String> =
            tgt_pks.iter().map(|v| target_pk_to_string(v)).collect();

        let missing_in_target: Vec<_> = src_set.difference(&tgt_set).collect();
        let extra_in_target: Vec<_> = tgt_set.difference(&src_set).collect();

        if !missing_in_target.is_empty() {
            issues.push(format!(
                "{table}: {} rows missing in target",
                missing_in_target.len()
            ));
        }
        if !extra_in_target.is_empty() {
            issues.push(format!(
                "{table}: {} extra rows in target",
                extra_in_target.len()
            ));
        }
    }

    if issues.is_empty() {
        Ok(CheckResult {
            id: "B1b".into(),
            description: "Snapshot content spot-check".into(),
            status: CheckStatus::Pass,
            detail: "PK sets match".into(),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "B1b".into(),
            description: "Snapshot content spot-check".into(),
            status: CheckStatus::Fail,
            detail: format!("{} issues", issues.len()),
            error: Some(issues.join("; ")),
            duration_ms: None,
        })
    }
}

/// B3: Verify no duplicate PKs in target tables.
pub async fn check_b3_no_duplicates(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let mut dup_tables: Vec<String> = Vec::new();

    for table in &ctx.tables.clone() {
        let dup_count = ctx.target.count_duplicates_by_pk(table, "id").await?;
        if dup_count > 0 {
            dup_tables.push(format!("{table}: {dup_count} dups"));
        }
    }

    if dup_tables.is_empty() {
        Ok(CheckResult {
            id: "B3".into(),
            description: "No duplicate PKs".into(),
            status: CheckStatus::Pass,
            detail: format!("{} tables clean", ctx.tables.len()),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "B3".into(),
            description: "No duplicate PKs".into(),
            status: CheckStatus::Fail,
            detail: format!("{} tables with dups", dup_tables.len()),
            error: Some(dup_tables.join("; ")),
            duration_ms: None,
        })
    }
}

// ── D-series: CDC operation checks ──────────────────────────────────────────

/// Combined single-row CDC flow: D1 (INSERT), D2 (UPDATE), E1 (sequential UPDATEs), D3 (DELETE).
///
/// Uses the first table in the list. Assumes PK column is "id" and there's a "status" column.
pub async fn run_single_row_cdc_flow(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let table = ctx.tables.first().cloned().ok_or_else(|| {
        anyhow::anyhow!("no tables configured")
    })?;

    let caps = ctx.target.capabilities();
    let timeout = Duration::from_secs(45);
    let poll_interval = Duration::from_millis(500);

    // ── D1: INSERT ──────────────────────────────────────────────────────
    let mut insert_values: HashMap<String, SqlValue> = HashMap::new();
    insert_values.insert("customer_id".into(), SqlValue::Int(TEST_CUSTOMER_ID));
    insert_values.insert("total".into(), SqlValue::Float(123.45));
    insert_values.insert("status".into(), SqlValue::Text(STATUS_D1.into()));

    let pk = ctx.source.insert_row(&table, &insert_values, "id").await?;
    let pk_target = source_to_target_value(&pk);

    // Wait for row to appear in target
    {
        let table_ref = table.clone();
        let pk_ref = pk_target.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move { target.row_exists(&t, "id", &p).await }
            },
            timeout,
            poll_interval,
            "D1 INSERT visible in target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    }

    // Verify the row is live
    settle(ctx).await;
    let is_live = ctx.target.row_is_live(&table, "id", &pk_target).await?;
    if !is_live {
        return Ok(CheckResult {
            id: "CDC".into(),
            description: "Single-row CDC flow (D1/D2/E1/D3)".into(),
            status: CheckStatus::Fail,
            detail: "D1 INSERT".into(),
            error: Some("inserted row is not live in target".into()),
            duration_ms: None,
        });
    }

    // ── D2: UPDATE ──────────────────────────────────────────────────────
    let mut update_values: HashMap<String, SqlValue> = HashMap::new();
    update_values.insert("status".into(), SqlValue::Text(STATUS_D2.into()));

    ctx.source
        .update_row(&table, "id", &pk, &update_values)
        .await?;

    // Wait for updated value in target
    {
        let table_ref = table.clone();
        let pk_ref = pk_target.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move {
                    let val = target.fetch_value(&t, "id", &p, "status").await?;
                    Ok(val == Some(targets::SqlValue::Text(STATUS_D2.into())))
                }
            },
            timeout,
            poll_interval,
            "D2 UPDATE visible in target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    }

    // ── E1: Sequential UPDATEs ──────────────────────────────────────────
    let mut mid_values: HashMap<String, SqlValue> = HashMap::new();
    mid_values.insert("status".into(), SqlValue::Text(STATUS_E1_MID.into()));
    ctx.source.update_row(&table, "id", &pk, &mid_values).await?;

    let mut end_values: HashMap<String, SqlValue> = HashMap::new();
    end_values.insert("status".into(), SqlValue::Text(STATUS_E1_END.into()));
    ctx.source.update_row(&table, "id", &pk, &end_values).await?;

    // Wait for final value
    {
        let table_ref = table.clone();
        let pk_ref = pk_target.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move {
                    let val = target.fetch_value(&t, "id", &p, "status").await?;
                    Ok(val == Some(targets::SqlValue::Text(STATUS_E1_END.into())))
                }
            },
            timeout,
            poll_interval,
            "E1 sequential UPDATEs final value in target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    }

    // ── D3: DELETE ──────────────────────────────────────────────────────
    ctx.source.delete_row(&table, "id", &pk).await?;

    if caps.supports_hard_delete {
        // Wait for row to disappear
        let table_ref = table.clone();
        let pk_ref = pk_target.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move {
                    let exists = target.row_exists(&t, "id", &p).await?;
                    Ok(!exists)
                }
            },
            timeout,
            poll_interval,
            "D3 DELETE row removed from target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    } else {
        // Soft delete: wait for row to be marked as not live
        let table_ref = table.clone();
        let pk_ref = pk_target.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move {
                    let live = target.row_is_live(&t, "id", &p).await?;
                    Ok(!live)
                }
            },
            timeout,
            poll_interval,
            "D3 DELETE row soft-deleted in target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    }

    Ok(CheckResult {
        id: "CDC".into(),
        description: "Single-row CDC flow (D1/D2/E1/D3)".into(),
        status: CheckStatus::Pass,
        detail: "INSERT -> UPDATE -> seq UPDATEs -> DELETE".into(),
        error: None,
        duration_ms: None,
    })
}

/// D5: Multi-row INSERT — 10 rows in a single transaction.
pub async fn check_d5_multi_row_insert(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let table = ctx.tables.first().cloned().ok_or_else(|| {
        anyhow::anyhow!("no tables configured")
    })?;

    let timeout = Duration::from_secs(45);
    let poll_interval = Duration::from_millis(500);

    // Build 10 rows
    let rows: Vec<HashMap<String, SqlValue>> = (0..10)
        .map(|i| {
            let mut row = HashMap::new();
            row.insert("customer_id".into(), SqlValue::Int(TEST_CUSTOMER_ID));
            row.insert("total".into(), SqlValue::Float((10 + i) as f64));
            row.insert(
                "status".into(),
                SqlValue::Text(format!("e2e_d5_{i}")),
            );
            row
        })
        .collect();

    let pks = ctx.source.insert_many(&table, &rows, "id").await?;

    if pks.len() != 10 {
        return Ok(CheckResult {
            id: "D5".into(),
            description: "Multi-row INSERT (10 rows in 1 TX)".into(),
            status: CheckStatus::Fail,
            detail: format!("{} PKs returned", pks.len()),
            error: Some(format!("expected 10 PKs, got {}", pks.len())),
            duration_ms: None,
        });
    }

    // Wait for all 10 rows to appear in target
    let last_pk = source_to_target_value(pks.last().unwrap());
    {
        let table_ref = table.clone();
        let pk_ref = last_pk.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move { target.row_exists(&t, "id", &p).await }
            },
            timeout,
            poll_interval,
            "D5 last row visible in target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    }

    settle(ctx).await;

    // Verify all 10 exist
    let mut found = 0;
    for pk in &pks {
        let tpk = source_to_target_value(pk);
        if ctx.target.row_exists(&table, "id", &tpk).await? {
            found += 1;
        }
    }

    if found == 10 {
        Ok(CheckResult {
            id: "D5".into(),
            description: "Multi-row INSERT (10 rows in 1 TX)".into(),
            status: CheckStatus::Pass,
            detail: "10/10 rows".into(),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "D5".into(),
            description: "Multi-row INSERT (10 rows in 1 TX)".into(),
            status: CheckStatus::Fail,
            detail: format!("{found}/10 rows"),
            error: Some(format!("only {found} of 10 rows found in target")),
            duration_ms: None,
        })
    }
}

/// D4: TOAST column survives unrelated UPDATE.
///
/// CRITICAL test: PG TOAST storage means that a large text column might
/// not be included in the WAL event if it was not changed. dbmazz must
/// handle this correctly (preserve the previous value).
///
/// Steps:
///   1. Ensure TOAST column exists on source table
///   2. INSERT row with 9KB text in TOAST column
///   3. Wait for row in target, verify big text arrived
///   4. UPDATE an unrelated column (status)
///   5. Wait for status update in target
///   6. Verify TOAST column still has the 9KB text
pub async fn check_d4_toast_update(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let table = ctx.tables.first().cloned().ok_or_else(|| {
        anyhow::anyhow!("no tables configured")
    })?;

    let timeout = Duration::from_secs(45);
    let poll_interval = Duration::from_millis(500);
    let big_text = toast_big_text();

    // Ensure TOAST column exists
    ctx.source
        .ensure_column(&table, TOAST_COLUMN, TOAST_COLUMN_TYPE)
        .await?;

    // INSERT with big text
    let mut values: HashMap<String, SqlValue> = HashMap::new();
    values.insert("customer_id".into(), SqlValue::Int(TEST_CUSTOMER_ID));
    values.insert("total".into(), SqlValue::Float(777.77));
    values.insert("status".into(), SqlValue::Text(STATUS_D4_INI.into()));
    values.insert(
        TOAST_COLUMN.into(),
        SqlValue::Text(format!("{TOAST_BIG_TEXT_MARKER}{big_text}")),
    );

    let pk = ctx.source.insert_row(&table, &values, "id").await?;
    let pk_target = source_to_target_value(&pk);

    // Wait for row in target
    {
        let table_ref = table.clone();
        let pk_ref = pk_target.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move { target.row_exists(&t, "id", &p).await }
            },
            timeout,
            poll_interval,
            "D4 INSERT visible in target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    }

    settle(ctx).await;

    // Verify big text arrived
    let val = ctx
        .target
        .fetch_value(&table, "id", &pk_target, TOAST_COLUMN)
        .await?;
    match &val {
        Some(targets::SqlValue::Text(s)) if s.contains(TOAST_BIG_TEXT_MARKER) => {}
        _ => {
            return Ok(CheckResult {
                id: "D4".into(),
                description: "TOAST column survives unrelated UPDATE".into(),
                status: CheckStatus::Fail,
                detail: "initial INSERT".into(),
                error: Some("TOAST text not found in target after INSERT".into()),
                duration_ms: None,
            });
        }
    }

    // UPDATE unrelated column (status), NOT the TOAST column
    let mut update_values: HashMap<String, SqlValue> = HashMap::new();
    update_values.insert("status".into(), SqlValue::Text(STATUS_D4_UPD.into()));
    ctx.source
        .update_row(&table, "id", &pk, &update_values)
        .await?;

    // Wait for status update in target
    {
        let table_ref = table.clone();
        let pk_ref = pk_target.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move {
                    let val = target.fetch_value(&t, "id", &p, "status").await?;
                    Ok(val == Some(targets::SqlValue::Text(STATUS_D4_UPD.into())))
                }
            },
            timeout,
            poll_interval,
            "D4 status UPDATE visible in target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    }

    settle(ctx).await;

    // Verify TOAST column still has the big text
    let val_after = ctx
        .target
        .fetch_value(&table, "id", &pk_target, TOAST_COLUMN)
        .await?;
    match &val_after {
        Some(targets::SqlValue::Text(s)) if s.contains(TOAST_BIG_TEXT_MARKER) => Ok(CheckResult {
            id: "D4".into(),
            description: "TOAST column survives unrelated UPDATE".into(),
            status: CheckStatus::Pass,
            detail: format!("{}B preserved", big_text.len() + TOAST_BIG_TEXT_MARKER.len()),
            error: None,
            duration_ms: None,
        }),
        _ => Ok(CheckResult {
            id: "D4".into(),
            description: "TOAST column survives unrelated UPDATE".into(),
            status: CheckStatus::Fail,
            detail: "after unrelated UPDATE".into(),
            error: Some(
                "TOAST column value lost or corrupted after unrelated UPDATE".into(),
            ),
            duration_ms: None,
        }),
    }
}

// ── C-series: Type fidelity ─────────────────────────────────────────────────

/// C10: NULL roundtrip — verify NULL survives source -> target.
pub async fn check_c10_null_roundtrip(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let table = ctx.tables.first().cloned().ok_or_else(|| {
        anyhow::anyhow!("no tables configured")
    })?;

    let timeout = Duration::from_secs(45);
    let poll_interval = Duration::from_millis(500);

    // INSERT with NULL in the description column (TOAST-able)
    let mut values: HashMap<String, SqlValue> = HashMap::new();
    values.insert("customer_id".into(), SqlValue::Int(TEST_CUSTOMER_ID));
    values.insert("total".into(), SqlValue::Float(0.01));
    values.insert("status".into(), SqlValue::Text(STATUS_C10.into()));
    values.insert(TOAST_COLUMN.into(), SqlValue::Null);

    let pk = ctx.source.insert_row(&table, &values, "id").await?;
    let pk_target = source_to_target_value(&pk);

    // Wait for row in target
    {
        let table_ref = table.clone();
        let pk_ref = pk_target.clone();
        wait_until(
            || {
                let t = table_ref.clone();
                let p = pk_ref.clone();
                let target = &ctx.target;
                async move { target.row_exists(&t, "id", &p).await }
            },
            timeout,
            poll_interval,
            "C10 INSERT with NULL visible in target",
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    }

    settle(ctx).await;

    // Verify NULL in target (the TOAST column was inserted as NULL)
    let val = ctx
        .target
        .fetch_value(&table, "id", &pk_target, TOAST_COLUMN)
        .await?;

    match val {
        Some(targets::SqlValue::Null) | None => Ok(CheckResult {
            id: "C10".into(),
            description: "NULL roundtrip".into(),
            status: CheckStatus::Pass,
            detail: "NULL preserved".into(),
            error: None,
            duration_ms: None,
        }),
        Some(other) => Ok(CheckResult {
            id: "C10".into(),
            description: "NULL roundtrip".into(),
            status: CheckStatus::Fail,
            detail: format!("got {other:?}"),
            error: Some(format!(
                "expected NULL in target, got {other:?}"
            )),
            duration_ms: None,
        }),
    }
}

// ── B2: Post-CDC delta ──────────────────────────────────────────────────────

/// B2: After CDC operations, verify that row counts changed by the expected delta.
///
/// Uses baselines stored in scratch by B1. The delta accounts for the
/// rows inserted/deleted by other tier1 checks.
pub async fn check_b2_post_cdc_delta(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    let baselines = ctx
        .scratch
        .get(SCRATCH_B1_BASELINES)
        .and_then(|v| v.downcast_ref::<HashMap<String, i64>>());

    let baselines = match baselines {
        Some(b) => b.clone(),
        None => {
            return Ok(CheckResult {
                id: "B2".into(),
                description: "Post-CDC delta (counts match baselines)".into(),
                status: CheckStatus::Skip,
                detail: "B1 baselines not available".into(),
                error: None,
                duration_ms: None,
            });
        }
    };

    settle(ctx).await;

    let mut issues: Vec<String> = Vec::new();

    for table in &ctx.tables.clone() {
        let src_count = ctx.source.count_rows(table).await?;
        let tgt_count = ctx.target.count_rows(table, true).await?;

        if src_count != tgt_count {
            let baseline = baselines.get(table).copied().unwrap_or(0);
            issues.push(format!(
                "{table}: baseline={baseline}, src_now={src_count}, tgt_now={tgt_count}"
            ));
        }
    }

    if issues.is_empty() {
        Ok(CheckResult {
            id: "B2".into(),
            description: "Post-CDC delta (counts match baselines)".into(),
            status: CheckStatus::Pass,
            detail: "source and target counts match".into(),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "B2".into(),
            description: "Post-CDC delta (counts match baselines)".into(),
            status: CheckStatus::Fail,
            detail: format!("{} tables diverged", issues.len()),
            error: Some(issues.join("; ")),
            duration_ms: None,
        })
    }
}

// ── H-series: Idempotency ───────────────────────────────────────────────────

/// H1: No-op idempotent — with no traffic, target counts should not drift.
pub async fn check_h1_no_op_idempotent(ctx: &mut TestContext) -> anyhow::Result<CheckResult> {
    // Take a snapshot of counts
    let mut before: HashMap<String, i64> = HashMap::new();
    for table in &ctx.tables.clone() {
        let count = ctx.target.count_rows(table, true).await?;
        before.insert(table.clone(), count);
    }

    // Wait 5 seconds with no traffic
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check counts again
    let mut drifts: Vec<String> = Vec::new();
    for table in &ctx.tables.clone() {
        let count = ctx.target.count_rows(table, true).await?;
        let prev = before.get(table).copied().unwrap_or(0);
        if count != prev {
            drifts.push(format!("{table}: {prev} -> {count}"));
        }
    }

    if drifts.is_empty() {
        Ok(CheckResult {
            id: "H1".into(),
            description: "No-op idempotent (no drift)".into(),
            status: CheckStatus::Pass,
            detail: "stable".into(),
            error: None,
            duration_ms: None,
        })
    } else {
        Ok(CheckResult {
            id: "H1".into(),
            description: "No-op idempotent (no drift)".into(),
            status: CheckStatus::Fail,
            detail: format!("{} tables drifted", drifts.len()),
            error: Some(drifts.join("; ")),
            duration_ms: None,
        })
    }
}

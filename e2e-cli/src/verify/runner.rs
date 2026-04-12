//! Verify runner — orchestrates tier execution.
//!
//! Builds a `TestContext` from datasource specs, runs precheck (health,
//! connectivity, wait for CDC stage), then executes tier1 checks in order.
//! Each check produces a `CheckResult`. The runner collects them into a
//! `VerifyReport`.

use std::collections::{HashMap, HashSet};
use std::any::Any;
use std::time::{Duration, Instant};

use crate::clients::dbmazz::DbmazzClient;
use crate::clients::source_pg::SourceClient;
use crate::clients::targets::TargetBackend;
use crate::config::schema::{SinkSpec, SourceSpec};
use crate::instantiate::{instantiate_backend, instantiate_source};
use crate::tui::report::*;

use super::tier1;

// ── Default timeouts ────────────────────────────────────────────────────────

const DBMAZZ_BASE_URL: &str = "http://localhost:8080";
const DBMAZZ_HTTP_TIMEOUT_SECS: u64 = 5;
const CDC_STAGE_TIMEOUT_SECS: u64 = 120;
const CDC_STAGE_POLL_INTERVAL_MS: u64 = 2000;

// ── TestContext ─────────────────────────────────────────────────────────────

/// Shared context passed to every check function.
///
/// Owns the connected clients and provides a scratch space for checks
/// to stash intermediate data (e.g. snapshot baselines for B2).
pub struct TestContext {
    pub source: Box<dyn SourceClient>,
    pub target: Box<dyn TargetBackend>,
    #[allow(dead_code)]
    pub dbmazz: DbmazzClient,
    pub tables: Vec<String>,
    #[allow(dead_code)]
    pub source_name: String,
    #[allow(dead_code)]
    pub sink_name: String,
    /// Scratch storage for inter-check data.
    /// Use `Box<dyn Any + Send + Sync>` and downcast when reading.
    pub scratch: HashMap<String, Box<dyn Any + Send + Sync>>,
}

// ── VerifyRunner ────────────────────────────────────────────────────────────

/// Orchestrates verification tiers for one source x sink pair.
pub struct VerifyRunner {
    src_name: String,
    src_spec: SourceSpec,
    sk_name: String,
    sk_spec: SinkSpec,
    quick: bool,
    skip_ids: HashSet<String>,
    /// Whether the daemon was configured to run the initial snapshot.
    /// When false, the snapshot-dependent checks (B1, B1b, B2) are
    /// auto-skipped with a clear reason instead of failing.
    do_snapshot: bool,
}

/// Tier 1 check IDs that compare source and target row counts and
/// therefore require the initial snapshot to have run. When the daemon
/// was started with `do_snapshot: false`, these checks are not
/// meaningful and are reported as SKIP with an explanation.
const SNAPSHOT_DEPENDENT_CHECKS: &[&str] = &["B1", "B1b", "B2"];

impl VerifyRunner {
    /// Create a new runner.
    ///
    /// `skip_ids` contains check IDs like "C3", "D7" to skip explicitly.
    /// `do_snapshot` should mirror `PipelineSettings.do_snapshot` from
    /// the config file — it determines whether snapshot-dependent
    /// checks are executed or auto-skipped.
    pub fn new(
        src_name: String,
        src_spec: SourceSpec,
        sk_name: String,
        sk_spec: SinkSpec,
        quick: bool,
        skip_ids: HashSet<String>,
        do_snapshot: bool,
    ) -> Self {
        Self {
            src_name,
            src_spec,
            sk_name,
            sk_spec,
            quick,
            skip_ids,
            do_snapshot,
        }
    }

    /// Execute the full verification suite.
    ///
    /// 1. Build TestContext (instantiate source, target, dbmazz)
    /// 2. Run precheck (health, source reachable, target reachable)
    /// 3. Wait for dbmazz to reach "cdc" stage
    /// 4. Run tier1 checks
    /// 5. Close connections
    /// 6. Return VerifyReport
    pub async fn run(&mut self) -> VerifyReport {
        let overall_start = Instant::now();
        let profile = format!("{} -> {}", self.src_name, self.sk_name);
        let mut tiers: Vec<TierResult> = Vec::new();

        // ── Build context ───────────────────────────────────────────────
        let mut source = match instantiate_source(&self.src_spec) {
            Ok(s) => s,
            Err(e) => {
                return self.abort_report(
                    &profile,
                    overall_start,
                    &format!("failed to instantiate source: {e}"),
                );
            }
        };

        let mut target = match instantiate_backend(&self.sk_spec) {
            Ok(t) => t,
            Err(e) => {
                return self.abort_report(
                    &profile,
                    overall_start,
                    &format!("failed to instantiate target: {e}"),
                );
            }
        };

        // ── Precheck tier ───────────────────────────────────────────────
        let mut precheck_tier = TierResult {
            name: "Precheck -- connectivity & readiness".into(),
            checks: Vec::new(),
        };

        // Health check
        let dbmazz = DbmazzClient::new(
            DBMAZZ_BASE_URL,
            Duration::from_secs(DBMAZZ_HTTP_TIMEOUT_SECS),
        );
        let health_result = self.run_precheck_health(&dbmazz).await;
        print_check(&health_result);
        let health_ok = health_result.status == CheckStatus::Pass;
        precheck_tier.checks.push(health_result);

        if !health_ok {
            tiers.push(precheck_tier);
            return VerifyReport {
                profile,
                tiers,
                elapsed_seconds: overall_start.elapsed().as_secs_f64(),
            };
        }

        // Source connectivity
        let src_result = self.run_precheck_source(&mut source).await;
        print_check(&src_result);
        let src_ok = src_result.status == CheckStatus::Pass;
        precheck_tier.checks.push(src_result);

        if !src_ok {
            tiers.push(precheck_tier);
            return VerifyReport {
                profile,
                tiers,
                elapsed_seconds: overall_start.elapsed().as_secs_f64(),
            };
        }

        // Target connectivity
        let tgt_result = self.run_precheck_target(&mut target).await;
        print_check(&tgt_result);
        let tgt_ok = tgt_result.status == CheckStatus::Pass;
        precheck_tier.checks.push(tgt_result);

        if !tgt_ok {
            tiers.push(precheck_tier);
            return VerifyReport {
                profile,
                tiers,
                elapsed_seconds: overall_start.elapsed().as_secs_f64(),
            };
        }

        // Wait for CDC stage
        let cdc_result = self.run_precheck_cdc_stage(&dbmazz).await;
        print_check(&cdc_result);
        let cdc_ok = cdc_result.status == CheckStatus::Pass;
        precheck_tier.checks.push(cdc_result);
        tiers.push(precheck_tier);

        if !cdc_ok {
            return VerifyReport {
                profile,
                tiers,
                elapsed_seconds: overall_start.elapsed().as_secs_f64(),
            };
        }

        // ── Build full context ──────────────────────────────────────────
        let tables = self.src_spec.tables().to_vec();
        let mut ctx = TestContext {
            source,
            target,
            dbmazz,
            tables,
            source_name: self.src_name.clone(),
            sink_name: self.sk_name.clone(),
            scratch: HashMap::new(),
        };

        // ── Tier 1: Correctness baseline ────────────────────────────────
        print_tier_header("Tier 1 -- Correctness baseline");

        let tier1_checks = self.build_tier1_checks();
        let mut tier1_result = TierResult {
            name: "Tier 1 -- Correctness baseline".into(),
            checks: Vec::new(),
        };

        for (id, desc, check_fn) in &tier1_checks {
            let result = self.run_check(id, desc, check_fn, &mut ctx).await;
            print_check(&result);
            tier1_result.checks.push(result);
        }

        tiers.push(tier1_result);

        // ── Cleanup ─────────────────────────────────────────────────────
        let _ = ctx.source.close().await;
        let _ = ctx.target.close().await;

        VerifyReport {
            profile,
            tiers,
            elapsed_seconds: overall_start.elapsed().as_secs_f64(),
        }
    }

    // ── Precheck helpers ────────────────────────────────────────────────

    async fn run_precheck_health(&self, dbmazz: &DbmazzClient) -> CheckResult {
        let start = Instant::now();
        match dbmazz
            .wait_healthy(
                Duration::from_secs(30),
                Duration::from_millis(1000),
            )
            .await
        {
            Ok(()) => CheckResult {
                id: "P1".into(),
                description: "dbmazz daemon healthy".into(),
                status: CheckStatus::Pass,
                detail: String::new(),
                error: None,
                duration_ms: Some(start.elapsed().as_millis() as u64),
            },
            Err(e) => CheckResult {
                id: "P1".into(),
                description: "dbmazz daemon healthy".into(),
                status: CheckStatus::Fail,
                detail: String::new(),
                error: Some(e.to_string()),
                duration_ms: Some(start.elapsed().as_millis() as u64),
            },
        }
    }

    async fn run_precheck_source(&self, source: &mut Box<dyn SourceClient>) -> CheckResult {
        let start = Instant::now();
        match source.connect().await {
            Ok(()) => CheckResult {
                id: "P2".into(),
                description: format!("source '{}' reachable", self.src_name),
                status: CheckStatus::Pass,
                detail: source.name().to_string(),
                error: None,
                duration_ms: Some(start.elapsed().as_millis() as u64),
            },
            Err(e) => CheckResult {
                id: "P2".into(),
                description: format!("source '{}' reachable", self.src_name),
                status: CheckStatus::Fail,
                detail: String::new(),
                error: Some(e.to_string()),
                duration_ms: Some(start.elapsed().as_millis() as u64),
            },
        }
    }

    async fn run_precheck_target(&self, target: &mut Box<dyn TargetBackend>) -> CheckResult {
        let start = Instant::now();
        match target.connect().await {
            Ok(()) => CheckResult {
                id: "P3".into(),
                description: format!("target '{}' reachable", self.sk_name),
                status: CheckStatus::Pass,
                detail: target.name().to_string(),
                error: None,
                duration_ms: Some(start.elapsed().as_millis() as u64),
            },
            Err(e) => CheckResult {
                id: "P3".into(),
                description: format!("target '{}' reachable", self.sk_name),
                status: CheckStatus::Fail,
                detail: String::new(),
                error: Some(e.to_string()),
                duration_ms: Some(start.elapsed().as_millis() as u64),
            },
        }
    }

    async fn run_precheck_cdc_stage(&self, dbmazz: &DbmazzClient) -> CheckResult {
        let start = Instant::now();
        match dbmazz
            .wait_for_stage(
                "cdc",
                Duration::from_secs(CDC_STAGE_TIMEOUT_SECS),
                Duration::from_millis(CDC_STAGE_POLL_INTERVAL_MS),
            )
            .await
        {
            Ok(_status) => CheckResult {
                id: "P4".into(),
                description: "dbmazz reached CDC stage".into(),
                status: CheckStatus::Pass,
                detail: String::new(),
                error: None,
                duration_ms: Some(start.elapsed().as_millis() as u64),
            },
            Err(e) => CheckResult {
                id: "P4".into(),
                description: "dbmazz reached CDC stage".into(),
                status: CheckStatus::Fail,
                detail: String::new(),
                error: Some(e.to_string()),
                duration_ms: Some(start.elapsed().as_millis() as u64),
            },
        }
    }

    // ── Check execution ─────────────────────────────────────────────────

    /// Run a single check. Catches panics/errors and converts to FAIL.
    /// Handles --skip by returning SKIP status, and auto-skips
    /// snapshot-dependent checks (B1, B1b, B2) when the daemon is
    /// running with `do_snapshot: false`.
    async fn run_check(
        &self,
        id: &str,
        description: &str,
        check_fn: &CheckFn,
        ctx: &mut TestContext,
    ) -> CheckResult {
        // Handle --skip (explicit user request)
        if self.skip_ids.contains(id) {
            return CheckResult {
                id: id.into(),
                description: description.into(),
                status: CheckStatus::Skip,
                detail: "skipped by --skip".into(),
                error: None,
                duration_ms: None,
            };
        }

        // Auto-skip snapshot-dependent checks when the daemon did not
        // run the initial snapshot. Running them produces nonsensical
        // failures (target = 0 rows, baselines missing) and obscures
        // real regressions.
        if !self.do_snapshot && SNAPSHOT_DEPENDENT_CHECKS.contains(&id) {
            return CheckResult {
                id: id.into(),
                description: description.into(),
                status: CheckStatus::Skip,
                detail: "do_snapshot=false in config".into(),
                error: None,
                duration_ms: None,
            };
        }

        let start = Instant::now();
        let result = check_fn(ctx).await;
        let elapsed = start.elapsed().as_millis() as u64;

        match result {
            Ok(mut check) => {
                check.duration_ms = Some(elapsed);
                check
            }
            Err(e) => CheckResult {
                id: id.into(),
                description: description.into(),
                status: CheckStatus::Fail,
                detail: String::new(),
                error: Some(format!("check panicked or errored: {e:#}")),
                duration_ms: Some(elapsed),
            },
        }
    }

    // ── Tier1 check list ────────────────────────────────────────────────

    fn build_tier1_checks(&self) -> Vec<(&'static str, &'static str, CheckFn)> {
        let mut checks: Vec<(&str, &str, CheckFn)> = vec![
            ("A1", "Target tables present", Box::new(|ctx| Box::pin(tier1::check_a1_target_tables_present(ctx)))),
            ("A2", "Source columns in target", Box::new(|ctx| Box::pin(tier1::check_a2_source_columns_in_target(ctx)))),
            ("A3", "Audit columns present", Box::new(|ctx| Box::pin(tier1::check_a3_audit_columns_present(ctx)))),
            ("A4", "Metadata table", Box::new(|ctx| Box::pin(tier1::check_a4_metadata_table(ctx)))),
            ("B1", "Snapshot row counts", Box::new(|ctx| Box::pin(tier1::check_b1_snapshot_counts(ctx)))),
            ("B1b", "Snapshot content spot-check", Box::new(|ctx| Box::pin(tier1::check_b1b_snapshot_content(ctx)))),
            ("B3", "No duplicate PKs", Box::new(|ctx| Box::pin(tier1::check_b3_no_duplicates(ctx)))),
            ("CDC", "Single-row CDC flow (D1/D2/E1/D3)", Box::new(|ctx| Box::pin(tier1::run_single_row_cdc_flow(ctx)))),
            ("D5", "Multi-row INSERT (10 rows in 1 TX)", Box::new(|ctx| Box::pin(tier1::check_d5_multi_row_insert(ctx)))),
            ("D4", "TOAST column survives unrelated UPDATE", Box::new(|ctx| Box::pin(tier1::check_d4_toast_update(ctx)))),
            ("C10", "NULL roundtrip", Box::new(|ctx| Box::pin(tier1::check_c10_null_roundtrip(ctx)))),
            ("B2", "Post-CDC delta (counts match baselines)", Box::new(|ctx| Box::pin(tier1::check_b2_post_cdc_delta(ctx)))),
            ("H1", "No-op idempotent (no drift)", Box::new(|ctx| Box::pin(tier1::check_h1_no_op_idempotent(ctx)))),
        ];

        if self.quick {
            // In quick mode, skip slow checks
            checks.retain(|(id, _, _)| !["D4", "H1"].contains(id));
        }

        checks
    }

    /// Build an abort report with a single FAIL check.
    fn abort_report(&self, profile: &str, start: Instant, message: &str) -> VerifyReport {
        VerifyReport {
            profile: profile.to_string(),
            tiers: vec![TierResult {
                name: "Setup".into(),
                checks: vec![CheckResult {
                    id: "SETUP".into(),
                    description: "Initialize verify context".into(),
                    status: CheckStatus::Fail,
                    detail: String::new(),
                    error: Some(message.to_string()),
                    duration_ms: Some(start.elapsed().as_millis() as u64),
                }],
            }],
            elapsed_seconds: start.elapsed().as_secs_f64(),
        }
    }
}

// ── Check function type ─────────────────────────────────────────────────────

/// Type alias for an async check function.
///
/// Each check receives `&mut TestContext` and returns a `CheckResult`.
/// The outer `Result` catches unexpected panics; the inner `CheckResult`
/// carries PASS/FAIL/SKIP status.
type CheckFn = Box<
    dyn Fn(
            &mut TestContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<CheckResult>> + '_>,
        > + Send
        + Sync,
>;

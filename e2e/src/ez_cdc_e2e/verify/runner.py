"""Verify runner — orchestrates tier execution and result collection.

Takes a TestContext, runs the requested tiers in order, and returns a
VerifyReport. Handles --skip filtering, streams results to the console
as they happen (for live feedback), and collects everything for the
final summary + optional JSON export.

In PR 1 only Tier 1 is implemented. Tier 2 is added in PR 2.
"""

from __future__ import annotations

import time
from typing import Optional

from rich.console import Console
from rich.text import Text

from ..backends.base import TargetBackend
from ..dbmazz import DbmazzClient, DbmazzError
from ..profiles import ProfileSpec, load_backend_class
from ..source.base import SourceClient
from ..source.postgres import PostgresSource
from ..tui.report import (
    CheckResult,
    CheckStatus,
    TierResult,
    VerifyReport,
    format_check,
    format_step,
    format_step_ok,
    format_tier_header,
    format_totals,
)
from . import tier1
from .common import PrecheckError, TestContext, precheck


class VerifyRunner:
    """Orchestrates a verify run for a single profile."""

    def __init__(
        self,
        profile: ProfileSpec,
        console: Console,
        quick: bool = False,
        skip_ids: Optional[set[str]] = None,
    ) -> None:
        self.profile = profile
        self.console = console
        self.quick = quick
        self.skip_ids = skip_ids or set()

    def run(self) -> VerifyReport:
        """Execute the verify run and return the final report.

        Prints progress to self.console as it runs. The caller is responsible
        for handling the return value (printing summary, writing JSON, setting
        exit code).
        """
        report = VerifyReport(profile=self.profile.name)
        start = time.time()

        # Build the context (connects source, target, dbmazz).
        try:
            ctx = self._build_context()
        except Exception as e:
            self.console.print(Text(f"Failed to initialize test context: {e}", style="error"))
            report.elapsed_seconds = time.time() - start
            return report

        try:
            # Precheck before running any validation.
            self.console.print(format_step("Running prechecks..."))
            try:
                precheck(ctx)
            except PrecheckError as e:
                self.console.print(Text(f"Precheck failed: {e}", style="error"))
                report.elapsed_seconds = time.time() - start
                return report

            # Wait for dbmazz to reach CDC stage before starting tier checks.
            self.console.print(format_step("Waiting for dbmazz to reach CDC stage..."))
            t0 = time.time()
            try:
                ctx.dbmazz.wait_for_stage("cdc", timeout=180.0)
            except DbmazzError as e:
                self.console.print(Text(f"dbmazz did not reach CDC stage: {e}", style="error"))
                report.elapsed_seconds = time.time() - start
                return report
            self.console.print(format_step_ok(
                "Waiting for dbmazz to reach CDC stage",
                duration=f"{time.time() - t0:.0f}s",
            ))

            # Tier 1 — always runs (it's the baseline).
            tier1_result = self._run_tier1(ctx)
            report.tiers.append(tier1_result)

            # Tier 2 is added in PR 2. For now, nothing else runs.

        finally:
            self._close_context(ctx)

        report.elapsed_seconds = time.time() - start
        return report

    # ── tier execution ───────────────────────────────────────────────────────

    def _run_tier1(self, ctx: TestContext) -> TierResult:
        tier = TierResult(name="Tier 1 — Correctness baseline")
        self.console.print(format_tier_header(tier.name))

        # Schema (read-only)
        self._run_check(tier, ctx, tier1.check_a1_target_tables_present, "A1")
        self._run_check(tier, ctx, tier1.check_a2_source_columns_in_target, "A2")
        self._run_check(tier, ctx, tier1.check_a3_audit_columns_present, "A3")
        self._run_check(tier, ctx, tier1.check_a4_metadata_table, "A4")

        # Snapshot baseline
        self._run_check(tier, ctx, tier1.check_b1_snapshot_counts, "B1")
        self._run_check(tier, ctx, tier1.check_b3_no_duplicates, "B3")

        # Single-row CDC flow (D1 → D2 → E1 → D3). This phase is a group of
        # checks that share state internally. If D1, D2, E1, or D3 is in
        # skip_ids, the whole phase still runs but skipped checks are omitted
        # from the report.
        phase_results = tier1.run_single_row_cdc_flow(ctx)
        for r in phase_results:
            if r.id in self.skip_ids:
                self._append_skipped(tier, r.id, r.description)
            else:
                self._append_result(tier, r)

        # Multi-row CDC
        self._run_check(tier, ctx, tier1.check_d5_multi_row_insert, "D5")

        # TOAST — critical
        self._run_check(tier, ctx, tier1.check_d4_toast_update, "D4")

        # NULL roundtrip
        self._run_check(tier, ctx, tier1.check_c10_null_roundtrip, "C10")

        # Post-CDC delta (B2) — runs after all mutations
        self._run_check(tier, ctx, tier1.check_b2_post_cdc_delta, "B2")

        # Idempotency (H1)
        self._run_check(tier, ctx, tier1.check_h1_no_op_idempotent, "H1")

        return tier

    def _run_check(self, tier: TierResult, ctx: TestContext, check_fn, check_id: str) -> None:
        """Run a single check function and append its result to the tier.

        Skipped if the check id is in self.skip_ids. Any exception becomes a
        FAIL result so the run continues.
        """
        if check_id in self.skip_ids:
            self._append_skipped(tier, check_id, f"(skipped via --skip)")
            return

        try:
            result = check_fn(ctx)
        except Exception as e:
            result = CheckResult(
                id=check_id,
                description=f"{check_id} check",
                status=CheckStatus.FAIL,
                error=f"uncaught exception: {e}",
            )
        self._append_result(tier, result)

    def _append_result(self, tier: TierResult, result: CheckResult) -> None:
        tier.checks.append(result)
        self.console.print(format_check(result))

    def _append_skipped(self, tier: TierResult, check_id: str, reason: str) -> None:
        result = CheckResult(
            id=check_id,
            description=reason,
            status=CheckStatus.SKIP,
            detail="--skip",
        )
        tier.checks.append(result)
        self.console.print(format_check(result))

    # ── context lifecycle ────────────────────────────────────────────────────

    def _build_context(self) -> TestContext:
        """Instantiate source, target, and dbmazz clients from the profile."""
        source: SourceClient = PostgresSource(self.profile.source_dsn)
        source.connect()

        backend_cls = load_backend_class(self.profile)
        target: TargetBackend = self._instantiate_backend(backend_cls)
        target.connect()

        dbmazz = DbmazzClient(self.profile.dbmazz_http_url)

        return TestContext(
            profile=self.profile,
            source=source,
            target=target,
            dbmazz=dbmazz,
            console=self.console,
        )

    def _instantiate_backend(self, backend_cls) -> TargetBackend:
        """Build a target backend from profile + env.

        Each backend has different constructor args. This is the place where
        profile-specific wiring happens.
        """
        name = self.profile.name

        if name == "pg-target":
            return backend_cls(
                dsn="postgres://postgres:postgres@localhost:25432/dbmazz_target",
                schema="public",
            )

        if name == "starrocks":
            return backend_cls(
                host="localhost",
                port=9030,
                user="root",
                password="",
                database="dbmazz",
            )

        if name == "snowflake":
            return backend_cls(env_file=self.profile.requires_env_file)

        raise ValueError(f"unknown profile for backend instantiation: {name}")

    def _close_context(self, ctx: TestContext) -> None:
        try:
            ctx.source.close()
        except Exception:
            pass
        try:
            ctx.target.close()
        except Exception:
            pass
        try:
            ctx.dbmazz.close()
        except Exception:
            pass


def print_report_summary(console: Console, report: VerifyReport) -> None:
    """Print the final totals line."""
    console.print(format_totals(report))

"""Verify runner — orchestrates tier execution and result collection.

Takes a (source_spec, sink_spec) pair, builds a TestContext from them,
runs the requested tiers in order, and returns a VerifyReport. Handles
--skip filtering, streams results to the console as they happen, and
collects everything for the final summary + optional JSON export.

PR 4: the runner is now constructed via VerifyRunner.from_specs(...) which
takes datasource specs instead of a ProfileSpec. The original __init__
that took a ProfileSpec is gone — profiles.py was removed in chunk 9.

In PR 1 only Tier 1 is implemented. Tier 2 is added in PR 2.
"""

from __future__ import annotations

import time
from typing import Optional

from rich.console import Console
from rich.text import Text

from ..datasources.schema import SinkSpec, SourceSpec
from ..dbmazz import DbmazzClient, DbmazzError
from ..instantiate import (
    instantiate_backend_from_spec,
    instantiate_source_from_spec,
)
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
    """Orchestrates a verify run for a single (source, sink) pair.

    Constructed via the `from_specs` classmethod which takes datasource
    specs and the names. The runner builds the TestContext (source +
    target + dbmazz clients) lazily when run() is called, so construction
    is cheap and never raises on missing services.
    """

    def __init__(
        self,
        *,
        src_name: str,
        src_spec: SourceSpec,
        sk_name: str,
        sk_spec: SinkSpec,
        console: Console,
        quick: bool = False,
        skip_ids: Optional[set[str]] = None,
    ) -> None:
        self.src_name = src_name
        self.src_spec = src_spec
        self.sk_name = sk_name
        self.sk_spec = sk_spec
        self.console = console
        self.quick = quick
        self.skip_ids = skip_ids or set()

    @classmethod
    def from_specs(
        cls,
        *,
        src_name: str,
        src_spec: SourceSpec,
        sk_name: str,
        sk_spec: SinkSpec,
        console: Console,
        quick: bool = False,
        skip_ids: Optional[set[str]] = None,
    ) -> "VerifyRunner":
        """Build a runner from datasource specs (PR 4 entry point)."""
        return cls(
            src_name=src_name,
            src_spec=src_spec,
            sk_name=sk_name,
            sk_spec=sk_spec,
            console=console,
            quick=quick,
            skip_ids=skip_ids,
        )

    def run(self) -> VerifyReport:
        """Execute the verify run and return the final report.

        Prints progress to self.console as it runs. The caller is responsible
        for handling the return value (printing summary, writing JSON, setting
        exit code).
        """
        report_name = f"{self.src_name}-to-{self.sk_name}"
        report = VerifyReport(profile=report_name)
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

        # Schema (pre-snapshot — tables and source columns exist)
        self._run_check(tier, ctx, tier1.check_a1_target_tables_present, "A1")
        self._run_check(tier, ctx, tier1.check_a2_source_columns_in_target, "A2")

        # Snapshot baseline (B1 includes the settle wait)
        self._run_check(tier, ctx, tier1.check_b1_snapshot_counts, "B1")
        self._run_check(tier, ctx, tier1.check_b1b_snapshot_content, "B1b")
        self._run_check(tier, ctx, tier1.check_b3_no_duplicates, "B3")

        # Schema (post-snapshot — audit columns and metadata are created by the
        # sink's normalizer/MERGE, which may not run until after the first flush)
        self._run_check(tier, ctx, tier1.check_a3_audit_columns_present, "A3")
        self._run_check(tier, ctx, tier1.check_a4_metadata_table, "A4")

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
        """Instantiate source, target, and dbmazz clients from the specs."""
        source = instantiate_source_from_spec(self.src_spec)
        source.connect()

        target = instantiate_backend_from_spec(self.sk_spec)
        target.connect()

        # All managed setups expose dbmazz on localhost:8080. For pure BYOD
        # the same is true because compose still publishes the dbmazz container.
        dbmazz = DbmazzClient("http://localhost:8080")

        return TestContext(
            source=source,
            target=target,
            dbmazz=dbmazz,
            console=self.console,
            tables=tuple(getattr(self.src_spec, "tables", ())),
            source_name=self.src_name,
            sink_name=self.sk_name,
        )

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

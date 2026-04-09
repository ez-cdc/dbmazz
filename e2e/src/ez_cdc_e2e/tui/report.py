"""Verify output formatter.

Renders tier headers, individual check results, and final totals with the
EZ-CDC theme. Used by verify/runner.py to display test progress and results.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from rich.console import Console
from rich.text import Text


# ── Symbols (Unicode, not emoji) ─────────────────────────────────────────────

SYM_PASS = "✓"
SYM_FAIL = "✗"
SYM_SKIP = "⊘"
SYM_STEP = "→"


# ── Data model ───────────────────────────────────────────────────────────────

class CheckStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class CheckResult:
    """Result of a single validation check."""
    id: str                           # "A1", "D4", "C1", etc.
    description: str                  # "Target tables present"
    status: CheckStatus
    detail: str = ""                  # "2/2", "md5: 3a4f...", "4.2 KB text intact"
    error: Optional[str] = None       # error message if status=FAIL
    duration_ms: Optional[int] = None


@dataclass
class TierResult:
    """Result of an entire tier (group of checks)."""
    name: str                         # "Tier 1 — Correctness baseline"
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.PASS)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.FAIL)

    @property
    def skipped(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.SKIP)


@dataclass
class VerifyReport:
    """Top-level report for an entire verify run."""
    profile: str                      # "starrocks", "pg-target", "snowflake"
    tiers: list[TierResult] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    @property
    def total_passed(self) -> int:
        return sum(t.passed for t in self.tiers)

    @property
    def total_failed(self) -> int:
        return sum(t.failed for t in self.tiers)

    @property
    def total_skipped(self) -> int:
        return sum(t.skipped for t in self.tiers)

    @property
    def ok(self) -> bool:
        return self.total_failed == 0


# ── Formatting helpers ───────────────────────────────────────────────────────

def format_step(text: str) -> Text:
    """Format a setup step like '→ Starting compose...'."""
    t = Text()
    t.append(SYM_STEP + "  ", style="step")
    t.append(text, style="default")
    return t


def format_step_ok(text: str, duration: str = "") -> Text:
    """Format a completed step with a checkmark."""
    t = format_step(text)
    t.append("  ")
    t.append(SYM_PASS, style="pass")
    if duration:
        t.append(f"  ({duration})", style="muted")
    return t


def format_check(check: CheckResult) -> Text:
    """Format a single check result line.

    For PASS/SKIP, shows ID + description + optional detail.
    For FAIL, additionally appends the error message on a continuation line
    so the user knows what broke without having to check a JSON report.
    """
    t = Text()
    t.append("  ")  # indent

    # symbol + id
    if check.status == CheckStatus.PASS:
        t.append(SYM_PASS, style="pass")
    elif check.status == CheckStatus.FAIL:
        t.append(SYM_FAIL, style="fail")
    else:
        t.append(SYM_SKIP, style="skip")

    t.append("  ")
    t.append(f"{check.id:<4}", style="check.id")
    t.append(" ")
    t.append(f"{check.description:<45}", style="default")

    # detail column
    if check.detail:
        t.append("  ")
        t.append(check.detail, style="check.detail")

    # For failures, append the error message on a second line so the user
    # can see what went wrong directly in the terminal output.
    if check.status == CheckStatus.FAIL and check.error:
        t.append("\n")
        t.append("       ")  # indent under the id
        t.append("└─ ", style="dim")
        t.append(check.error, style="fail")

    return t


def format_tier_header(tier_name: str) -> Text:
    """Format a tier header like 'Tier 1 — Correctness baseline'."""
    t = Text()
    t.append("\n")
    t.append(tier_name, style="bold")
    return t


def format_totals(report: VerifyReport) -> Text:
    """Format the final totals line with rule separators."""
    rule = "━" * 60
    t = Text()
    t.append("\n")
    t.append(rule, style="rule")
    t.append("\n ")

    status_style = "pass" if report.ok else "fail"
    status_symbol = SYM_PASS if report.ok else SYM_FAIL
    t.append(status_symbol + "  ", style=status_style)

    t.append(f"{report.total_passed} passed", style="pass")
    t.append("   ")
    t.append(f"{report.total_failed} failed", style="fail" if report.total_failed > 0 else "muted")
    t.append("   ")
    t.append(f"{report.total_skipped} skipped", style="skip" if report.total_skipped > 0 else "muted")
    t.append(f"        elapsed {report.elapsed_seconds:.1f}s", style="muted")
    t.append("\n")
    t.append(rule, style="rule")
    return t


def print_check(console: Console, check: CheckResult) -> None:
    """Print a single check result to the console."""
    console.print(format_check(check))


def print_tier_header(console: Console, tier_name: str) -> None:
    console.print(format_tier_header(tier_name))


def print_report_summary(console: Console, report: VerifyReport) -> None:
    """Print the final totals line."""
    console.print(format_totals(report))


def report_to_json(report: VerifyReport) -> dict:
    """Serialize a VerifyReport to a JSON-friendly dict (for --json-report)."""
    return {
        "profile": report.profile,
        "elapsed_seconds": report.elapsed_seconds,
        "ok": report.ok,
        "totals": {
            "passed": report.total_passed,
            "failed": report.total_failed,
            "skipped": report.total_skipped,
        },
        "tiers": [
            {
                "name": tier.name,
                "passed": tier.passed,
                "failed": tier.failed,
                "skipped": tier.skipped,
                "checks": [
                    {
                        "id": c.id,
                        "description": c.description,
                        "status": c.status.value,
                        "detail": c.detail,
                        "error": c.error,
                        "duration_ms": c.duration_ms,
                    }
                    for c in tier.checks
                ],
            }
            for tier in report.tiers
        ],
    }

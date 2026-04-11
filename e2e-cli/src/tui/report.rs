//! Verify output formatter.
//!
//! Renders tier headers, individual check results, and final totals with the
//! EZ-CDC theme. Used by the verify module to display test progress and results.

use console::style;
use serde::Serialize;

use super::theme::{
    DIM, ERROR, GRAY_400, SUCCESS, SYM_ARROW, SYM_CHECK, SYM_CROSS, SYM_SKIP, WARNING,
};

// ── Helpers ───────────────────────────────────────────────────────────────

/// Parse "#RRGGBB" hex to ANSI 256-color index.
fn hex_to_ansi(hex: &str) -> u8 {
    let hex = hex.trim_start_matches('#');
    let r = u8::from_str_radix(&hex[0..2], 16).unwrap_or(0);
    let g = u8::from_str_radix(&hex[2..4], 16).unwrap_or(0);
    let b = u8::from_str_radix(&hex[4..6], 16).unwrap_or(0);
    let ri = (u16::from(r) * 5 / 255) as u8;
    let gi = (u16::from(g) * 5 / 255) as u8;
    let bi = (u16::from(b) * 5 / 255) as u8;
    16 + 36 * ri + 6 * gi + bi
}

// ── Data model ────────────────────────────────────────────────────────────

/// Status of a single verification check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Pass,
    Fail,
    Skip,
}

/// Result of a single validation check.
#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    /// Check identifier, e.g. "A1", "D4".
    pub id: String,
    /// Human-readable description, e.g. "Target tables present".
    pub description: String,
    /// Pass, Fail, or Skip.
    pub status: CheckStatus,
    /// Detail string, e.g. "2/2", "md5: 3a4f...".
    #[serde(default)]
    pub detail: String,
    /// Error message when status is Fail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Time taken in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

/// Result of an entire tier (group of checks).
#[derive(Debug, Clone, Serialize)]
pub struct TierResult {
    /// Tier name, e.g. "Tier 1 -- Correctness baseline".
    pub name: String,
    /// Individual check results.
    #[serde(default)]
    pub checks: Vec<CheckResult>,
}

impl TierResult {
    /// Count of passed checks.
    pub fn passed(&self) -> usize {
        self.checks
            .iter()
            .filter(|c| c.status == CheckStatus::Pass)
            .count()
    }

    /// Count of failed checks.
    pub fn failed(&self) -> usize {
        self.checks
            .iter()
            .filter(|c| c.status == CheckStatus::Fail)
            .count()
    }

    /// Count of skipped checks.
    pub fn skipped(&self) -> usize {
        self.checks
            .iter()
            .filter(|c| c.status == CheckStatus::Skip)
            .count()
    }
}

/// Top-level report for an entire verify run.
#[derive(Debug, Clone, Serialize)]
pub struct VerifyReport {
    /// Profile name, e.g. "starrocks", "pg-target", "snowflake".
    pub profile: String,
    /// Tier results in order.
    pub tiers: Vec<TierResult>,
    /// Total elapsed time in seconds.
    pub elapsed_seconds: f64,
}

impl VerifyReport {
    /// Total passed across all tiers.
    pub fn total_passed(&self) -> usize {
        self.tiers.iter().map(|t| t.passed()).sum()
    }

    /// Total failed across all tiers.
    pub fn total_failed(&self) -> usize {
        self.tiers.iter().map(|t| t.failed()).sum()
    }

    /// Total skipped across all tiers.
    pub fn total_skipped(&self) -> usize {
        self.tiers.iter().map(|t| t.skipped()).sum()
    }

    /// True if there are zero failures.
    pub fn ok(&self) -> bool {
        self.total_failed() == 0
    }
}

// ── Formatting ────────────────────────────────────────────────────────────

/// Print a single check result line.
///
/// Format: `  {symbol}  {id}  {description}  {detail}  ({duration}ms)`
/// For failures, the error is printed on a continuation line.
pub fn print_check(check: &CheckResult) {
    let success_c = hex_to_ansi(SUCCESS);
    let error_c = hex_to_ansi(ERROR);
    let warning_c = hex_to_ansi(WARNING);
    let dim_c = hex_to_ansi(DIM);
    let brand_c = hex_to_ansi(GRAY_400);

    // Symbol
    let symbol = match check.status {
        CheckStatus::Pass => style(SYM_CHECK).color256(success_c),
        CheckStatus::Fail => style(SYM_CROSS).color256(error_c),
        CheckStatus::Skip => style(SYM_SKIP).color256(warning_c),
    };

    // ID (bright)
    let id = style(format!("{:<4}", check.id)).color256(brand_c).bold();

    // Description (padded)
    let desc = format!("{:<45}", check.description);

    // Detail (dim)
    let detail = if check.detail.is_empty() {
        String::new()
    } else {
        format!("  {}", style(&check.detail).color256(dim_c))
    };

    // Duration (dim)
    let timing = match check.duration_ms {
        Some(ms) => format!("  {}", style(format!("({ms}ms)")).color256(dim_c)),
        None => String::new(),
    };

    println!("  {symbol}  {id} {desc}{detail}{timing}");

    // Error continuation line for failures
    if check.status == CheckStatus::Fail {
        if let Some(err) = &check.error {
            let dim_prefix = style("\u{2514}\u{2500} ").color256(dim_c);
            let err_styled = style(err).color256(error_c);
            println!("       {dim_prefix}{err_styled}");
        }
    }
}

/// Print a step indicator line (e.g. "-> Starting compose...").
pub fn print_step(text: &str) {
    let arrow_c = hex_to_ansi(super::theme::INFO);
    println!(
        "{}  {text}",
        style(SYM_ARROW).color256(arrow_c),
    );
}

/// Print a tier header in bold.
pub fn print_tier_header(name: &str) {
    println!();
    println!("{}", style(name).bold());
}

/// Print the final summary line with totals and elapsed time.
pub fn print_report_summary(report: &VerifyReport) {
    let success_c = hex_to_ansi(SUCCESS);
    let error_c = hex_to_ansi(ERROR);
    let warning_c = hex_to_ansi(WARNING);
    let dim_c = hex_to_ansi(DIM);

    let rule = "\u{2501}".repeat(60);
    let rule_styled = style(&rule).color256(dim_c);

    println!();
    println!("{rule_styled}");

    // Status symbol
    let (sym, sym_color) = if report.ok() {
        (SYM_CHECK, success_c)
    } else {
        (SYM_CROSS, error_c)
    };

    // Totals
    let passed = style(format!("{} passed", report.total_passed())).color256(success_c);

    let failed_count = report.total_failed();
    let failed = if failed_count > 0 {
        style(format!("{failed_count} failed")).color256(error_c)
    } else {
        style(format!("{failed_count} failed")).color256(dim_c)
    };

    let skipped_count = report.total_skipped();
    let skipped = if skipped_count > 0 {
        style(format!("{skipped_count} skipped")).color256(warning_c)
    } else {
        style(format!("{skipped_count} skipped")).color256(dim_c)
    };

    let elapsed = style(format!("elapsed {:.1}s", report.elapsed_seconds)).color256(dim_c);

    println!(
        " {}  {passed}   {failed}   {skipped}        {elapsed}",
        style(sym).color256(sym_color),
    );

    println!("{rule_styled}");
}

/// Serialize a `VerifyReport` to a JSON `serde_json::Value` for `--json-report`.
pub fn report_to_json(report: &VerifyReport) -> serde_json::Value {
    serde_json::json!({
        "profile": report.profile,
        "elapsed_seconds": report.elapsed_seconds,
        "ok": report.ok(),
        "totals": {
            "passed": report.total_passed(),
            "failed": report.total_failed(),
            "skipped": report.total_skipped(),
        },
        "tiers": report.tiers.iter().map(|tier| {
            serde_json::json!({
                "name": tier.name,
                "passed": tier.passed(),
                "failed": tier.failed(),
                "skipped": tier.skipped(),
                "checks": tier.checks.iter().map(|c| {
                    serde_json::json!({
                        "id": c.id,
                        "description": c.description,
                        "status": c.status,
                        "detail": c.detail,
                        "error": c.error,
                        "duration_ms": c.duration_ms,
                    })
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_check(status: CheckStatus) -> CheckResult {
        CheckResult {
            id: "A1".into(),
            description: "Test check".into(),
            status,
            detail: "2/2".into(),
            error: None,
            duration_ms: Some(42),
        }
    }

    fn make_tier() -> TierResult {
        TierResult {
            name: "Tier 1 -- Correctness baseline".into(),
            checks: vec![
                make_check(CheckStatus::Pass),
                make_check(CheckStatus::Pass),
                make_check(CheckStatus::Fail),
                make_check(CheckStatus::Skip),
            ],
        }
    }

    #[test]
    fn tier_counts() {
        let tier = make_tier();
        assert_eq!(tier.passed(), 2);
        assert_eq!(tier.failed(), 1);
        assert_eq!(tier.skipped(), 1);
    }

    #[test]
    fn verify_report_ok_when_no_failures() {
        let report = VerifyReport {
            profile: "starrocks".into(),
            tiers: vec![TierResult {
                name: "Tier 1".into(),
                checks: vec![
                    make_check(CheckStatus::Pass),
                    make_check(CheckStatus::Pass),
                    make_check(CheckStatus::Skip),
                ],
            }],
            elapsed_seconds: 1.5,
        };
        assert!(report.ok());
        assert_eq!(report.total_passed(), 2);
        assert_eq!(report.total_failed(), 0);
        assert_eq!(report.total_skipped(), 1);
    }

    #[test]
    fn verify_report_not_ok_when_failures() {
        let report = VerifyReport {
            profile: "pg-target".into(),
            tiers: vec![make_tier()],
            elapsed_seconds: 3.7,
        };
        assert!(!report.ok());
        assert_eq!(report.total_passed(), 2);
        assert_eq!(report.total_failed(), 1);
        assert_eq!(report.total_skipped(), 1);
    }

    #[test]
    fn report_totals_across_multiple_tiers() {
        let report = VerifyReport {
            profile: "starrocks".into(),
            tiers: vec![
                TierResult {
                    name: "Tier 1".into(),
                    checks: vec![
                        make_check(CheckStatus::Pass),
                        make_check(CheckStatus::Fail),
                    ],
                },
                TierResult {
                    name: "Tier 2".into(),
                    checks: vec![
                        make_check(CheckStatus::Pass),
                        make_check(CheckStatus::Pass),
                        make_check(CheckStatus::Skip),
                    ],
                },
            ],
            elapsed_seconds: 5.0,
        };
        assert_eq!(report.total_passed(), 3);
        assert_eq!(report.total_failed(), 1);
        assert_eq!(report.total_skipped(), 1);
        assert!(!report.ok());
    }

    #[test]
    fn report_empty_tiers_is_ok() {
        let report = VerifyReport {
            profile: "empty".into(),
            tiers: vec![],
            elapsed_seconds: 0.0,
        };
        assert!(report.ok());
        assert_eq!(report.total_passed(), 0);
        assert_eq!(report.total_failed(), 0);
        assert_eq!(report.total_skipped(), 0);
    }

    #[test]
    fn report_to_json_structure() {
        let report = VerifyReport {
            profile: "starrocks".into(),
            tiers: vec![TierResult {
                name: "Tier 1".into(),
                checks: vec![CheckResult {
                    id: "A1".into(),
                    description: "Tables exist".into(),
                    status: CheckStatus::Pass,
                    detail: "2/2".into(),
                    error: None,
                    duration_ms: Some(100),
                }],
            }],
            elapsed_seconds: 2.5,
        };

        let json = report_to_json(&report);
        assert_eq!(json["profile"], "starrocks");
        assert_eq!(json["ok"], true);
        assert_eq!(json["elapsed_seconds"], 2.5);
        assert_eq!(json["totals"]["passed"], 1);
        assert_eq!(json["totals"]["failed"], 0);
        assert_eq!(json["totals"]["skipped"], 0);

        let tiers = json["tiers"].as_array().expect("tiers should be array");
        assert_eq!(tiers.len(), 1);
        assert_eq!(tiers[0]["name"], "Tier 1");

        let checks = tiers[0]["checks"]
            .as_array()
            .expect("checks should be array");
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0]["id"], "A1");
        assert_eq!(checks[0]["status"], "pass");
        assert_eq!(checks[0]["duration_ms"], 100);
    }
}

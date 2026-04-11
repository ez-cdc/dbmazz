//! EZ-CDC color palette and status symbols.
//!
//! Colors sourced from the brand palette (Tailwind Blue primary, Slate grays).
//! Hex values work with `console::Style::color256()` and `console::style()`.

#![allow(dead_code)]

// ── Brand palette (Tailwind Blue) ─────────────────────────────────────────

pub const PRIMARY_400: &str = "#60A5FA"; // brand (dark-friendly)
pub const PRIMARY_500: &str = "#3B82F6"; // info
pub const PRIMARY_600: &str = "#2563EB"; // brand (light-friendly), gradient anchor
pub const PRIMARY_700: &str = "#1D4ED8"; // gradient end

// ── Grays (Tailwind Slate) ────────────────────────────────────────────────

pub const GRAY_400: &str = "#94A3B8"; // tagline, muted
pub const GRAY_500: &str = "#64748B"; // dim text
pub const GRAY_600: &str = "#475569"; // very dim

// ── Semantic colors ───────────────────────────────────────────────────────

pub const BRAND_PRIMARY: &str = PRIMARY_400;
pub const BRAND_SECONDARY: &str = PRIMARY_600;
pub const SUCCESS: &str = "#10B981";
pub const ERROR: &str = "#EF4444";
pub const WARNING: &str = "#F59E0B";
pub const INFO: &str = "#3B82F6";
pub const DIM: &str = GRAY_500;
pub const ACCENT: &str = PRIMARY_500;

// ── Status symbols (Unicode, not emoji) ───────────────────────────────────

pub const SYM_CHECK: &str = "\u{2713}"; // ✓
pub const SYM_CROSS: &str = "\u{2717}"; // ✗
pub const SYM_SKIP: &str = "\u{2298}"; // ⊘
pub const SYM_BULLET: &str = "\u{25CF}"; // ●
pub const SYM_BULLET_EMPTY: &str = "\u{25CB}"; // ○
pub const SYM_ARROW: &str = "\u{2192}"; // →
pub const SYM_DIAMOND: &str = "\u{25C6}"; // ◆
pub const SYM_DIAMOND_EMPTY: &str = "\u{25C7}"; // ◇
pub const SYM_RUNNING: &str = "\u{25B6}"; // ▶
pub const SYM_PAUSED: &str = "\u{23F8}"; // ⏸
pub const SYM_STOPPED: &str = "\u{25A0}"; // ■

// ── Gradient helper ───────────────────────────────────────────────────────

/// Parse a "#RRGGBB" hex string into (r, g, b).
fn hex_to_rgb(hex: &str) -> (u8, u8, u8) {
    let hex = hex.trim_start_matches('#');
    let r = u8::from_str_radix(&hex[0..2], 16).unwrap_or(0);
    let g = u8::from_str_radix(&hex[2..4], 16).unwrap_or(0);
    let b = u8::from_str_radix(&hex[4..6], 16).unwrap_or(0);
    (r, g, b)
}

/// Linear interpolation between two hex colors, returning `steps` hex strings.
///
/// Used for banner gradients. `start` and `end` are "#RRGGBB" strings.
pub fn interpolate_hex(start: &str, end: &str, steps: usize) -> Vec<String> {
    if steps < 2 {
        return vec![start.to_string()];
    }

    let (r1, g1, b1) = hex_to_rgb(start);
    let (r2, g2, b2) = hex_to_rgb(end);

    (0..steps)
        .map(|i| {
            let t = i as f64 / (steps - 1) as f64;
            let r = (f64::from(r1) + (f64::from(r2) - f64::from(r1)) * t).round() as u8;
            let g = (f64::from(g1) + (f64::from(g2) - f64::from(g1)) * t).round() as u8;
            let b = (f64::from(b1) + (f64::from(b2) - f64::from(b1)) * t).round() as u8;
            format!("#{r:02X}{g:02X}{b:02X}")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_to_rgb_parses_correctly() {
        assert_eq!(hex_to_rgb("#FF0000"), (255, 0, 0));
        assert_eq!(hex_to_rgb("#00FF00"), (0, 255, 0));
        assert_eq!(hex_to_rgb("#0000FF"), (0, 0, 255));
        assert_eq!(hex_to_rgb("#60A5FA"), (96, 165, 250));
    }

    #[test]
    fn interpolate_single_step() {
        let result = interpolate_hex("#FF0000", "#0000FF", 1);
        assert_eq!(result, vec!["#FF0000"]);
    }

    #[test]
    fn interpolate_two_steps() {
        let result = interpolate_hex("#FF0000", "#0000FF", 2);
        assert_eq!(result, vec!["#FF0000", "#0000FF"]);
    }

    #[test]
    fn interpolate_three_steps() {
        let result = interpolate_hex("#000000", "#FF0000", 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], "#000000");
        assert_eq!(result[1], "#800000");
        assert_eq!(result[2], "#FF0000");
    }
}

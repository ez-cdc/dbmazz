//! Banner rendering for the EZ-CDC CLI.
//!
//! Two variants:
//! - `render_banner()` — ANSI Shadow block art with vertical gradient. Used
//!   in the main menu and quickstart.
//! - `render_banner_compact()` — single-line styled "ez-cdc" + version. Used
//!   in subcommands like verify, up, down, etc.
//!
//! Gradients interpolated between primary.600 and primary.400.

use console::style;

use super::theme::{interpolate_hex, GRAY_400, PRIMARY_400, PRIMARY_600};

// ── ANSI Shadow block art ─────────────────────────────────────────────────

// 6 lines, 44 columns wide. "EZ-CDC" rendered in ANSI Shadow figlet font.
const BANNER_LINES: &[&str] = &[
    "\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}     \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557} \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}",
    "\u{2588}\u{2588}\u{2554}\u{2550}\u{2550}\u{2550}\u{2550}\u{255D}\u{255A}\u{2550}\u{2550}\u{2588}\u{2588}\u{2588}\u{2554}\u{255D}    \u{2588}\u{2588}\u{2554}\u{2550}\u{2550}\u{2550}\u{2550}\u{255D}\u{2588}\u{2588}\u{2554}\u{2550}\u{2550}\u{2588}\u{2588}\u{2557}\u{2588}\u{2588}\u{2554}\u{2550}\u{2550}\u{2550}\u{2550}\u{255D}",
    "\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}    \u{2588}\u{2588}\u{2588}\u{2554}\u{255D}     \u{2588}\u{2588}\u{2551}     \u{2588}\u{2588}\u{2551}  \u{2588}\u{2588}\u{2551}\u{2588}\u{2588}\u{2551}     ",
    "\u{2588}\u{2588}\u{2554}\u{2550}\u{2550}\u{255D}   \u{2588}\u{2588}\u{2588}\u{2554}\u{255D}      \u{2588}\u{2588}\u{2551}     \u{2588}\u{2588}\u{2551}  \u{2588}\u{2588}\u{2551}\u{2588}\u{2588}\u{2551}     ",
    "\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}    \u{255A}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2554}\u{255D}\u{255A}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2557}",
    "\u{255A}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{255D}\u{255A}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{255D}     \u{255A}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{255D}\u{255A}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{255D}  \u{255A}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{255D}",
];

const BANNER_MIN_WIDTH: usize = 56;
const TAGLINE: &str = "Real-time CDC, radically simplified";
const LEFT_PAD: &str = "    "; // 4 spaces

/// Print the full ANSI Shadow banner with a vertical gradient.
///
/// Each line of the ASCII art gets its own color, from `PRIMARY_600` at the
/// top to `PRIMARY_400` at the bottom. Falls back to compact banner if the
/// terminal is narrower than 56 columns.
pub fn render_banner() {
    let width = terminal_width();
    if width < BANNER_MIN_WIDTH {
        render_banner_compact();
        return;
    }

    let colors = interpolate_hex(PRIMARY_600, PRIMARY_400, BANNER_LINES.len());

    println!();
    for (line, color_hex) in BANNER_LINES.iter().zip(colors.iter()) {
        let (r, g, b) = parse_hex(color_hex);
        print!("{LEFT_PAD}");
        println!("{}", style(line).color256(to_ansi256(r, g, b)).bold());
    }
    println!();

    // Tagline centered under the ASCII art.
    let art_width: usize = 44;
    let tagline_padding = LEFT_PAD.len() + (art_width.saturating_sub(TAGLINE.len())) / 2;
    let pad = " ".repeat(tagline_padding);
    let (r, g, b) = parse_hex(GRAY_400);
    println!("{pad}{}", style(TAGLINE).color256(to_ansi256(r, g, b)));
    println!();
}

/// Print a compact single-line banner: styled "ez-cdc" + version.
///
/// Used for subcommand output headers (verify, up, down, etc.).
pub fn render_banner_compact() {
    let version = env!("CARGO_PKG_VERSION");
    let (r, g, b) = parse_hex(PRIMARY_400);
    let (dr, dg, db) = parse_hex(GRAY_400);

    println!();
    println!(
        "{LEFT_PAD}{}  {}",
        style("ez-cdc").color256(to_ansi256(r, g, b)).bold(),
        style(format!("v{version}")).color256(to_ansi256(dr, dg, db)),
    );
    println!();
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Parse a "#RRGGBB" hex string into (r, g, b).
fn parse_hex(hex: &str) -> (u8, u8, u8) {
    let hex = hex.trim_start_matches('#');
    let r = u8::from_str_radix(&hex[0..2], 16).unwrap_or(0);
    let g = u8::from_str_radix(&hex[2..4], 16).unwrap_or(0);
    let b = u8::from_str_radix(&hex[4..6], 16).unwrap_or(0);
    (r, g, b)
}

/// Convert RGB to the closest ANSI 256-color code.
///
/// Uses the 6x6x6 color cube (indices 16..=231). Each channel is mapped
/// from 0..=255 to 0..=5.
fn to_ansi256(r: u8, g: u8, b: u8) -> u8 {
    let ri = (u16::from(r) * 5 / 255) as u8;
    let gi = (u16::from(g) * 5 / 255) as u8;
    let bi = (u16::from(b) * 5 / 255) as u8;
    16 + 36 * ri + 6 * gi + bi
}

/// Get terminal width, defaulting to 80 if unavailable.
fn terminal_width() -> usize {
    console::Term::stdout()
        .size_checked()
        .map(|(_, w)| w as usize)
        .unwrap_or(80)
}

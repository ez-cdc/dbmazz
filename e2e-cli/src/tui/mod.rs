//! TUI foundation — theme, banner, prompts, verify report formatting,
//! and the interactive quickstart dashboard.
//!
//! Uses the `console` crate for terminal styling. The `crossterm`/`ratatui`
//! stack powers the interactive dashboard (quickstart).

pub mod banner;
pub mod dashboard;
pub mod prompts;
pub mod report;
pub mod theme;

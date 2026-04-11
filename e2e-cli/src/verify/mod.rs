//! Verification engine — orchestrates tier checks against a running CDC pipeline.
//!
//! Submodules:
//! - `polling` — async poll-until helpers (`wait_until`, `wait_until_with_value`)
//! - `runner` — `VerifyRunner` that builds context and runs tiers
//! - `tier1` — correctness baseline checks (A, B, C, D, H series)

pub mod polling;
pub mod runner;
pub mod tier1;

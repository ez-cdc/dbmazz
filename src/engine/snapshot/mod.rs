// Copyright 2025
// Licensed under the Elastic License v2.0

//! Snapshot / backfill engine module.
//!
//! Implements the Flink CDC concurrent snapshot algorithm:
//! - WAL consumer and snapshot worker run in parallel
//! - Watermarks (LW/HW) via `pg_logical_emit_message`
//! - `SharedState::should_emit()` for O(log n) deduplication
//! - Resumable: completed chunks are stored in `dbmazz_snapshot_state`

pub mod chunker;
pub mod state_store;
pub mod worker;

pub use worker::run_snapshot;

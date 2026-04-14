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
pub mod utils;
pub mod worker;

pub use worker::run_snapshot;

/// Quote a SQL identifier to prevent SQL injection.
/// Wraps in double quotes and escapes embedded double quotes.
/// Handles schema-qualified names like "public.orders" → "public"."orders".
pub fn quote_ident(name: &str) -> String {
    if name.contains('.') {
        name.split('.')
            .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(".")
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

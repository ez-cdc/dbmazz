// Copyright 2025
// Licensed under the Elastic License v2.0

//! dbmazz — Change Data Capture daemon.
//!
//! Reads database write-ahead logs (WAL) and streams changes to supported
//! sinks (StarRocks, PostgreSQL, Snowflake).
//!
//! This library crate is the core of dbmazz. The binary entrypoint
//! in `main.rs` parses configuration and starts the engine.
//!
//! # Organization
//!
//! | Module | Role |
//! |--------|------|
//! | [`config`] | Environment-based configuration |
//! | [`connectors`] | Sink implementations (StarRocks, PG, Snowflake) |
//! | [`control`] | HTTP control plane for worker-agent management |
//! | [`core`] | Generic abstractions (CdcRecord, DataType, Source, Sink) |
//! | [`engine`] | CDC orchestration (setup, run, snapshot) |
//! | [`pipeline`] | Batching + dispatch to sink |
//! | [`replication`] | WAL handling (pgoutput parsing, LSN tracking) |
//! | [`source`] | Source connectors (PostgreSQL, MySQL) |
//! | [`state_store`] | LSN checkpoint persistence |
//! | [`utils`] | Shared utilities |

#![warn(clippy::all)]

pub mod config;
pub mod connectors;
pub mod control;
pub mod core;
pub mod engine;
pub mod pipeline;
pub mod replication;
pub mod source;
pub mod state_store;
pub mod utils;

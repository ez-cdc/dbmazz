// Copyright 2025
// Licensed under the Elastic License v2.0

//! Connectors module for EZ-CDC
//!
//! This module provides a unified interface for CDC sources and sinks.
//! Each connector type is self-contained in its own submodule with:
//! - Configuration and validation
//! - Connection handling
//! - Data type mappings
//! - Protocol-specific logic
//!
//! # Sources
//! - `postgres` - PostgreSQL logical replication using pgoutput
//!
//! # Sinks
//! - `starrocks` - StarRocks via Stream Load HTTP API

pub mod sinks;
pub mod sources;

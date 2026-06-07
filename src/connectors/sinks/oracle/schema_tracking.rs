// Copyright 2025
// Licensed under the Elastic License v2.0

//! Schema state tracking for the Oracle sink.
//!
//! Provides in-memory schema caching and difference computation for
//! schema evolution. Currently a minimal stub — full implementation
//! will use an Oracle metadata table for persistent schema tracking.

use std::collections::HashMap;
use std::sync::Arc;

use crate::core::traits::SourceTableSchema;

/// In-memory schema cache: qualified table name → `SourceTableSchema`.
pub type SchemaState = Arc<HashMap<String, SourceTableSchema>>;

/// Create an initial schema state from the provided source schemas.
pub fn initialize_state(source_schemas: &[SourceTableSchema]) -> SchemaState {
    let mut map = HashMap::with_capacity(source_schemas.len());
    for schema in source_schemas {
        let key = format!("{}.{}", schema.schema, schema.name);
        map.insert(key, schema.clone());
    }
    Arc::new(map)
}

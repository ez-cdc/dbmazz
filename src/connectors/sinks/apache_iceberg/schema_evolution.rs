// Copyright 2025
// Licensed under the Elastic License v2.0

//! Apache Iceberg schema evolution — ALTER TABLE ADD COLUMN via REST catalog API.
//!
//! Iceberg supports schema evolution natively through the REST catalog commit
//! endpoint. This module:
//!
//! 1. Fetches the current table metadata (including the current schema) via
//!    `GET /v1/{prefix}/namespaces/{ns}/tables/{table}`.
//! 2. Computes which source columns are missing from the target schema.
//! 3. Adds new fields to the schema and commits the updated schema via
//!    `POST /v1/{prefix}/namespaces/{ns}/tables/{table}` with a
//!    `CommitTableRequest` containing an `add-schema` update.
//!
//! # Conservative policy
//!
//! - **ADD COLUMN** → auto-applied via schema evolution commit.
//! - **DROP / MODIFY / RENAME** → handled by the shared
//!   `compute_schema_evolution_plan` which logs the events and excludes them
//!   from the pending diffs. The Iceberg table retains the dead column / old
//!   type — Iceberg never deletes column data from files on drop.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use tracing::{info, warn};

use super::types::TypeMapper;
use crate::connectors::sinks::schema_evolution::SchemaDiff;
use crate::core::traits::SourceTableSchema;

// ---------------------------------------------------------------------------
// Iceberg schema evolution machinery
// ---------------------------------------------------------------------------

/// Handles schema evolution for Apache Iceberg tables via the REST catalog
/// API. Owned by `IcebergSink` and shared between `setup()` and
/// `write_batch()`.
pub struct IcebergSchemaEvolution {
    client: reqwest::Client,
    catalog_url: String,
    namespace: String,
    type_mapper: TypeMapper,
}

impl IcebergSchemaEvolution {
    pub fn new(client: reqwest::Client, catalog_url: String, namespace: String) -> Self {
        Self {
            client,
            catalog_url,
            namespace,
            type_mapper: TypeMapper::new(),
        }
    }

    // -----------------------------------------------------------------------
    // Setup-time reconciliation
    // -----------------------------------------------------------------------

    /// Reconcile target schema with source schema on startup.
    ///
    /// For each table, computes the columns present in source but missing from
    /// the Iceberg table's current schema and commits a schema update via the
    /// REST catalog API. Halts loud on any failure.
    pub async fn reconcile_target_schema(
        &self,
        source_schemas: &[SourceTableSchema],
    ) -> Result<()> {
        for source in source_schemas {
            let table_name = &source.name;

            // Fetch current table metadata to get the existing schema.
            let current_schema =
                self.fetch_current_schema(table_name)
                    .await
                    .with_context(|| {
                        format!(
                            "iceberg_sink: failed to fetch current schema for table {}",
                            table_name
                        )
                    })?;

            // Build set of existing field names (Iceberg field names are
            // case-sensitive; we match source column names as-is).
            let existing_fields: std::collections::HashSet<String> = current_schema
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect();

            // Compute diff: source columns not present in target.
            let mut diff = SchemaDiff::default();
            for (i, src_col) in source.columns.iter().enumerate() {
                if !existing_fields.contains(&src_col.name) {
                    diff.added
                        .push(crate::connectors::sinks::schema_evolution::AddedColumn {
                            name: src_col.name.clone(),
                            data_type: src_col.data_type.clone(),
                            nullable: true,
                            #[allow(clippy::cast_possible_wrap)]
                            ordinal: i as i32,
                            pg_type_id: src_col.pg_type_id,
                        });
                }
            }

            if diff.added.is_empty() {
                continue;
            }

            info!(
                table = %source.name,
                added_count = diff.added.len(),
                "iceberg_sink: reconcile_on_startup applying schema evolution"
            );
            self.apply_diff(table_name, &diff).await.with_context(|| {
                format!(
                    "iceberg_sink: reconcile_on_startup failed for table {}",
                    source.name
                )
            })?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Apply a diff to add columns
    // -----------------------------------------------------------------------

    /// Apply a `SchemaDiff` to the target Iceberg table by committing a new
    /// schema via the REST catalog API.
    ///
    /// Steps:
    /// 1. Fetch current table metadata (gets the latest schema with field IDs).
    /// 2. Build new fields for each column in `diff.added`, assigning fresh
    ///    column IDs.
    /// 3. POST a `CommitTableRequest` with an `add-schema` update.
    pub async fn apply_diff(&self, table_name: &str, diff: &SchemaDiff) -> Result<()> {
        if diff.added.is_empty() {
            return Ok(());
        }

        // 1. Fetch current table metadata.
        let current_schema = self
            .fetch_current_schema(table_name)
            .await
            .with_context(|| {
                format!(
                    "iceberg_sink: apply_diff failed to fetch schema for table {}",
                    table_name
                )
            })?;

        let mut max_id = current_schema.max_field_id;
        let original_field_count = current_schema.fields.len();
        let mut fields = current_schema.fields;

        // 2. Build new fields for columns not already present.
        let existing_names: std::collections::HashSet<String> =
            fields.iter().map(|f| f.name.clone()).collect();

        for added in &diff.added {
            if existing_names.contains(&added.name) {
                warn!(
                    column = %added.name,
                    table = %table_name,
                    "iceberg_sink: column already exists in target schema, skipping"
                );
                continue;
            }

            max_id += 1;
            let iceberg_type = self.type_mapper.to_iceberg_type(&added.data_type);
            let field = IcebergField {
                id: max_id,
                name: added.name.clone(),
                field_type: Value::String(iceberg_type),
                required: !added.nullable,
            };
            fields.push(field);
        }

        if fields.len() == original_field_count {
            // No new fields were added (all were already present).
            return Ok(());
        }

        // 3. Build schema JSON and commit.
        let schema_fields: Vec<Value> = fields
            .iter()
            .map(|f| {
                json!({
                    "id": f.id,
                    "name": f.name,
                    "type": f.field_type,
                    "required": f.required,
                })
            })
            .collect();

        let schema_json = json!({
            "type": "struct",
            "fields": schema_fields,
        });

        let commit_body = json!({
            "requirements": [
                {
                    "type": "assert-current-schema-id",
                    "current-schema-id": current_schema.schema_id
                }
            ],
            "updates": [
                {
                    "action": "add-schema",
                    "schema": schema_json,
                    "last-column-id": max_id
                },
                {
                    "action": "set-current-schema",
                    "schema-id": current_schema.schema_id + 1
                }
            ]
        });

        let url = format!(
            "{}/namespaces/{}/tables/{}",
            self.catalog_url, self.namespace, table_name
        );

        let resp = self
            .client
            .post(&url)
            .json(&commit_body)
            .send()
            .await
            .with_context(|| {
                format!(
                    "iceberg_sink: HTTP POST failed for schema commit on table {}",
                    table_name
                )
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "iceberg_sink: schema commit returned HTTP {} for table {}: {}",
                status,
                table_name,
                body,
            );
        }

        info!(
            table = %table_name,
            added_count = diff.added.len(),
            "iceberg_sink: schema evolution committed successfully"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Fetch the current schema for a table from the Iceberg REST catalog.
    ///
    /// Sends `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` and extracts
    /// the schema from the response metadata.
    async fn fetch_current_schema(&self, table_name: &str) -> Result<IcebergSchema> {
        let url = format!(
            "{}/namespaces/{}/tables/{}",
            self.catalog_url, self.namespace, table_name
        );

        let resp = self.client.get(&url).send().await.with_context(|| {
            format!(
                "iceberg_sink: HTTP GET failed to fetch table metadata for {}",
                table_name
            )
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "iceberg_sink: GET table returned HTTP {} for table {}: {}",
                status,
                table_name,
                body,
            );
        }

        let table_meta: Value = resp.json().await.with_context(|| {
            format!(
                "iceberg_sink: failed to parse JSON response for table {}",
                table_name
            )
        })?;

        // Navigate through the Iceberg REST response to extract the schema.
        // The response contains a `metadata` object with `schema` and
        // optionally `current-schema-id` plus `schemas` array.
        //
        // Response shape (Iceberg REST v1 LoadTableResult):
        // {
        //   "metadata": {
        //     "schema": { "type": "struct", "fields": [...] },
        //     "current-schema-id": <int>,
        //     "schemas": [ { "schema-id": <int>, "type": "struct", "fields": [...] } ],
        //     ...
        //   },
        //   "config": { ... }
        // }

        let metadata = table_meta
            .get("metadata")
            .context("iceberg_sink: response missing 'metadata' field")?;

        // Prefer `current-schema-id` + `schemas` array over the deprecated
        // top-level `schema` field.
        let (schema_id, schema_value) = if let Some(schemas) =
            metadata.get("schemas").and_then(|v| v.as_array())
        {
            let current_schema_id = metadata
                .get("current-schema-id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // Find the schema matching the current-schema-id.
            let matched = schemas
                .iter()
                .find(|s| s.get("schema-id").and_then(|v| v.as_i64()) == Some(current_schema_id))
                .or_else(|| schemas.first());

            match matched {
                Some(s) => (current_schema_id, s.clone()),
                None => {
                    // Fallback: try the top-level `schema` field.
                    let schema = metadata.get("schema").context(
                        "iceberg_sink: no 'schemas' array or 'schema' field in metadata",
                    )?;
                    (0, schema.clone())
                }
            }
        } else {
            // Fallback to the top-level `schema` field.
            let schema = metadata
                .get("schema")
                .context("iceberg_sink: no 'schema' field in metadata")?;
            (0, schema.clone())
        };

        let type_val = schema_value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("struct");
        if type_val != "struct" {
            anyhow::bail!(
                "iceberg_sink: unexpected schema type '{}' for table {} (expected 'struct')",
                type_val,
                table_name,
            );
        }

        let fields_array = schema_value
            .get("fields")
            .and_then(|v| v.as_array())
            .context("iceberg_sink: schema missing 'fields' array")?;

        let mut fields = Vec::with_capacity(fields_array.len());
        let mut max_field_id = 0i64;

        for field_val in fields_array {
            let id = field_val.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
            let name = field_val
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let required = field_val
                .get("required")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            // The type can be either a simple string (e.g., "string",
            // "int", "long") or a complex object (e.g., {"type": "decimal",
            // "precision": 10, "scale": 2}). We store it as a serde_json
            // Value for round-trip fidelity.
            let field_type = field_val.get("type").cloned().unwrap_or(json!("string"));

            if id > max_field_id {
                max_field_id = id;
            }

            fields.push(IcebergField {
                id,
                name,
                field_type,
                required,
            });
        }

        Ok(IcebergSchema {
            schema_id,
            fields,
            max_field_id,
        })
    }
}

// ---------------------------------------------------------------------------
// Internal types for Iceberg schema representation
// ---------------------------------------------------------------------------

/// Represents a single field in an Iceberg schema.
#[derive(Debug, Clone)]
struct IcebergField {
    id: i64,
    name: String,
    /// Iceberg type — either a simple string like `"string"`, `"int"`,
    /// `"long"`, or a type-object like `{"type": "decimal", "precision": 10,
    /// "scale": 2}`. Stored as `serde_json::Value` for round-trip fidelity.
    field_type: Value,
    required: bool,
}

/// Parsed Iceberg schema from the REST catalog response.
#[derive(Debug, Clone)]
struct IcebergSchema {
    schema_id: i64,
    fields: Vec<IcebergField>,
    max_field_id: i64,
}

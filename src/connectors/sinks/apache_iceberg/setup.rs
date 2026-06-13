// Copyright 2025
// Licensed under the Elastic License v2.0

//! Apache Iceberg REST catalog setup: creates target namespace and tables.
//!
//! Uses the Iceberg REST API Catalog specification to create:
//! - Namespace (equivalent to a database/schema)
//! - Tables with Iceberg schema that mirrors the source columns + audit columns
//!
//! ## API Endpoints
//!
//! | Operation | Method | Path |
//! |-----------|--------|------|
//! | Create namespace | POST | `/v1/{prefix}/namespaces` |
//! | Create table | POST | `/v1/{prefix}/namespaces/{namespace}/tables` |

use anyhow::{Context, Result};
use reqwest::StatusCode;
use serde_json::json;
use tracing::info;

use super::types::TypeMapper;
use crate::core::traits::SourceTableSchema;

/// Audit columns appended to every Iceberg table.
///
/// These columns track the CDC operation metadata independently of the
/// source schema.
const AUDIT_COLUMNS: &[(&str, &str, bool)] = &[
    ("_dbmazz_op_type", "string", false),
    ("_dbmazz_is_deleted", "boolean", false),
    ("_dbmazz_synced_at", "string", false),
    ("_dbmazz_cdc_version", "long", false),
];

/// Creates the target namespace if it does not already exist.
///
/// Sends a POST to `/v1/{prefix}/namespaces` with the namespace name.
/// If the namespace already exists (HTTP 409 Conflict), the response is
/// silently ignored — the namespace is already available.
async fn create_namespace(
    client: &reqwest::Client,
    catalog_url: &str,
    namespace: &str,
) -> Result<()> {
    let url = format!("{}/namespaces", catalog_url);

    let body = json!({
        "namespace": [namespace]
    });

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .context("Failed to send create namespace request")?;

    let status = resp.status();
    if status == StatusCode::CONFLICT {
        // Namespace already exists — this is not an error.
        info!("  [OK] Namespace '{}' already exists", namespace);
        return Ok(());
    }

    if !status.is_success() {
        let text = resp
            .text()
            .await
            .unwrap_or_else(|_| "<no body>".to_string());
        anyhow::bail!(
            "Failed to create namespace '{}': HTTP {} - {}",
            namespace,
            status,
            text
        );
    }

    info!("  [OK] Namespace '{}' created", namespace);
    Ok(())
}

/// Creates an Iceberg table with schema matching the source columns plus
/// audit columns.
///
/// Sends a POST to `/v1/{prefix}/namespaces/{namespace}/tables` with the
/// full Iceberg schema definition. If the table already exists (HTTP 409
/// Conflict), it is silently skipped.
async fn create_table(
    client: &reqwest::Client,
    catalog_url: &str,
    namespace: &str,
    table_name: &str,
    type_mapper: &TypeMapper,
    source_schema: &SourceTableSchema,
) -> Result<()> {
    let url = format!("{}/namespaces/{}/tables", catalog_url, namespace);

    // Build Iceberg schema fields from source columns.
    let mut fields: Vec<serde_json::Value> = Vec::new();
    for (i, col) in source_schema.columns.iter().enumerate() {
        let iceberg_type = type_mapper.to_iceberg_type(&col.data_type);
        // Iceberg field IDs are 1-based; source columns start at 1.
        let field_id = (i + 1) as i32;
        fields.push(json!({
            "id": field_id,
            "name": col.name,
            "type": iceberg_type,
            "required": !col.nullable,
        }));
    }

    // Append audit columns with IDs following the source columns.
    let base_id = source_schema.columns.len() as i32;
    for (i, (name, typ, required)) in AUDIT_COLUMNS.iter().enumerate() {
        let field_id = base_id + (i as i32) + 1;
        fields.push(json!({
            "id": field_id,
            "name": name,
            "type": typ,
            "required": required,
        }));
    }

    let body = json!({
        "name": table_name,
        "schema": {
            "type": "struct",
            "fields": fields,
        },
        "partition-spec": json!({
            "fields": [],
            "spec-id": 0,
        }),
        "write-order": json!({
            "fields": [],
            "order-id": 0,
        }),
    });

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("Failed to send create table request for '{}'", table_name))?;

    let status = resp.status();
    if status == StatusCode::CONFLICT {
        // Table already exists — this is not an error.
        info!("  [OK] Table '{}' already exists", table_name);
        return Ok(());
    }

    if !status.is_success() {
        let text = resp
            .text()
            .await
            .unwrap_or_else(|_| "<no body>".to_string());
        anyhow::bail!(
            "Failed to create table '{}': HTTP {} - {}",
            table_name,
            status,
            text
        );
    }

    info!("  [OK] Table '{}' created", table_name);
    Ok(())
}

/// Runs the full Iceberg catalog setup: creates the namespace and all
/// target tables defined in the source schemas.
///
/// # Arguments
///
/// * `client` - Reqwest HTTP client for REST API calls
/// * `catalog_url` - Base URL for the Iceberg REST catalog (e.g. `http://host:8181/v1/warehouse`)
/// * `namespace` - Target namespace to create / use
/// * `source_schemas` - List of source table schemas to replicate
/// * `type_mapper` - Mapper from CDC `DataType` to Iceberg type strings
pub async fn run_setup(
    client: &reqwest::Client,
    catalog_url: &str,
    namespace: &str,
    source_schemas: &[SourceTableSchema],
    type_mapper: &TypeMapper,
) -> Result<()> {
    info!("Apache Iceberg Setup:");
    info!("  Catalog URL: {}", catalog_url);
    info!("  Namespace:   {}", namespace);
    info!("  Tables:      {}", source_schemas.len());

    // 1. Create namespace
    create_namespace(client, catalog_url, namespace).await?;

    // 2. Create each target table
    for schema in source_schemas {
        create_table(
            client,
            catalog_url,
            namespace,
            &schema.name,
            type_mapper,
            schema,
        )
        .await?;
    }

    info!("  [OK] Apache Iceberg setup complete");
    Ok(())
}

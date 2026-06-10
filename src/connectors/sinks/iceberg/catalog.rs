// Copyright 2026
// Licensed under the Elastic License v2.0

//! REST catalog wrapper for the Iceberg sink.
//!
//! Thin layer over `iceberg-catalog-rest` + the OpenDAL S3 storage
//! factory: namespace bootstrap, table create/load with schema
//! validation, and a cheap reachability probe.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;
use tracing::info;

use crate::core::traits::SourceTableSchema;

use super::config::IcebergSinkConfig;
use super::schema::{build_iceberg_schema, iceberg_table_name, validate_existing_schema};

/// Catalog handle shared by the sink and its commit path.
#[derive(Clone)]
pub struct CatalogHandle {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
}

/// `iceberg-catalog-rest` 0.9.1 maps GET 404s to `ErrorKind::Unexpected`
/// with a "... does not exist" message instead of the dedicated
/// `NamespaceNotFound`/`TableNotFound` kinds — match both.
fn is_not_found(e: &iceberg::Error) -> bool {
    matches!(
        e.kind(),
        iceberg::ErrorKind::NamespaceNotFound | iceberg::ErrorKind::TableNotFound
    ) || (e.kind() == iceberg::ErrorKind::Unexpected && e.message().contains("does not exist"))
}

/// Same caveat as [`is_not_found`], for creation conflicts.
fn is_already_exists(e: &iceberg::Error) -> bool {
    matches!(
        e.kind(),
        iceberg::ErrorKind::NamespaceAlreadyExists | iceberg::ErrorKind::TableAlreadyExists
    ) || (e.kind() == iceberg::ErrorKind::Unexpected && e.message().contains("already exists"))
}

impl CatalogHandle {
    /// Connect to the configured REST catalog with S3 storage.
    pub async fn connect(config: &IcebergSinkConfig) -> Result<Self> {
        let storage = Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        });

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(storage)
            .load("dbmazz", config.catalog_props())
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to Iceberg REST catalog at {}",
                    config.catalog_uri
                )
            })?;

        let namespace = NamespaceIdent::from_vec(vec![config.namespace.clone()])
            .context("Invalid Iceberg namespace")?;

        Ok(Self {
            catalog: Arc::new(catalog),
            namespace,
        })
    }

    /// The underlying catalog, for transaction commits.
    pub fn as_catalog(&self) -> &dyn Catalog {
        self.catalog.as_ref()
    }

    /// Reachability probe used by `validate_connection` — a read-only
    /// round-trip that exercises auth and routing without writing.
    ///
    /// Uses `list_namespaces` (GET) rather than `namespace_exists`: some
    /// REST catalog servers (e.g. tabulario/iceberg-rest) reject the
    /// spec's HEAD endpoints with 400.
    pub async fn probe(&self) -> Result<()> {
        self.catalog
            .list_namespaces(None)
            .await
            .context("Iceberg catalog probe failed")?;
        Ok(())
    }

    /// Create the target namespace if it does not exist.
    ///
    /// Existence is checked with `get_namespace` (GET) instead of
    /// `namespace_exists` (HEAD) for server compatibility; a concurrent
    /// creation race resolves via `NamespaceAlreadyExists`.
    pub async fn ensure_namespace(&self) -> Result<()> {
        match self.catalog.get_namespace(&self.namespace).await {
            Ok(_) => return Ok(()),
            Err(e) if is_not_found(&e) => {}
            Err(e) => {
                return Err(e).context("Failed to check namespace existence");
            }
        }

        match self
            .catalog
            .create_namespace(&self.namespace, HashMap::new())
            .await
        {
            Ok(_) => {
                info!(namespace = ?self.namespace, "Created Iceberg namespace");
                Ok(())
            }
            Err(e) if is_already_exists(&e) => Ok(()),
            Err(e) => {
                Err(e).with_context(|| format!("Failed to create namespace '{:?}'", self.namespace))
            }
        }
    }

    fn ident_for(&self, source: &SourceTableSchema) -> TableIdent {
        TableIdent::new(
            self.namespace.clone(),
            iceberg_table_name(&source.schema, &source.name),
        )
    }

    /// `TableIdent` for a pipeline `qualified_name` ("schema.table").
    pub fn ident_for_qualified(&self, qualified_name: &str) -> TableIdent {
        let (schema, table) = match qualified_name.split_once('.') {
            Some((s, t)) => (s, t),
            None => ("", qualified_name),
        };
        TableIdent::new(self.namespace.clone(), iceberg_table_name(schema, table))
    }

    /// Load a table by pipeline qualified name.
    pub async fn load_table(&self, qualified_name: &str) -> Result<Table> {
        let ident = self.ident_for_qualified(qualified_name);
        self.catalog
            .load_table(&ident)
            .await
            .with_context(|| format!("Failed to load Iceberg table '{}'", ident))
    }

    /// Ensure the Iceberg table for a source table exists and is
    /// column-compatible. Returns the (created or loaded) table.
    ///
    /// Existence is checked by loading (GET) rather than `table_exists`
    /// (HEAD), which some REST catalog servers reject with 400.
    pub async fn ensure_table(&self, source: &SourceTableSchema) -> Result<Table> {
        let ident = self.ident_for(source);

        match self.catalog.load_table(&ident).await {
            Ok(table) => {
                validate_existing_schema(source, table.metadata().current_schema())?;
                info!(table = %ident, "Reusing existing Iceberg table");
                return Ok(table);
            }
            Err(e) if is_not_found(&e) => {}
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("Failed to load existing table '{}'", ident));
            }
        }

        let schema = build_iceberg_schema(source)?;
        let creation = TableCreation::builder()
            .name(ident.name().to_string())
            .schema(schema)
            .build();

        let table = match self.catalog.create_table(&self.namespace, creation).await {
            Ok(table) => table,
            // Lost a creation race (another worker) — load and validate.
            Err(e) if is_already_exists(&e) => {
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .with_context(|| format!("Failed to load table '{}' after race", ident))?;
                validate_existing_schema(source, table.metadata().current_schema())?;
                table
            }
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("Failed to create Iceberg table '{}'", ident));
            }
        };

        info!(
            table = %ident,
            columns = source.columns.len(),
            "Created Iceberg table"
        );
        Ok(table)
    }
}

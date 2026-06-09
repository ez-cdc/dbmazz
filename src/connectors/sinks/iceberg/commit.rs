// Copyright 2025
// Licensed under the Elastic License v2.0

//! Iceberg commit manager.
//!
//! Commits staged Parquet files as Iceberg snapshots using AppendFiles
//! (INSERT/UPDATE) and RowDelta (DELETE) protocols.

use anyhow::{Context, Result};
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use tracing::info;

use super::catalog::IcebergCatalog;
use super::client::S3Client;
use super::config::IcebergSinkConfig;

/// Tracks staged files for a single table before commit.
pub struct StagedFiles {
    /// Table name (e.g., "public.users")
    pub table_name: String,
    /// Files accumulated for this table
    pub files: Vec<StagedFile>,
    /// Total bytes accumulated
    pub total_bytes: u64,
}

/// A single staged Parquet file.
pub struct StagedFile {
    /// File name in the staging prefix
    pub file_name: String,
    /// Number of records in the file
    pub record_count: usize,
    /// File size in bytes
    pub byte_count: u64,
    /// Whether this file contains DELETE operations
    pub has_deletes: bool,
}

impl StagedFiles {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            files: Vec::new(),
            total_bytes: 0,
        }
    }

    pub fn add_file(
        &mut self,
        file_name: String,
        record_count: usize,
        byte_count: u64,
        has_deletes: bool,
    ) {
        self.total_bytes += byte_count;
        self.files.push(StagedFile {
            file_name,
            record_count,
            byte_count,
            has_deletes,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

/// Commit manager for Iceberg tables.
pub struct CommitManager {
    s3_client: S3Client,
    config: IcebergSinkConfig,
    batch_counter: i64,
}

impl CommitManager {
    pub fn new(s3_client: S3Client, config: IcebergSinkConfig) -> Self {
        Self {
            s3_client,
            config,
            batch_counter: 0,
        }
    }

    /// Move staged files to the data prefix and commit an Iceberg snapshot.
    ///
    /// The commit flow:
    /// 1. Copy each staged file from staging prefix to data prefix via S3 CopyObject
    /// 2. Construct `DataFile` entries for the Iceberg manifest
    /// 3. Load the target Iceberg table from the catalog
    /// 4. Create a `Transaction` with a `FastAppend` action
    /// 5. Commit the transaction to the catalog (creates a new snapshot)
    /// 6. Clean up staging files on success
    pub async fn commit_staged_files(
        &mut self,
        staged: &mut StagedFiles,
        catalog: &IcebergCatalog,
    ) -> Result<i64> {
        if staged.is_empty() {
            return Ok(0);
        }

        self.batch_counter += 1;
        let batch_id = self.batch_counter;

        // Parse table name into namespace and table for catalog operations
        let parts: Vec<&str> = staged.table_name.splitn(2, '.').collect();
        let (namespace, table_name) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("default", parts[0])
        };

        // Load the Iceberg table from the catalog
        let table = catalog
            .load_table(namespace, table_name)
            .await
            .with_context(|| {
                format!(
                    "Failed to load table '{}' for commit batch {}",
                    staged.table_name, batch_id
                )
            })?;

        // Build an S3 URI prefix for data file paths
        let bucket = &self.config.bucket;
        // Strip trailing slash from prefix for clean URI construction
        let base_prefix = self.config.prefix.trim_end_matches('/');

        let data_prefix_path = if base_prefix.is_empty() {
            format!("s3://{}/", bucket)
        } else {
            format!("s3://{}/{}/", bucket, base_prefix)
        };

        // Phase 1: Copy each staged file from staging to data prefix
        let mut data_files = Vec::with_capacity(staged.files.len());

        for file in &staged.files {
            let staging_key = S3Client::staging_key(&staged.table_name, &file.file_name);
            let data_key = S3Client::data_key(&staged.table_name, &file.file_name);

            // Copy from staging to data location using S3 CopyObject
            self.s3_client
                .copy_object(&staging_key, &data_key)
                .await
                .with_context(|| {
                    format!(
                        "Failed to copy staged file '{}' to data prefix for table '{}'",
                        file.file_name, staged.table_name
                    )
                })?;

            // Construct the full data file path URI (Iceberg uses URIs, not bare keys)
            let file_path = format!("{}{}", data_prefix_path, data_key);

            let data_file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(file_path)
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file.byte_count)
                .record_count(file.record_count as u64)
                .build()
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to build DataFile entry for '{}': {}",
                        file.file_name,
                        e
                    )
                })?;

            data_files.push(data_file);
        }

        // Phase 2: Create Iceberg snapshot via catalog transaction
        {
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);

            // Apply the fast-append action to the transaction
            let tx = action
                .apply(tx)
                .map_err(|e| anyhow::anyhow!("Failed to apply append action: {}", e))?;

            // Commit the transaction to the catalog
            tx.commit(catalog.as_catalog_ref()).await.with_context(|| {
                format!(
                    "Failed to commit Iceberg snapshot for table '{}' batch {}",
                    staged.table_name, batch_id
                )
            })?;
        }

        // Phase 3: Clean up staging files after successful commit
        let staging_keys: Vec<String> = staged
            .files
            .iter()
            .map(|f| {
                let staging_key = S3Client::staging_key(&staged.table_name, &f.file_name);
                self.s3_client.full_key(&staging_key)
            })
            .collect();

        if !staging_keys.is_empty() {
            self.s3_client
                .delete_objects(&staging_keys)
                .await
                .with_context(|| {
                    format!(
                        "Failed to clean up staging files for table '{}' batch {}",
                        staged.table_name, batch_id
                    )
                })?;
        }

        info!(
            "Committed batch {}: {} files, {} records, {} bytes for table '{}'",
            batch_id,
            staged.file_count(),
            staged.files.iter().map(|f| f.record_count).sum::<usize>(),
            staged.total_bytes,
            staged.table_name
        );

        staged.files.clear();
        staged.total_bytes = 0;

        Ok(batch_id)
    }

    pub fn next_batch_id(&mut self) -> i64 {
        self.batch_counter += 1;
        self.batch_counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_staged_files_tracking() {
        let mut staged = StagedFiles::new("public.users".to_string());
        assert!(staged.is_empty());

        staged.add_file("batch_1_uuid.parquet".to_string(), 100, 4096, false);
        assert!(!staged.is_empty());
        assert_eq!(staged.file_count(), 1);
        assert_eq!(staged.total_bytes, 4096);

        staged.add_file("batch_2_uuid.parquet".to_string(), 200, 8192, true);
        assert_eq!(staged.file_count(), 2);
        assert_eq!(staged.total_bytes, 12288);
    }
}

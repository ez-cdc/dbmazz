// Copyright 2025
// Licensed under the Elastic License v2.0

//! S3-compatible object store client for the Iceberg sink.
//!
//! Wraps the AWS SDK for Rust S3 client with configurable auth
//! (static keys, default chain, role assumption) and endpoint
//! override for S3-compatible stores (MinIO, GCS, LocalStack).

use anyhow::{Context, Result};
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3InnerClient;
use bytes::Bytes;

use super::config::IcebergSinkConfig;

/// Maximum keys per DeleteObjects request.
const MAX_DELETE_KEYS: usize = 1000;

/// S3-compatible object store client.
#[derive(Clone)]
pub struct S3Client {
    client: S3InnerClient,
    bucket: String,
    prefix: String,
    endpoint: String,
}

impl std::fmt::Debug for S3Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Client")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl S3Client {
    /// Build a new S3 client from Iceberg sink configuration.
    pub async fn new(config: &IcebergSinkConfig) -> Result<Self> {
        let region = Region::new(config.region.clone());

        let mut s3_config_builder = aws_sdk_s3::config::Builder::new()
            .region(region)
            .force_path_style(config.force_path_style);

        if !config.access_key_id.is_empty() && !config.secret_access_key.is_empty() {
            s3_config_builder = s3_config_builder.credentials_provider(Credentials::new(
                &config.access_key_id,
                &config.secret_access_key,
                None,
                None,
                "static",
            ));
        } else if !config.role_arn.is_empty() {
            // Use STS AssumeRoleProvider for proper role assumption.
            // Use static keys or default chain when role_arn is not set
            // Role assumption via STS is handled by the AWS SDK's default chain
            // For explicit STS, use AssumeRoleProvider which requires owned strings
            let role_arn = config.role_arn.clone();
            let region_str = config.region.clone();
            let provider = AssumeRoleProvider::builder(&role_arn)
                .region(Region::new(region_str))
                .build()
                .await;
            s3_config_builder = s3_config_builder.credentials_provider(provider);
        }

        // Use custom endpoint if provided
        if !config.endpoint.is_empty() {
            s3_config_builder = s3_config_builder.endpoint_url(&config.endpoint);
        }

        let s3_config = s3_config_builder.build();
        let client = S3InnerClient::from_conf(s3_config);

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            endpoint: config.endpoint.clone(),
        })
    }

    /// Returns the full S3 key for a given path.
    pub(crate) fn full_key(&self, key: &str) -> String {
        format!(
            "{}/{}",
            self.prefix.trim_end_matches('/'),
            key.trim_start_matches('/')
        )
    }

    /// Upload a single object to S3.
    pub async fn put_object(&self, key: &str, body: Bytes) -> Result<()> {
        let full_key = self.full_key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(ByteStream::from(body))
            .send()
            .await
            .with_context(|| format!("Failed to upload s3://{}/{}", self.bucket, full_key))?;
        Ok(())
    }

    /// Upload an object from a byte stream.
    pub async fn put_object_stream(&self, key: &str, body: ByteStream) -> Result<()> {
        let full_key = self.full_key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(body)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to upload stream to s3://{}/{}",
                    self.bucket, full_key
                )
            })?;
        Ok(())
    }

    /// List objects under a given prefix.
    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let mut keys = Vec::new();
        let mut token = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);
            if let Some(t) = token {
                req = req.continuation_token(t);
            }

            let resp = req
                .send()
                .await
                .with_context(|| format!("Failed to list s3://{}/{}", self.bucket, full_prefix))?;

            let contents = resp.contents();
            for obj in contents {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }

            if resp.is_truncated() == Some(true) {
                token = resp.next_continuation_token().map(|t| t.to_string());
            } else {
                break;
            }
        }

        Ok(keys)
    }

    /// Batch delete objects from S3.
    pub async fn delete_objects(&self, keys: &[String]) -> Result<()> {
        for chunk in keys.chunks(MAX_DELETE_KEYS) {
            let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = chunk
                .iter()
                .map(|k| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(k)
                        .build()
                        .expect("valid object identifier")
                })
                .collect();

            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(
                    aws_sdk_s3::types::Delete::builder()
                        .set_objects(Some(objects))
                        .build()
                        .expect("valid delete"),
                )
                .send()
                .await
                .with_context(|| {
                    format!(
                        "Failed to delete {} objects from {}",
                        chunk.len(),
                        self.bucket
                    )
                })?;
        }
        Ok(())
    }

    /// Check if an object exists in S3.
    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        let full_key = self.full_key(key);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_not_found() {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!(
                        "Failed to check object existence: {}",
                        service_err
                    ))
                }
            }
        }
    }

    /// Validate connection by uploading and deleting a test object.
    pub async fn validate_connection(&self) -> Result<()> {
        let check_key = format!("_dbmazz_check_{}", uuid::Uuid::new_v4());
        let body = Bytes::from("ok");
        self.put_object(&check_key, body).await?;
        // put_object already applies self.full_key internally;
        // pass the raw check_key to avoid double-prefixing.
        self.delete_objects(&[check_key]).await?;
        Ok(())
    }

    /// Copy an object within the bucket (e.g. from staging to data prefix).
    pub(crate) async fn copy_object(&self, source_key: &str, dest_key: &str) -> Result<()> {
        let source_full = self.full_key(source_key);
        let dest_full = self.full_key(dest_key);
        let copy_source = format!("{}/{}", self.bucket, source_full);

        self.client
            .copy_object()
            .bucket(&self.bucket)
            .copy_source(copy_source)
            .key(&dest_full)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to copy s3://{}/{} -> s3://{}/{}",
                    self.bucket, source_full, self.bucket, dest_full
                )
            })?;
        Ok(())
    }

    /// Generate the staging key for an in-flight Parquet file.
    pub fn staging_key(table_name: &str, file_name: &str) -> String {
        format!("_staging/{}/{}", table_name, file_name)
    }

    /// Generate the data key for a committed Parquet file.
    pub fn data_key(table_name: &str, file_name: &str) -> String {
        format!("{}/{}", table_name, file_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_staging_key() {
        assert_eq!(
            S3Client::staging_key("public.users", "batch_1_uuid.parquet"),
            "_staging/public.users/batch_1_uuid.parquet"
        );
    }

    #[test]
    fn test_data_key() {
        assert_eq!(
            S3Client::data_key("public.users", "batch_1_uuid.parquet"),
            "public.users/batch_1_uuid.parquet"
        );
    }
}

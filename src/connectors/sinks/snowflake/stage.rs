// Copyright 2025
// Licensed under the Elastic License v2.0

//! Stage Manager for Snowflake file uploads.
//!
//! Implements the PUT protocol:
//! 1. Execute PUT command → Snowflake returns stageInfo with cloud credentials
//! 2. Upload file directly to cloud storage (S3/GCS/Azure) using temp credentials
//!
//! The endpoint `/queries/v1/query-request` is undocumented but used by all
//! official Snowflake drivers (Go, Python, Java, Node.js). It is de facto stable.

use anyhow::{anyhow, Context, Result};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, warn};

use super::client::SnowflakeClient;

/// Cloud storage type for the internal stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudType {
    S3,
    Gcs,
    Azure,
    LocalFs,
}

/// Credentials returned by Snowflake for direct cloud upload.
#[derive(Debug, Clone)]
pub enum CloudCredentials {
    S3 {
        key_id: String,
        secret_key: String,
        token: String,
    },
    Gcs {
        presigned_url: String,
    },
    Azure {
        sas_token: String,
        endpoint: String,
    },
}

/// Stage info returned by Snowflake PUT command.
#[derive(Debug, Clone)]
pub struct StageInfo {
    pub location_type: CloudType,
    pub location: String,
    pub path: String,
    pub credentials: CloudCredentials,
    pub region: String,
    pub endpoint: String,
}

/// Manages file uploads to a Snowflake internal stage.
pub struct StageManager {
    client: Arc<SnowflakeClient>,
    stage_name: String,
}

/// Internal struct for deserializing stageInfo from Snowflake response.
#[derive(Deserialize, Debug)]
struct StageInfoResponse {
    #[serde(rename = "locationType")]
    location_type: String,
    location: String,
    path: Option<String>,
    region: Option<String>,
    #[serde(rename = "endPoint")]
    end_point: Option<String>,
    creds: Option<StageCredsResponse>,
    #[serde(rename = "presignedUrl")]
    presigned_url: Option<String>,
}

#[derive(Deserialize, Debug)]
struct StageCredsResponse {
    #[serde(rename = "AWS_KEY_ID")]
    aws_key_id: Option<String>,
    #[serde(rename = "AWS_SECRET_KEY")]
    aws_secret_key: Option<String>,
    #[serde(rename = "AWS_TOKEN")]
    aws_token: Option<String>,
    #[serde(rename = "AZURE_SAS_TOKEN")]
    azure_sas_token: Option<String>,
}

impl StageManager {
    pub fn new(client: Arc<SnowflakeClient>, stage_name: String) -> Self {
        Self { client, stage_name }
    }

    /// Uploads a file (Parquet bytes) to the Snowflake internal stage.
    ///
    /// 1. Sends PUT command to get stageInfo (cloud credentials + location)
    /// 2. Uploads bytes directly to cloud storage
    pub async fn upload(&self, file_name: &str, data: Vec<u8>) -> Result<()> {
        let data_len = data.len();

        // Step 1: Get stageInfo from PUT command
        let stage_info_json = self
            .client
            .put_stage_info(&self.stage_name, file_name)
            .await
            .context("Failed to get stageInfo")?;

        let stage_info = parse_stage_info(stage_info_json)?;

        debug!(
            "Stage upload: {} → {:?} ({} bytes, location: {})",
            file_name, stage_info.location_type, data_len, stage_info.location
        );

        // Step 2: Upload to cloud storage
        match &stage_info.credentials {
            CloudCredentials::S3 {
                key_id,
                secret_key,
                token,
            } => {
                self.upload_s3(&stage_info, file_name, data, key_id, secret_key, token)
                    .await
            }
            CloudCredentials::Gcs { presigned_url } => self.upload_gcs(presigned_url, data).await,
            CloudCredentials::Azure {
                sas_token,
                endpoint,
            } => {
                self.upload_azure(&stage_info, file_name, data, sas_token, endpoint)
                    .await
            }
        }
    }

    /// Upload to S3 using temporary STS credentials.
    async fn upload_s3(
        &self,
        info: &StageInfo,
        file_name: &str,
        data: Vec<u8>,
        key_id: &str,
        secret_key: &str,
        token: &str,
    ) -> Result<()> {
        // Build S3 URL: https://bucket.s3.region.amazonaws.com/path/filename
        let parts: Vec<&str> = info.location.splitn(2, '/').collect();
        let bucket = parts[0];
        let prefix = if parts.len() > 1 { parts[1] } else { "" };
        let key = format!("{}{}{}", prefix, info.path, file_name);

        let region = if info.region.is_empty() {
            "us-east-1"
        } else {
            &info.region
        };

        let endpoint = if !info.endpoint.is_empty() {
            format!("https://{}", info.endpoint)
        } else {
            format!("https://{}.s3.{}.amazonaws.com", bucket, region)
        };

        let url = format!("{}/{}", endpoint, key);

        // Use AWS SigV4 signing via reqwest with manual headers
        // For simplicity, use the pre-signed approach with STS credentials
        let resp = self
            .client
            .http_client()
            .put(&url)
            .header("x-amz-security-token", token)
            .header(CONTENT_TYPE, "application/octet-stream")
            .header(CONTENT_LENGTH, data.len().to_string())
            .header("x-amz-server-side-encryption", "aws:kms")
            // AWS SDK-style auth header with STS credentials
            .basic_auth(key_id, Some(secret_key))
            .body(data)
            .send()
            .await
            .context("S3 upload failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "S3 upload failed (HTTP {}): {}",
                status,
                body.chars().take(500).collect::<String>()
            ));
        }

        debug!("S3 upload complete: {}", url);
        Ok(())
    }

    /// Upload to GCS using a presigned URL.
    async fn upload_gcs(&self, presigned_url: &str, data: Vec<u8>) -> Result<()> {
        let resp = self
            .client
            .http_client()
            .put(presigned_url)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(data)
            .send()
            .await
            .context("GCS upload failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "GCS upload failed (HTTP {}): {}",
                status,
                body.chars().take(500).collect::<String>()
            ));
        }

        debug!("GCS upload complete");
        Ok(())
    }

    /// Upload to Azure using a SAS token.
    async fn upload_azure(
        &self,
        info: &StageInfo,
        file_name: &str,
        data: Vec<u8>,
        sas_token: &str,
        endpoint: &str,
    ) -> Result<()> {
        let url = format!(
            "https://{}/{}{}/{}?{}",
            endpoint, info.location, info.path, file_name, sas_token
        );

        let resp = self
            .client
            .http_client()
            .put(&url)
            .header(CONTENT_TYPE, "application/octet-stream")
            .header("x-ms-blob-type", "BlockBlob")
            .body(data)
            .send()
            .await
            .context("Azure upload failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Azure upload failed (HTTP {}): {}",
                status,
                body.chars().take(500).collect::<String>()
            ));
        }

        debug!("Azure upload complete");
        Ok(())
    }
}

/// Parses the stageInfo JSON from a Snowflake PUT response.
fn parse_stage_info(json: serde_json::Value) -> Result<StageInfo> {
    let info: StageInfoResponse =
        serde_json::from_value(json).context("Failed to parse stageInfo")?;

    let location_type = match info.location_type.to_uppercase().as_str() {
        "S3" => CloudType::S3,
        "GCS" => CloudType::Gcs,
        "AZURE" => CloudType::Azure,
        "LOCAL_FS" => CloudType::LocalFs,
        other => {
            warn!("Unknown stage location type: {}, defaulting to S3", other);
            CloudType::S3
        }
    };

    let credentials = match location_type {
        CloudType::S3 => {
            let creds = info.creds.unwrap_or(StageCredsResponse {
                aws_key_id: None,
                aws_secret_key: None,
                aws_token: None,
                azure_sas_token: None,
            });
            CloudCredentials::S3 {
                key_id: creds.aws_key_id.unwrap_or_default(),
                secret_key: creds.aws_secret_key.unwrap_or_default(),
                token: creds.aws_token.unwrap_or_default(),
            }
        }
        CloudType::Gcs => CloudCredentials::Gcs {
            presigned_url: info.presigned_url.unwrap_or_default(),
        },
        CloudType::Azure => {
            let creds = info.creds.unwrap_or(StageCredsResponse {
                aws_key_id: None,
                aws_secret_key: None,
                aws_token: None,
                azure_sas_token: None,
            });
            CloudCredentials::Azure {
                sas_token: creds.azure_sas_token.unwrap_or_default(),
                endpoint: info.end_point.clone().unwrap_or_default(),
            }
        }
        CloudType::LocalFs => {
            return Err(anyhow!(
                "LOCAL_FS stage type is not supported for remote operations"
            ));
        }
    };

    Ok(StageInfo {
        location_type,
        location: info.location,
        path: info.path.unwrap_or_default(),
        credentials,
        region: info.region.unwrap_or_default(),
        endpoint: info.end_point.unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_stage_info_s3() {
        let json = serde_json::json!({
            "locationType": "S3",
            "location": "my-bucket/stage/prefix/",
            "path": "data/",
            "region": "us-east-1",
            "endPoint": null,
            "creds": {
                "AWS_KEY_ID": "AKIATEST",
                "AWS_SECRET_KEY": "secret123",
                "AWS_TOKEN": "token456"
            }
        });

        let info = parse_stage_info(json).unwrap();
        assert_eq!(info.location_type, CloudType::S3);
        assert_eq!(info.location, "my-bucket/stage/prefix/");
        assert_eq!(info.path, "data/");
        assert_eq!(info.region, "us-east-1");

        if let CloudCredentials::S3 {
            key_id,
            secret_key,
            token,
        } = &info.credentials
        {
            assert_eq!(key_id, "AKIATEST");
            assert_eq!(secret_key, "secret123");
            assert_eq!(token, "token456");
        } else {
            panic!("Expected S3 credentials");
        }
    }

    #[test]
    fn test_parse_stage_info_gcs() {
        let json = serde_json::json!({
            "locationType": "GCS",
            "location": "my-bucket/prefix/",
            "presignedUrl": "https://storage.googleapis.com/my-bucket/...",
            "creds": {}
        });

        let info = parse_stage_info(json).unwrap();
        assert_eq!(info.location_type, CloudType::Gcs);

        if let CloudCredentials::Gcs { presigned_url } = &info.credentials {
            assert!(presigned_url.starts_with("https://"));
        } else {
            panic!("Expected GCS credentials");
        }
    }

    #[test]
    fn test_parse_stage_info_azure() {
        let json = serde_json::json!({
            "locationType": "AZURE",
            "location": "container/path/",
            "endPoint": "myaccount.blob.core.windows.net",
            "creds": {
                "AZURE_SAS_TOKEN": "sv=2021-08-06&sig=abc"
            }
        });

        let info = parse_stage_info(json).unwrap();
        assert_eq!(info.location_type, CloudType::Azure);

        if let CloudCredentials::Azure {
            sas_token,
            endpoint,
        } = &info.credentials
        {
            assert!(sas_token.contains("sv="));
            assert!(endpoint.contains("blob.core.windows.net"));
        } else {
            panic!("Expected Azure credentials");
        }
    }
}

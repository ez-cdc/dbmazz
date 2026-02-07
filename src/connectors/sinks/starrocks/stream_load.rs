// Copyright 2025
// Licensed under the Elastic License v2.0

//! StarRocks Stream Load HTTP Client
//!
//! This module implements the HTTP Stream Load protocol for StarRocks data ingestion.
//! Stream Load is StarRocks' primary mechanism for high-throughput data loading.
//!
//! ## Protocol Details
//!
//! StarRocks Stream Load uses a two-phase protocol:
//!
//! 1. **FE (Frontend) Request**: Initial request goes to FE (typically port 8040)
//! 2. **307 Redirect**: FE returns a redirect to a BE (Backend) node
//! 3. **BE Request**: Actual data upload goes to BE
//!
//! The protocol requires proper handling of:
//! - `Expect: 100-continue` header for large payloads
//! - 307 redirects with potential 127.0.0.1 rewriting
//! - JSON format with array stripping
//!
//! ## Why libcurl?
//!
//! We use libcurl (via the `curl` crate) instead of reqwest/hyper because:
//! - Correct handling of `Expect: 100-continue` protocol
//! - Proven reliability with StarRocks Stream Load
//! - Better control over HTTP/1.1 chunked transfers

use anyhow::{Result, anyhow};
use curl::easy::{Easy, List};
use std::sync::Arc;
use tracing::{debug, info};

/// Result of a successful Stream Load operation.
#[derive(Debug, Clone)]
pub struct StreamLoadResult {
    /// Status from StarRocks (e.g., "Success", "Publish Timeout")
    #[allow(dead_code)]
    pub status: String,
    /// Number of rows successfully loaded
    pub loaded_rows: u64,
    /// Message from StarRocks
    #[allow(dead_code)]
    pub message: String,
}

/// Options for a Stream Load request.
#[derive(Debug, Clone, Default)]
pub struct StreamLoadOptions {
    /// Columns to update (for partial updates). If None, full row update.
    #[allow(dead_code)]
    pub partial_columns: Option<Vec<String>>,
    /// Maximum ratio of filtered (rejected) rows. Default: 0.0
    #[allow(dead_code)]
    pub max_filter_ratio: Option<f64>,
}

/// HTTP client for StarRocks Stream Load API.
///
/// This client handles the Stream Load protocol including:
/// - FE to BE redirect handling
/// - 127.0.0.1 address rewriting
/// - Proper `Expect: 100-continue` handling
pub struct StreamLoadClient {
    /// Base URL for FE (e.g., "http://starrocks:8040")
    base_url: String,
    /// Target database
    database: String,
    /// Username for authentication
    user: String,
    /// Password for authentication
    password: String,
}

impl StreamLoadClient {
    /// Creates a new Stream Load client.
    pub fn new(base_url: String, database: String, user: String, password: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            database,
            user,
            password,
        }
    }

    /// Verifies connectivity to the StarRocks HTTP endpoint.
    ///
    /// Performs a simple GET request to validate network connectivity.
    pub async fn verify_connection(&self) -> Result<()> {
        let url = self.base_url.clone();

        tokio::task::spawn_blocking(move || {
            let mut easy = Easy::new();
            easy.url(&url)?;
            easy.timeout(std::time::Duration::from_secs(10))?;
            easy.connect_timeout(std::time::Duration::from_secs(5))?;

            let mut response = Vec::new();
            {
                let mut transfer = easy.transfer();
                transfer.write_function(|data| {
                    response.extend_from_slice(data);
                    Ok(data.len())
                })?;
                transfer.perform()?;
            }

            let code = easy.response_code()?;
            if code >= 500 {
                return Err(anyhow!("StarRocks HTTP endpoint returned error: {}", code));
            }

            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Task join error: {}", e))?
    }

    /// Sends data to StarRocks via Stream Load.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Target table name
    /// * `body` - JSON array data to load
    /// * `options` - Stream Load options (partial update, filter ratio, etc.)
    ///
    /// # Returns
    ///
    /// Stream Load result with status and row count
    pub async fn send(
        &self,
        table_name: &str,
        body: Arc<Vec<u8>>,
        options: StreamLoadOptions,
    ) -> Result<StreamLoadResult> {
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            self.base_url, self.database, table_name
        );
        let user = self.user.clone();
        let password = self.password.clone();
        let table = table_name.to_string();

        // Execute in blocking context to avoid blocking async runtime
        tokio::task::spawn_blocking(move || {
            Self::send_sync(&url, &user, &password, &table, body, options)
        })
        .await
        .map_err(|e| anyhow!("Task join error: {}", e))?
    }

    /// Synchronous Stream Load implementation.
    fn send_sync(
        url: &str,
        user: &str,
        password: &str,
        table_name: &str,
        body: Arc<Vec<u8>>,
        options: StreamLoadOptions,
    ) -> Result<StreamLoadResult> {
        // Extract hostname for redirect rewriting
        let original_hostname = Self::extract_hostname(url)?;

        let mut easy = Easy::new();

        // Don't follow redirects automatically - we handle them manually
        easy.follow_location(false)?;

        // Configure request
        easy.url(url)?;
        easy.put(true)?;
        easy.username(user)?;
        easy.password(password)?;

        // Build headers
        let headers = Self::build_headers(&options)?;
        easy.http_headers(headers)?;

        // Configure body upload
        let body_len = body.len();
        easy.post_field_size(body_len as u64)?;
        easy.upload(true)?;

        let body_for_read = body.clone();
        let mut offset: usize = 0;
        easy.read_function(move |buf| {
            let remaining = &body_for_read[offset..];
            let to_copy = remaining.len().min(buf.len());
            if to_copy == 0 {
                return Ok(0);
            }
            buf[..to_copy].copy_from_slice(&remaining[..to_copy]);
            offset += to_copy;
            Ok(to_copy)
        })?;

        easy.timeout(std::time::Duration::from_secs(30))?;

        // Execute request and capture response
        let mut response_body = Vec::new();
        let mut redirect_location = None;

        {
            let mut transfer = easy.transfer();

            // Capture redirect location from headers
            transfer.header_function(|header| {
                let header_str = String::from_utf8_lossy(header);
                if header_str.to_lowercase().starts_with("location:") {
                    redirect_location = Some(header_str[9..].trim().to_string());
                }
                true
            })?;

            transfer.write_function(|data| {
                response_body.extend_from_slice(data);
                Ok(data.len())
            })?;

            transfer.perform()?;
        }

        let response_code = easy.response_code()?;

        // Handle 307 redirect to BE
        if response_code == 307 {
            if let Some(location) = redirect_location {
                // Rewrite 127.0.0.1 to original hostname (StarRocks bug)
                let corrected_location = if location.contains("127.0.0.1") {
                    let rewritten = location.replace("127.0.0.1", &original_hostname);
                    debug!("Redirect rewritten: {} -> {}", location, rewritten);
                    rewritten
                } else {
                    location
                };

                // Follow redirect to BE
                return Self::send_to_be(&corrected_location, user, password, options, body);
            }
        }

        // Parse response
        Self::parse_response(&response_body, response_code, table_name, &options)
    }

    /// Sends data to BE after redirect.
    fn send_to_be(
        be_url: &str,
        user: &str,
        password: &str,
        options: StreamLoadOptions,
        body: Arc<Vec<u8>>,
    ) -> Result<StreamLoadResult> {
        let mut easy = Easy::new();

        easy.url(be_url)?;
        easy.put(true)?;
        easy.username(user)?;
        easy.password(password)?;

        let headers = Self::build_headers(&options)?;
        easy.http_headers(headers)?;

        let body_len = body.len();
        easy.post_field_size(body_len as u64)?;
        easy.upload(true)?;

        let body_for_read = body.clone();
        let mut offset: usize = 0;
        easy.read_function(move |buf| {
            let remaining = &body_for_read[offset..];
            let to_copy = remaining.len().min(buf.len());
            if to_copy == 0 {
                return Ok(0);
            }
            buf[..to_copy].copy_from_slice(&remaining[..to_copy]);
            offset += to_copy;
            Ok(to_copy)
        })?;

        easy.timeout(std::time::Duration::from_secs(30))?;

        let mut response_body = Vec::new();
        {
            let mut transfer = easy.transfer();
            transfer.write_function(|data| {
                response_body.extend_from_slice(data);
                Ok(data.len())
            })?;
            transfer.perform()?;
        }

        let response_code = easy.response_code()?;
        Self::parse_response(&response_body, response_code, "BE", &options)
    }

    /// Builds HTTP headers for Stream Load request.
    fn build_headers(options: &StreamLoadOptions) -> Result<List> {
        let mut headers = List::new();

        // Required headers
        headers.append("Expect: 100-continue")?;
        headers.append("format: json")?;
        headers.append("strip_outer_array: true")?;
        headers.append("ignore_json_size: true")?;

        // Optional filter ratio
        if let Some(ratio) = options.max_filter_ratio {
            headers.append(&format!("max_filter_ratio: {}", ratio))?;
        }

        // Partial update headers
        if let Some(ref cols) = options.partial_columns {
            headers.append("partial_update: true")?;
            headers.append("partial_update_mode: row")?;
            headers.append(&format!("columns: {}", cols.join(",")))?;
        }

        Ok(headers)
    }

    /// Parses the Stream Load response.
    fn parse_response(
        response_body: &[u8],
        response_code: u32,
        table_name: &str,
        options: &StreamLoadOptions,
    ) -> Result<StreamLoadResult> {
        let response_str = String::from_utf8_lossy(response_body).to_string();

        let resp_json: serde_json::Value = serde_json::from_str(&response_str)
            .unwrap_or(serde_json::json!({
                "Status": "Unknown",
                "Message": response_str.clone()
            }));

        let status = resp_json["Status"].as_str().unwrap_or("Unknown").to_string();
        let loaded_rows = resp_json["NumberLoadedRows"].as_u64().unwrap_or(0);
        let message = resp_json["Message"].as_str().unwrap_or("").to_string();

        // Validate HTTP response
        if response_code >= 400 {
            return Err(anyhow!(
                "HTTP {}: {} - {}",
                response_code,
                status,
                message
            ));
        }

        // Validate StarRocks response
        // "Publish Timeout" is acceptable - data was written
        if status != "Success" && status != "Publish Timeout" {
            return Err(anyhow!("Stream Load failed: {} - {}", status, message));
        }

        let update_type = if options.partial_columns.is_some() {
            "partial"
        } else {
            "full"
        };

        debug!(
            "Sent {} rows to StarRocks ({}.{})",
            loaded_rows, table_name, update_type
        );

        Ok(StreamLoadResult {
            status,
            loaded_rows,
            message,
        })
    }

    /// Extracts hostname from URL.
    fn extract_hostname(url: &str) -> Result<String> {
        let url_parts: Vec<&str> = url.split('/').collect();
        if url_parts.len() < 3 {
            return Err(anyhow!("Invalid URL format: {}", url));
        }

        let host_port = url_parts[2];
        let hostname = host_port.split(':').next().unwrap_or(host_port);

        Ok(hostname.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_hostname() {
        assert_eq!(
            StreamLoadClient::extract_hostname("http://starrocks:8040/api/db/table").unwrap(),
            "starrocks"
        );

        assert_eq!(
            StreamLoadClient::extract_hostname("http://192.168.1.100:8040/api/db/table").unwrap(),
            "192.168.1.100"
        );

        assert!(StreamLoadClient::extract_hostname("invalid").is_err());
    }

    #[test]
    fn test_build_headers() {
        let options = StreamLoadOptions::default();
        let headers = StreamLoadClient::build_headers(&options);
        assert!(headers.is_ok());
    }

    #[test]
    fn test_build_headers_with_partial_update() {
        let options = StreamLoadOptions {
            partial_columns: Some(vec!["col1".to_string(), "col2".to_string()]),
            max_filter_ratio: Some(0.1),
        };
        let headers = StreamLoadClient::build_headers(&options);
        assert!(headers.is_ok());
    }

    #[test]
    fn test_parse_response_success() {
        let response = r#"{"Status": "Success", "NumberLoadedRows": 100, "Message": "OK"}"#;
        let result = StreamLoadClient::parse_response(
            response.as_bytes(),
            200,
            "test_table",
            &StreamLoadOptions::default(),
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.status, "Success");
        assert_eq!(result.loaded_rows, 100);
    }

    #[test]
    fn test_parse_response_failure() {
        let response = r#"{"Status": "Fail", "Message": "Column not found"}"#;
        let result = StreamLoadClient::parse_response(
            response.as_bytes(),
            200,
            "test_table",
            &StreamLoadOptions::default(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_response_http_error() {
        let response = r#"{"Status": "Error", "Message": "Internal error"}"#;
        let result = StreamLoadClient::parse_response(
            response.as_bytes(),
            500,
            "test_table",
            &StreamLoadOptions::default(),
        );

        assert!(result.is_err());
    }
}

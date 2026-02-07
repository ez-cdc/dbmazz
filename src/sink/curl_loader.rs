use anyhow::{Result, anyhow};
use curl::easy::{Easy, List};
use std::sync::Arc;
use tracing::{debug, info};

// TODO: This module handles FE->BE redirects from StarRocks with 127.0.0.1 rewriting.
// It has not been validated whether this implementation is optimal in terms of:
// - Performance (double request overhead, connection handling)
// - libcurl best practices (connection pooling, reuse, timeouts)
// - Redirect error handling (redirect limit, loops)
// Consider refactoring with an async HTTP client (hyper/reqwest) or validating with benchmarks.

/// Result of a Stream Load
#[derive(Debug)]
pub struct LoadResult {
    pub status: String,
    pub loaded_rows: u64,
    pub message: String,
}

/// Stream Load client using libcurl (supports Expect: 100-continue correctly)
pub struct CurlStreamLoader {
    base_url: String,
    database: String,
    user: String,
    pass: String,
}

impl CurlStreamLoader {
    pub fn new(base_url: String, database: String, user: String, pass: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            database,
            user,
            pass,
        }
    }

    /// Verifies that the StarRocks HTTP endpoint (port 8040) is accessible
    /// Performs a simple GET to the base endpoint to validate connectivity
    pub async fn verify_connection(&self) -> Result<()> {
        let url = self.base_url.clone();

        tokio::task::spawn_blocking(move || {
            let mut easy = Easy::new();
            easy.url(&url)?;
            easy.timeout(std::time::Duration::from_secs(10))?;
            easy.connect_timeout(std::time::Duration::from_secs(5))?;

            // We only want to verify connectivity, we don't care about the response
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

    /// Sends data to StarRocks via Stream Load (executes in thread pool to avoid blocking async)
    pub async fn send(
        &self,
        table_name: &str,
        body: Arc<Vec<u8>>,
        partial_columns: Option<Vec<String>>,
    ) -> Result<LoadResult> {
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            self.base_url, self.database, table_name
        );
        let user = self.user.clone();
        let pass = self.pass.clone();
        let table = table_name.to_string();
        let body = body.clone();

        // spawn_blocking to avoid blocking the async runtime
        tokio::task::spawn_blocking(move || {
            Self::send_sync(&url, &user, &pass, &table, body, partial_columns)
        })
        .await
        .map_err(|e| anyhow!("Task join error: {}", e))?
    }

    fn send_sync(
        url: &str,
        user: &str,
        pass: &str,
        table_name: &str,
        body: Arc<Vec<u8>>,
        partial_columns: Option<Vec<String>>,
    ) -> Result<LoadResult> {
        // Extract original hostname to rewrite 127.0.0.1 redirects
        let original_hostname = Self::extract_hostname(url)?;

        let mut easy = Easy::new();

        // DO NOT follow redirects automatically - we'll handle them manually
        easy.follow_location(false)?;

        // URL and PUT method
        easy.url(url)?;
        easy.put(true)?;

        // Basic authentication
        easy.username(user)?;
        easy.password(pass)?;

        // Headers
        let mut headers = List::new();
        headers.append("Expect: 100-continue")?; // CRITICAL: wait for confirmation before sending body
        headers.append("format: json")?;
        headers.append("strip_outer_array: true")?;
        headers.append("ignore_json_size: true")?;
        headers.append("max_filter_ratio: 0.2")?;

        // Partial update headers if present
        let partial_cols_clone = partial_columns.clone();
        if let Some(ref cols) = partial_columns {
            headers.append("partial_update: true")?;
            headers.append("partial_update_mode: row")?;
            headers.append(&format!("columns: {}", cols.join(",")))?;
            debug!("Partial update for {}: {} columns", table_name, cols.len());
        }

        easy.http_headers(headers)?;

        // Configure body - libcurl will handle 100-continue protocol correctly
        let body_len = body.len();
        easy.post_field_size(body_len as u64)?;
        easy.upload(true)?;  // Enable upload mode for PUT
        
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
        
        // Timeout
        easy.timeout(std::time::Duration::from_secs(30))?;

        // Buffer for response and headers
        let mut response_body = Vec::new();
        let mut redirect_location = None;

        {
            let mut transfer = easy.transfer();

            // Capture headers to detect redirects
            transfer.header_function(|header| {
                let header_str = String::from_utf8_lossy(header);
                if header_str.to_lowercase().starts_with("location:") {
                    redirect_location = Some(
                        header_str[9..].trim().to_string()
                    );
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

        // If it's a redirect (307), follow it manually with hostname rewriting
        if response_code == 307 {
            if let Some(location) = redirect_location {
                // Rewrite 127.0.0.1 with original hostname
                let corrected_location = if location.contains("127.0.0.1") {
                    let rewritten = location.replace("127.0.0.1", &original_hostname);
                    debug!("Redirect rewritten: {} -> {}", location, rewritten);
                    rewritten
                } else {
                    location
                };

                // Make second request to BE (redirect)
                return Self::send_to_be(&corrected_location, user, pass, partial_cols_clone, body.clone());
            }
        }

        let response_body = String::from_utf8_lossy(&response_body).to_string();

        // Parse JSON response
        let resp_json: serde_json::Value = serde_json::from_str(&response_body)
            .unwrap_or(serde_json::json!({"Status": "Unknown", "Message": response_body.clone()}));
        
        let status = resp_json["Status"].as_str().unwrap_or("Unknown").to_string();
        let loaded_rows = resp_json["NumberLoadedRows"].as_u64().unwrap_or(0);
        let message = resp_json["Message"].as_str().unwrap_or("").to_string();
        
        // Validate HTTP response
        if response_code >= 400 {
            return Err(anyhow!(
                "HTTP {}: {} - {}", 
                response_code, status, message
            ));
        }
        
        // Validate StarRocks response
        if status != "Success" && status != "Publish Timeout" {
            // "Publish Timeout" is OK - the data was written
            return Err(anyhow!(
                "Stream Load failed: {} - {}", 
                status, message
            ));
        }


        debug!(
            "Sent {} rows to StarRocks ({}.{})",
            loaded_rows,
            table_name.split('.').next_back().unwrap_or(table_name),
            if partial_columns.is_some() { "partial" } else { "full" }
        );

        Ok(LoadResult {
            status,
            loaded_rows,
            message,
        })
    }


    /// Extracts the hostname from a URL (e.g., "http://starrocks:8030" -> "starrocks")
    fn extract_hostname(url: &str) -> Result<String> {
        let url_parts: Vec<&str> = url.split('/').collect();
        if url_parts.len() < 3 {
            return Err(anyhow!("Invalid URL format"));
        }
        
        let host_port = url_parts[2];
        let hostname = host_port.split(':').next().unwrap_or(host_port);
        
        Ok(hostname.to_string())
    }


    /// Sends data to BE after following a redirect (second request)
    fn send_to_be(
        be_url: &str,
        user: &str,
        pass: &str,
        partial_columns: Option<Vec<String>>,
        body: Arc<Vec<u8>>,
    ) -> Result<LoadResult> {
        let mut easy = Easy::new();

        // BE URL (already corrected)
        easy.url(be_url)?;
        easy.put(true)?;

        // Authentication
        easy.username(user)?;
        easy.password(pass)?;

        // Recreate headers (List is not Clone)
        let mut headers = List::new();
        headers.append("Expect: 100-continue")?;
        headers.append("format: json")?;
        headers.append("strip_outer_array: true")?;
        headers.append("ignore_json_size: true")?;
        headers.append("max_filter_ratio: 0.2")?;
        
        if let Some(ref cols) = partial_columns {
            headers.append("partial_update: true")?;
            headers.append("partial_update_mode: row")?;
            headers.append(&format!("columns: {}", cols.join(",")))?;
        }


        easy.http_headers(headers)?;

        // Configure body
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


        // Timeout
        easy.timeout(std::time::Duration::from_secs(30))?;

        // Buffer for response
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
        let response_body = String::from_utf8_lossy(&response_body).to_string();

        // Parse JSON response
        let resp_json: serde_json::Value = serde_json::from_str(&response_body)
            .unwrap_or(serde_json::json!({"Status": "Unknown", "Message": response_body.clone()}));
        
        let status = resp_json["Status"].as_str().unwrap_or("Unknown").to_string();
        let loaded_rows = resp_json["NumberLoadedRows"].as_u64().unwrap_or(0);
        let message = resp_json["Message"].as_str().unwrap_or("").to_string();
        
        // Validate HTTP response
        if response_code >= 400 {
            return Err(anyhow!(
                "HTTP {}: {} - {}", 
                response_code, status, message
            ));
        }
        
        // Validar respuesta de StarRocks
        if status != "Success" && status != "Publish Timeout" {
            return Err(anyhow!(
                "Stream Load failed: {} - {}", 
                status, message
            ));
        }
        
        Ok(LoadResult {
            status,
            loaded_rows,
            message,
        })
    }
}

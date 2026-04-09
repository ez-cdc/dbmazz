// Copyright 2025
// Licensed under the Elastic License v2.0

//! Snowflake HTTP Client
//!
//! Handles session authentication (password + JWT key-pair) and SQL execution
//! via the `/queries/v1/query-request` endpoint. All Snowflake communication
//! goes through this client, including PUT stage operations.
//!
//! ## Auth Flows
//!
//! - **Password**: POST `/session/v1/login-request` with LOGIN_NAME + PASSWORD
//! - **JWT key-pair**: Load RSA private key, generate JWT, POST with AUTHENTICATOR=SNOWFLAKE_JWT

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::config::SnowflakeSinkConfig;

/// Query execution status
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryStatus {
    Running,
    Success,
    Failed(String),
}

/// Result of a SQL query execution.
#[derive(Debug, Clone)]
pub struct QueryResult {
    #[allow(dead_code)]
    pub success: bool,
    pub data: Option<serde_json::Value>,
    #[allow(dead_code)]
    pub query_id: String,
    #[allow(dead_code)]
    pub message: String,
}

/// Snowflake HTTP client for session management and SQL execution.
pub struct SnowflakeClient {
    http: reqwest::Client,
    account_url: String,
    session: Arc<RwLock<SessionState>>,
    config: SnowflakeSinkConfig,
}

struct SessionState {
    token: String,
    master_token: String,
}

// ---------------------------------------------------------------------------
// Login request/response types (serde)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct LoginRequest {
    data: LoginData,
}

#[derive(Serialize)]
struct LoginData {
    #[serde(rename = "LOGIN_NAME")]
    login_name: String,
    #[serde(rename = "PASSWORD", skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(rename = "AUTHENTICATOR", skip_serializing_if = "Option::is_none")]
    authenticator: Option<String>,
    #[serde(rename = "TOKEN", skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    #[serde(rename = "ACCOUNT_NAME")]
    account_name: String,
    #[serde(rename = "CLIENT_APP_ID")]
    client_app_id: String,
    #[serde(rename = "CLIENT_APP_VERSION")]
    client_app_version: String,
}

#[derive(Deserialize)]
struct LoginResponse {
    data: Option<LoginResponseData>,
    success: bool,
    message: Option<String>,
}

#[derive(Deserialize)]
struct LoginResponseData {
    token: Option<String>,
    #[serde(rename = "masterToken")]
    master_token: Option<String>,
}

// ---------------------------------------------------------------------------
// Query request/response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct QueryRequest {
    #[serde(rename = "sqlText")]
    sql_text: String,
    #[serde(rename = "asyncExec")]
    async_exec: bool,
    #[serde(rename = "sequenceId")]
    sequence_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<QueryParameters>,
}

#[derive(Serialize)]
struct QueryParameters {
    #[serde(rename = "QUERY_TAG", skip_serializing_if = "Option::is_none")]
    query_tag: Option<String>,
    #[serde(
        rename = "MULTI_STATEMENT_COUNT",
        skip_serializing_if = "Option::is_none"
    )]
    multi_statement_count: Option<String>,
}

#[derive(Deserialize)]
struct QueryResponse {
    data: Option<QueryResponseData>,
    success: bool,
    message: Option<String>,
    code: Option<String>,
}

#[derive(Deserialize)]
struct QueryResponseData {
    #[serde(rename = "queryId", default)]
    query_id: String,
    #[serde(rename = "rowset", default)]
    rowset: Option<serde_json::Value>,
    #[serde(rename = "rowtype", default)]
    #[allow(dead_code)]
    rowtype: Option<serde_json::Value>,
    // Stage info fields (populated for PUT commands)
    #[serde(rename = "stageInfo", default)]
    pub stage_info: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Token refresh request
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct TokenRenewRequest {
    data: TokenRenewData,
}

#[derive(Serialize)]
struct TokenRenewData {
    #[serde(rename = "oldSessionToken")]
    old_session_token: String,
    #[serde(rename = "requestType")]
    request_type: String,
}

#[derive(Deserialize)]
struct TokenRenewResponse {
    data: Option<TokenRenewResponseData>,
    success: bool,
    message: Option<String>,
}

#[derive(Deserialize)]
struct TokenRenewResponseData {
    #[serde(rename = "sessionToken")]
    session_token: Option<String>,
}

impl SnowflakeClient {
    /// Connects to Snowflake and establishes a session.
    pub async fn connect(config: &SnowflakeSinkConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;

        let account_url = config.account_url.trim_end_matches('/').to_string();

        let client = Self {
            http,
            account_url,
            session: Arc::new(RwLock::new(SessionState {
                token: String::new(),
                master_token: String::new(),
            })),
            config: config.clone(),
        };

        client.login().await?;
        client.use_context().await?;

        Ok(client)
    }

    /// Authenticates with Snowflake via password or JWT key-pair.
    async fn login(&self) -> Result<()> {
        let login_data = if self.config.use_jwt_auth() {
            let jwt = self.generate_jwt()?;
            LoginData {
                login_name: self.config.user.clone(),
                password: None,
                authenticator: Some("SNOWFLAKE_JWT".to_string()),
                token: Some(jwt),
                account_name: self.config.account.clone(),
                client_app_id: "dbmazz".to_string(),
                client_app_version: env!("CARGO_PKG_VERSION").to_string(),
            }
        } else {
            LoginData {
                login_name: self.config.user.clone(),
                password: Some(self.config.password.clone()),
                authenticator: None,
                token: None,
                account_name: self.config.account.clone(),
                client_app_id: "dbmazz".to_string(),
                client_app_version: env!("CARGO_PKG_VERSION").to_string(),
            }
        };

        let url = format!(
            "{}/session/v1/login-request?databaseName={}&schemaName={}&warehouse={}",
            self.account_url, self.config.database, self.config.schema, self.config.warehouse
        );

        let resp = self
            .http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .json(&LoginRequest { data: login_data })
            .send()
            .await
            .context("Failed to connect to Snowflake")?;

        let status = resp.status();
        let body: LoginResponse = resp
            .json()
            .await
            .context("Failed to parse login response")?;

        if !body.success {
            return Err(anyhow!(
                "Snowflake login failed (HTTP {}): {}",
                status,
                body.message.unwrap_or_else(|| "unknown error".to_string())
            ));
        }

        let data = body
            .data
            .ok_or_else(|| anyhow!("No data in login response"))?;
        let token = data
            .token
            .ok_or_else(|| anyhow!("No session token in login response"))?;
        let master_token = data.master_token.unwrap_or_default();

        {
            let mut session = self.session.write().await;
            session.token = token;
            session.master_token = master_token;
        }

        info!(
            "Snowflake session established (account: {})",
            self.config.account
        );
        Ok(())
    }

    /// Sets USE WAREHOUSE and USE ROLE if configured.
    async fn use_context(&self) -> Result<()> {
        self.execute(&format!("USE WAREHOUSE {}", self.config.warehouse))
            .await?;

        if !self.config.role.is_empty() {
            self.execute(&format!("USE ROLE {}", self.config.role))
                .await?;
        }

        self.execute(&format!("USE DATABASE {}", self.config.database))
            .await?;

        Ok(())
    }

    /// Generates a JWT token for key-pair auth.
    fn generate_jwt(&self) -> Result<String> {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

        let pem_bytes = std::fs::read(&self.config.private_key_path)
            .context("Failed to read private key file")?;

        let pem_str = std::str::from_utf8(&pem_bytes)?;

        let encoding_key = if !self.config.private_key_passphrase.is_empty() {
            // Encrypted key — use PKCS#8 with passphrase
            // The rsa + pem crates handle decryption
            let pem_parsed = pem::parse(pem_str)?;
            let der = pem_parsed.contents();
            // Decrypt if needed (pkcs8 encrypted PEM)
            // For now, try to load directly — encrypted keys need additional handling
            EncodingKey::from_rsa_der(der)
        } else {
            EncodingKey::from_rsa_pem(&pem_bytes)?
        };

        // Generate public key fingerprint: SHA256 of DER-encoded public key
        let fingerprint = self.public_key_fingerprint(&pem_bytes)?;

        // Account name uppercase, dot-separated parts only (no region URL)
        let account_upper = self
            .config
            .account
            .split('.')
            .next()
            .unwrap_or(&self.config.account)
            .to_uppercase();
        let user_upper = self.config.user.to_uppercase();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = serde_json::json!({
            "iss": format!("{}.{}.SHA256:{}", account_upper, user_upper, fingerprint),
            "sub": format!("{}.{}", account_upper, user_upper),
            "iat": now,
            "exp": now + (59 * 60), // 59 minutes
        });

        let header = Header::new(Algorithm::RS256);
        let token = encode(&header, &claims, &encoding_key)?;

        debug!("JWT generated for {}.{}", account_upper, user_upper);
        Ok(token)
    }

    /// Computes SHA256 fingerprint of the public key (for JWT issuer claim).
    fn public_key_fingerprint(&self, pem_bytes: &[u8]) -> Result<String> {
        let pem_str = std::str::from_utf8(pem_bytes)?;
        let pem_parsed = pem::parse(pem_str)?;
        let der = pem_parsed.contents();

        // Extract public key from private key DER
        use rsa::pkcs8::DecodePrivateKey;
        let private_key = rsa::RsaPrivateKey::from_pkcs8_der(der)
            .or_else(|_| {
                // Try PEM format directly
                rsa::RsaPrivateKey::from_pkcs8_pem(pem_str)
            })
            .context("Failed to parse RSA private key")?;

        use rsa::pkcs8::EncodePublicKey;
        let public_key_der = private_key
            .to_public_key()
            .to_public_key_der()
            .context("Failed to encode public key to DER")?;

        let mut hasher = Sha256::new();
        hasher.update(public_key_der.as_bytes());
        let hash = hasher.finalize();

        Ok(base64::engine::general_purpose::STANDARD.encode(hash))
    }

    /// Executes a SQL statement synchronously and returns the result.
    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        debug!("Snowflake SQL: {}", sql);

        let token = self.session.read().await.token.clone();
        let request_id = uuid_v4();

        let url = format!(
            "{}/queries/v1/query-request?requestId={}",
            self.account_url, request_id
        );

        let resp = self
            .http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Snowflake Token=\"{}\"", token))
            .json(&QueryRequest {
                sql_text: sql.to_string(),
                async_exec: false,
                sequence_id: 0,
                parameters: Some(QueryParameters {
                    query_tag: Some("dbmazz-cdc".to_string()),
                    multi_statement_count: None,
                }),
            })
            .send()
            .await
            .context("Snowflake query request failed")?;

        let status = resp.status();
        let body: QueryResponse = resp
            .json()
            .await
            .context("Failed to parse query response")?;

        if !body.success {
            // Check for session token expiry
            if body.code.as_deref() == Some("390112") {
                warn!("Snowflake session expired, refreshing...");
                self.refresh_token().await?;
                // Retry once
                return self.execute_inner(sql).await;
            }

            return Err(anyhow!(
                "Snowflake query failed (HTTP {}, code: {}): {}\nSQL: {}",
                status,
                body.code.unwrap_or_default(),
                body.message.unwrap_or_else(|| "unknown error".to_string()),
                sql,
            ));
        }

        let data = body.data;
        let query_id = data
            .as_ref()
            .map(|d| d.query_id.clone())
            .unwrap_or_default();

        Ok(QueryResult {
            success: true,
            data: data.and_then(|d| d.rowset),
            query_id,
            message: body.message.unwrap_or_default(),
        })
    }

    /// Internal execute without retry (to avoid infinite recursion on token refresh).
    async fn execute_inner(&self, sql: &str) -> Result<QueryResult> {
        let token = self.session.read().await.token.clone();
        let request_id = uuid_v4();

        let url = format!(
            "{}/queries/v1/query-request?requestId={}",
            self.account_url, request_id
        );

        let resp = self
            .http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Snowflake Token=\"{}\"", token))
            .json(&QueryRequest {
                sql_text: sql.to_string(),
                async_exec: false,
                sequence_id: 0,
                parameters: Some(QueryParameters {
                    query_tag: Some("dbmazz-cdc".to_string()),
                    multi_statement_count: None,
                }),
            })
            .send()
            .await?;

        let body: QueryResponse = resp.json().await?;

        if !body.success {
            return Err(anyhow!(
                "Snowflake query failed after retry: {}",
                body.message.unwrap_or_default()
            ));
        }

        let data = body.data;
        let query_id = data
            .as_ref()
            .map(|d| d.query_id.clone())
            .unwrap_or_default();

        Ok(QueryResult {
            success: true,
            data: data.and_then(|d| d.rowset),
            query_id,
            message: body.message.unwrap_or_default(),
        })
    }

    /// Executes a PUT command and returns the stageInfo from the response.
    pub async fn put_stage_info(
        &self,
        stage_name: &str,
        file_name: &str,
    ) -> Result<serde_json::Value> {
        let sql = format!(
            "PUT file:///{} @{} AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
            file_name, stage_name
        );

        let token = self.session.read().await.token.clone();
        let request_id = uuid_v4();

        let url = format!(
            "{}/queries/v1/query-request?requestId={}",
            self.account_url, request_id
        );

        let resp = self
            .http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Snowflake Token=\"{}\"", token))
            .json(&QueryRequest {
                sql_text: sql,
                async_exec: false,
                sequence_id: 0,
                parameters: None,
            })
            .send()
            .await
            .context("PUT stage info request failed")?;

        let body: QueryResponse = resp.json().await?;

        if !body.success {
            return Err(anyhow!(
                "PUT command failed: {}",
                body.message.unwrap_or_default()
            ));
        }

        body.data
            .and_then(|d| d.stage_info)
            .ok_or_else(|| anyhow!("No stageInfo in PUT response"))
    }

    /// Refreshes the session token using the master token.
    pub async fn refresh_token(&self) -> Result<()> {
        let master_token = self.session.read().await.master_token.clone();
        let old_token = self.session.read().await.token.clone();

        if master_token.is_empty() {
            // No master token — re-login
            info!("No master token, performing full re-login");
            return self.login().await;
        }

        let url = format!("{}/session/token-request", self.account_url);

        let resp = self
            .http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(
                AUTHORIZATION,
                format!("Snowflake Token=\"{}\"", master_token),
            )
            .json(&TokenRenewRequest {
                data: TokenRenewData {
                    old_session_token: old_token,
                    request_type: "RENEW".to_string(),
                },
            })
            .send()
            .await?;

        let body: TokenRenewResponse = resp.json().await?;

        if body.success {
            if let Some(data) = body.data {
                if let Some(new_token) = data.session_token {
                    let mut session = self.session.write().await;
                    session.token = new_token;
                    info!("Snowflake session token refreshed");
                    return Ok(());
                }
            }
        }

        // Fallback: full re-login
        warn!(
            "Token refresh failed: {}, performing full re-login",
            body.message.unwrap_or_default()
        );
        self.login().await
    }

    /// Returns auth headers for direct HTTP requests (stage upload).
    #[allow(dead_code)]
    pub async fn auth_headers(&self) -> Result<HeaderMap> {
        let token = self.session.read().await.token.clone();
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Snowflake Token=\"{}\"", token))?,
        );
        Ok(headers)
    }

    /// Returns a reference to the internal HTTP client (for stage uploads).
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http
    }

    /// Returns the account URL.
    #[allow(dead_code)]
    pub fn account_url(&self) -> &str {
        &self.account_url
    }
}

/// Generates a simple UUID v4 for request IDs.
fn uuid_v4() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Simple pseudo-UUID from timestamp + random-ish bits
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (now >> 96) as u32,
        (now >> 80) as u16,
        (now >> 68) as u16 & 0xFFF,
        ((now >> 52) as u16 & 0x3FFF) | 0x8000,
        now as u64 & 0xFFFF_FFFF_FFFF,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_v4_format() {
        let id = uuid_v4();
        assert_eq!(id.len(), 36);
        assert_eq!(&id[8..9], "-");
        assert_eq!(&id[13..14], "-");
        assert_eq!(&id[14..15], "4"); // version
        assert_eq!(&id[18..19], "-");
        assert_eq!(&id[23..24], "-");
    }
}

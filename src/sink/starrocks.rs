use async_trait::async_trait;
use anyhow::{Result, anyhow};
use sonic_rs::{Value, Object as Map, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use chrono::Utc;
use mysql_async::{Pool, Conn, OptsBuilder, prelude::Queryable};
use tracing::info;

use crate::sink::Sink;
use crate::sink::curl_loader::CurlStreamLoader;
use crate::source::parser::{CdcMessage, TupleData, Tuple};
use crate::pipeline::schema_cache::{SchemaCache, TableSchema, SchemaDelta};

pub struct StarRocksSink {
    curl_loader: CurlStreamLoader,
    database: String,
    mysql_pool: Option<Pool>,  // MySQL pool for DDL (port 9030)
}

/// Check if a table is internal to dbmazz and should not be replicated
fn is_internal_table(table_name: &str) -> bool {
    table_name.starts_with("dbmazz_") ||
    table_name.starts_with("_dbmazz_") ||
    table_name == "dbmazz_checkpoints"
}

impl StarRocksSink {
    pub fn new(base_url: String, database: String, user: String, pass: String) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        
        info!("StarRocksSink initialized:");
        info!("  base_url: {}", base_url);
        info!("  database: {}", database);

        // Extract host from base_url for MySQL connection
        let mysql_host = base_url
            .replace("http://", "")
            .replace("https://", "")
            .split(':')
            .next()
            .unwrap_or("starrocks")
            .to_string();

        // Create MySQL pool for DDL (port 9030)
        // StarRocks doesn't support all MySQL variables, use prefer_socket=false
        let mysql_opts = OptsBuilder::default()
            .ip_or_hostname(mysql_host)
            .tcp_port(9030)
            .user(Some(user.clone()))
            .pass(Some(pass.clone()))
            .db_name(Some(database.clone()))
            .prefer_socket(false);  // Avoids "Unknown system variable 'socket'" error

        // Create CurlStreamLoader for Stream Load (uses libcurl with 100-continue)
        let curl_loader = CurlStreamLoader::new(
            base_url.clone(),
            database.clone(),
            user.clone(),
            pass.clone(),
        );
        
        Self {
            curl_loader,
            database,
            mysql_pool: Some(Pool::new(mysql_opts)),
        }
    }

    /// Verifies that the StarRocks HTTP endpoint (port 8040) is accessible
    /// This should be called during setup to fail early if there are network issues
    pub async fn verify_http_connection(&self) -> Result<()> {
        self.curl_loader.verify_connection().await
    }

    /// Converts a Tuple to JSON using the table schema (includes all columns)
    fn tuple_to_json(
        &self,
        tuple: &Tuple,
        schema: &TableSchema
    ) -> Result<Map> {
        self.tuple_to_json_selective(tuple, schema, false).map(|(row, _)| row)
    }


    /// Converts a Tuple to JSON with option to exclude TOAST columns
    /// Returns (row, included_columns) for use in partial update
    fn tuple_to_json_selective(
        &self,
        tuple: &Tuple,
        schema: &TableSchema,
        exclude_toast: bool
    ) -> Result<(Map, Vec<String>)> {
        let column_count = schema.columns.len();
        let mut row = Map::with_capacity(column_count);
        let mut included_columns = Vec::with_capacity(column_count);

        // Iterate over columns and data in parallel
        for (idx, (column, data)) in schema.columns.iter().zip(tuple.cols.iter()).enumerate() {
            // If exclude_toast=true and this column is TOAST, skip
            if exclude_toast && tuple.is_toast_column(idx) {
                continue;
            }
            
            let value = match data {
                TupleData::Null => json!(null),
                TupleData::Toast => {
                    if exclude_toast {
                        // Already skipped above, but for safety
                        continue;
                    }
                    // TOAST = very large data not included in WAL
                    // If not excluding, use null (indicates unchanged value)
                    json!(null)
                },
                TupleData::Text(bytes) => {
                    // Convert bytes to string and then to appropriate type
                    let text = String::from_utf8_lossy(bytes);
                    self.convert_pg_value(&text, column.type_id)
                }
            };
            
            row.insert(column.name.as_str(), value);
            included_columns.push(column.name.clone());
        }
        
        Ok((row, included_columns))
    }


    /// Converts a PostgreSQL value to the appropriate JSON type
    fn convert_pg_value(&self, text: &str, pg_type_id: u32) -> Value {
        match pg_type_id {
            // Boolean
            16 => {
                match text.to_lowercase().as_str() {
                    "t" | "true" | "1" => json!(true),
                    _ => json!(false),
                }
            },
            // Integer types (INT2, INT4, INT8)
            21 | 23 | 20 => {
                text.parse::<i64>()
                    .map(|n| json!(n))
                    .unwrap_or_else(|_| json!(text))
            },
            // Float types (FLOAT4, FLOAT8)
            700 | 701 => {
                text.parse::<f64>()
                    .map(|f| json!(f))
                    .unwrap_or_else(|_| json!(text))
            },
            // NUMERIC/DECIMAL - keep as string for precision
            1700 => json!(text),
            // Timestamp types
            1114 | 1184 => json!(text),
            // Default: string
            _ => json!(text),
        }
    }


    /// Serializes a batch to JSON bytes without duplicating buffers.
    fn build_body(&self, rows: Vec<Map>) -> Result<Arc<Vec<u8>>> {
        if rows.is_empty() {
            return Ok(Arc::new(Vec::new()));
        }

        let row_count = rows.len();
        let mut json_values = Vec::with_capacity(row_count);
        for obj in rows {
            json_values.push(Value::from(obj));
        }
        let body = sonic_rs::to_string(&json_values)?;
        Ok(Arc::new(body.into_bytes()))
    }

    /// Sends with retries in case of failure using a prebuilt body.
    async fn send_body_with_retry(
        &self,
        table_name: &str,
        body: Arc<Vec<u8>>,
        partial_columns: Option<Vec<String>>,
        max_retries: u32,
    ) -> Result<()> {
        let mut attempt = 0;
        
        loop {
            match self.curl_loader.send(table_name, body.clone(), partial_columns.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_retries {
                        return Err(anyhow!(
                            "Failed after {} attempts: {}", 
                            max_retries, 
                            e
                        ));
                    }

                    info!(
                        "Retry {}/{} for {}: {}",
                        attempt,
                        max_retries,
                        table_name,
                        e
                    );

                    // Exponential backoff: 100ms, 200ms, 400ms...
                    tokio::time::sleep(
                        Duration::from_millis(100 * 2_u64.pow(attempt))
                    ).await;
                }
            }
        }
    }

    /// Sends a batch of rows to StarRocks via Stream Load (full row) with retries.
    async fn send_with_retry(
        &self,
        table_name: &str,
        rows: Vec<Map>,
        max_retries: u32
    ) -> Result<()> {
        let body = self.build_body(rows)?;
        if body.is_empty() {
            return Ok(());
        }
        self.send_body_with_retry(table_name, body, None, max_retries).await
    }


    /// Executes DDL in StarRocks via MySQL protocol
    async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let pool = self.mysql_pool.as_ref()
            .ok_or_else(|| anyhow!("MySQL pool not initialized"))?;
        
        let mut conn: Conn = pool.get_conn().await
            .map_err(|e| anyhow!("Failed to get MySQL connection: {}", e))?;
        
        conn.query_drop(sql).await
            .map_err(|e| anyhow!("DDL execution failed: {}", e))?;
        
        Ok(())
    }


    /// Converts PostgreSQL type to StarRocks type
    fn pg_type_to_starrocks(&self, pg_type: u32) -> &'static str {
        match pg_type {
            16 => "BOOLEAN",           // bool
            21 => "SMALLINT",          // int2
            23 => "INT",               // int4
            20 => "BIGINT",            // int8
            700 => "FLOAT",            // float4
            701 => "DOUBLE",           // float8
            1700 => "DECIMAL(38,9)",   // numeric
            1114 => "DATETIME",        // timestamp
            1184 => "DATETIME",        // timestamptz
            25 => "STRING",            // text
            1043 => "STRING",          // varchar
            1042 => "STRING",          // char
            3802 => "JSON",            // jsonb
            _ => "STRING",             // default
        }
    }


    /// Applies schema changes (adds new columns)
    pub async fn apply_schema_delta(&self, delta: &SchemaDelta) -> Result<()> {
        for col in &delta.added_columns {
            let sr_type = self.pg_type_to_starrocks(col.pg_type_id);
            let sql = format!(
                "ALTER TABLE {}.{} ADD COLUMN {} {}",
                self.database, delta.table_name, col.name, sr_type
            );

            // Try to execute DDL, ignore error if column already exists
            match self.execute_ddl(&sql).await {
                Ok(_) => {
                    info!(
                        "Schema evolution: added column {} ({}) to {}",
                        col.name, sr_type, delta.table_name
                    );
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    // StarRocks returns "Duplicate column name" if column already exists
                    if err_msg.contains("Duplicate column") || err_msg.contains("already exists") {
                        info!(
                            "Column {} already exists in {}, skipping",
                            col.name, delta.table_name
                        );
                    } else {
                        return Err(anyhow!(
                            "Failed to add column {} to {}: {}",
                            col.name, delta.table_name, err_msg
                        ));
                    }
                }
            }
        }
        Ok(())
    }


    /// Sends partial update with retries in case of failure
    async fn send_partial_update_with_retry(
        &self,
        table_name: &str,
        rows: Vec<Map>,
        columns: &[String],
        max_retries: u32
    ) -> Result<()> {
        let body = self.build_body(rows)?;
        if body.is_empty() {
            return Ok(());
        }
        let columns_vec = columns.to_vec();
        self.send_body_with_retry(table_name, body, Some(columns_vec), max_retries).await
    }
}

#[async_trait]
impl Sink for StarRocksSink {
    async fn push_batch(
        &mut self, 
        batch: &[CdcMessage],
        schema_cache: &SchemaCache,
        lsn: u64
    ) -> Result<()> {
        // Cache timestamp for entire batch (avoids repeated calls)
        let synced_at = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        // Structure: (relation_id, toast_bitmap) -> (rows, columns)
        // Group by table AND TOAST pattern to optimize partial updates
        #[derive(Hash, Eq, PartialEq)]
        struct BatchKey {
            relation_id: u32,
            toast_bitmap: u64,
        }
        
        let mut batches: HashMap<BatchKey, (Vec<Map>, Option<Vec<String>>)> = HashMap::new();
        
        for msg in batch {
            match msg {
                CdcMessage::Insert { relation_id, tuple } => {
                    if let Some(schema) = schema_cache.get(*relation_id) {
                        // Skip internal dbmazz tables
                        if is_internal_table(&schema.name) {
                            continue;
                        }
                        // INSERTs are always full row (even if they have TOAST, we send null)
                        let mut row = self.tuple_to_json(tuple, schema)?;

                        // CDC audit columns
                        row.insert("dbmazz_op_type", json!(0)); // 0 = INSERT
                        row.insert("dbmazz_is_deleted", json!(false));
                        row.insert("dbmazz_synced_at", json!(&synced_at));
                        row.insert("dbmazz_cdc_version", json!(lsn as i64));


                        let key = BatchKey {
                            relation_id: *relation_id,
                            toast_bitmap: 0  // Full row
                        };
                        batches.entry(key)
                            .or_insert_with(|| (Vec::new(), None))
                            .0.push(row);
                    }
                },

                CdcMessage::Update { relation_id, new_tuple, .. } => {
                    if let Some(schema) = schema_cache.get(*relation_id) {
                        // Skip internal dbmazz tables
                        if is_internal_table(&schema.name) {
                            continue;
                        }
                        // Use POPCNT (SIMD) to detect TOAST quickly: O(1)
                        let has_toast = new_tuple.has_toast();

                        let (mut row, columns) = if has_toast {
                            // Partial update: exclude TOAST columns
                            let (r, mut cols) = self.tuple_to_json_selective(
                                new_tuple, schema, true
                            )?;

                            // Add audit columns to the list
                            cols.push("dbmazz_op_type".to_string());
                            cols.push("dbmazz_is_deleted".to_string());
                            cols.push("dbmazz_synced_at".to_string());
                            cols.push("dbmazz_cdc_version".to_string());


                            (r, Some(cols))
                        } else {
                            // Full row update (no TOAST)
                            (self.tuple_to_json(new_tuple, schema)?, None)
                        };

                        // CDC audit columns
                        row.insert("dbmazz_op_type", json!(1)); // 1 = UPDATE
                        row.insert("dbmazz_is_deleted", json!(false));
                        row.insert("dbmazz_synced_at", json!(&synced_at));
                        row.insert("dbmazz_cdc_version", json!(lsn as i64));

                        let key = BatchKey {
                            relation_id: *relation_id,
                            toast_bitmap: new_tuple.toast_bitmap
                        };

                        let entry = batches.entry(key).or_insert_with(|| (Vec::new(), columns.clone()));
                        entry.0.push(row);
                    }
                },

                CdcMessage::Delete { relation_id, old_tuple: Some(old) } => {
                    if let Some(schema) = schema_cache.get(*relation_id) {
                        // Skip internal dbmazz tables
                        if is_internal_table(&schema.name) {
                            continue;
                        }
                        // DELETEs are always full row (we need all fields)
                        let mut row = self.tuple_to_json(old, schema)?;

                        // CDC audit columns
                        row.insert("dbmazz_op_type", json!(2)); // 2 = DELETE
                        row.insert("dbmazz_is_deleted", json!(true)); // Soft delete
                        row.insert("dbmazz_synced_at", json!(&synced_at));
                        row.insert("dbmazz_cdc_version", json!(lsn as i64));

                        let key = BatchKey {
                            relation_id: *relation_id,
                            toast_bitmap: 0  // Full row
                        };
                        batches.entry(key)
                            .or_insert_with(|| (Vec::new(), None))
                            .0.push(row);
                    }
                },
                CdcMessage::Delete { old_tuple: None, .. } => {
                    // Ignore deletes without old tuple
                },

                // Begin, Commit, Relation, KeepAlive, Unknown - don't need sink
                _ => {}
            }
        }

        // Send each batch grouped by (table, toast_signature)
        for (key, (rows, columns)) in batches {
            if let Some(schema) = schema_cache.get(key.relation_id) {
                if let Some(cols) = columns {
                    // Partial update
                    self.send_partial_update_with_retry(&schema.name, rows, &cols, 3).await?;
                } else {
                    // Full row
                    self.send_with_retry(&schema.name, rows, 3).await?;
                }
            }
        }
        
        Ok(())
    }
    
    async fn apply_schema_delta(&self, delta: &SchemaDelta) -> Result<()> {
        self.apply_schema_delta(delta).await
    }
}

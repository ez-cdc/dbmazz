use async_trait::async_trait;
use anyhow::{Result, anyhow};
use reqwest::Client;
use sonic_rs::{Value, Object as Map, json, JsonValueTrait};
use std::collections::HashMap;
use std::time::Duration;
use chrono::Utc;

use crate::sink::Sink;
use crate::source::parser::{CdcMessage, TupleData, Tuple};
use crate::pipeline::schema_cache::{SchemaCache, TableSchema};

pub struct StarRocksSink {
    client: Client,
    base_url: String,  // e.g., http://starrocks:8030
    database: String,
    user: String,
    pass: String,
}

impl StarRocksSink {
    pub fn new(base_url: String, database: String, user: String, pass: String) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        
        println!("StarRocksSink initialized:");
        println!("  base_url: {}", base_url);
        println!("  database: {}", database);
        
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(10)
                .pool_idle_timeout(Duration::from_secs(90))
                .tcp_keepalive(Duration::from_secs(60))
                .build()
                .unwrap_or_else(|_| Client::new()),
            base_url,
            database,
            user,
            pass,
        }
    }
    
    /// Convierte un Tuple a JSON usando el schema de la tabla
    fn tuple_to_json(
        &self,
        tuple: &Tuple,
        schema: &TableSchema
    ) -> Result<Map> {
        let mut row = Map::new();
        
        // Iterar sobre columnas y datos en paralelo
        for (column, data) in schema.columns.iter().zip(tuple.0.iter()) {
            let value = match data {
                TupleData::Null => json!(null),
                TupleData::Toast => {
                    // TOAST = dato muy grande no incluido en el WAL
                    // Usamos null por ahora (el valor no cambió)
                    json!(null)
                },
                TupleData::Text(bytes) => {
                    // Convertir bytes a string y luego al tipo apropiado
                    let text = String::from_utf8_lossy(bytes);
                    self.convert_pg_value(&text, column.type_id)
                }
            };
            
            row.insert(column.name.as_str(), value);
        }
        
        Ok(row)
    }
    
    /// Convierte un valor de PostgreSQL al tipo JSON apropiado
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
            // NUMERIC/DECIMAL - mantener como string para precisión
            1700 => json!(text),
            // Timestamp types
            1114 | 1184 => json!(text),
            // Default: string
            _ => json!(text),
        }
    }
    
    /// Envía un batch de filas a StarRocks via Stream Load
    async fn send_to_starrocks(
        &self,
        table_name: &str,
        rows: Vec<Map>
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            self.base_url, self.database, table_name
        );
        
        let json_values: Vec<Value> = rows.into_iter().map(|obj| Value::from(obj)).collect();
        let body = sonic_rs::to_string(&json_values)?;
        
        let response = self.client
            .put(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .header("Expect", "100-continue")  // Requerido por StarRocks
            .header("format", "json")
            .header("strip_outer_array", "true")
            .header("ignore_json_size", "true")
            .header("max_filter_ratio", "0.2")  // Tolera hasta 20% de errores (DELETEs pueden tener campos NULL)
            .body(body)
            .send()
            .await?;
        
        let status = response.status();
        let resp_text = response.text().await?;
        
        if !status.is_success() {
            return Err(anyhow!(
                "StarRocks Stream Load failed ({}): {}", 
                status, 
                resp_text
            ));
        }
        
        // Parsear respuesta JSON de StarRocks
        let resp_json: Value = sonic_rs::from_str(&resp_text)
            .unwrap_or(json!({"Status": "Unknown"}));
            
        let sr_status = resp_json["Status"].as_str().unwrap_or("Unknown");
        
        if sr_status != "Success" && sr_status != "Publish Timeout" {
            // "Publish Timeout" es OK - los datos se escribieron
            return Err(anyhow!(
                "StarRocks Stream Load error ({}): {}", 
                sr_status, 
                resp_text
            ));
        }
        
        let loaded_rows = resp_json["NumberLoadedRows"]
            .as_u64()
            .unwrap_or(json_values.len() as u64);
            
        println!(
            "✅ Sent {} rows to StarRocks ({}.{})", 
            loaded_rows,
            self.database,
            table_name
        );
        
        Ok(())
    }
    
    /// Envía con reintentos en caso de fallo
    async fn send_with_retry(
        &self,
        table_name: &str,
        rows: Vec<Map>,
        max_retries: u32
    ) -> Result<()> {
        let mut attempt = 0;
        let rows_clone = rows.clone();
        
        loop {
            match self.send_to_starrocks(table_name, rows_clone.clone()).await {
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
                    
                    eprintln!(
                        "⚠️  Retry {}/{} for {}: {}", 
                        attempt, 
                        max_retries, 
                        table_name, 
                        e
                    );
                    
                    // Backoff exponencial: 100ms, 200ms, 400ms...
                    tokio::time::sleep(
                        Duration::from_millis(100 * 2_u64.pow(attempt))
                    ).await;
                }
            }
        }
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
        // Agrupar mensajes por tabla (relation_id)
        let mut tables: HashMap<u32, Vec<Map>> = HashMap::new();
        
        for msg in batch {
            match msg {
                CdcMessage::Insert { relation_id, tuple } => {
                    if let Some(schema) = schema_cache.get(*relation_id) {
                        let mut row = self.tuple_to_json(tuple, schema)?;
                        
                        // Columnas de auditoría CDC
                        row.insert("dbmazz_op_type", json!(0)); // 0 = INSERT
                        row.insert("dbmazz_is_deleted", json!(false));
                        row.insert("dbmazz_synced_at", 
                            json!(Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()));
                        row.insert("dbmazz_cdc_version", json!(lsn as i64));
                        
                        tables.entry(*relation_id)
                            .or_insert_with(Vec::new)
                            .push(row);
                    }
                },
                
                CdcMessage::Update { relation_id, new_tuple, .. } => {
                    if let Some(schema) = schema_cache.get(*relation_id) {
                        let mut row = self.tuple_to_json(new_tuple, schema)?;
                        
                        // Columnas de auditoría CDC
                        row.insert("dbmazz_op_type", json!(1)); // 1 = UPDATE
                        row.insert("dbmazz_is_deleted", json!(false));
                        row.insert("dbmazz_synced_at", 
                            json!(Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()));
                        row.insert("dbmazz_cdc_version", json!(lsn as i64));
                        
                        tables.entry(*relation_id)
                            .or_insert_with(Vec::new)
                            .push(row);
                    }
                },
                
                CdcMessage::Delete { relation_id, old_tuple } => {
                    if let Some(old) = old_tuple {
                        if let Some(schema) = schema_cache.get(*relation_id) {
                            let mut row = self.tuple_to_json(old, schema)?;
                            
                            // Columnas de auditoría CDC
                            row.insert("dbmazz_op_type", json!(2)); // 2 = DELETE
                            row.insert("dbmazz_is_deleted", json!(true)); // Soft delete
                            row.insert("dbmazz_synced_at", 
                                json!(Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()));
                            row.insert("dbmazz_cdc_version", json!(lsn as i64));
                            
                            tables.entry(*relation_id)
                                .or_insert_with(Vec::new)
                                .push(row);
                        }
                    }
                },
                
                // Begin, Commit, Relation, KeepAlive, Unknown - no necesitan sink
                _ => {}
            }
        }
        
        // Enviar cada tabla por separado
        for (relation_id, rows) in tables {
            if let Some(schema) = schema_cache.get(relation_id) {
                self.send_with_retry(&schema.name, rows, 3).await?;
            }
        }
        
        Ok(())
    }
}

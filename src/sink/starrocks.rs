use async_trait::async_trait;
use anyhow::{Result, anyhow};
use reqwest::Client;
use serde_json::{Value, Map, json};
use std::collections::HashMap;
use std::time::Duration;

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
    ) -> Result<Map<String, Value>> {
        let mut row = Map::new();
        
        // Iterar sobre columnas y datos en paralelo
        for (column, data) in schema.columns.iter().zip(tuple.0.iter()) {
            let value = match data {
                TupleData::Null => Value::Null,
                TupleData::Toast => {
                    // TOAST = dato muy grande no incluido en el WAL
                    // Usamos null por ahora (el valor no cambió)
                    Value::Null
                },
                TupleData::Text(bytes) => {
                    // Convertir bytes a string y luego al tipo apropiado
                    let text = String::from_utf8_lossy(bytes);
                    self.convert_pg_value(&text, column.type_id)
                }
            };
            
            row.insert(column.name.clone(), value);
        }
        
        Ok(row)
    }
    
    /// Convierte un valor de PostgreSQL al tipo JSON apropiado
    fn convert_pg_value(&self, text: &str, pg_type_id: u32) -> Value {
        match pg_type_id {
            // Boolean
            16 => {
                match text.to_lowercase().as_str() {
                    "t" | "true" | "1" => Value::Bool(true),
                    _ => Value::Bool(false),
                }
            },
            // Integer types (INT2, INT4, INT8)
            21 | 23 | 20 => {
                text.parse::<i64>()
                    .map(|n| json!(n))
                    .unwrap_or(Value::String(text.to_string()))
            },
            // Float types (FLOAT4, FLOAT8)
            700 | 701 => {
                text.parse::<f64>()
                    .ok()
                    .and_then(|f| serde_json::Number::from_f64(f))
                    .map(Value::Number)
                    .unwrap_or(Value::String(text.to_string()))
            },
            // NUMERIC/DECIMAL - mantener como string para precisión
            1700 => Value::String(text.to_string()),
            // Timestamp types
            1114 | 1184 => Value::String(text.to_string()),
            // Default: string
            _ => Value::String(text.to_string()),
        }
    }
    
    /// Envía un batch de filas a StarRocks via Stream Load
    async fn send_to_starrocks(
        &self,
        table_name: &str,
        rows: Vec<Map<String, Value>>
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            self.base_url, self.database, table_name
        );
        
        let json_values: Vec<Value> = rows.into_iter().map(Value::Object).collect();
        let body = serde_json::to_string(&json_values)?;
        
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
        let resp_json: Value = serde_json::from_str(&resp_text)
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
        rows: Vec<Map<String, Value>>,
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
        schema_cache: &SchemaCache
    ) -> Result<()> {
        // Agrupar mensajes por tabla (relation_id)
        let mut tables: HashMap<u32, Vec<Map<String, Value>>> = HashMap::new();
        
        for msg in batch {
            match msg {
                CdcMessage::Insert { relation_id, tuple } => {
                    if let Some(schema) = schema_cache.get(*relation_id) {
                        let mut row = self.tuple_to_json(tuple, schema)?;
                        row.insert("op_type".to_string(), json!(0)); // 0 = UPSERT
                        tables.entry(*relation_id)
                            .or_insert_with(Vec::new)
                            .push(row);
                    }
                },
                
                CdcMessage::Update { relation_id, new_tuple, .. } => {
                    if let Some(schema) = schema_cache.get(*relation_id) {
                        let mut row = self.tuple_to_json(new_tuple, schema)?;
                        row.insert("op_type".to_string(), json!(0)); // 0 = UPSERT
                        tables.entry(*relation_id)
                            .or_insert_with(Vec::new)
                            .push(row);
                    }
                },
                
                CdcMessage::Delete { relation_id, old_tuple } => {
                    if let Some(old) = old_tuple {
                        if let Some(schema) = schema_cache.get(*relation_id) {
                            let mut row = self.tuple_to_json(old, schema)?;
                            row.insert("op_type".to_string(), json!(1)); // 1 = DELETE
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

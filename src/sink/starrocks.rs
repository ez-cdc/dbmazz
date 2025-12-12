use async_trait::async_trait;
use anyhow::{Result, anyhow};
use sonic_rs::{Value, Object as Map, json, JsonValueTrait};
use std::collections::HashMap;
use std::time::Duration;
use chrono::Utc;
use mysql_async::{Pool, Conn, OptsBuilder, prelude::Queryable};

use crate::sink::Sink;
use crate::sink::curl_loader::CurlStreamLoader;
use crate::source::parser::{CdcMessage, TupleData, Tuple};
use crate::pipeline::schema_cache::{SchemaCache, TableSchema, SchemaDelta};

pub struct StarRocksSink {
    curl_loader: CurlStreamLoader,
    database: String,
    mysql_pool: Option<Pool>,  // Pool MySQL para DDL (puerto 9030)
}

impl StarRocksSink {
    pub fn new(base_url: String, database: String, user: String, pass: String) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        
        println!("StarRocksSink initialized:");
        println!("  base_url: {}", base_url);
        println!("  database: {}", database);
        
        // Extraer host del base_url para conexión MySQL
        let mysql_host = base_url
            .replace("http://", "")
            .replace("https://", "")
            .split(':')
            .next()
            .unwrap_or("starrocks")
            .to_string();
        
        // Crear pool MySQL para DDL (puerto 9030)
        // StarRocks no soporta todas las variables de MySQL, usar prefer_socket=false
        let mysql_opts = OptsBuilder::default()
            .ip_or_hostname(mysql_host)
            .tcp_port(9030)
            .user(Some(user.clone()))
            .pass(Some(pass.clone()))
            .db_name(Some(database.clone()))
            .prefer_socket(false);  // Evita el error "Unknown system variable 'socket'"
        
        // Crear CurlStreamLoader para Stream Load (usa libcurl con 100-continue)
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
    
    /// Convierte un Tuple a JSON usando el schema de la tabla (incluye todas las columnas)
    fn tuple_to_json(
        &self,
        tuple: &Tuple,
        schema: &TableSchema
    ) -> Result<Map> {
        self.tuple_to_json_selective(tuple, schema, false).map(|(row, _)| row)
    }
    
    /// Convierte un Tuple a JSON con opcion de excluir columnas TOAST
    /// Retorna (row, columnas_incluidas) para usar en partial update
    fn tuple_to_json_selective(
        &self,
        tuple: &Tuple,
        schema: &TableSchema,
        exclude_toast: bool
    ) -> Result<(Map, Vec<String>)> {
        let mut row = Map::new();
        let mut included_columns = Vec::new();
        
        // Iterar sobre columnas y datos en paralelo
        for (idx, (column, data)) in schema.columns.iter().zip(tuple.cols.iter()).enumerate() {
            // Si exclude_toast=true y esta columna es TOAST, skip
            if exclude_toast && tuple.is_toast_column(idx) {
                continue;
            }
            
            let value = match data {
                TupleData::Null => json!(null),
                TupleData::Toast => {
                    if exclude_toast {
                        // Ya fue skipped arriba, pero por seguridad
                        continue;
                    }
                    // TOAST = dato muy grande no incluido en el WAL
                    // Si no excluimos, usamos null (indica valor sin cambios)
                    json!(null)
                },
                TupleData::Text(bytes) => {
                    // Convertir bytes a string y luego al tipo apropiado
                    let text = String::from_utf8_lossy(bytes);
                    self.convert_pg_value(&text, column.type_id)
                }
            };
            
            row.insert(column.name.as_str(), value);
            included_columns.push(column.name.clone());
        }
        
        Ok((row, included_columns))
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
    
    /// Envía un batch de filas a StarRocks via Stream Load (full row)
    async fn send_to_starrocks(
        &self,
        table_name: &str,
        rows: Vec<Map>
    ) -> Result<()> {
        self.send_to_starrocks_internal(table_name, rows, None).await
    }
    
    /// Envía un batch de filas a StarRocks via Stream Load con Partial Update
    async fn send_partial_update(
        &self,
        table_name: &str,
        rows: Vec<Map>,
        columns: &[String]
    ) -> Result<()> {
        self.send_to_starrocks_internal(table_name, rows, Some(columns)).await
    }
    
    /// Implementacion interna de Stream Load con soporte para partial update
    async fn send_to_starrocks_internal(
        &self,
        table_name: &str,
        rows: Vec<Map>,
        partial_columns: Option<&[String]>  // Si Some, usa partial update
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        
        // Serializar rows a JSON
        let json_values: Vec<Value> = rows.into_iter().map(|obj| Value::from(obj)).collect();
        let body = sonic_rs::to_string(&json_values)?;
        
        // Convertir a Vec<u8> y Option<Vec<String>> para curl_loader
        let body_bytes = body.into_bytes();
        let partial_cols = partial_columns.map(|cols| cols.to_vec());
        
        // Usar CurlStreamLoader (maneja 100-continue y redirects automáticamente)
        let _result = self.curl_loader.send(
            table_name,
            body_bytes,
            partial_cols,
        ).await?;
        
        Ok(())
    }
    
    /// Envía con reintentos en caso de fallo (full row)
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
    
    /// Ejecuta DDL en StarRocks via MySQL protocol
    async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let pool = self.mysql_pool.as_ref()
            .ok_or_else(|| anyhow!("MySQL pool not initialized"))?;
        
        let mut conn: Conn = pool.get_conn().await
            .map_err(|e| anyhow!("Failed to get MySQL connection: {}", e))?;
        
        conn.query_drop(sql).await
            .map_err(|e| anyhow!("DDL execution failed: {}", e))?;
        
        Ok(())
    }
    
    /// Convierte tipo PostgreSQL a tipo StarRocks
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
    
    /// Aplica cambios de schema (agrega columnas nuevas)
    pub async fn apply_schema_delta(&self, delta: &SchemaDelta) -> Result<()> {
        for col in &delta.added_columns {
            let sr_type = self.pg_type_to_starrocks(col.pg_type_id);
            let sql = format!(
                "ALTER TABLE {}.{} ADD COLUMN {} {}",
                self.database, delta.table_name, col.name, sr_type
            );
            
            // Intentar ejecutar DDL, ignorar error si columna ya existe
            match self.execute_ddl(&sql).await {
                Ok(_) => {
                    println!(
                        "✅ Schema evolution: added column {} ({}) to {}", 
                        col.name, sr_type, delta.table_name
                    );
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    // StarRocks retorna "Duplicate column name" si columna ya existe
                    if err_msg.contains("Duplicate column") || err_msg.contains("already exists") {
                        println!(
                            "⚠️  Column {} already exists in {}, skipping",
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
    
    /// Envía partial update con reintentos en caso de fallo
    async fn send_partial_update_with_retry(
        &self,
        table_name: &str,
        rows: Vec<Map>,
        columns: &[String],
        max_retries: u32
    ) -> Result<()> {
        let mut attempt = 0;
        let rows_clone = rows.clone();
        let columns_vec = columns.to_vec();
        
        loop {
            match self.send_partial_update(table_name, rows_clone.clone(), &columns_vec).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_retries {
                        return Err(anyhow!(
                            "Partial update failed after {} attempts: {}", 
                            max_retries, 
                            e
                        ));
                    }
                    
                    eprintln!(
                        "⚠️  Retry partial update {}/{} for {}: {}", 
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
        // Cache timestamp para toda el batch (evita llamadas repetidas)
        let synced_at = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        
        // Estructura: (relation_id, toast_bitmap) -> (rows, columns)
        // Agrupamos por tabla Y por patron de TOAST para optimizar partial updates
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
                        // INSERTs siempre son full row (aunque tengan TOAST, enviamos null)
                        let mut row = self.tuple_to_json(tuple, schema)?;
                        
                        // Columnas de auditoría CDC
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
                        // Usar POPCNT (SIMD) para detectar TOAST rapido: O(1)
                        let has_toast = new_tuple.has_toast();
                        
                        let (mut row, columns) = if has_toast {
                            // Partial update: excluir columnas TOAST
                            let (r, mut cols) = self.tuple_to_json_selective(
                                new_tuple, schema, true
                            )?;
                            
                            // Agregar columnas de auditoria a la lista
                            cols.push("dbmazz_op_type".to_string());
                            cols.push("dbmazz_is_deleted".to_string());
                            cols.push("dbmazz_synced_at".to_string());
                            cols.push("dbmazz_cdc_version".to_string());
                            
                            (r, Some(cols))
                        } else {
                            // Full row update (sin TOAST)
                            (self.tuple_to_json(new_tuple, schema)?, None)
                        };
                        
                        // Columnas de auditoría CDC
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
                
                CdcMessage::Delete { relation_id, old_tuple } => {
                    if let Some(old) = old_tuple {
                        if let Some(schema) = schema_cache.get(*relation_id) {
                            // DELETEs siempre son full row (necesitamos todos los campos)
                            let mut row = self.tuple_to_json(old, schema)?;
                            
                            // Columnas de auditoría CDC
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
                    }
                },
                
                // Begin, Commit, Relation, KeepAlive, Unknown - no necesitan sink
                _ => {}
            }
        }
        
        // Enviar cada batch agrupado por (tabla, toast_signature)
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

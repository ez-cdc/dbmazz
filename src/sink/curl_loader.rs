use anyhow::{Result, anyhow};
use curl::easy::{Easy, List};
use std::io::Read;

/// Resultado de un Stream Load
#[derive(Debug)]
pub struct LoadResult {
    pub status: String,
    pub loaded_rows: u64,
    pub message: String,
}

/// Cliente Stream Load usando libcurl (soporta Expect: 100-continue correctamente)
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

    /// Envía datos a StarRocks via Stream Load (ejecuta en thread pool para no bloquear async)
    pub async fn send(
        &self,
        table_name: &str,
        body: Vec<u8>,
        partial_columns: Option<Vec<String>>,
    ) -> Result<LoadResult> {
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            self.base_url, self.database, table_name
        );
        let user = self.user.clone();
        let pass = self.pass.clone();
        let table = table_name.to_string();
        
        // spawn_blocking para no bloquear el runtime async
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
        body: Vec<u8>,
        partial_columns: Option<Vec<String>>,
    ) -> Result<LoadResult> {
        let mut easy = Easy::new();
        
        // URL y método PUT
        easy.url(url)?;
        easy.put(true)?;
        
        // Autenticación básica
        easy.username(user)?;
        easy.password(pass)?;
        
        // Headers
        let mut headers = List::new();
        headers.append("Expect: 100-continue")?; // CRÍTICO: esperar confirmación antes de enviar body
        headers.append("format: json")?;
        headers.append("strip_outer_array: true")?;
        headers.append("ignore_json_size: true")?;
        headers.append("max_filter_ratio: 0.2")?;
        
        // Headers de partial update si existen
        if let Some(ref cols) = partial_columns {
            headers.append("partial_update: true")?;
            headers.append("partial_update_mode: row")?;
            headers.append(&format!("columns: {}", cols.join(",")))?;
            println!("🔄 Partial update for {}: {} columns", table_name, cols.len());
        }
        
        easy.http_headers(headers)?;
        
        // Configurar body - libcurl manejará el protocolo 100-continue correctamente
        let body_len = body.len();
        easy.post_field_size(body_len as u64)?;
        easy.upload(true)?;  // Habilitar modo upload para PUT
        
        let mut body_cursor = std::io::Cursor::new(body);
        easy.read_function(move |buf| {
            body_cursor.read(buf).map_err(|_| curl::easy::ReadError::Abort)
        })?;
        
        // Seguir redirects automáticamente (FE 307 -> BE)
        // CRÍTICO: unrestricted_auth permite enviar credenciales en redirects cross-port
        easy.follow_location(true)?;
        easy.unrestricted_auth(true)?;  // Equivalente a curl --location-trusted
        easy.max_redirections(3)?;
        
        // Timeout
        easy.timeout(std::time::Duration::from_secs(30))?;
        
        // Buffer para la respuesta
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
        
        // Parsear respuesta JSON
        let resp_json: serde_json::Value = serde_json::from_str(&response_body)
            .unwrap_or(serde_json::json!({"Status": "Unknown", "Message": response_body.clone()}));
        
        let status = resp_json["Status"].as_str().unwrap_or("Unknown").to_string();
        let loaded_rows = resp_json["NumberLoadedRows"].as_u64().unwrap_or(0);
        let message = resp_json["Message"].as_str().unwrap_or("").to_string();
        
        // Validar respuesta HTTP
        if response_code >= 400 {
            return Err(anyhow!(
                "HTTP {}: {} - {}", 
                response_code, status, message
            ));
        }
        
        // Validar respuesta de StarRocks
        if status != "Success" && status != "Publish Timeout" {
            // "Publish Timeout" es OK - los datos se escribieron
            return Err(anyhow!(
                "Stream Load failed: {} - {}", 
                status, message
            ));
        }
        
        println!(
            "✅ Sent {} rows to StarRocks ({}.{})", 
            loaded_rows,
            table_name.split('.').last().unwrap_or(table_name),
            if partial_columns.is_some() { "partial" } else { "full" }
        );
        
        Ok(LoadResult {
            status,
            loaded_rows,
            message,
        })
    }
}


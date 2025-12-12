use anyhow::{Context, Result};
use std::env;

/// Configuración central de dbmazz desde variables de entorno
#[derive(Debug, Clone)]
pub struct Config {
    // PostgreSQL
    pub database_url: String,
    pub slot_name: String,
    pub publication_name: String,
    pub tables: Vec<String>,
    
    // StarRocks
    pub starrocks_url: String,
    pub starrocks_db: String,
    pub starrocks_user: String,
    pub starrocks_pass: String,
    
    // Pipeline
    pub flush_size: usize,
    pub flush_interval_ms: u64,
    
    // gRPC
    pub grpc_port: u16,
}

impl Config {
    /// Cargar configuración desde variables de entorno
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            // PostgreSQL
            database_url: env::var("DATABASE_URL")
                .context("DATABASE_URL must be set")?,
            slot_name: env::var("SLOT_NAME")
                .unwrap_or_else(|_| "dbmazz_slot".to_string()),
            publication_name: env::var("PUBLICATION_NAME")
                .unwrap_or_else(|_| "dbmazz_pub".to_string()),
            tables: env::var("TABLES")
                .unwrap_or_else(|_| "orders,order_items".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            
            // StarRocks
            starrocks_url: env::var("STARROCKS_URL")
                .context("STARROCKS_URL must be set")?,
            starrocks_db: env::var("STARROCKS_DB")
                .context("STARROCKS_DB must be set")?,
            starrocks_user: env::var("STARROCKS_USER")
                .unwrap_or_else(|_| "root".to_string()),
            starrocks_pass: env::var("STARROCKS_PASS")
                .unwrap_or_else(|_| "".to_string()),
            
            // Pipeline
            flush_size: env::var("FLUSH_SIZE")
                .unwrap_or_else(|_| "10000".to_string())
                .parse()
                .unwrap_or(10000),
            flush_interval_ms: env::var("FLUSH_INTERVAL_MS")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .unwrap_or(5000),
            
            // gRPC
            grpc_port: env::var("GRPC_PORT")
                .unwrap_or_else(|_| "50051".to_string())
                .parse()
                .unwrap_or(50051),
        })
    }
    
    /// Imprimir banner con configuración
    pub fn print_banner(&self) {
        println!("Starting dbmazz (High Performance Mode)...");
        println!("Source: Postgres ({})", self.slot_name);
        println!("Target: StarRocks ({})", self.starrocks_db);
        println!("Flush: {} msgs or {}ms interval", self.flush_size, self.flush_interval_ms);
        println!("gRPC: port {}", self.grpc_port);
        println!("Tables: {:?}", self.tables);
    }
}



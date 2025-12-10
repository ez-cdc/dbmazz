mod source;
mod sink;
mod pipeline;
mod state_store;

use anyhow::Result;
use bytes::{Buf, Bytes};
use dotenvy::dotenv;
use futures::StreamExt;
use std::env;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::pipeline::Pipeline;
use crate::sink::starrocks::StarRocksSink;
use crate::source::parser::{PgOutputParser, CdcEvent};
use crate::source::postgres::{PostgresSource, build_standby_status_update};
use crate::state_store::StateStore;
use futures::SinkExt;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let slot_name = env::var("SLOT_NAME").unwrap_or_else(|_| "dbmazz_slot".to_string());
    let publication_name =
        env::var("PUBLICATION_NAME").unwrap_or_else(|_| "dbmazz_pub".to_string());

    let starrocks_url = env::var("STARROCKS_URL").expect("STARROCKS_URL must be set");
    let starrocks_db = env::var("STARROCKS_DB").expect("STARROCKS_DB must be set");
    let starrocks_user = env::var("STARROCKS_USER").unwrap_or_else(|_| "root".to_string());
    let starrocks_pass = env::var("STARROCKS_PASS").unwrap_or_else(|_| "".to_string());

    // Flush configuration
    let flush_size: usize = env::var("FLUSH_SIZE")
        .unwrap_or_else(|_| "10000".to_string())
        .parse()
        .unwrap_or(1000);
    let flush_interval_ms: u64 = env::var("FLUSH_INTERVAL_MS")
        .unwrap_or_else(|_| "5000".to_string())
        .parse()
        .unwrap_or(5000);

    println!("Starting dbmazz (High Performance Mode)...");
    println!("Source: Postgres ({})", slot_name);
    println!("Target: StarRocks ({})", starrocks_db);
    println!("Flush: {} msgs or {}ms interval", flush_size, flush_interval_ms);

    // 1. Init StateStore y recuperar checkpoint
    let state_store = StateStore::new(&database_url).await?;
    let last_lsn = state_store.load_checkpoint(&slot_name).await?;
    let start_lsn = last_lsn.unwrap_or(0);
    
    if start_lsn > 0 {
        println!("Checkpoint: Resuming from LSN 0x{:X}", start_lsn);
    } else {
        println!("Checkpoint: Starting from beginning (no previous checkpoint)");
    }

    // 2. Init Source con LSN de checkpoint
    let source =
        PostgresSource::new(&database_url, slot_name.clone(), publication_name).await?;
    let replication_stream = source.start_replication_from(start_lsn).await?;
    tokio::pin!(replication_stream);

    // 3. Init Sink
    let sink = Box::new(StarRocksSink::new(
        starrocks_url,
        starrocks_db,
        starrocks_user,
        starrocks_pass
    ));

    // 4. Init Pipeline con canal de feedback para checkpoints
    let (tx, rx) = mpsc::channel(flush_size * 2); // Buffer 2x flush_size
    let (feedback_tx, mut feedback_rx) = mpsc::channel::<u64>(100); // Canal de feedback
    
    let pipeline = Pipeline::new(rx, sink, flush_size, Duration::from_millis(flush_interval_ms))
        .with_feedback_channel(feedback_tx);
    tokio::spawn(pipeline.run());

    println!("Connected! Streaming CDC events...");

    // Track last LSN for keepalive responses
    let mut current_lsn: u64 = start_lsn;

    // 5. Main Loop: Read WAL y procesar feedback de checkpoints
    loop {
        tokio::select! {
            // Leer mensajes del stream de replicación
            data_res = replication_stream.next() => {
                match data_res {
                    Some(Ok(data)) => {
                        let mut bytes = data;
                        if bytes.is_empty() {
                            continue;
                        }

                        let tag = bytes.get_u8();

                        match tag {
                            b'w' => {
                                // XLogData
                                if bytes.len() < 24 {
                                    continue;
                                }
                                let _wal_start = bytes.get_u64();
                                let wal_end = bytes.get_u64();
                                let _timestamp = bytes.get_u64();
                                
                                current_lsn = wal_end; // Actualizar LSN actual

                                if bytes.is_empty() {
                                    continue;
                                }

                                let pgoutput_tag = bytes[0];
                                let pgoutput_body = bytes.slice(1..);

                                match PgOutputParser::parse(pgoutput_tag, pgoutput_body) {
                                    Ok(Some(cdc_msg)) => {
                                        let event = CdcEvent {
                                            lsn: wal_end,
                                            message: cdc_msg,
                                        };
                                        
                                        if let Err(e) = tx.send(event).await {
                                            eprintln!("Failed to send to pipeline: {}", e);
                                            break;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => eprintln!("Parse error: {}", e),
                                }
                            }
                            b'k' => {
                                // PrimaryKeepAlive
                                if bytes.len() < 17 {
                                    continue;
                                }
                                let wal_end = bytes.get_u64();
                                let _timestamp = bytes.get_u64();
                                let reply_requested = bytes.get_u8();
                                
                                current_lsn = wal_end;

                                // Responder si PostgreSQL lo solicita
                                if reply_requested == 1 {
                                    let status = build_standby_status_update(current_lsn);
                                    if let Err(e) = replication_stream.send(status).await {
                                        eprintln!("Failed to send keepalive response: {}", e);
                                    }
                                }
                            }
                            _ => {
                                eprintln!("Unknown replication message tag: {}", tag);
                            }
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("Replication stream error: {}", e);
                        break;
                    }
                    None => {
                        eprintln!("Replication stream ended");
                        break;
                    }
                }
            }

            // Recibir confirmaciones del sink (feedback)
            Some(confirmed_lsn) = feedback_rx.recv() => {
                // 1. Guardar checkpoint en tabla local
                if let Err(e) = state_store.save_checkpoint(&slot_name, confirmed_lsn).await {
                    eprintln!("Failed to save checkpoint: {}", e);
                    continue;
                }

                // 2. Confirmar a PostgreSQL via StandbyStatusUpdate
                let status = build_standby_status_update(confirmed_lsn);
                if let Err(e) = replication_stream.send(status).await {
                    eprintln!("Failed to send status update to PostgreSQL: {}", e);
                    continue;
                }

                println!("✓ Checkpoint confirmed: LSN 0x{:X}", confirmed_lsn);
            }
        }
    }

    Ok(())
}

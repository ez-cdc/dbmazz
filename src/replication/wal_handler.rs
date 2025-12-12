use anyhow::Result;
use bytes::{Buf, Bytes};
use futures::SinkExt;
use tokio::sync::mpsc;

use crate::source::parser::{CdcEvent, PgOutputParser};
use crate::source::postgres::build_standby_status_update;
use crate::grpc::state::SharedState;

/// Tipos de mensajes de replicación de PostgreSQL
#[derive(Debug)]
pub enum WalMessage {
    /// XLogData: Datos del WAL con LSN
    XLogData { lsn: u64, data: Bytes },
    /// KeepAlive: Mensaje de keep-alive con LSN
    KeepAlive { lsn: u64, reply_requested: bool },
    /// Tipo desconocido
    Unknown(u8),
}

/// Parsear un mensaje de replicación desde bytes
pub fn parse_replication_message(bytes: &mut Bytes) -> Option<WalMessage> {
    if bytes.is_empty() {
        return None;
    }

    let tag = bytes.get_u8();

    match tag {
        b'w' => {
            // XLogData
            if bytes.len() < 24 {
                return None;
            }
            let _wal_start = bytes.get_u64();
            let wal_end = bytes.get_u64();
            let _timestamp = bytes.get_u64();
            
            // Usar slice en lugar de clone para zero-copy
            Some(WalMessage::XLogData {
                lsn: wal_end,
                data: bytes.slice(..),
            })
        }
        b'k' => {
            // PrimaryKeepAlive
            if bytes.len() < 17 {
                return None;
            }
            let wal_end = bytes.get_u64();
            let _timestamp = bytes.get_u64();
            let reply_requested = bytes.get_u8() == 1;
            
            Some(WalMessage::KeepAlive {
                lsn: wal_end,
                reply_requested,
            })
        }
        _ => Some(WalMessage::Unknown(tag)),
    }
}

/// Procesar datos XLogData
pub async fn handle_xlog_data(
    mut data: Bytes,
    lsn: u64,
    tx: &mpsc::Sender<CdcEvent>,
    shared_state: &SharedState,
    flush_size: usize,
) -> Result<()> {
    // Actualizar LSN en SharedState
    shared_state.update_lsn(lsn);

    if data.is_empty() {
        return Ok(());
    }

    let pgoutput_tag = data[0];
    let pgoutput_body = data.slice(1..);

    match PgOutputParser::parse(pgoutput_tag, pgoutput_body) {
        Ok(Some(cdc_msg)) => {
            let event = CdcEvent {
                lsn,
                message: cdc_msg,
            };
            
            shared_state.increment_events();
            
            // Update pending events count
            let capacity = tx.capacity();
            let pending = (flush_size * 2) - capacity;
            shared_state.set_pending(pending as u64);
            
            if let Err(e) = tx.send(event).await {
                eprintln!("Failed to send to pipeline: {}", e);
                return Err(e.into());
            }
        }
        Ok(None) => {}
        Err(e) => eprintln!("Parse error: {}", e),
    }

    Ok(())
}

/// Manejar mensaje KeepAlive
pub async fn handle_keepalive<S>(
    lsn: u64,
    reply_requested: bool,
    replication_stream: &mut S,
) -> Result<()>
where
    S: SinkExt<Bytes> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    if reply_requested {
        let status = build_standby_status_update(lsn);
        if let Err(e) = replication_stream.send(status).await {
            eprintln!("Failed to send keepalive response: {}", e);
            return Err(e.into());
        }
    }
    Ok(())
}


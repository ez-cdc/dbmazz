// Copyright 2025
// Licensed under the Elastic License v2.0

//! PostgreSQL WAL replication protocol handling
//!
//! This module provides utilities for working with the PostgreSQL
//! streaming replication protocol, including:
//! - Parsing replication messages (XLogData, KeepAlive)
//! - Building Standby Status Update messages
//! - Timestamp conversion utilities
//!
//! # Protocol Reference
//! - PostgreSQL Streaming Replication Protocol:
//!   https://www.postgresql.org/docs/current/protocol-replication.html

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::time::{SystemTime, UNIX_EPOCH};

/// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
/// Difference from Unix epoch in microseconds
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;

/// PostgreSQL replication message types
#[derive(Debug)]
pub enum WalMessage {
    /// XLogData: WAL data with LSN and pgoutput payload
    XLogData {
        /// End LSN of this WAL record
        lsn: u64,
        /// pgoutput message data
        data: Bytes,
    },

    /// KeepAlive: Server heartbeat message
    KeepAlive {
        /// Current WAL end position
        lsn: u64,
        /// Whether the server is requesting a reply
        reply_requested: bool,
    },

    /// Unknown message type
    Unknown(u8),
}

/// Parse a replication protocol message
///
/// # Arguments
/// * `bytes` - Raw message bytes from the replication stream
///
/// # Returns
/// Parsed WalMessage or None if the message is incomplete
pub fn parse_replication_message(bytes: &mut Bytes) -> Option<WalMessage> {
    if bytes.is_empty() {
        return None;
    }

    let tag = bytes.get_u8();

    match tag {
        b'w' => {
            // XLogData message
            // Format: 'w' + walStart(8) + walEnd(8) + timestamp(8) + data
            if bytes.len() < 24 {
                return None;
            }
            let _wal_start = bytes.get_u64();
            let wal_end = bytes.get_u64();
            let _timestamp = bytes.get_u64();

            // Use slice for zero-copy access to remaining data
            Some(WalMessage::XLogData {
                lsn: wal_end,
                data: bytes.slice(..),
            })
        }
        b'k' => {
            // PrimaryKeepAlive message
            // Format: 'k' + walEnd(8) + timestamp(8) + replyRequested(1)
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

/// Generate timestamp in PostgreSQL format (microseconds since 2000-01-01)
pub fn pg_timestamp() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let usec = now.as_micros() as i64;
    usec - PG_EPOCH_OFFSET_USEC
}

/// Convert PostgreSQL timestamp to Unix timestamp (seconds)
pub fn pg_timestamp_to_unix(pg_ts: i64) -> i64 {
    (pg_ts + PG_EPOCH_OFFSET_USEC) / 1_000_000
}

/// Convert Unix timestamp (seconds) to PostgreSQL timestamp
pub fn unix_to_pg_timestamp(unix_ts: i64) -> i64 {
    (unix_ts * 1_000_000) - PG_EPOCH_OFFSET_USEC
}

/// Build a Standby Status Update message to confirm LSN to PostgreSQL
///
/// This message informs PostgreSQL that the client has received, flushed,
/// and applied WAL up to the specified LSN. PostgreSQL uses this to
/// manage WAL retention and determine which WAL can be recycled.
///
/// # Message Format (34 bytes total)
/// - tag: 'r' (1 byte) - StandbyStatusUpdate identifier
/// - walWritePos: u64 (8 bytes) - The last WAL position written to client
/// - walFlushPos: u64 (8 bytes) - The last WAL position flushed to client disk
/// - walApplyPos: u64 (8 bytes) - The last WAL position applied to destination
/// - timestamp: i64 (8 bytes) - Client timestamp (microseconds since 2000-01-01)
/// - reply: u8 (1 byte) - 1 if reply is requested, 0 otherwise
///
/// # Arguments
/// * `lsn` - The LSN to confirm (used for all three positions)
///
/// # Returns
/// Bytes containing the formatted status update message
pub fn build_standby_status_update(lsn: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(34);
    buf.put_u8(b'r'); // StandbyStatusUpdate tag
    buf.put_u64(lsn); // walWritePos
    buf.put_u64(lsn); // walFlushPos (same as write - we're confirming everything)
    buf.put_u64(lsn); // walApplyPos (same - data has been applied to sink)
    buf.put_i64(pg_timestamp()); // timestamp
    buf.put_u8(0); // reply not requested
    buf.freeze()
}

/// Build a Standby Status Update requesting a reply from the server
///
/// Same as `build_standby_status_update` but sets the reply flag.
/// This is useful when you want to verify the server received your update.
pub fn build_standby_status_update_with_reply(lsn: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(34);
    buf.put_u8(b'r');
    buf.put_u64(lsn);
    buf.put_u64(lsn);
    buf.put_u64(lsn);
    buf.put_i64(pg_timestamp());
    buf.put_u8(1); // reply requested
    buf.freeze()
}

/// Build a Hot Standby Feedback message
///
/// This message allows the standby to inform the primary about its
/// transaction state to prevent vacuum from removing rows still
/// needed by the standby.
///
/// # Message Format (14 bytes total)
/// - tag: 'h' (1 byte) - Hot Standby Feedback identifier
/// - timestamp: i64 (8 bytes) - Client timestamp
/// - xmin: u32 (4 bytes) - Oldest visible transaction ID (0 = no feedback)
/// - epoch: u32 (4 bytes) - Transaction ID epoch
///
/// Note: This is typically not needed for CDC use cases.
pub fn build_hot_standby_feedback(xmin: u32, epoch: u32) -> Bytes {
    let mut buf = BytesMut::with_capacity(17);
    buf.put_u8(b'h'); // HotStandbyFeedback tag
    buf.put_i64(pg_timestamp());
    buf.put_u32(xmin);
    buf.put_u32(epoch);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_timestamp_positive() {
        // Timestamp should be positive for dates after 2000-01-01
        let ts = pg_timestamp();
        assert!(ts > 0, "Timestamp should be positive, got {}", ts);
    }

    #[test]
    fn test_timestamp_conversion_roundtrip() {
        let unix_ts = 1704067200i64; // 2024-01-01 00:00:00 UTC
        let pg_ts = unix_to_pg_timestamp(unix_ts);
        let back_to_unix = pg_timestamp_to_unix(pg_ts);
        assert_eq!(unix_ts, back_to_unix);
    }

    #[test]
    fn test_standby_status_update_format() {
        let lsn = 0x16B374D848u64;
        let msg = build_standby_status_update(lsn);

        // Should be exactly 34 bytes
        assert_eq!(msg.len(), 34);

        // First byte should be 'r'
        assert_eq!(msg[0], b'r');

        // LSN should appear three times (bytes 1-8, 9-16, 17-24)
        let write_pos = u64::from_be_bytes(msg[1..9].try_into().unwrap());
        let flush_pos = u64::from_be_bytes(msg[9..17].try_into().unwrap());
        let apply_pos = u64::from_be_bytes(msg[17..25].try_into().unwrap());

        assert_eq!(write_pos, lsn);
        assert_eq!(flush_pos, lsn);
        assert_eq!(apply_pos, lsn);

        // Reply flag should be 0
        assert_eq!(msg[33], 0);
    }

    #[test]
    fn test_standby_status_update_with_reply() {
        let msg = build_standby_status_update_with_reply(1000);

        // Reply flag should be 1
        assert_eq!(msg[33], 1);
    }

    #[test]
    fn test_parse_xlogdata() {
        // Build a mock XLogData message
        let mut buf = BytesMut::new();
        buf.put_u8(b'w'); // XLogData tag
        buf.put_u64(100); // walStart
        buf.put_u64(200); // walEnd
        buf.put_u64(pg_timestamp() as u64); // timestamp
        buf.put_slice(b"payload"); // data

        let mut bytes = buf.freeze();
        let msg = parse_replication_message(&mut bytes).unwrap();

        match msg {
            WalMessage::XLogData { lsn, data } => {
                assert_eq!(lsn, 200);
                assert_eq!(&data[..], b"payload");
            }
            _ => panic!("Expected XLogData"),
        }
    }

    #[test]
    fn test_parse_keepalive() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'k'); // KeepAlive tag
        buf.put_u64(500); // walEnd
        buf.put_u64(pg_timestamp() as u64); // timestamp
        buf.put_u8(1); // reply requested

        let mut bytes = buf.freeze();
        let msg = parse_replication_message(&mut bytes).unwrap();

        match msg {
            WalMessage::KeepAlive {
                lsn,
                reply_requested,
            } => {
                assert_eq!(lsn, 500);
                assert!(reply_requested);
            }
            _ => panic!("Expected KeepAlive"),
        }
    }

    #[test]
    fn test_parse_unknown() {
        let mut bytes = Bytes::from_static(&[b'?', 1, 2, 3]);
        let msg = parse_replication_message(&mut bytes).unwrap();

        match msg {
            WalMessage::Unknown(tag) => assert_eq!(tag, b'?'),
            _ => panic!("Expected Unknown"),
        }
    }

    #[test]
    fn test_parse_incomplete() {
        // Empty buffer
        let mut bytes = Bytes::new();
        assert!(parse_replication_message(&mut bytes).is_none());

        // XLogData with insufficient header
        let mut bytes = Bytes::from_static(&[b'w', 1, 2, 3]);
        assert!(parse_replication_message(&mut bytes).is_none());

        // KeepAlive with insufficient data
        let mut bytes = Bytes::from_static(&[b'k', 1, 2, 3]);
        assert!(parse_replication_message(&mut bytes).is_none());
    }
}

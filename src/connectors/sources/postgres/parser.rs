// Copyright 2025
// Licensed under the Elastic License v2.0

//! pgoutput protocol parser for PostgreSQL logical replication
//!
//! This module implements a high-performance parser for the pgoutput
//! logical replication protocol. It uses SIMD-optimized operations
//! where possible for maximum throughput.
//!
//! # Protocol Reference
//! - PostgreSQL Logical Replication Message Formats:
//!   https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
//!
//! # Message Types
//! - 'B' - Begin
//! - 'C' - Commit
//! - 'R' - Relation (schema)
//! - 'I' - Insert
//! - 'U' - Update
//! - 'D' - Delete
//! - 'k' - KeepAlive (from replication protocol)

use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes};
use memchr::memchr;
use simdutf8::basic::from_utf8;

/// Parsed CDC message from pgoutput
#[derive(Debug, Clone)]
pub enum CdcMessage {
    /// Transaction begin
    Begin {
        /// Final LSN of the transaction
        final_lsn: u64,
        /// Commit timestamp (microseconds since 2000-01-01)
        timestamp: u64,
        /// Transaction ID
        xid: u32,
    },

    /// Transaction commit
    Commit {
        /// Flags (currently unused)
        flags: u8,
        /// LSN of the commit record
        commit_lsn: u64,
        /// End LSN of the transaction
        end_lsn: u64,
        /// Commit timestamp (microseconds since 2000-01-01)
        timestamp: u64,
    },

    /// Relation (table) definition
    Relation {
        /// Relation OID
        id: u32,
        /// Schema name
        namespace: String,
        /// Table name
        name: String,
        /// Replica identity setting
        replica_identity: u8,
        /// Column definitions
        columns: Vec<Column>,
    },

    /// Insert operation
    Insert {
        /// Relation OID
        relation_id: u32,
        /// New tuple data
        tuple: Tuple,
    },

    /// Update operation
    Update {
        /// Relation OID
        relation_id: u32,
        /// Old tuple (if REPLICA IDENTITY FULL or key)
        old_tuple: Option<Tuple>,
        /// New tuple data
        new_tuple: Tuple,
    },

    /// Delete operation
    Delete {
        /// Relation OID
        relation_id: u32,
        /// Old tuple (if REPLICA IDENTITY FULL or key)
        old_tuple: Option<Tuple>,
    },

    /// KeepAlive message from replication protocol
    KeepAlive {
        /// Current WAL end position
        wal_end: u64,
        /// Server timestamp
        timestamp: u64,
        /// Whether reply is requested
        reply_requested: bool,
    },

    /// Logical replication message (via pg_logical_emit_message)
    /// Used for snapshot watermarks (LW/HW markers)
    LogicalMessage {
        /// True if emitted inside a transaction
        transactional: bool,
        /// LSN of the message
        lsn: u64,
        /// Message prefix (e.g. "dbmazz")
        prefix: String,
        /// Message content (e.g. "LW:table:1:1000" or "HW:table:1:1000")
        content: Bytes,
    },

    /// Unknown message type
    Unknown,
}

/// Column definition from Relation message
#[derive(Debug, Clone)]
pub struct Column {
    /// Column flags (1 = part of key)
    pub flags: u8,
    /// Column name
    pub name: String,
    /// PostgreSQL type OID
    pub type_id: u32,
    /// Type modifier (e.g., precision for numeric)
    pub type_mod: i32,
}

impl Column {
    /// Check if this column is part of the replica identity key
    pub fn is_key(&self) -> bool {
        self.flags & 1 != 0
    }
}

/// Tuple data from Insert/Update/Delete messages
#[derive(Debug, Clone)]
pub struct Tuple {
    /// Column values
    pub cols: Vec<TupleData>,
    /// Bitmap of TOAST columns (efficient check via bit ops)
    pub toast_bitmap: u64,
}

impl Tuple {
    /// O(1) - Check if any column has unchanged TOAST data
    #[inline]
    pub fn has_toast(&self) -> bool {
        self.toast_bitmap != 0
    }

    /// POPCNT instruction - count TOAST columns
    #[inline]
    pub fn toast_count(&self) -> u32 {
        self.toast_bitmap.count_ones()
    }

    /// Check if a specific column is TOAST - O(1)
    #[inline]
    pub fn is_toast_column(&self, idx: usize) -> bool {
        if idx >= 64 {
            return false;
        }
        self.toast_bitmap & (1u64 << idx) != 0
    }

    /// Iterate over TOAST column indices using CTZ
    pub fn toast_indices(&self) -> ToastIterator {
        ToastIterator {
            bitmap: self.toast_bitmap,
        }
    }
}

/// Iterator that uses CTZ (Count Trailing Zeros) for efficient bit scanning
pub struct ToastIterator {
    bitmap: u64,
}

impl Iterator for ToastIterator {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.bitmap == 0 {
            return None;
        }
        // CTZ: find lowest set bit in O(1) with SIMD instruction
        let idx = self.bitmap.trailing_zeros() as usize;
        // Clear lowest bit: x & (x-1) removes the lowest set bit
        self.bitmap &= self.bitmap - 1;
        Some(idx)
    }
}

/// Column value data
#[derive(Debug, Clone)]
pub enum TupleData {
    /// NULL value
    Null,
    /// Text value (zero-copy reference to buffer)
    Text(Bytes),
    /// Unchanged TOAST value (not sent, use previous value)
    Toast,
}

impl TupleData {
    /// Check if the value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, TupleData::Null)
    }

    /// Check if the value is unchanged TOAST
    pub fn is_toast(&self) -> bool {
        matches!(self, TupleData::Toast)
    }

    /// Get the text value as a string slice
    pub fn as_str(&self) -> Option<&str> {
        match self {
            TupleData::Text(bytes) => std::str::from_utf8(bytes).ok(),
            _ => None,
        }
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            TupleData::Text(bytes) => Some(bytes),
            _ => None,
        }
    }
}

/// High-performance pgoutput protocol parser
pub struct PgOutputParser;

impl PgOutputParser {
    /// Parse a pgoutput message
    ///
    /// # Arguments
    /// * `tag` - Message type tag (first byte)
    /// * `body` - Message body (remaining bytes)
    ///
    /// # Returns
    /// Parsed CdcMessage or None if the message should be skipped
    pub fn parse(tag: u8, mut body: Bytes) -> Result<Option<CdcMessage>> {
        match tag {
            b'B' => Self::parse_begin(&mut body),
            b'C' => Self::parse_commit(&mut body),
            b'R' => Self::parse_relation(&mut body),
            b'I' => Self::parse_insert(&mut body),
            b'U' => Self::parse_update(&mut body),
            b'D' => Self::parse_delete(&mut body),
            b'k' => Self::parse_keepalive(&mut body),
            b'M' => Self::parse_logical_message(&mut body),
            _ => Ok(Some(CdcMessage::Unknown)),
        }
    }

    fn parse_begin(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        if data.len() < 20 {
            return Ok(None);
        }
        let final_lsn = data.get_u64();
        let timestamp = data.get_u64();
        let xid = data.get_u32();
        Ok(Some(CdcMessage::Begin {
            final_lsn,
            timestamp,
            xid,
        }))
    }

    fn parse_commit(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        if data.len() < 25 {
            return Ok(None);
        }
        let flags = data.get_u8();
        let commit_lsn = data.get_u64();
        let end_lsn = data.get_u64();
        let timestamp = data.get_u64();
        Ok(Some(CdcMessage::Commit {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        }))
    }

    fn parse_relation(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        if data.remaining() < 4 {
            return Err(anyhow!("EOF in relation"));
        }
        let id = data.get_u32();
        let namespace = Self::read_string(data)?;
        let name = Self::read_string(data)?;
        let replica_identity = data.get_u8();
        let num_columns = data.get_u16();

        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            let flags = data.get_u8();
            let col_name = Self::read_string(data)?;
            let type_id = data.get_u32();
            let type_mod = data.get_i32();
            columns.push(Column {
                flags,
                name: col_name,
                type_id,
                type_mod,
            });
        }

        Ok(Some(CdcMessage::Relation {
            id,
            namespace,
            name,
            replica_identity,
            columns,
        }))
    }

    fn parse_insert(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        let relation_id = data.get_u32();
        let tag = data.get_u8();
        if tag != b'N' {
            return Err(anyhow!("Expected 'N' tag in INSERT, got '{}'", tag as char));
        }
        let tuple = Self::read_tuple(data)?;
        Ok(Some(CdcMessage::Insert { relation_id, tuple }))
    }

    fn parse_update(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        let relation_id = data.get_u32();
        let mut old_tuple = None;
        let mut tag = data.get_u8();

        // 'K' = key columns only, 'O' = old tuple (REPLICA IDENTITY FULL)
        if tag == b'K' || tag == b'O' {
            old_tuple = Some(Self::read_tuple(data)?);
            tag = data.get_u8();
        }

        if tag != b'N' {
            return Err(anyhow!("Expected 'N' tag in UPDATE, got '{}'", tag as char));
        }
        let new_tuple = Self::read_tuple(data)?;

        Ok(Some(CdcMessage::Update {
            relation_id,
            old_tuple,
            new_tuple,
        }))
    }

    fn parse_delete(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        let relation_id = data.get_u32();
        let tag = data.get_u8();

        let old_tuple = if tag == b'K' || tag == b'O' {
            Some(Self::read_tuple(data)?)
        } else {
            None
        };

        Ok(Some(CdcMessage::Delete {
            relation_id,
            old_tuple,
        }))
    }

    /// Parse a logical replication message ('M' tag — from pg_logical_emit_message)
    ///
    /// Format: Byte1(transactional) | UInt64(lsn) | CString(prefix) | UInt32(content_len) | Byte[n](content)
    fn parse_logical_message(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        if data.remaining() < 9 {
            return Ok(None);
        }
        let transactional = data.get_u8() != 0;
        let lsn = data.get_u64();
        let prefix = Self::read_string(data)?;
        if data.remaining() < 4 {
            return Ok(None);
        }
        let content_len = data.get_u32() as usize;
        if data.remaining() < content_len {
            return Ok(None);
        }
        let content = data.split_to(content_len);
        Ok(Some(CdcMessage::LogicalMessage {
            transactional,
            lsn,
            prefix,
            content,
        }))
    }

    fn parse_keepalive(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        let wal_end = data.get_u64();
        let timestamp = data.get_u64();
        let reply_requested = data.get_u8() != 0;
        Ok(Some(CdcMessage::KeepAlive {
            wal_end,
            timestamp,
            reply_requested,
        }))
    }

    /// Read a null-terminated string using SIMD-optimized search
    fn read_string(data: &mut Bytes) -> Result<String> {
        // Use memchr for SIMD search of null terminator
        let len = match memchr(0, data) {
            Some(i) => i,
            None => return Err(anyhow!("String missing null terminator")),
        };

        let bytes = data.split_to(len);
        data.advance(1); // skip null

        // Use simdutf8 for fast UTF-8 validation
        let s = from_utf8(&bytes)?;
        Ok(s.to_string())
    }

    /// Read a tuple (row data)
    fn read_tuple(data: &mut Bytes) -> Result<Tuple> {
        let num_cols = data.get_u16();
        let mut cols = Vec::with_capacity(num_cols as usize);
        let mut toast_bitmap: u64 = 0;

        for idx in 0..num_cols {
            let tag = data.get_u8();
            match tag {
                b'n' => cols.push(TupleData::Null),
                b'u' => {
                    cols.push(TupleData::Toast);
                    // Set bit in bitmap if idx < 64
                    if idx < 64 {
                        toast_bitmap |= 1u64 << idx;
                    }
                }
                b't' => {
                    let len = data.get_u32() as usize;
                    let val = data.split_to(len); // Zero-copy slice
                    cols.push(TupleData::Text(val));
                }
                _ => return Err(anyhow!("Unknown column tag '{}'", tag as char)),
            }
        }

        Ok(Tuple { cols, toast_bitmap })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_toast_bitmap() {
        let tuple = Tuple {
            cols: vec![
                TupleData::Text(Bytes::from("value")),
                TupleData::Toast,
                TupleData::Null,
                TupleData::Toast,
            ],
            toast_bitmap: 0b1010, // columns 1 and 3 are TOAST
        };

        assert!(tuple.has_toast());
        assert_eq!(tuple.toast_count(), 2);
        assert!(!tuple.is_toast_column(0));
        assert!(tuple.is_toast_column(1));
        assert!(!tuple.is_toast_column(2));
        assert!(tuple.is_toast_column(3));
    }

    #[test]
    fn test_toast_iterator() {
        let tuple = Tuple {
            cols: vec![],
            toast_bitmap: 0b101010, // columns 1, 3, 5
        };

        let indices: Vec<usize> = tuple.toast_indices().collect();
        assert_eq!(indices, vec![1, 3, 5]);
    }

    #[test]
    fn test_tuple_data_accessors() {
        let text = TupleData::Text(Bytes::from("hello"));
        assert!(!text.is_null());
        assert!(!text.is_toast());
        assert_eq!(text.as_str(), Some("hello"));
        assert_eq!(text.as_bytes(), Some(b"hello".as_slice()));

        let null = TupleData::Null;
        assert!(null.is_null());
        assert!(!null.is_toast());
        assert_eq!(null.as_str(), None);

        let toast = TupleData::Toast;
        assert!(!toast.is_null());
        assert!(toast.is_toast());
        assert_eq!(toast.as_str(), None);
    }

    #[test]
    fn test_column_is_key() {
        let key_col = Column {
            flags: 1,
            name: "id".to_string(),
            type_id: 23,
            type_mod: -1,
        };
        assert!(key_col.is_key());

        let regular_col = Column {
            flags: 0,
            name: "name".to_string(),
            type_id: 25,
            type_mod: -1,
        };
        assert!(!regular_col.is_key());
    }
}

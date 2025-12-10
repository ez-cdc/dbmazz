use anyhow::{Result, anyhow};
use bytes::{Buf, Bytes};
use memchr::memchr;
use simdutf8::basic::from_utf8;

/// Wrapper que incluye LSN del WAL para checkpointing
#[derive(Debug, Clone)]
pub struct CdcEvent {
    pub lsn: u64,  // LSN del WAL donde ocurrió este evento
    pub message: CdcMessage,
}

#[derive(Debug, Clone)]
pub enum CdcMessage {
    Begin {
        final_lsn: u64,
        timestamp: u64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: u64,
    },
    Relation {
        id: u32,
        namespace: String,
        name: String,
        replica_identity: u8,
        columns: Vec<Column>,
    },
    Insert {
        relation_id: u32,
        tuple: Tuple,
    },
    Update {
        relation_id: u32,
        old_tuple: Option<Tuple>,
        new_tuple: Tuple,
    },
    Delete {
        relation_id: u32,
        old_tuple: Option<Tuple>,
    },
    KeepAlive {
        wal_end: u64,
        timestamp: u64,
        reply_requested: bool,
    },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub flags: u8,
    pub name: String,
    pub type_id: u32,
    pub type_mod: i32,
}

#[derive(Debug, Clone)]
pub struct Tuple(pub Vec<TupleData>);

#[derive(Debug, Clone)]
pub enum TupleData {
    Null,
    Text(Bytes), // Zero-copy: Holds reference to original buffer if possible (Bytes is RefCounted)
    Toast,
}

pub struct PgOutputParser;

impl PgOutputParser {
    pub fn parse(tag: u8, mut body: Bytes) -> Result<Option<CdcMessage>> {
        match tag {
            b'B' => Self::parse_begin(&mut body),
            b'C' => Self::parse_commit(&mut body),
            b'R' => Self::parse_relation(&mut body),
            b'I' => Self::parse_insert(&mut body),
            b'U' => Self::parse_update(&mut body),
            b'D' => Self::parse_delete(&mut body),
            b'k' => Self::parse_keepalive(&mut body),
            _ => Ok(Some(CdcMessage::Unknown)),
        }
    }

    fn parse_begin(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        if data.len() < 20 { return Ok(None); }
        let final_lsn = data.get_u64();
        let timestamp = data.get_u64();
        let xid = data.get_u32();
        Ok(Some(CdcMessage::Begin { final_lsn, timestamp, xid }))
    }

    fn parse_commit(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        if data.len() < 25 { return Ok(None); }
        let flags = data.get_u8();
        let commit_lsn = data.get_u64();
        let end_lsn = data.get_u64();
        let timestamp = data.get_u64();
        Ok(Some(CdcMessage::Commit { flags, commit_lsn, end_lsn, timestamp }))
    }

    fn parse_relation(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        if data.remaining() < 4 { return Err(anyhow!("EOF in relation")); }
        let id = data.get_u32();
        let namespace = Self::read_string(data)?;
        let name = Self::read_string(data)?;
        let replica_identity = data.get_u8();
        let num_columns = data.get_u16();
        
        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            let flags = data.get_u8();
            let name = Self::read_string(data)?;
            let type_id = data.get_u32();
            let type_mod = data.get_i32();
            columns.push(Column { flags, name, type_id, type_mod });
        }
        
        Ok(Some(CdcMessage::Relation { id, namespace, name, replica_identity, columns }))
    }

    fn parse_insert(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        let relation_id = data.get_u32();
        let char_n = data.get_u8(); // 'N'
        if char_n != b'N' { return Err(anyhow!("Expected 'N'")); }
        let tuple = Self::read_tuple(data)?;
        Ok(Some(CdcMessage::Insert { relation_id, tuple }))
    }

    fn parse_update(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        let relation_id = data.get_u32();
        let mut old_tuple = None;
        let mut tag = data.get_u8();
        if tag == b'K' || tag == b'O' {
            old_tuple = Some(Self::read_tuple(data)?);
            tag = data.get_u8();
        }
        if tag != b'N' { return Err(anyhow!("Expected 'N'")); }
        let new_tuple = Self::read_tuple(data)?;
        Ok(Some(CdcMessage::Update { relation_id, old_tuple, new_tuple }))
    }

    fn parse_delete(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        let relation_id = data.get_u32();
        let tag = data.get_u8();
        let old_tuple = if tag == b'K' || tag == b'O' {
             Some(Self::read_tuple(data)?)
        } else {
            None
        };
        Ok(Some(CdcMessage::Delete { relation_id, old_tuple }))
    }

    fn parse_keepalive(data: &mut Bytes) -> Result<Option<CdcMessage>> {
        let wal_end = data.get_u64();
        let timestamp = data.get_u64();
        let reply_requested = data.get_u8() != 0;
        Ok(Some(CdcMessage::KeepAlive { wal_end, timestamp, reply_requested }))
    }

    fn read_string(data: &mut Bytes) -> Result<String> {
        // Use memchr for SIMD search of null terminator
        let len = match memchr(0, data) {
            Some(i) => i,
            None => return Err(anyhow!("String missing null terminator")),
        };
        
        let bytes = data.split_to(len);
        data.advance(1); // skip null
        
        // Use simdutf8 for validation (faster than std)
        let s = from_utf8(&bytes)?;
        Ok(s.to_string())
    }

    fn read_tuple(data: &mut Bytes) -> Result<Tuple> {
        let num_cols = data.get_u16();
        let mut cols = Vec::with_capacity(num_cols as usize);
        for _ in 0..num_cols {
            let tag = data.get_u8();
            match tag {
                b'n' => cols.push(TupleData::Null),
                b'u' => cols.push(TupleData::Toast),
                b't' => {
                    let len = data.get_u32() as usize;
                    let val = data.split_to(len); // Zero-copy slice
                    cols.push(TupleData::Text(val));
                }
                _ => return Err(anyhow!("Unknown column tag {}", tag)),
            }
        }
        Ok(Tuple(cols))
    }
}


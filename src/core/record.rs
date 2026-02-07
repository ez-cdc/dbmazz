use crate::core::position::SourcePosition;
use serde::{Deserialize, Serialize};

/// Database-agnostic CDC record representing a change event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CdcRecord {
    Insert {
        table: TableRef,
        columns: Vec<ColumnValue>,
        position: SourcePosition,
    },
    Update {
        table: TableRef,
        old_columns: Option<Vec<ColumnValue>>,
        new_columns: Vec<ColumnValue>,
        position: SourcePosition,
    },
    Delete {
        table: TableRef,
        columns: Vec<ColumnValue>,
        position: SourcePosition,
    },
    SchemaChange {
        table: TableRef,
        columns: Vec<ColumnDef>,
        position: SourcePosition,
    },
    Begin {
        xid: u64,
    },
    Commit {
        xid: u64,
        position: SourcePosition,
    },
    Heartbeat {
        position: SourcePosition,
    },
}

/// Reference to a database table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
}

impl TableRef {
    pub fn new(schema: Option<String>, name: String) -> Self {
        Self { schema, name }
    }

    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }
}

/// A column value in a CDC record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnValue {
    pub name: String,
    pub value: Value,
}

impl ColumnValue {
    pub fn new(name: String, value: Value) -> Self {
        Self { name, value }
    }
}

/// A column definition for schema changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
}

/// Generic value type supporting common database types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(String),
    Timestamp(i64),
    Decimal(String),
    Uuid(String),
    /// Represents a TOAST value that hasn't changed (PostgreSQL)
    Unchanged,
}

#[allow(dead_code)]
impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn is_unchanged(&self) -> bool {
        matches!(self, Value::Unchanged)
    }
}

/// Database-agnostic data type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal { precision: u8, scale: u8 },
    String,
    Text,
    Bytes,
    Json,
    Jsonb,
    Uuid,
    Date,
    Time,
    Timestamp,
    TimestampTz,
}

#[allow(dead_code)]
impl DataType {
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal { .. }
        )
    }

    pub fn is_text(&self) -> bool {
        matches!(self, DataType::String | DataType::Text)
    }

    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            DataType::Date | DataType::Time | DataType::Timestamp | DataType::TimestampTz
        )
    }
}

use serde::{Deserialize, Serialize};
use std::fmt;

/// Checkpoint position for different source types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SourcePosition {
    /// PostgreSQL LSN (Log Sequence Number)
    Lsn(u64),
    /// MySQL GTID Set
    GtidSet(String),
    /// Kafka-style offset
    Offset(i64),
    /// Binlog file position (MySQL)
    FilePosition { file: String, position: u64 },
}

impl SourcePosition {
    /// Creates a new LSN position
    pub fn lsn(lsn: u64) -> Self {
        Self::Lsn(lsn)
    }

    /// Creates a new GTID set position
    pub fn gtid_set(gtid: String) -> Self {
        Self::GtidSet(gtid)
    }

    /// Creates a new offset position
    pub fn offset(offset: i64) -> Self {
        Self::Offset(offset)
    }

    /// Creates a new file position
    pub fn file_position(file: String, position: u64) -> Self {
        Self::FilePosition { file, position }
    }

    /// Compares two positions, returns true if this position is ahead of the other
    /// Returns None if positions are incomparable (different types)
    pub fn is_ahead_of(&self, other: &SourcePosition) -> Option<bool> {
        match (self, other) {
            (SourcePosition::Lsn(a), SourcePosition::Lsn(b)) => Some(a > b),
            (SourcePosition::Offset(a), SourcePosition::Offset(b)) => Some(a > b),
            (
                SourcePosition::FilePosition {
                    file: f1,
                    position: p1,
                },
                SourcePosition::FilePosition {
                    file: f2,
                    position: p2,
                },
            ) => {
                if f1 == f2 {
                    Some(p1 > p2)
                } else {
                    // File names typically contain sequence numbers
                    // e.g., binlog.000001, binlog.000002
                    Some(f1 > f2)
                }
            }
            (SourcePosition::GtidSet(a), SourcePosition::GtidSet(b)) => {
                // GTID comparison is complex, for now just compare strings
                // In production, this should use proper GTID set comparison
                Some(a > b)
            }
            _ => None, // Incomparable types
        }
    }
}

impl fmt::Display for SourcePosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourcePosition::Lsn(lsn) => write!(f, "LSN:{}", lsn),
            SourcePosition::GtidSet(gtid) => write!(f, "GTID:{}", gtid),
            SourcePosition::Offset(offset) => write!(f, "Offset:{}", offset),
            SourcePosition::FilePosition { file, position } => {
                write!(f, "File:{}:Pos:{}", file, position)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_comparison() {
        let pos1 = SourcePosition::lsn(100);
        let pos2 = SourcePosition::lsn(200);

        assert_eq!(pos2.is_ahead_of(&pos1), Some(true));
        assert_eq!(pos1.is_ahead_of(&pos2), Some(false));
        assert_eq!(pos1.is_ahead_of(&pos1), Some(false));
    }

    #[test]
    fn test_offset_comparison() {
        let pos1 = SourcePosition::offset(1000);
        let pos2 = SourcePosition::offset(2000);

        assert_eq!(pos2.is_ahead_of(&pos1), Some(true));
        assert_eq!(pos1.is_ahead_of(&pos2), Some(false));
    }

    #[test]
    fn test_file_position_comparison() {
        let pos1 = SourcePosition::file_position("binlog.000001".to_string(), 100);
        let pos2 = SourcePosition::file_position("binlog.000001".to_string(), 200);
        let pos3 = SourcePosition::file_position("binlog.000002".to_string(), 50);

        assert_eq!(pos2.is_ahead_of(&pos1), Some(true));
        assert_eq!(pos3.is_ahead_of(&pos1), Some(true));
        assert_eq!(pos3.is_ahead_of(&pos2), Some(true));
    }

    #[test]
    fn test_incompatible_comparison() {
        let lsn = SourcePosition::lsn(100);
        let offset = SourcePosition::offset(100);

        assert_eq!(lsn.is_ahead_of(&offset), None);
        assert_eq!(offset.is_ahead_of(&lsn), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(SourcePosition::lsn(12345).to_string(), "LSN:12345");
        assert_eq!(
            SourcePosition::gtid_set("abc-123".to_string()).to_string(),
            "GTID:abc-123"
        );
        assert_eq!(SourcePosition::offset(9999).to_string(), "Offset:9999");
        assert_eq!(
            SourcePosition::file_position("binlog.001".to_string(), 500).to_string(),
            "File:binlog.001:Pos:500"
        );
    }
}

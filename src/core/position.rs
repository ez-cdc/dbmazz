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
    /// MySQL native checkpoint: the (binlog_file, position, gtid_executed) triple.
    ///
    /// All three are emitted on every binlog event. `gtid_executed` is the
    /// MySQL `Gtid_set` accumulated up to this event (formatted as
    /// `uuid:1-N,uuid:1-M`). Restart prefers `gtid_executed` when non-empty
    /// (failover-safe), falling back to `(file, position)` otherwise.
    MysqlBinlog {
        file: String,
        position: u64,
        gtid_executed: String,
    },
}

#[allow(dead_code)]
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

    /// Creates a new MySQL binlog triple position.
    pub fn mysql_binlog(file: String, position: u64, gtid_executed: String) -> Self {
        Self::MysqlBinlog {
            file,
            position,
            gtid_executed,
        }
    }

    /// Compares two positions, returns `Some(true)` iff `self` is strictly
    /// ahead of `other`. Returns `None` for incomparable pairs.
    ///
    /// `GtidSet` pairs are intentionally **incomparable** (returns `None`):
    /// MySQL GTID sets are partial orders (set inclusion), not totally
    /// ordered. Callers needing partial-order semantics MUST use
    /// `crate::source::mysql::gtid::GtidSet::is_superset_of` directly.
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
            (
                SourcePosition::MysqlBinlog {
                    file: f1,
                    position: p1,
                    ..
                },
                SourcePosition::MysqlBinlog {
                    file: f2,
                    position: p2,
                    ..
                },
            ) => {
                // Compare by (file, position) — gtid_executed is metadata for restart.
                // Binlog file names contain sequence numbers (binlog.000001, binlog.000002),
                // so lexicographic ordering is correct when files differ.
                if f1 == f2 {
                    Some(p1 > p2)
                } else {
                    Some(f1 > f2)
                }
            }
            // GTID sets are partial orders; lexicographic string compare is
            // meaningless. Use `GtidSet::is_superset_of` for set inclusion.
            (SourcePosition::GtidSet(_), SourcePosition::GtidSet(_)) => None,
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
            SourcePosition::MysqlBinlog {
                file,
                position,
                gtid_executed,
            } => {
                if gtid_executed.is_empty() {
                    write!(f, "MySQL:{}:{}", file, position)
                } else {
                    write!(f, "MySQL:{}:{}:gtid={}", file, position, gtid_executed)
                }
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

    #[test]
    fn test_mysql_binlog_comparison() {
        let pos1 = SourcePosition::mysql_binlog("binlog.000001".to_string(), 100, String::new());
        let pos2 = SourcePosition::mysql_binlog("binlog.000001".to_string(), 200, String::new());
        let pos3 = SourcePosition::mysql_binlog("binlog.000002".to_string(), 50, String::new());

        assert_eq!(pos2.is_ahead_of(&pos1), Some(true));
        assert_eq!(pos3.is_ahead_of(&pos1), Some(true));
        assert_eq!(pos3.is_ahead_of(&pos2), Some(true));
        assert_eq!(pos1.is_ahead_of(&pos1), Some(false));
    }

    #[test]
    fn test_mysql_binlog_comparison_ignores_gtid_field() {
        // gtid_executed is metadata, not used for ordering.
        let pos1 = SourcePosition::mysql_binlog(
            "binlog.000001".to_string(),
            100,
            "uuid-a:1-1".to_string(),
        );
        let pos2 = SourcePosition::mysql_binlog(
            "binlog.000001".to_string(),
            100,
            "uuid-a:1-100".to_string(),
        );
        // Same (file, position) → not ahead either way regardless of gtid_executed.
        assert_eq!(pos1.is_ahead_of(&pos2), Some(false));
        assert_eq!(pos2.is_ahead_of(&pos1), Some(false));
    }

    #[test]
    fn test_mysql_binlog_display() {
        let with_gtid = SourcePosition::mysql_binlog(
            "binlog.000003".to_string(),
            12345,
            "1d51b75e-aaaa-bbbb-cccc-1234567890ab:1-42".to_string(),
        );
        assert_eq!(
            with_gtid.to_string(),
            "MySQL:binlog.000003:12345:gtid=1d51b75e-aaaa-bbbb-cccc-1234567890ab:1-42"
        );

        let without_gtid =
            SourcePosition::mysql_binlog("binlog.000003".to_string(), 12345, String::new());
        assert_eq!(without_gtid.to_string(), "MySQL:binlog.000003:12345");
    }

    #[test]
    fn test_gtid_set_is_ahead_of_returns_none() {
        // GTID sets are partial orders; is_ahead_of must return None for any
        // pair, including identical sets. Callers must use
        // `GtidSet::is_superset_of` for set-inclusion semantics.
        let a = SourcePosition::gtid_set("uuid-A:1-100".into());
        let b = SourcePosition::gtid_set("uuid-B:1-100".into());
        let same = SourcePosition::gtid_set("uuid-A:1-100".into());

        assert_eq!(a.is_ahead_of(&b), None);
        assert_eq!(b.is_ahead_of(&a), None);
        assert_eq!(a.is_ahead_of(&same), None);
        assert_eq!(same.is_ahead_of(&a), None);
    }

    #[test]
    fn test_mysql_binlog_serde_roundtrip() {
        let pos = SourcePosition::mysql_binlog(
            "binlog.000007".to_string(),
            999_999,
            "1d51b75e-aaaa-bbbb-cccc-1234567890ab:1-100".to_string(),
        );
        let json = serde_json::to_string(&pos).unwrap();
        let back: SourcePosition = serde_json::from_str(&json).unwrap();
        assert_eq!(pos, back);
    }
}

// Copyright 2025
// Licensed under the Elastic License v2.0

//! MySQL GTID (Global Transaction Identifier) set parsing and comparison utilities.
//!
//! Provides the [`GtidSet`] type for parsing MySQL GTID set strings (e.g.
//! `UUID1:N1[-M1],UUID2:N2[-M2]`) and performing set membership checks
//! required by the Offset Signal Algorithm (T6).
//!
//! # GTID Format
//!
//! A MySQL GTID set has the form:
//! ```text
//! UUID1:transaction_id1[-transaction_idN],UUID2:transaction_id1[-transaction_idN]
//! ```
//!
//! Each UUID identifies a server, and the transaction IDs (or intervals)
//! represent committed transactions on that server.
//!
//! # Examples
//!
//! ```ignore
//! use crate::source::mysql::gtid::GtidSet;
//!
//! let set = GtidSet::parse("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5:1-49").unwrap();
//! assert!(set.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5", 42));
//! assert!(!set.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5", 50));
//! ```

use std::collections::HashMap;

use anyhow::{Context, Result};

/// A single GTID consisting of a source UUID and transaction number.
///
/// Example: `3E11FA47-71CA-11E7-81E2-2115B5C6C7F5:127`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Gtid {
    pub uuid: String,
    pub transaction_id: u64,
}

/// A contiguous range of GTIDs belonging to the same source UUID.
///
/// The interval is inclusive on both ends: `[start, end]`.
/// A single transaction is represented as `start == end`.
#[derive(Debug, Clone)]
pub struct GtidInterval {
    pub uuid: String,
    pub start: u64,
    pub end: u64,
}

/// A parsed MySQL GTID set.
///
/// Internally stores a map from UUID to a list of `(start, end)` inclusive ranges.
/// Ranges are stored in parsed order and may overlap (MySQL does not guarantee
/// canonical ordering in all server outputs).
///
/// # Empty Set
///
/// An empty string or whitespace-only string parses to an empty set containing
/// no intervals. All membership checks on an empty set return `false`.
#[derive(Debug, Clone)]
pub struct GtidSet {
    pub intervals: HashMap<String, Vec<(u64, u64)>>,
}

impl GtidSet {
    /// Parse a GTID set string into structured data.
    ///
    /// # Format
    ///
    /// The input string must follow MySQL GTID set syntax:
    /// ```text
    /// UUID1:N1[-M1][:N2[-M2]],UUID2:N1[-M2]
    /// ```
    ///
    /// Multiple intervals for the same UUID are separated by `:` after the UUID.
    /// Multiple UUIDs are separated by `,`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The format is invalid (missing colon, malformed numbers)
    /// - Transaction IDs or interval bounds are not valid [`u64`] values
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbmazz::source::mysql::gtid::GtidSet;
    /// let set = GtidSet::parse("uuid:1-10,uuid2:20-30").unwrap();
    /// assert!(set.contains("uuid", 5));
    /// assert!(!set.contains("uuid2", 35));
    /// ```
    pub fn parse(s: &str) -> Result<Self> {
        let mut intervals: HashMap<String, Vec<(u64, u64)>> = HashMap::new();

        let s = s.trim();
        if s.is_empty() {
            return Ok(Self { intervals });
        }

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            // Split by ':' to get UUID and interval(s)
            let colon_pos = part
                .find(':')
                .context("Invalid GTID format: missing colon")?;
            let uuid = part[..colon_pos].to_string();
            let range_str = &part[colon_pos + 1..];

            let entry = intervals.entry(uuid).or_default();

            for range_part in range_str.split(':') {
                let range_part = range_part.trim();
                if range_part.is_empty() {
                    continue;
                }

                if let Some(dash_pos) = range_part.find('-') {
                    let start: u64 = range_part[..dash_pos]
                        .parse()
                        .context("Invalid GTID interval start")?;
                    let end: u64 = range_part[dash_pos + 1..]
                        .parse()
                        .context("Invalid GTID interval end")?;
                    entry.push((start, end));
                } else {
                    let single: u64 = range_part
                        .parse()
                        .context("Invalid GTID transaction number")?;
                    entry.push((single, single));
                }
            }
        }

        Ok(Self { intervals })
    }

    /// Check if a single GTID (`UUID:N`) is contained in this set.
    ///
    /// Returns `true` if the given `uuid` has an interval that covers
    /// `transaction_id` (inclusive on both ends).
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbmazz::source::mysql::gtid::GtidSet;
    /// let set = GtidSet::parse("uuid:1-100").unwrap();
    /// assert!(set.contains("uuid", 50));
    /// assert!(!set.contains("uuid", 101));
    /// assert!(!set.contains("other", 50));
    /// ```
    pub fn contains(&self, uuid: &str, transaction_id: u64) -> bool {
        if let Some(ranges) = self.intervals.get(uuid) {
            for &(start, end) in ranges {
                if transaction_id >= start && transaction_id <= end {
                    return true;
                }
            }
        }
        false
    }

    /// Check if a GTID string (`UUID:N`) is contained in this set.
    ///
    /// This is a convenience wrapper around [`contains`](Self::contains) that
    /// parses the string first.
    ///
    /// # Errors
    ///
    /// Returns an error if the GTID string is malformed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbmazz::source::mysql::gtid::GtidSet;
    /// let set = GtidSet::parse("uuid:1-100").unwrap();
    /// assert!(set.contains_gtid_str("uuid:50").unwrap());
    /// assert!(!set.contains_gtid_str("uuid:101").unwrap());
    /// ```
    pub fn contains_gtid_str(&self, gtid_str: &str) -> Result<bool> {
        let colon_pos = gtid_str
            .find(':')
            .context("Invalid GTID format: missing colon")?;
        let uuid = &gtid_str[..colon_pos];
        let txn: u64 = gtid_str[colon_pos + 1..]
            .parse()
            .context("Invalid GTID transaction number")?;
        Ok(self.contains(uuid, txn))
    }

    /// Check if a GTID is in the range `(low_set, high_set]`.
    ///
    /// Returns `true` if the GTID is **not** in `low_set` **and** is in `high_set`.
    /// This is the core predicate used by the Offset Signal Algorithm (T6)
    /// to determine if a transaction falls within a delta window between
    /// two GTID sets.
    ///
    /// # Arguments
    ///
    /// * `gtid_str` - A single GTID string in `UUID:N` format.
    /// * `low_set` - The lower bound GTID set string.
    /// * `high_set` - The upper bound GTID set string.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the input strings are malformed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbmazz::source::mysql::gtid::GtidSet;
    /// // 127 is NOT in low_set but IS in high_set → true
    /// let result = GtidSet::gtid_in_range(
    ///     "uuid:127",
    ///     "uuid:1-100",
    ///     "uuid:1-153",
    /// ).unwrap();
    /// assert!(result);
    ///
    /// // 50 IS in low_set → false
    /// let result = GtidSet::gtid_in_range(
    ///     "uuid:50",
    ///     "uuid:1-100",
    ///     "uuid:1-153",
    /// ).unwrap();
    /// assert!(!result);
    ///
    /// // 200 is NOT in high_set → false
    /// let result = GtidSet::gtid_in_range(
    ///     "uuid:200",
    ///     "uuid:1-100",
    ///     "uuid:1-153",
    /// ).unwrap();
    /// assert!(!result);
    /// ```
    pub fn gtid_in_range(gtid_str: &str, low_set: &str, high_set: &str) -> Result<bool> {
        let colon_pos = gtid_str.find(':').context("Invalid GTID format")?;
        let uuid = &gtid_str[..colon_pos];
        let txn: u64 = gtid_str[colon_pos + 1..]
            .parse()
            .context("Invalid GTID transaction number")?;

        let low = GtidSet::parse(low_set)?;
        let high = GtidSet::parse(high_set)?;

        Ok(!low.contains(uuid, txn) && high.contains(uuid, txn))
    }

    /// Format the GTID set back into its canonical string representation.
    ///
    /// UUIDs are sorted lexicographically for deterministic output.
    /// Multiple intervals for the same UUID are output in their stored order.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbmazz::source::mysql::gtid::GtidSet;
    /// let set = GtidSet::parse("uuid2:1-5,uuid1:10-20").unwrap();
    /// let output = set.to_string();
    /// assert!(output.contains("uuid1:10-20"));
    /// assert!(output.contains("uuid2:1-5"));
    /// ```
    pub fn format(&self) -> String {
        let mut parts = Vec::new();
        let mut uuids: Vec<&String> = self.intervals.keys().collect();
        uuids.sort();
        for uuid in uuids {
            if let Some(ranges) = self.intervals.get(uuid.as_str()) {
                let range_strs: Vec<String> = ranges
                    .iter()
                    .map(|&(start, end)| {
                        if start == end {
                            format!("{}", start)
                        } else {
                            format!("{}-{}", start, end)
                        }
                    })
                    .collect();
                parts.push(format!("{}:{}", uuid, range_strs.join(":")));
            }
        }
        parts.join(",")
    }
}

impl std::fmt::Display for GtidSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format())
    }
}

impl std::str::FromStr for GtidSet {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        GtidSet::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_gtid() {
        let set = GtidSet::parse("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5:1").unwrap();
        assert!(set.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5", 1));
        assert!(!set.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5", 2));
    }

    #[test]
    fn test_parse_interval() {
        let set = GtidSet::parse("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5:1-100").unwrap();
        assert!(set.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5", 1));
        assert!(set.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5", 50));
        assert!(set.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5", 100));
        assert!(!set.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5", 101));
    }

    #[test]
    fn test_parse_multiple_uuids() {
        let set = GtidSet::parse("uuid1:1-10,uuid2:20-30").unwrap();
        assert!(set.contains("uuid1", 5));
        assert!(!set.contains("uuid1", 15));
        assert!(set.contains("uuid2", 25));
        assert!(!set.contains("uuid2", 35));
    }

    #[test]
    fn test_multiple_intervals_same_uuid() {
        let set = GtidSet::parse("uuid:1-10:20-30").unwrap();
        assert!(set.contains("uuid", 5));
        assert!(!set.contains("uuid", 15));
        assert!(set.contains("uuid", 25));
        assert!(!set.contains("uuid", 35));
    }

    #[test]
    fn test_gtid_in_range() {
        // 127 is in (1-100, 1-153] — NOT in low_set, IS in high_set
        let result = GtidSet::gtid_in_range("uuid:127", "uuid:1-100", "uuid:1-153").unwrap();
        assert!(result);

        // 50 IS in low_set, so NOT in (low_set, high_set]
        let result2 = GtidSet::gtid_in_range("uuid:50", "uuid:1-100", "uuid:1-153").unwrap();
        assert!(!result2);

        // 200 is NOT in high_set, so NOT in (low_set, high_set]
        let result3 = GtidSet::gtid_in_range("uuid:200", "uuid:1-100", "uuid:1-153").unwrap();
        assert!(!result3);
    }

    #[test]
    fn test_gtid_in_range_different_uuids() {
        // Different UUID in low vs high — GTID only in high_set
        let result = GtidSet::gtid_in_range("uuid2:50", "uuid1:1-100", "uuid2:1-100").unwrap();
        assert!(result);

        // GTID in both low and high (different UUIDs)
        let result2 = GtidSet::gtid_in_range("uuid1:50", "uuid1:1-100", "uuid2:1-100").unwrap();
        assert!(!result2);
    }

    #[test]
    fn test_empty_set() {
        let set = GtidSet::parse("").unwrap();
        assert!(!set.contains("any", 1));

        let set = GtidSet::parse("   ").unwrap();
        assert!(!set.contains("any", 1));
    }

    #[test]
    fn test_roundtrip() {
        let input =
            "3E11FA47-71CA-11E7-81E2-2115B5C6C7F5:1-49,5A11FB48-82DB-23F8-92F3-3226C6D7D8G6:50-100";
        let set = GtidSet::parse(input).unwrap();
        let output = set.to_string();
        // Should contain both UUIDs with their intervals
        assert!(output.contains("3E11FA47-71CA-11E7-81E2-2115B5C6C7F5"));
        assert!(output.contains("5A11FB48-82DB-23F8-92F3-3226C6D7D8G6"));
        assert!(output.contains("1-49"));
        assert!(output.contains("50-100"));
    }

    #[test]
    fn test_roundtrip_single_transactions() {
        let input = "uuid:1:5:10";
        let set = GtidSet::parse(input).unwrap();
        let output = set.to_string();
        // Each single transaction is rendered individually
        assert!(output.contains("1"));
        assert!(output.contains("5"));
        assert!(output.contains("10"));
    }

    #[test]
    fn test_contains_gtid_str() {
        let set = GtidSet::parse("uuid:1-100").unwrap();
        assert!(set.contains_gtid_str("uuid:50").unwrap());
        assert!(!set.contains_gtid_str("uuid:101").unwrap());
    }

    #[test]
    fn test_contains_gtid_str_invalid() {
        let set = GtidSet::parse("uuid:1-100").unwrap();
        assert!(set.contains_gtid_str("uuid:invalid").is_err());
        assert!(set.contains_gtid_str("no-colon").is_err());
    }

    #[test]
    fn test_parse_error_missing_colon() {
        let result = GtidSet::parse("no-colon-here");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_invalid_number() {
        let result = GtidSet::parse("uuid:abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_invalid_interval() {
        let result = GtidSet::parse("uuid:1-abc");
        assert!(result.is_err());

        let result = GtidSet::parse("uuid:abc-10");
        assert!(result.is_err());
    }

    #[test]
    fn test_display_trait() {
        let set = GtidSet::parse("uuid:1-100").unwrap();
        let display_str = format!("{}", set);
        assert_eq!(display_str, "uuid:1-100");
    }

    #[test]
    fn test_from_str_trait() {
        let set: GtidSet = "uuid:1-100".parse().unwrap();
        assert!(set.contains("uuid", 50));
    }

    #[test]
    fn test_contains_after_multiple_uuids() {
        let set = GtidSet::parse("uuid1:1-10,uuid2:20-30:40-50,uuid3:60").unwrap();
        assert!(set.contains("uuid2", 25));
        assert!(set.contains("uuid2", 45));
        assert!(set.contains("uuid3", 60));
        assert!(!set.contains("uuid2", 35));
        assert!(!set.contains("uuid3", 61));
    }

    #[test]
    fn test_whitespace_handling() {
        let set = GtidSet::parse(" uuid1:1-10 , uuid2:20-30 ").unwrap();
        assert!(set.contains("uuid1", 5));
        assert!(set.contains("uuid2", 25));
    }

    #[test]
    fn test_gtid_struct_equality() {
        let g1 = Gtid {
            uuid: "uuid1".to_string(),
            transaction_id: 42,
        };
        let g2 = Gtid {
            uuid: "uuid1".to_string(),
            transaction_id: 42,
        };
        let g3 = Gtid {
            uuid: "uuid1".to_string(),
            transaction_id: 43,
        };
        assert_eq!(g1, g2);
        assert_ne!(g1, g3);
    }
}

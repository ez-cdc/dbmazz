// Copyright 2025
// Licensed under the Elastic License v2.0

//! MySQL GTID set parsing and comparison.
//!
//! Provides [`GtidSet`] for parsing GTID set strings
//! (`UUID1:N1[-M1],UUID2:N2[-M2]`) and set membership checks
//! used by the Offset Signal Algorithm (T6).

use std::collections::HashMap;

use anyhow::{Context, Result};

/// A single GTID with a source UUID and transaction number.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Gtid {
    pub uuid: String,
    pub transaction_id: u64,
}

/// A contiguous range of GTIDs for the same source UUID, inclusive on both ends.
#[derive(Debug, Clone)]
pub struct GtidInterval {
    pub uuid: String,
    pub start: u64,
    pub end: u64,
}

/// A parsed MySQL GTID set — map from UUID to inclusive `(start, end)` intervals.
///
/// Parsed order is preserved; intervals may overlap.
/// An empty or whitespace-only string parses to an empty set.
#[derive(Debug, Clone, Default)]
pub struct GtidSet {
    pub intervals: HashMap<String, Vec<(u64, u64)>>,
}

impl GtidSet {
    /// Parse a GTID set string (`UUID1:N1[-M1],UUID2:N2[-M2]`).
    ///
    /// # Errors
    ///
    /// Returns an error if the format is invalid or transaction IDs are not valid [`u64`] values.
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

    /// Check if a single GTID (`UUID:N`) is in this set.
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

    /// Convenience wrapper around [`contains`](Self::contains) that parses a `UUID:N` string.
    ///
    /// # Errors
    ///
    /// Returns an error if the GTID string is malformed.
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

    /// Returns `true` if the GTID is in `(low_set, high_set]` — not in `low_set` but in `high_set`.
    ///
    /// Core predicate of the Offset Signal Algorithm (T6) for determining
    /// whether a transaction falls within a delta window between two GTID sets.
    ///
    /// # Errors
    ///
    /// Returns an error if any input strings are malformed.
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

    /// Append a single GTID `(uuid, transaction_id)` to this set.
    ///
    /// If the new transaction is contiguous with the last interval for that
    /// UUID (i.e. `last.end + 1 == transaction_id`), the interval is extended
    /// in place. Otherwise a new singleton interval is added. This keeps the
    /// serialized form compact for the common case of monotonic GTID
    /// streaming from a single source.
    pub fn add_gtid(&mut self, uuid: String, transaction_id: u64) {
        let entry = self.intervals.entry(uuid).or_default();
        if let Some(last) = entry.last_mut() {
            if last.1 + 1 == transaction_id {
                last.1 = transaction_id;
                return;
            }
            if transaction_id >= last.0 && transaction_id <= last.1 {
                // Already covered; idempotent on duplicate event delivery.
                return;
            }
        }
        entry.push((transaction_id, transaction_id));
    }

    /// Returns `true` if the set has no entries.
    pub fn is_empty(&self) -> bool {
        self.intervals.values().all(|v| v.is_empty())
    }

    /// Returns `true` iff every `(uuid, txn)` covered by `other` is also
    /// covered by `self`. Used by the DBLog incremental snapshot
    /// reconciliation (T4) to detect when the binlog stream consumer has
    /// drained past a chunk's HIGH watermark.
    ///
    /// Complexity: O(I·log I) per UUID where I is the interval count for
    /// that UUID, independent of the transaction-id ranges. We never
    /// iterate individual GTID numbers — that would blow up for sources
    /// that have executed millions of transactions.
    pub fn is_superset_of(&self, other: &GtidSet) -> bool {
        for (uuid, other_ranges) in &other.intervals {
            let self_ranges = match self.intervals.get(uuid) {
                Some(r) if !r.is_empty() => r,
                _ => return false,
            };

            // Sort and coalesce self's intervals for this UUID once.
            let mut sorted = self_ranges.clone();
            sorted.sort_by_key(|&(s, _)| s);
            let mut merged: Vec<(u64, u64)> = Vec::with_capacity(sorted.len());
            for r in sorted {
                if let Some(last) = merged.last_mut() {
                    if r.0 <= last.1.saturating_add(1) {
                        last.1 = last.1.max(r.1);
                        continue;
                    }
                }
                merged.push(r);
            }

            for &(o_start, o_end) in other_ranges {
                let covered = merged.iter().any(|&(s, e)| s <= o_start && o_end <= e);
                if !covered {
                    return false;
                }
            }
        }
        true
    }

    /// Format the GTID set into its canonical string representation (UUIDs are sorted lexicographically).
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
    fn test_add_gtid_extends_contiguous() {
        let mut set = GtidSet::parse("uuid:1-5").unwrap();
        set.add_gtid("uuid".to_string(), 6);
        assert_eq!(set.format(), "uuid:1-6");
        set.add_gtid("uuid".to_string(), 7);
        assert_eq!(set.format(), "uuid:1-7");
    }

    #[test]
    fn test_add_gtid_new_singleton_when_gap() {
        let mut set = GtidSet::parse("uuid:1-5").unwrap();
        set.add_gtid("uuid".to_string(), 10);
        // Gap: keeps as separate interval. Format keeps insertion order per uuid.
        assert!(set.contains("uuid", 5));
        assert!(set.contains("uuid", 10));
        assert!(!set.contains("uuid", 6));
    }

    #[test]
    fn test_add_gtid_idempotent_on_duplicate() {
        let mut set = GtidSet::parse("uuid:1-5").unwrap();
        set.add_gtid("uuid".to_string(), 3);
        assert_eq!(set.format(), "uuid:1-5");
    }

    #[test]
    fn test_add_gtid_new_uuid() {
        let mut set = GtidSet::parse("uuid-a:1-5").unwrap();
        set.add_gtid("uuid-b".to_string(), 1);
        assert!(set.contains("uuid-a", 3));
        assert!(set.contains("uuid-b", 1));
    }

    #[test]
    fn test_superset_empty_empty() {
        let a = GtidSet::parse("").unwrap();
        let b = GtidSet::parse("").unwrap();
        assert!(a.is_superset_of(&b));
    }

    #[test]
    fn test_superset_self() {
        let a = GtidSet::parse("uuid-a:1-100,uuid-b:1-50").unwrap();
        assert!(a.is_superset_of(&a.clone()));
    }

    #[test]
    fn test_superset_proper() {
        let big = GtidSet::parse("uuid-a:1-100").unwrap();
        let small = GtidSet::parse("uuid-a:50-90").unwrap();
        assert!(big.is_superset_of(&small));
        assert!(!small.is_superset_of(&big));
    }

    #[test]
    fn test_superset_different_uuids() {
        let a = GtidSet::parse("uuid-a:1-100").unwrap();
        let b = GtidSet::parse("uuid-b:1-100").unwrap();
        assert!(!a.is_superset_of(&b));
        assert!(!b.is_superset_of(&a));
    }

    #[test]
    fn test_superset_overlapping_but_not_superset() {
        // a covers 1-50, b covers 25-100 — neither is superset of the other.
        let a = GtidSet::parse("uuid:1-50").unwrap();
        let b = GtidSet::parse("uuid:25-100").unwrap();
        assert!(!a.is_superset_of(&b));
        assert!(!b.is_superset_of(&a));
    }

    #[test]
    fn test_superset_split_intervals_get_coalesced() {
        // self has [1-50, 51-100] (split intervals, contiguous);
        // is_superset_of must coalesce them and recognise [1-100] coverage.
        let self_set = GtidSet::parse("uuid:1-50:51-100").unwrap();
        let other = GtidSet::parse("uuid:1-100").unwrap();
        assert!(self_set.is_superset_of(&other));
    }

    #[test]
    fn test_superset_huge_range_does_not_iterate_txns() {
        // If is_superset_of iterated each txn, this would take seconds.
        // The interval-aware impl is constant-time in this metric.
        let big = GtidSet::parse("uuid:1-100000000").unwrap();
        let small = GtidSet::parse("uuid:50000000-60000000").unwrap();
        assert!(big.is_superset_of(&small));
    }

    #[test]
    fn test_add_gtid_to_empty() {
        let mut set = GtidSet::parse("").unwrap();
        assert!(set.is_empty());
        set.add_gtid("uuid".to_string(), 1);
        assert_eq!(set.format(), "uuid:1");
        assert!(!set.is_empty());
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

# Oracle Sink Type Mapping Bugs

## Summary

This document catalogs all bugs found in `types.rs` for the Oracle sink connector.
Each finding includes severity, root cause, fix suggestion, and a test command.

---

## BUG-1 [HIGH] — `value_to_oracle_expr` for `Value::Timestamp`: format mask expects `.FF9` but missing when subsec_nanos = 0

- **File**: `/Users/nahuelmazzitelli/.ez-cdc-runs/ez-cdc-run-oracle-65f41d2e/dbmazz/src/connectors/sinks/oracle/types.rs`
- **Lines**: 151–158
- **Root cause**: `NaiveDateTime`'s `Display` impl outputs `%Y-%m-%d %H:%M:%S` with **no** fractional part when subsec_nanos = 0 (e.g., `"2024-01-15 10:30:00"`). The `TO_TIMESTAMP` format mask is `"YYYY-MM-DD HH24:MI:SS.FF9"`, which **requires** a fractional-seconds component. Oracle raises `ORA-01862: the numeric value does not match the format string` because the input string has no `.` followed by digits, but the mask demands `.FF9`.

- **Fix**: Conditionally include `.FF9` in the format mask only when subsec_nanos > 0, or use `naive.format("%Y-%m-%d %H:%M:%S%.f")` in the output and always use `"YYYY-MM-DD HH24:MI:SS"` (dropping `.FF9`).

- **Test**: Confirm that `value_to_oracle_expr(&Value::Timestamp(0))` produces `TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')` (no `.FF9` in mask).

---

## BUG-2 [HIGH] — `value_to_oracle_date_expr` for `Value::Timestamp` (non-Date): format mask `YYYY-MM-DD HH24:MI:SS` rejects fractional seconds

- **File**: `/Users/nahuelmazzitelli/.ez-cdc-runs/ez-cdc-run-oracle-65f41d2e/dbmazz/src/connectors/sinks/oracle/types.rs`
- **Lines**: 39–53
- **Root cause**: The `else` branch (lines 48–53) formats the timestamp using `%Y-%m-%d %H:%M:%S%.f`, which includes fractional seconds when non-zero (e.g., `"2024-01-15 10:30:00.123456"`). But the `TO_TIMESTAMP` mask is `"YYYY-MM-DD HH24:MI:SS"` — **without** a `.FF` suffix. Oracle raises `ORA-01861: literal does not match format string` because the `".123456"` suffix is not consumed by the format mask.

- **Fix**: Change the format mask to `"YYYY-MM-DD HH24:MI:SS.FF6"` to match whatever fractional seconds the `%.f` formatter produces, and ensure the formatted string always includes at least a `.0` when subsec_nanos = 0 by using a format like `%.6f` instead of `%.f`.

- **Test**: `value_to_oracle_date_expr(&Value::Timestamp(1705312200123456000), &ts_col)` → verify mask includes `.FF6`.

---

## BUG-3 [HIGH] — `value_to_oracle_date_expr` for `Value::String` (non-Date): format mask `YYYY-MM-DD HH24:MI:SS` rejects fractional seconds from snapshot

- **File**: `/Users/nahuelmazzitelli/.ez-cdc-runs/ez-cdc-run-oracle-65f41d2e/dbmazz/src/connectors/sinks/oracle/types.rs`
- **Lines**: 34–37
- **Root cause**: Snapshot strings arrive from PG as `"2024-01-15 10:30:00.123456+00"`. The timezone is stripped but fractional seconds are **kept** (cleaned string: `"2024-01-15 10:30:00.123456"`). The `TO_TIMESTAMP` mask `"YYYY-MM-DD HH24:MI:SS"` has no `.FF` element, so Oracle rejects the `.123456` suffix. Same failure as BUG-2 but for snapshot data.

- **Fix**: Change format mask to `"YYYY-MM-DD HH24:MI:SS.FF6"` (or `.FF9`) to accept optional fractional seconds. No change to the string-cleaning logic is needed.

- **Test**: `value_to_oracle_date_expr(&Value::String("2024-01-15 10:30:00.123456+00".into()), &ts_col)` → mask includes `.FF6`.

---

## BUG-4 [MEDIUM] — `data_type_to_oracle` for `DataType::Time`: maps wall-clock time to `INTERVAL DAY TO SECOND`, but TO_TIMESTAMP wrapper will fail

- **File**: `/Users/nahuelmazzitelli/.ez-cdc-runs/ez-cdc-run-oracle-65f41d2e/dbmazz/src/connectors/sinks/oracle/types.rs`
- **Lines**: 92
- **Root cause**: `DataType::Time` maps to `"INTERVAL DAY TO SECOND(0)"`. In Oracle, `INTERVAL DAY TO SECOND` stores **durations**, not wall-clock times. A PG `TIME` value like `"14:30:00"` representing 2:30 PM would be semantically wrong as a duration. Moreover, because `is_date_column()` returns `true` for `DataType::Time` (line 13), the value goes through `value_to_oracle_date_expr`, which wraps it in `TO_TIMESTAMP('14:30:00', 'YYYY-MM-DD HH24:MI:SS')` — which will fail because `"14:30:00"` has no date component to match `YYYY-MM-DD`.

- **Fix options**:
  - Map `DataType::Time` to `INTERVAL DAY TO SECOND` AND handle the value conversion to `INTERVAL '0 14:30:00' DAY TO SECOND` literal format. Remove `Time` from `is_date_column()`.
  - Or map to `VARCHAR2(8)` / `VARCHAR2(12)` and store as a string.

- **Test**: `data_type_to_oracle(&DataType::Time)` returns `"INTERVAL DAY TO SECOND(0)"` — verify decision with team.

---

## BUG-5 [HIGH] — Negative epoch timestamps: incorrect secs/subsec_nanos decomposition due to truncation-toward-zero division

- **File**: `/Users/nahuelmazzitelli/.ez-cdc-runs/ez-cdc-run-oracle-65f41d2e/dbmazz/src/connectors/sinks/oracle/types.rs`
- **Lines**: 153–154 (and lines 41–42)
- **Root cause**: For negative `ts` (epoch nanos before 1970), `ts / 1_000_000_000` in Rust truncates toward zero, not toward negative infinity. Example: `ts = -1_000_000_001` → `secs = -1` (should be `-2`), `subsec_nanos = (-1 % 1_000_000_000).abs() = 1`. Then `from_timestamp_opt(-1, 1)` gives `1969-12-31 23:59:59.000000001` instead of the correct `1969-12-31 23:59:58.999999999` — a **1-second error** for timestamps before 1970 that are not exact multiples of 1 second.

  This bug exists in **both** `value_to_oracle_expr` (lines 153–154) and `value_to_oracle_date_expr` (lines 41–42).

- **Fix**: Use floor-style Euclidean division:
  ```rust
  let secs = ts.div_euclid(1_000_000_000);
  let subsec_nanos = ts.rem_euclid(1_000_000_000) as u32;
  ```

- **Test**: `value_to_oracle_expr(&Value::Timestamp(-1_000_000_001))` → `TO_TIMESTAMP('1969-12-31 23:59:58.999999999', 'YYYY-MM-DD HH24:MI:SS.FF9')`

---

## BUG-6 [LOW] — `value_to_oracle_date_expr` string cleaning: can produce empty string for unusual timezone-only inputs

- **File**: `/Users/nahuelmazzitelli/.ez-cdc-runs/ez-cdc-run-oracle-65f41d2e/dbmazz/src/connectors/sinks/oracle/types.rs`
- **Lines**: 26–29
- **Root cause**: The `trim_end_matches` closure removes `+`, `-`, `:`, and `0-9`. If the input string is just a timezone like `"+00"` (hypothetical edge case), every character matches the predicate and the string is reduced to `""`. This then produces `TO_TIMESTAMP('', 'YYYY-MM-DD HH24:MI:SS')` which would fail in Oracle. Realistic risk is low because date/time values should always have date components.

- **Fix**: Guard against empty cleaned string:
  ```rust
  if cleaned.is_empty() { return "NULL".to_string(); }
  ```

- **Test**: Hard to trigger naturally; unit-test with `Value::String("+00".into())`.

---

## Additional Observations

### Observation A — Redundant `if` in `build_using_select` (mod.rs lines 169–176)

Not a bug, but both branches of `if is_date_column(sc)` call `value_to_oracle_typed_expr(val, sc)` identically. The condition is dead code and should be simplified to a single call.

### Observation B — Snowflake and StarRocks sinks have the same negative-epoch division bug

Both sinks (`snowflake/types.rs` line 96, `starrocks/types.rs` line 161) use `ts / 1_000_000` for microsecond precision, which has the same truncation-toward-zero issue for negative timestamps. If those sinks need to support dates before 1970, they should also be fixed.

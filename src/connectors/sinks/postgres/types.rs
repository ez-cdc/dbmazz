// Copyright 2025
// Licensed under the Elastic License v2.0

//! PostgreSQL source → PostgreSQL target type mapping.
//!
//! Mostly identity mapping since both sides are PostgreSQL.
//! Notable exceptions: serial types become plain integers (no sequence in target).

/// Map a PostgreSQL type OID to the target column type DDL string.
/// Returns the SQL type name suitable for CREATE TABLE.
pub fn pg_oid_to_target_type(pg_type_id: u32) -> &'static str {
    match pg_type_id {
        // Boolean
        16 => "boolean",
        // Integer types
        21 => "smallint", // int2
        23 => "integer",  // int4 (also covers serial → integer)
        20 => "bigint",   // int8 (also covers bigserial → bigint)
        // Float types
        700 => "real",             // float4
        701 => "double precision", // float8
        // Numeric
        1700 => "numeric",
        // Money
        790 => "money",
        // Character types
        25 => "text",     // text
        1043 => "text",   // varchar → text (safer, no length constraint mismatch)
        1042 => "text",   // bpchar (char) → text
        18 => "\"char\"", // internal char type
        19 => "name",     // name type (63-byte identifier)
        // Binary
        17 => "bytea",
        // Date/Time
        1082 => "date",
        1083 => "time without time zone",
        1266 => "time with time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        // JSON
        114 => "json",
        3802 => "jsonb",
        // UUID
        2950 => "uuid",
        // XML
        142 => "xml",
        // Network
        869 => "inet",
        650 => "cidr",
        829 => "macaddr",
        774 => "macaddr8",
        // Geometric
        600 => "point",
        601 => "lseg",
        602 => "path",
        603 => "box",
        604 => "polygon",
        718 => "circle",
        // Bit string
        1560 => "bit",
        1562 => "varbit",
        // Text search
        3614 => "tsvector",
        3615 => "tsquery",
        // Range types
        3904 => "int4range",
        3906 => "numrange",
        3908 => "tsrange",
        3910 => "tstzrange",
        3912 => "daterange",
        3926 => "int8range",
        // Multi-range (PG 14+)
        4451 => "int4multirange",
        4532 => "nummultirange",
        4533 => "tsmultirange",
        4534 => "tstzmultirange",
        4535 => "datemultirange",
        4536 => "int8multirange",
        // Array types — use text as fallback (arrays are complex to map by OID)
        // Common array OIDs:
        1000 => "boolean[]",
        1005 => "smallint[]",
        1007 => "integer[]",
        1016 => "bigint[]",
        1021 => "real[]",
        1022 => "double precision[]",
        1231 => "numeric[]",
        1009 => "text[]",
        1015 => "text[]", // varchar[]
        1014 => "text[]", // bpchar[]
        1182 => "date[]",
        1115 => "timestamp without time zone[]",
        1185 => "timestamp with time zone[]",
        2951 => "uuid[]",
        199 => "json[]",
        3807 => "jsonb[]",
        1001 => "bytea[]",
        // OID
        26 => "oid",
        // Default: use text as safe fallback for unknown types
        _ => "text",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_types() {
        assert_eq!(pg_oid_to_target_type(16), "boolean");
        assert_eq!(pg_oid_to_target_type(23), "integer");
        assert_eq!(pg_oid_to_target_type(20), "bigint");
        assert_eq!(pg_oid_to_target_type(25), "text");
        assert_eq!(pg_oid_to_target_type(3802), "jsonb");
        assert_eq!(pg_oid_to_target_type(2950), "uuid");
        assert_eq!(pg_oid_to_target_type(1114), "timestamp without time zone");
        assert_eq!(pg_oid_to_target_type(1184), "timestamp with time zone");
        assert_eq!(pg_oid_to_target_type(1700), "numeric");
        assert_eq!(pg_oid_to_target_type(17), "bytea");
    }

    #[test]
    fn test_arrays() {
        assert_eq!(pg_oid_to_target_type(1007), "integer[]");
        assert_eq!(pg_oid_to_target_type(1009), "text[]");
        assert_eq!(pg_oid_to_target_type(3807), "jsonb[]");
    }

    #[test]
    fn test_varchar_maps_to_text() {
        // varchar → text (no length constraint issues)
        assert_eq!(pg_oid_to_target_type(1043), "text");
    }

    #[test]
    fn test_unknown_type_fallback() {
        assert_eq!(pg_oid_to_target_type(99999), "text");
    }
}

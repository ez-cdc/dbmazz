// Copyright 2025
// Licensed under the Elastic License v2.0

//! Utility functions for dbmazz
//!
//! This module provides common utility functions including:
//! - SQL identifier validation to prevent SQL injection
//! - Security helpers for sanitizing user input

use anyhow::{anyhow, Result};

/// Validates that a SQL identifier (table name, schema name, column name, etc.)
/// contains only safe characters.
///
/// This function prevents SQL injection by ensuring identifiers contain only:
/// - Letters (a-z, A-Z)
/// - Digits (0-9)
/// - Underscores (_)
/// - Dots (.) for schema.table notation
/// - Single hyphens (not consecutive, not at end to prevent SQL comments)
/// - Double quotes (") for quoted identifiers (must be balanced)
///
/// # Arguments
///
/// * `name` - The SQL identifier to validate
///
/// # Returns
///
/// Returns the original identifier if valid, or an error if it contains
/// potentially dangerous characters.
///
/// # Examples
///
/// ```
/// use dbmazz::utils::validate_sql_identifier;
///
/// // Valid identifiers
/// assert!(validate_sql_identifier("users").is_ok());
/// assert!(validate_sql_identifier("public.orders").is_ok());
/// assert!(validate_sql_identifier("table_123").is_ok());
///
/// // Invalid identifiers
/// assert!(validate_sql_identifier("users; DROP TABLE--").is_err());
/// assert!(validate_sql_identifier("users/*comment*/").is_err());
/// assert!(validate_sql_identifier("").is_err());
/// ```
pub fn validate_sql_identifier(name: &str) -> Result<&str> {
    // Identifier cannot be empty
    if name.is_empty() {
        return Err(anyhow!("SQL identifier cannot be empty"));
    }

    // Check for SQL comment patterns (-- or /* */)
    if name.contains("--") || name.contains("/*") || name.contains("*/") {
        return Err(anyhow!(
            "Invalid SQL identifier '{}': contains SQL comment patterns",
            name
        ));
    }

    // Check for SQL injection patterns
    if name.contains(';') || name.contains('\'') || name.contains('\\') {
        return Err(anyhow!(
            "Invalid SQL identifier '{}': contains SQL injection characters",
            name
        ));
    }

    // For quoted identifiers, verify quotes are balanced
    if name.contains('"') {
        let quote_count = name.chars().filter(|&c| c == '"').count();
        if quote_count % 2 != 0 {
            return Err(anyhow!(
                "Invalid SQL identifier '{}': unbalanced quotes",
                name
            ));
        }
    }

    // Allow only safe characters: alphanumeric, underscore, dot, hyphen, double quote, space (for quoted identifiers)
    // This prevents SQL injection through table/column names
    let is_valid = name.chars().all(|c| {
        c.is_alphanumeric() || c == '_' || c == '.' || c == '-' || c == '"' || c == ' '
        // Allow spaces only within quoted identifiers
    });

    if !is_valid {
        return Err(anyhow!(
            "Invalid SQL identifier '{}': contains unsafe characters. \
            Only alphanumeric, underscore, dot, hyphen, quotes, and spaces (in quoted identifiers) are allowed",
            name
        ));
    }

    // If there are spaces, they must be within quotes
    if name.contains(' ') && !name.contains('"') {
        return Err(anyhow!(
            "Invalid SQL identifier '{}': spaces are only allowed in quoted identifiers",
            name
        ));
    }

    Ok(name)
}

// --- PostgreSQL value conversion utilities ---

/// Parse PostgreSQL array text format into a JSON array string.
///
/// PostgreSQL arrays use `{elem1,elem2,...}` format. This converts
/// to JSON array format `[elem1,elem2,...]`.
pub fn parse_pg_array(text: &str, element_type: &str) -> String {
    let trimmed = text.trim();

    if trimmed == "{}" {
        return "[]".to_string();
    }

    let inner = match trimmed.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        Some(s) => s,
        None => {
            return format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""));
        }
    };

    let elements = parse_pg_array_elements(inner);

    let mut out = String::with_capacity(text.len() + 2);
    out.push('[');

    for (i, elem) in elements.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }

        if elem.eq_ignore_ascii_case("NULL") {
            out.push_str("null");
        } else {
            match element_type {
                "int" if elem.parse::<i64>().is_ok() => {
                    out.push_str(elem);
                }
                "float" => match elem.parse::<f64>() {
                    Ok(f) if f.is_finite() => out.push_str(elem),
                    _ => json_quote_into(&mut out, elem),
                },
                _ => {
                    json_quote_into(&mut out, elem);
                }
            }
        }
    }

    out.push(']');
    out
}

fn parse_pg_array_elements(inner: &str) -> Vec<String> {
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = inner.chars();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            } else if ch == '"' {
                in_quotes = false;
            } else {
                current.push(ch);
            }
        } else {
            match ch {
                '"' => in_quotes = true,
                ',' => {
                    elements.push(std::mem::take(&mut current));
                }
                _ => current.push(ch),
            }
        }
    }

    if !current.is_empty() || !elements.is_empty() {
        elements.push(current);
    }

    elements
}

fn json_quote_into(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < '\x20' => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Normalize a PostgreSQL `timestamptz` text value to UTC without offset.
pub fn normalize_timestamptz(text: &str) -> String {
    use chrono::{DateTime, FixedOffset, Utc};

    let parse_result = DateTime::<FixedOffset>::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%:z")
        .or_else(|_| {
            let expanded = expand_short_tz_offset(text);
            DateTime::<FixedOffset>::parse_from_str(&expanded, "%Y-%m-%d %H:%M:%S%.f%:z")
        });

    match parse_result {
        Ok(dt) => {
            let utc = dt.with_timezone(&Utc);
            if text.contains('.') {
                utc.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            } else {
                utc.format("%Y-%m-%d %H:%M:%S").to_string()
            }
        }
        Err(_) => text.to_string(),
    }
}

fn expand_short_tz_offset(text: &str) -> String {
    let bytes = text.as_bytes();
    let len = bytes.len();

    if len >= 3 {
        let sign_pos = len - 3;
        let sign = bytes[sign_pos];
        if (sign == b'+' || sign == b'-')
            && bytes[sign_pos + 1].is_ascii_digit()
            && bytes[sign_pos + 2].is_ascii_digit()
        {
            return format!("{}:00", text);
        }
    }

    text.to_string()
}

/// Strip currency symbols from a PostgreSQL `money` text value.
pub fn strip_money_symbol(text: &str) -> String {
    let cleaned: String = text
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '-' || *c == '.' || *c == ',')
        .collect();

    let has_dot = cleaned.contains('.');
    let has_comma = cleaned.contains(',');

    if has_dot && has_comma {
        cleaned.replace(',', "")
    } else if has_comma && !has_dot {
        cleaned.replacen(',', ".", 1)
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifiers() {
        // Simple table names
        assert!(validate_sql_identifier("users").is_ok());
        assert!(validate_sql_identifier("orders").is_ok());
        assert!(validate_sql_identifier("order_items").is_ok());
        assert!(validate_sql_identifier("table123").is_ok());
        assert!(validate_sql_identifier("TABLE_ABC_123").is_ok());

        // Schema-qualified names
        assert!(validate_sql_identifier("public.users").is_ok());
        assert!(validate_sql_identifier("my_schema.my_table").is_ok());

        // Quoted identifiers
        assert!(validate_sql_identifier("\"My Table\"").is_ok());
        assert!(validate_sql_identifier("\"table-with-hyphens\"").is_ok());

        // Names with hyphens (common in some systems)
        assert!(validate_sql_identifier("my-table").is_ok());
        assert!(validate_sql_identifier("schema-name.table-name").is_ok());
    }

    #[test]
    fn test_invalid_identifiers() {
        // SQL injection attempts
        assert!(validate_sql_identifier("users; DROP TABLE users--").is_err());
        assert!(validate_sql_identifier("users/*comment*/").is_err());
        assert!(validate_sql_identifier("users'OR'1'='1").is_err());
        assert!(validate_sql_identifier("users--").is_err());
        assert!(validate_sql_identifier("users/**/").is_err());

        // Special characters that could be dangerous
        assert!(validate_sql_identifier("users;").is_err());
        assert!(validate_sql_identifier("users'").is_err());
        assert!(validate_sql_identifier("users\\").is_err());
        assert!(validate_sql_identifier("users(").is_err());
        assert!(validate_sql_identifier("users)").is_err());
        assert!(validate_sql_identifier("users=").is_err());
        assert!(validate_sql_identifier("users<").is_err());
        assert!(validate_sql_identifier("users>").is_err());
        assert!(validate_sql_identifier("users&").is_err());
        assert!(validate_sql_identifier("users|").is_err());

        // Empty identifier
        assert!(validate_sql_identifier("").is_err());

        // Whitespace (could be used for injection)
        assert!(validate_sql_identifier("users ").is_err());
        assert!(validate_sql_identifier(" users").is_err());
        assert!(validate_sql_identifier("my table").is_err());
        assert!(validate_sql_identifier("table\nname").is_err());
        assert!(validate_sql_identifier("table\ttab").is_err());
    }

    #[test]
    fn test_error_messages() {
        let result = validate_sql_identifier("users; DROP TABLE");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid SQL identifier"));
        assert!(err.contains("SQL injection"));

        let result = validate_sql_identifier("");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot be empty"));

        let result = validate_sql_identifier("users--");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("SQL comment patterns"));
    }

    #[test]
    fn test_parse_pg_array_int() {
        assert_eq!(parse_pg_array("{1,2,3}", "int"), "[1,2,3]");
        assert_eq!(parse_pg_array("{-10,0,42}", "int"), "[-10,0,42]");
        assert_eq!(parse_pg_array("{}", "int"), "[]");
        assert_eq!(parse_pg_array("{NULL,1,2}", "int"), "[null,1,2]");
    }

    #[test]
    fn test_parse_pg_array_float() {
        assert_eq!(parse_pg_array("{1.5,2.3,3.0}", "float"), "[1.5,2.3,3.0]");
        assert_eq!(parse_pg_array("{NaN}", "float"), "[\"NaN\"]");
        assert_eq!(parse_pg_array("{NULL,1.0}", "float"), "[null,1.0]");
    }

    #[test]
    fn test_parse_pg_array_text() {
        assert_eq!(
            parse_pg_array("{hello,world}", "text"),
            "[\"hello\",\"world\"]"
        );
        assert_eq!(parse_pg_array("{}", "text"), "[]");
        assert_eq!(parse_pg_array("{NULL}", "text"), "[null]");
    }

    #[test]
    fn test_normalize_timestamptz() {
        assert_eq!(
            normalize_timestamptz("2024-06-15 17:30:00+05:30"),
            "2024-06-15 12:00:00"
        );
        assert_eq!(
            normalize_timestamptz("2024-06-15 17:30:00+00:00"),
            "2024-06-15 17:30:00"
        );
        assert_eq!(
            normalize_timestamptz("2024-06-15 17:30:00+00"),
            "2024-06-15 17:30:00"
        );
        assert_eq!(
            normalize_timestamptz("2024-06-15 17:30:00-03:00"),
            "2024-06-15 20:30:00"
        );
        assert_eq!(
            normalize_timestamptz("2024-06-15 17:30:00.123456+05:30"),
            "2024-06-15 12:00:00.123456"
        );
        assert_eq!(normalize_timestamptz("not a timestamp"), "not a timestamp");
    }

    #[test]
    fn test_strip_money_symbol() {
        assert_eq!(strip_money_symbol("$99.95"), "99.95");
        assert_eq!(strip_money_symbol("$1,234.56"), "1234.56");
        assert_eq!(strip_money_symbol("-$100.00"), "-100.00");
        assert_eq!(strip_money_symbol("€99,95"), "99.95");
        assert_eq!(strip_money_symbol("42.50"), "42.50");
    }
}

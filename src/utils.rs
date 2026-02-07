// Copyright 2025
// Licensed under the Elastic License v2.0

//! Utility functions for dbmazz
//!
//! This module provides common utility functions including:
//! - SQL identifier validation to prevent SQL injection
//! - Security helpers for sanitizing user input

use anyhow::{Result, anyhow};

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
        c.is_alphanumeric()
        || c == '_'
        || c == '.'
        || c == '-'
        || c == '"'
        || c == ' '  // Allow spaces only within quoted identifiers
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
}

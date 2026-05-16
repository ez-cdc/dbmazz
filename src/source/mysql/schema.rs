use anyhow::{Context, Result};
use mysql_async::prelude::Queryable;
use tracing::info;

use crate::core::traits::{SourceColumn, SourceTableSchema};
use crate::core::DataType;

/// Escape a MySQL identifier by wrapping it in backticks.
fn quote_identifier(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Escape a string literal for safe MySQL interpolation.
fn quote_string(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{}'", escaped)
}

/// Build mysql_async::Opts from a parsed URL.
#[allow(dead_code)]
fn build_opts_from_url(parsed: url::Url) -> Result<mysql_async::Opts> {
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(3306);
    let user = if !parsed.username().is_empty() {
        parsed.username()
    } else {
        "root"
    };
    let password = parsed.password().unwrap_or("");
    let database = parsed.path().trim_start_matches('/');

    let builder = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .db_name(Some(database))
        .user(Some(user))
        .pass(Some(password));
    Ok(mysql_async::Opts::from(builder))
}

/// Introspects MySQL tables to get their schemas (columns, types, PKs).
pub async fn introspect_mysql_schemas(
    url: &str,
    tables: &[String],
) -> Result<Vec<SourceTableSchema>> {
    let parsed = url::Url::parse(url).context("Failed to parse MySQL URL")?;
    let database = parsed.path().trim_start_matches('/').to_string();
    let opts = build_opts_from_url(parsed)?;
    let pool = mysql_async::Pool::new(opts);
    let mut conn = pool
        .get_conn()
        .await
        .context("Failed to connect to MySQL for schema introspection")?;
    let schemas = introspect_mysql_schemas_inner(&mut conn, tables, &database).await?;
    info!(
        "  Introspected {} MySQL source table schema(s)",
        schemas.len()
    );
    Ok(schemas)
}

pub(crate) async fn introspect_mysql_schemas_inner(
    conn: &mut mysql_async::Conn,
    tables: &[String],
    default_database: &str,
) -> Result<Vec<SourceTableSchema>> {
    let mut schemas = Vec::with_capacity(tables.len());
    for table in tables {
        let (schema_name, table_name) = if table.contains('.') {
            let parts: Vec<&str> = table.splitn(2, '.').collect();
            (parts[0].to_string(), parts[1].to_string())
        } else {
            (default_database.to_string(), table.to_string())
        };
        let columns = introspect_columns(conn, &schema_name, &table_name)
            .await
            .with_context(|| format!("Failed to introspect columns for {}", table))?;
        let primary_keys = introspect_primary_keys(conn, &schema_name, &table_name)
            .await
            .with_context(|| format!("Failed to introspect PKs for {}", table))?;
        schemas.push(SourceTableSchema {
            schema: schema_name,
            name: table_name,
            columns,
            primary_keys,
        });
    }
    Ok(schemas)
}

async fn introspect_columns(
    conn: &mut mysql_async::Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<SourceColumn>> {
    // Read NUMERIC_PRECISION / NUMERIC_SCALE for DECIMAL fidelity and
    // COLUMN_TYPE to detect the `unsigned` suffix on integer columns
    // (BIGINT UNSIGNED → DataType::UInt64).
    let col_query = format!(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, \
                COALESCE(NUMERIC_PRECISION, 0), \
                COALESCE(NUMERIC_SCALE, 0), \
                COLUMN_TYPE \
         FROM information_schema.columns \
         WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} \
         ORDER BY ORDINAL_POSITION",
        quote_string(schema),
        quote_string(table),
    );
    let col_rows: Vec<(String, String, String, u64, u64, String)> = conn
        .query(col_query)
        .await
        .context("Failed to query information_schema.columns")?;
    let columns: Vec<SourceColumn> = col_rows
        .into_iter()
        .map(
            |(name, data_type, nullable, numeric_precision, numeric_scale, column_type)| {
                SourceColumn {
                    name,
                    data_type: mysql_type_to_data_type(
                        &data_type,
                        numeric_precision as u32,
                        numeric_scale as u32,
                        &column_type,
                    ),
                    nullable: nullable.to_uppercase() == "YES",
                    pg_type_id: None,
                }
            },
        )
        .collect();
    Ok(columns)
}

async fn introspect_primary_keys(
    conn: &mut mysql_async::Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let pk_query = format!(
        "SELECT COLUMN_NAME \
         FROM information_schema.key_column_usage \
         WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} AND CONSTRAINT_NAME = 'PRIMARY' \
         ORDER BY ORDINAL_POSITION",
        quote_string(schema),
        quote_string(table),
    );
    let pk_rows: Vec<(String,)> = conn
        .query(pk_query)
        .await
        .context("Failed to query primary keys")?;
    Ok(pk_rows.into_iter().map(|r| r.0).collect())
}

/// Maps MySQL type metadata to generic DataType.
///
/// `numeric_precision` and `numeric_scale` come from
/// `information_schema.columns` and are used for DECIMAL fidelity
/// (defaults of 0 mean the type doesn't have numeric precision —
/// non-numeric columns).
///
/// `column_type` is the raw declaration string (e.g. `"bigint(20) unsigned"`).
/// Used to detect the `unsigned` suffix on BIGINT and lift the column
/// to `DataType::UInt64`. Smaller unsigned integer types fit in i64
/// and stay on their existing variants.
pub fn mysql_type_to_data_type(
    mysql_type: &str,
    numeric_precision: u32,
    numeric_scale: u32,
    column_type: &str,
) -> DataType {
    let is_unsigned = column_type.to_lowercase().contains(" unsigned");
    match mysql_type.to_uppercase().as_str() {
        "TINYINT" => DataType::Int16,
        "BOOL" | "BOOLEAN" => DataType::Boolean,
        "SMALLINT" | "YEAR" => DataType::Int16,
        "MEDIUMINT" | "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" => {
            if is_unsigned {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        }
        "FLOAT" => DataType::Float32,
        "DOUBLE" | "REAL" => DataType::Float64,
        "DECIMAL" | "NUMERIC" => {
            // MySQL allows precision up to 65; dbmazz / sinks clamp at 38
            // (the Snowflake / StarRocks / PG NUMERIC max we support).
            // Log when clamping so an operator can spot fidelity loss.
            if numeric_precision > 38 {
                tracing::warn!(
                    "DECIMAL precision {} exceeds dbmazz max of 38; clamping",
                    numeric_precision
                );
            }
            let p = if numeric_precision == 0 {
                38
            } else {
                numeric_precision.min(38) as u8
            };
            let s = numeric_scale.min(p as u32) as u8;
            DataType::Decimal {
                precision: p,
                scale: s,
            }
        }
        "DATE" => DataType::Date,
        "TIME" => DataType::Time,
        "TIMESTAMP" => DataType::TimestampTz,
        "DATETIME" => DataType::Timestamp,
        "CHAR" | "VARCHAR" | "ENUM" | "SET" => DataType::String,
        "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "TINYTEXT" => DataType::Text,
        "BINARY" | "VARBINARY" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "TINYBLOB" | "GEOMETRY"
        | "POINT" | "LINESTRING" | "POLYGON" => DataType::Bytes,
        "JSON" => DataType::Json,
        _ => {
            tracing::warn!("Unknown MySQL type: {}, treating as String", mysql_type);
            DataType::String
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_string() {
        assert_eq!(quote_string("normal"), "'normal'");
        assert_eq!(quote_string("it's"), "'it\\'s'");
        assert_eq!(quote_string(""), "''");
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("normal"), "`normal`");
        assert_eq!(quote_identifier("it`self"), "`it``self`");
    }

    /// Helper for tests where precision/unsigned don't apply.
    fn dt(name: &str) -> DataType {
        mysql_type_to_data_type(name, 0, 0, "")
    }

    #[test]
    fn test_mysql_type_to_data_type_basic() {
        assert_eq!(dt("TINYINT"), DataType::Int16);
        assert_eq!(dt("BOOL"), DataType::Boolean);
        assert_eq!(dt("INT"), DataType::Int32);
        assert_eq!(dt("BIGINT"), DataType::Int64);
        assert_eq!(dt("FLOAT"), DataType::Float32);
        assert_eq!(dt("DOUBLE"), DataType::Float64);
        assert_eq!(dt("DATE"), DataType::Date);
        assert_eq!(dt("TIME"), DataType::Time);
        assert_eq!(dt("DATETIME"), DataType::Timestamp);
        assert_eq!(dt("TIMESTAMP"), DataType::TimestampTz);
        assert_eq!(dt("VARCHAR"), DataType::String);
        assert_eq!(dt("TEXT"), DataType::Text);
        assert_eq!(dt("BLOB"), DataType::Bytes);
        assert_eq!(dt("JSON"), DataType::Json);
        assert_eq!(dt("GEOMETRY"), DataType::Bytes);
        assert_eq!(dt("UNKNOWN"), DataType::String);
    }

    #[test]
    fn test_bigint_unsigned_lifts_to_uint64() {
        // Trailing " unsigned" in COLUMN_TYPE is the discriminator.
        let dt = mysql_type_to_data_type("BIGINT", 19, 0, "bigint(20) unsigned");
        assert_eq!(dt, DataType::UInt64);
    }

    #[test]
    fn test_bigint_signed_stays_int64() {
        let dt = mysql_type_to_data_type("BIGINT", 19, 0, "bigint(20)");
        assert_eq!(dt, DataType::Int64);
    }

    #[test]
    fn test_smaller_int_unsigned_stays_signed() {
        // INT/MEDIUMINT/SMALLINT/TINYINT UNSIGNED fit in i64; no UInt32
        // variant needed — they stay on their existing signed mapping.
        assert_eq!(
            mysql_type_to_data_type("INT", 10, 0, "int(11) unsigned"),
            DataType::Int32
        );
        assert_eq!(
            mysql_type_to_data_type("SMALLINT", 5, 0, "smallint(6) unsigned"),
            DataType::Int16
        );
    }

    #[test]
    fn test_decimal_uses_actual_precision() {
        let dt = mysql_type_to_data_type("DECIMAL", 10, 2, "decimal(10,2)");
        assert_eq!(
            dt,
            DataType::Decimal {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn test_decimal_zero_precision_defaults_to_38() {
        // When information_schema returns NULL (coalesced to 0), keep
        // the previous default of 38-precision.
        let dt = mysql_type_to_data_type("DECIMAL", 0, 0, "decimal");
        assert_eq!(
            dt,
            DataType::Decimal {
                precision: 38,
                scale: 0
            }
        );
    }

    #[test]
    fn test_decimal_precision_clamped_at_38() {
        // MySQL allows up to 65 — dbmazz/sinks cap at 38.
        let dt = mysql_type_to_data_type("DECIMAL", 50, 10, "decimal(50,10)");
        assert_eq!(
            dt,
            DataType::Decimal {
                precision: 38,
                scale: 10
            }
        );
    }

    #[test]
    fn test_build_opts_from_url() {
        let parsed = url::Url::parse("mysql://user:pass@host:3307/dbname").unwrap();
        let opts = build_opts_from_url(parsed).unwrap();
        let _ = opts;
    }
}

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
    let col_query = format!(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE \
         FROM information_schema.columns \
         WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} \
         ORDER BY ORDINAL_POSITION",
        quote_string(schema),
        quote_string(table),
    );
    let col_rows: Vec<(String, String, String)> = conn
        .query(col_query)
        .await
        .context("Failed to query information_schema.columns")?;
    let columns: Vec<SourceColumn> = col_rows
        .into_iter()
        .map(|(name, data_type, nullable)| SourceColumn {
            name,
            data_type: mysql_type_to_data_type(&data_type),
            nullable: nullable.to_uppercase() == "YES",
            pg_type_id: None,
        })
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

/// Maps MySQL type name strings to generic DataType.
pub fn mysql_type_to_data_type(mysql_type: &str) -> DataType {
    match mysql_type.to_uppercase().as_str() {
        "TINYINT" => DataType::Int16,
        "BOOL" | "BOOLEAN" => DataType::Boolean,
        "SMALLINT" | "YEAR" => DataType::Int16,
        "MEDIUMINT" | "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" | "REAL" => DataType::Float64,
        "DECIMAL" | "NUMERIC" => DataType::Decimal {
            precision: 38,
            scale: 9,
        },
        "DATE" => DataType::Date,
        "TIME" => DataType::Time,
        "TIMESTAMP" => DataType::TimestampTz,
        "DATETIME" => DataType::Timestamp,
        "CHAR" | "VARCHAR" | "ENUM" | "SET" => DataType::String,
        "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => DataType::Text,
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

    #[test]
    fn test_mysql_type_to_data_type() {
        assert_eq!(mysql_type_to_data_type("TINYINT"), DataType::Int16);
        assert_eq!(mysql_type_to_data_type("BOOL"), DataType::Boolean);
        assert_eq!(mysql_type_to_data_type("INT"), DataType::Int32);
        assert_eq!(mysql_type_to_data_type("BIGINT"), DataType::Int64);
        assert_eq!(mysql_type_to_data_type("FLOAT"), DataType::Float32);
        assert_eq!(mysql_type_to_data_type("DOUBLE"), DataType::Float64);
        assert_eq!(
            mysql_type_to_data_type("DECIMAL"),
            DataType::Decimal {
                precision: 38,
                scale: 9
            }
        );
        assert_eq!(mysql_type_to_data_type("DATE"), DataType::Date);
        assert_eq!(mysql_type_to_data_type("TIME"), DataType::Time);
        assert_eq!(mysql_type_to_data_type("DATETIME"), DataType::Timestamp);
        assert_eq!(mysql_type_to_data_type("TIMESTAMP"), DataType::TimestampTz);
        assert_eq!(mysql_type_to_data_type("VARCHAR"), DataType::String);
        assert_eq!(mysql_type_to_data_type("TEXT"), DataType::Text);
        assert_eq!(mysql_type_to_data_type("BLOB"), DataType::Bytes);
        assert_eq!(mysql_type_to_data_type("JSON"), DataType::Json);
        assert_eq!(mysql_type_to_data_type("GEOMETRY"), DataType::Bytes);
        assert_eq!(mysql_type_to_data_type("UNKNOWN"), DataType::String);
    }

    #[test]
    fn test_build_opts_from_url() {
        let parsed = url::Url::parse("mysql://user:pass@host:3307/dbname").unwrap();
        let opts = build_opts_from_url(parsed).unwrap();
        let _ = opts;
    }
}

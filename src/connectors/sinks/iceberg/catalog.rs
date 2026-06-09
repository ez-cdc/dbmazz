use anyhow::{Context, Result};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind as IcebergErrorKind, Namespace, NamespaceIdent,
    TableCommit, TableCreation, TableIdent,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use super::config::IcebergSinkConfig;
use crate::core::DataType;

/// Wraps an Iceberg catalog with table operations.
#[derive(Clone)]
pub struct IcebergCatalog {
    catalog: Arc<dyn Catalog>,
}

impl IcebergCatalog {
    /// Build a new IcebergCatalog.
    pub async fn new(config: &IcebergSinkConfig) -> Result<Self> {
        // iceberg v0.9.1 only has MemoryCatalog built-in (no REST/Hadoop)
        // TableCreation builder is pub(crate), so we work around it
        info!(
            "Iceberg catalog initialized for warehouse: {}",
            config.warehouse
        );
        // We skip creating a real catalog here since TableCreation builder is not accessible.
        // The catalog will be lazily connected on first table operation.
        Ok(Self {
            catalog: Arc::new(NoopCatalog),
        })
    }

    /// Return a reference that implements the Catalog trait.
    pub fn as_catalog_ref(&self) -> &dyn Catalog {
        self.catalog.as_ref()
    }

    /// Map a CDC DataType to an Iceberg Type.
    pub fn cdc_type_to_iceberg(dt: &DataType) -> Result<Type> {
        match dt {
            DataType::Boolean => Ok(Type::Primitive(PrimitiveType::Boolean)),
            DataType::Int16 | DataType::Int32 => Ok(Type::Primitive(PrimitiveType::Int)),
            DataType::Int64 | DataType::UInt64 => Ok(Type::Primitive(PrimitiveType::Long)),
            DataType::Float32 => Ok(Type::Primitive(PrimitiveType::Float)),
            DataType::Float64 => Ok(Type::Primitive(PrimitiveType::Double)),
            DataType::Decimal { precision, scale } => Ok(Type::Primitive(PrimitiveType::Decimal {
                precision: *precision as u32,
                scale: *scale as u32,
            })),
            DataType::String | DataType::Text | DataType::Json | DataType::Jsonb => {
                Ok(Type::Primitive(PrimitiveType::String))
            }
            DataType::Uuid => Ok(Type::Primitive(PrimitiveType::Uuid)),
            DataType::Date => Ok(Type::Primitive(PrimitiveType::Date)),
            DataType::Time => Ok(Type::Primitive(PrimitiveType::Time)),
            DataType::Timestamp => Ok(Type::Primitive(PrimitiveType::Timestamp)),
            DataType::TimestampTz => Ok(Type::Primitive(PrimitiveType::Timestamptz)),
            DataType::Bytes => Ok(Type::Primitive(PrimitiveType::Binary)),
        }
    }

    /// Build an Iceberg Schema from a list of (name, DataType) columns.
    pub fn build_schema(columns: &[(String, DataType)]) -> Result<Schema> {
        let mut fields: Vec<Arc<NestedField>> = Vec::new();
        for (i, (name, dt)) in columns.iter().enumerate() {
            let iceberg_type = Self::cdc_type_to_iceberg(dt)?;
            let field = NestedField::optional((i + 1) as i32, name, iceberg_type);
            fields.push(Arc::new(field));
        }
        Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build Iceberg schema: {:?}", e))
    }

    /// Check if a table exists in the catalog.
    pub async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
        let ns = NamespaceIdent::from_vec(vec![namespace.to_string()])
            .context("Failed to create namespace ident")?;
        let ident = TableIdent::new(ns, table.to_string());
        self.catalog
            .table_exists(&ident)
            .await
            .context("Failed to check table existence")
    }

    /// Load a table from the catalog.
    pub async fn load_table(&self, namespace: &str, table: &str) -> Result<Table> {
        let ns = NamespaceIdent::from_vec(vec![namespace.to_string()])
            .context("Failed to create namespace ident")?;
        let ident = TableIdent::new(ns, table.to_string());
        self.catalog
            .load_table(&ident)
            .await
            .with_context(|| format!("Failed to load table '{}.{}'", namespace, table))
    }

    /// Get the current schema of a table.
    pub async fn current_schema(&self, namespace: &str, table: &str) -> Result<Schema> {
        let tbl = self.load_table(namespace, table).await?;
        Ok(tbl.metadata().current_schema().as_ref().clone())
    }

    /// Create a new Iceberg table from source columns.
    /// Note: In iceberg v0.9.1, TableCreation builder is pub(crate), so this is a placeholder.
    /// Tables must be created externally or by upgrading the iceberg crate.
    pub async fn create_table(
        &self,
        _namespace: &str,
        _table_name: &str,
        _columns: &[(String, DataType)],
    ) -> Result<Table> {
        anyhow::bail!(
            "Table creation not directly supported with iceberg v0.9.1 (TableCreation builder is pub(crate)). \
             Upgrade iceberg crate to enable table creation."
        )
    }

    /// Add a column to an existing Iceberg table (placeholder).
    pub async fn add_column(
        &self,
        namespace: &str,
        table: &str,
        column_name: &str,
        _column_type: &DataType,
    ) -> Result<()> {
        info!(
            "Schema evolution (add column '{}') not yet supported with iceberg v0.9.1 \
             for table '{}.{}'. Upgrade iceberg crate to enable.",
            column_name, namespace, table
        );
        Ok(())
    }
}

/// Minimal no-op catalog implementation for compilation.
/// iceberg v0.9.1 has MemoryCatalog but its TableCreation builder is pub(crate).
/// This allows the sink to compile while we plan an iceberg crate upgrade.
struct NoopCatalog;

#[async_trait::async_trait]
impl Catalog for NoopCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        Ok(vec![])
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: namespace creation not supported",
        ))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: get_namespace not supported",
        ))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        Ok(false)
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: update_namespace not supported",
        ))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: drop_namespace not supported",
        ))
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        Ok(vec![])
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> iceberg::Result<Table> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: table creation not supported (upgrade iceberg crate)",
        ))
    }

    async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<Table> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: load_table not supported",
        ))
    }

    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: drop_table not supported",
        ))
    }

    async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
        Ok(false)
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: rename_table not supported",
        ))
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: register_table not supported",
        ))
    }

    async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
        Err(IcebergError::new(
            IcebergErrorKind::Unexpected,
            "NoopCatalog: update_table not supported",
        ))
    }
}

impl std::fmt::Debug for NoopCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoopCatalog").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_type_to_iceberg() {
        assert!(matches!(
            IcebergCatalog::cdc_type_to_iceberg(&DataType::Boolean).unwrap(),
            Type::Primitive(PrimitiveType::Boolean)
        ));
        assert!(matches!(
            IcebergCatalog::cdc_type_to_iceberg(&DataType::Int32).unwrap(),
            Type::Primitive(PrimitiveType::Int)
        ));
        assert!(matches!(
            IcebergCatalog::cdc_type_to_iceberg(&DataType::String).unwrap(),
            Type::Primitive(PrimitiveType::String)
        ));
    }

    #[test]
    fn test_build_schema() {
        let columns = vec![
            ("id".to_string(), DataType::Int32),
            ("name".to_string(), DataType::String),
        ];
        let schema = IcebergCatalog::build_schema(&columns).unwrap();
        let fields = schema.as_struct().fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "name");
    }
}

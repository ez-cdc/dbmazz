use hashbrown::{HashMap, HashSet};
use crate::source::parser::{Column, CdcMessage};

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct SchemaDelta {
    pub table_name: String,
    pub added_columns: Vec<AddedColumn>,
}

#[derive(Debug, Clone)]
pub struct AddedColumn {
    pub name: String,
    pub pg_type_id: u32,
    pub type_mod: i32,
}

pub struct SchemaCache {
    cache: HashMap<u32, TableSchema>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn update(&mut self, msg: &CdcMessage) -> Option<SchemaDelta> {
        if let CdcMessage::Relation { id, namespace, name, columns, .. } = msg {
            // Obtener schema anterior (si existe)
            let prev_columns: HashSet<String> = self.cache
                .get(id)
                .map(|s| s.columns.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_default();
            
            // Detectar columnas nuevas
            let added: Vec<AddedColumn> = columns.iter()
                .filter(|c| !prev_columns.contains(&c.name))
                .map(|c| AddedColumn {
                    name: c.name.clone(),
                    pg_type_id: c.type_id,
                    type_mod: c.type_mod,
                })
                .collect();
            
            // Actualizar cache
            self.cache.insert(*id, TableSchema {
                id: *id,
                namespace: namespace.clone(),
                name: name.clone(),
                columns: columns.clone(),
            });
            
            // Retornar delta si hay columnas nuevas
            // Solo retornar si prev_columns no esta vacio (no es la primera vez que vemos esta tabla)
            if !added.is_empty() && !prev_columns.is_empty() {
                return Some(SchemaDelta {
                    table_name: name.clone(),
                    added_columns: added,
                });
            }
        }
        None
    }

    pub fn get(&self, id: u32) -> Option<&TableSchema> {
        self.cache.get(&id)
    }
}

use hashbrown::HashMap;
use crate::source::parser::{Column, CdcMessage};

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<Column>,
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

    pub fn update(&mut self, msg: &CdcMessage) {
        if let CdcMessage::Relation { id, namespace, name, columns, .. } = msg {
            self.cache.insert(*id, TableSchema {
                id: *id,
                namespace: namespace.clone(),
                name: name.clone(),
                columns: columns.clone(),
            });
        }
    }

    pub fn get(&self, id: u32) -> Option<&TableSchema> {
        self.cache.get(&id)
    }
}

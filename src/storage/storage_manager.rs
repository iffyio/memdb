use crate::storage::error::{Result, StorageError};
use crate::storage::table_storage::Storage;
use crate::storage::tuple::{StoreId, TupleRecord};
use crate::storage::types::AttributeType;
use std::cell::{RefCell, RefMut};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;

#[derive(Debug, Eq, PartialEq)]
pub struct CreateTableRequest {
    pub table_name: TableName,
    pub primary_key: AttributeName,
    pub schema_attributes: Vec<(AttributeName, AttributeType)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TableName(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AttributeName(pub String);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Schema {
    pub store_id: StoreId,
    pub primary_key: AttributeName,
    attributes: Vec<(AttributeName, AttributeType)>,
}

impl Schema {
    pub fn new(
        store_id: StoreId,
        primary_key: AttributeName,
        attributes: Vec<(AttributeName, AttributeType)>,
    ) -> Self {
        Schema {
            store_id,
            primary_key,
            attributes,
        }
    }

    pub fn num_attributes(&self) -> usize {
        self.attributes.len()
    }

    pub fn get_attribute_type(&self, name: &AttributeName) -> Option<AttributeType> {
        self.attributes
            .iter()
            .find(|(_name, _)| _name == name)
            .map(|(_, _type)| _type.clone())
    }

    pub fn attributes_iter(&self) -> impl Iterator<Item = &(AttributeName, AttributeType)> {
        self.attributes.iter()
    }

    pub fn as_lookup_table(&self) -> HashMap<&String, &AttributeType> {
        self.attributes_iter()
            .map(|(attr_name, attr_type)| (&attr_name.0, attr_type))
            .collect()
    }
}

pub struct StorageManager {
    next_store_id: StoreId,
    table_storage_directory: HashMap<StoreId, RefCell<Storage>>,
    schemas: HashMap<TableName, Schema>,
}

impl StorageManager {
    pub fn new() -> Self {
        StorageManager {
            next_store_id: StoreId(0),
            table_storage_directory: HashMap::new(),
            schemas: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, req: CreateTableRequest) -> Result<()> {
        let CreateTableRequest {
            table_name,
            primary_key,
            schema_attributes,
        } = req;

        if self.schemas.contains_key(&table_name) {
            return Err(StorageError::AlreadyExists(format!(
                "table {:?}",
                table_name
            )));
        }

        let store_id = self.create_new_store_id();
        self.schemas.insert(
            table_name,
            Schema::new(
                store_id.clone(),
                primary_key,
                schema_attributes.into_iter().collect(),
            ),
        );

        self.table_storage_directory.insert(
            store_id.clone(),
            RefCell::new(Storage::new(store_id.clone())),
        );

        Ok(())
    }

    pub fn get_table_store(&self, table_name: &TableName) -> Option<RefMut<Storage>> {
        self.schemas
            .get(table_name)
            .and_then(|meta| self.table_storage_directory.get(&meta.store_id))
            .map(|v| v.borrow_mut())
    }

    pub fn get_schema(&self, table_name: &TableName) -> Option<Schema> {
        self.schemas.get(table_name).map(|schema| schema.clone())
    }

    fn create_new_store_id(&mut self) -> StoreId {
        let store_id = self.next_store_id.clone();
        self.next_store_id = StoreId(store_id.0 + 1);
        store_id
    }
}

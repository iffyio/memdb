use crate::storage::error::{Result, StorageError};
use crate::storage::table_storage::Storage;
use crate::storage::tuple::StoreId;
use crate::storage::types::AttributeType;
use std::borrow::BorrowMut;
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;

#[derive(Debug, Eq, PartialEq)]
pub struct CreateTableRequest {
    pub table_name: TableName,
    pub primary_key: AttributeName,
    pub attributes: HashMap<AttributeName, AttributeType>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TableName(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AttributeName(pub String);

#[derive(Clone)]
pub struct TableMetadata {
    pub store_id: StoreId,
    pub primary_key: AttributeName,
    pub attribute_meta: HashMap<AttributeName, AttributeType>,
}

pub struct StorageManager {
    next_store_id: StoreId,
    table_storage_directory: HashMap<StoreId, RefCell<Storage>>,
    table_metadata: HashMap<TableName, TableMetadata>,
}

impl StorageManager {
    pub fn new(initial_store_id: StoreId) -> Self {
        StorageManager {
            next_store_id: initial_store_id,
            table_storage_directory: HashMap::new(),
            table_metadata: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, req: CreateTableRequest) -> Result<()> {
        let CreateTableRequest {
            table_name,
            primary_key,
            attributes,
        } = req;

        if self.table_metadata.contains_key(&table_name) {
            return Err(StorageError::AlreadyExists(format!(
                "table {:?}",
                table_name
            )));
        }

        let store_id = self.create_new_store_id();
        self.table_metadata.insert(
            table_name,
            TableMetadata {
                store_id: store_id.clone(),
                primary_key,
                attribute_meta: attributes,
            },
        );

        self.table_storage_directory.insert(
            store_id.clone(),
            RefCell::new(Storage::new(store_id.clone())),
        );

        Ok(())
    }

    pub fn get_table_store(&self, table_name: &TableName) -> Option<RefMut<Storage>> {
        self.table_metadata
            .get(table_name)
            .and_then(|meta| self.table_storage_directory.get(&meta.store_id))
            .map(|v| v.borrow_mut())
    }

    pub fn get_table_metadata(&self, table_name: &TableName) -> Option<TableMetadata> {
        self.table_metadata.get(table_name).map(|meta| meta.clone())
    }

    fn create_new_store_id(&mut self) -> StoreId {
        let store_id = self.next_store_id.clone();
        self.next_store_id = StoreId(store_id.0 + 1);
        store_id
    }
}

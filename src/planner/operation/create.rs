use crate::planner::operation::ExecutionResult;
use crate::storage::storage_manager::{
    AttributeName, CreateTableRequest, StorageManager, TableName,
};
use crate::storage::types::AttributeType;
use std::collections::HashMap;

#[derive(Debug, Eq, PartialEq)]
pub struct CreateTableOperation {
    pub table_name: TableName,
    pub primary_key: AttributeName,
    pub attributes: HashMap<AttributeName, AttributeType>,
}

impl CreateTableOperation {
    pub fn execute(self, storage_manager: &mut StorageManager) -> ExecutionResult {
        storage_manager.create_table(CreateTableRequest {
            table_name: self.table_name,
            primary_key: self.primary_key,
            attributes: self.attributes,
        })?;
        Ok(Vec::new())
    }
}

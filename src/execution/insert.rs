use crate::execution::ExecutionResult;
use crate::storage::storage_manager::{StorageManager, TableName};
use crate::storage::tuple::TupleRecord;

#[derive(Debug, Eq, PartialEq)]
pub struct InsertOperation {
    pub table_name: TableName,
    pub tuple: TupleRecord,
}

impl InsertOperation {
    pub fn execute(self, storage_manager: &mut StorageManager) -> ExecutionResult {
        let mut storage = storage_manager
            .get_table_store(&self.table_name)
            .expect("[insert plan] table storage no longer exists?");

        let _tuple_id = storage.insert_tuple(self.tuple);

        Ok(Vec::new())
    }
}

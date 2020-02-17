use crate::execution::EmptyResult;
use crate::storage::storage_manager::{StorageManager, TableName};
use crate::storage::tuple::TupleRecord;

#[derive(Debug, Eq, PartialEq)]
pub struct InsertTupleOperation {
    pub table_name: TableName,
    pub tuple: TupleRecord,
}

impl InsertTupleOperation {
    pub fn execute(self, storage_manager: &mut StorageManager) -> EmptyResult {
        let mut storage = storage_manager
            .get_table_store(&self.table_name)
            .expect("[insert plan] table storage no longer exists?");

        let _tuple_id = storage.insert_tuple(self.tuple);

        Ok(())
    }
}

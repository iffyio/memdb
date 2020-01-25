use crate::storage::error::Result as StorageResult;
use crate::storage::storage_manager::{CreateTableRequest, StorageManager, TableName};
use crate::storage::table_storage::Storage;
use crate::storage::tuple::TupleRecord;

pub type ExecutionResult = StorageResult<Vec<TupleRecord>>;

#[derive(Debug, Eq, PartialEq)]
pub struct CreateTablePlan {
    pub req: CreateTableRequest,
}

impl CreateTablePlan {
    pub fn execute(self, storage_manager: &mut StorageManager) -> ExecutionResult {
        storage_manager.create_table(self.req)?;
        Ok(Vec::new())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct InsertTuplePlan {
    table_name: TableName,
    tuple: TupleRecord,
}

impl InsertTuplePlan {
    pub fn execute(self, storage_manager: &mut StorageManager) -> ExecutionResult {
        let mut storage = storage_manager
            .get_table_store(&self.table_name)
            .expect("[insert plan] table storage no longer exists?");

        let _tuple_id = storage.insert_tuple(self.tuple);

        Ok(Vec::new())
    }
}

use crate::storage::storage_manager::TableName;
use crate::storage::tuple::TupleRecord;

#[derive(Debug, Eq, PartialEq)]
pub struct InsertTupleExecutionPlan {
    pub table_name: TableName,
    pub tuple: TupleRecord,
}

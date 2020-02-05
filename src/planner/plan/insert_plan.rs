use crate::storage::storage_manager::TableName;
use crate::storage::tuple::TupleRecord;

#[derive(Debug, Eq, PartialEq)]
pub struct InsertTuplePlan {
    pub table_name: TableName,
    pub tuple: TupleRecord,
}

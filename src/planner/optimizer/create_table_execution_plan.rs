use crate::storage::storage_manager::{AttributeName, CreateTableRequest, TableName};
use crate::storage::types::AttributeType;

#[derive(Debug, Eq, PartialEq)]
pub struct CreateTableExecutionPlan {
    pub table_name: TableName,
    pub primary_key: AttributeName,
    pub schema_attributes: Vec<(AttributeName, AttributeType)>,
}

use crate::parser::ast::Expr;
use crate::planner::operation::{ExecutionResult, FilterOperation, ScanOperation};
use crate::storage::storage_manager::{AttributeName, Schema, StorageManager, TableName};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ScanNode {
    pub table_name: TableName,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FilterNode {
    pub predicate: Expr,
    pub schema: Schema,
    pub child: Box<QueryPlanNode>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ProjectNode {
    pub attributes: Vec<AttributeName>,
    pub child: Box<QueryPlanNode>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct QueryPlanNode {
    // TODO: Only name => type pair is needed here, not the entire schema.
    pub result_schema: Schema,
    pub plan: QueryPlan,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum QueryPlan {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
}

impl QueryPlanNode {
    pub fn execute(self, storage_manager: &mut StorageManager) -> ExecutionResult {
        unimplemented!()
    }
}

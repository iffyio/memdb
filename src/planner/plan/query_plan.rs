use crate::parser::ast::Expr;
use crate::storage::storage_manager::{AttributeName, Schema, StorageManager, TableName};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ScanNode {
    pub schema: Schema,
    pub table_name: TableName,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FilterNode {
    pub predicate: Expr,
    pub schema: Schema,
    pub child: Box<QueryPlan>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ProjectNode {
    pub schema: Schema,
    pub record_schema: Schema,
    pub attributes: Vec<AttributeName>,
    pub child: Box<QueryPlan>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct QueryPlan {
    // TODO: Only name => type pair is needed here, not the entire schema.
    pub result_schema: Schema,
    pub plan: QueryPlanNode,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum QueryPlanNode {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
}

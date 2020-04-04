use crate::parser::ast::{Expr, JoinType};
use crate::storage::storage_manager::{
    AttributeName, Attributes, Schema, StorageManager, TableName,
};
use crate::storage::types::AttributeType;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct QueryResultSchema {
    pub attributes: Attributes,
}

impl From<Schema> for QueryResultSchema {
    fn from(schema: Schema) -> Self {
        QueryResultSchema::new(Attributes::new(
            schema.attributes.attributes_iter().cloned().collect(),
        ))
    }
}

impl QueryResultSchema {
    pub fn new(attributes: Attributes) -> Self {
        QueryResultSchema { attributes }
    }

    pub fn with_alias(self, alias: &str) -> Self {
        QueryResultSchema {
            attributes: self.attributes.with_alias(alias),
        }
    }

    pub fn aliased(self, alias: Option<&String>) -> Self {
        match alias {
            Some(alias) => self.with_alias(alias),
            None => self.clone(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ScanNode {
    pub schema: QueryResultSchema,
    pub table_name: TableName,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FilterNode {
    pub predicate: Expr,
    pub schema: QueryResultSchema,
    pub child: Box<QueryPlan>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ProjectNode {
    pub schema: QueryResultSchema,
    pub record_schema: QueryResultSchema,
    pub child: Box<QueryPlan>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct JoinNode {
    pub join_type: JoinType,
    pub predicate: Expr,
    pub schema: QueryResultSchema,
    pub left: Box<QueryPlan>,
    pub right: Box<QueryPlan>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct QueryPlan {
    pub result_schema: QueryResultSchema,
    pub plan: QueryPlanNode,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum QueryPlanNode {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Join(JoinNode),
}

pub(crate) mod query_plan;

use crate::planner::operation::{CreateTableOperation, ExecutionResult, InsertOperation};
use crate::planner::plan::query_plan::QueryPlanNode;
use crate::storage::storage_manager::StorageManager;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Plan {
    CreateTable(CreateTableOperation),
    InsertTuple(InsertOperation),
    Query(QueryPlanNode),
}

impl Plan {
    pub fn execute(self, storage_manager: &mut StorageManager) -> ExecutionResult {
        match self {
            Plan::CreateTable(plan) => plan.execute(storage_manager),
            Plan::InsertTuple(plan) => plan.execute(storage_manager),
            Plan::Query(plan) => plan.execute(storage_manager),
        }
    }
}

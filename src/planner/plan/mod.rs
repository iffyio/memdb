pub(crate) mod execution;

use crate::planner::plan::execution::{CreateTablePlan, ExecutionResult, InsertTuplePlan};
use crate::storage::storage_manager::StorageManager;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Plan {
    CreateTable(CreateTablePlan),
    InsertTuple(InsertTuplePlan),
}

impl Plan {
    pub fn execute(self, storage_manager: &mut StorageManager) -> ExecutionResult {
        match self {
            Plan::CreateTable(plan) => plan.execute(storage_manager),
            Plan::InsertTuple(plan) => plan.execute(storage_manager),
        }
    }
}

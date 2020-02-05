pub(crate) mod create_plan;
pub(crate) mod insert_plan;
pub(crate) mod query_plan;

use crate::planner::plan::create_plan::CreateTablePlan;
use crate::planner::plan::insert_plan::InsertTuplePlan;
use crate::planner::plan::query_plan::QueryPlan;
use crate::storage::storage_manager::StorageManager;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Plan {
    CreateTable(CreateTablePlan),
    InsertTuple(InsertTuplePlan),
    Query(QueryPlan),
}

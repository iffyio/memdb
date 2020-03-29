mod create_table_execution_plan;
mod insert_tuple_execution_plan;
mod query_execution_plan;

pub(crate) use crate::planner::optimizer::create_table_execution_plan::CreateTableExecutionPlan;
pub(crate) use crate::planner::optimizer::insert_tuple_execution_plan::InsertTupleExecutionPlan;
pub(crate) use crate::planner::optimizer::query_execution_plan::QueryExecutionPlan;
pub(crate) use crate::planner::plan::create_plan::CreateTablePlan;
pub(crate) use crate::planner::plan::insert_plan::InsertTuplePlan;
pub(crate) use crate::planner::plan::query_plan::QueryPlan;
use crate::planner::plan::query_plan::{QueryPlanNode, QueryResultSchema};
pub(crate) use crate::planner::plan::Plan;
use crate::storage::storage_manager::Schema;

pub(crate) enum ExecutionPlan {
    CreateTable(create_table_execution_plan::CreateTableExecutionPlan),
    InsertTuple(insert_tuple_execution_plan::InsertTupleExecutionPlan),
    Query(query_execution_plan::QueryExecutionPlan),
}

impl ExecutionPlan {
    pub fn result_schema(&self) -> Option<QueryResultSchema> {
        match self {
            Self::Query(QueryExecutionPlan {
                plan: QueryPlanNode::Scan(node),
            }) => Some(node.schema.clone()),
            Self::Query(QueryExecutionPlan {
                plan: QueryPlanNode::Filter(node),
            }) => Some(node.schema.clone()),
            Self::Query(QueryExecutionPlan {
                plan: QueryPlanNode::Project(node),
            }) => Some(node.schema.clone()),
            Self::Query(QueryExecutionPlan {
                plan: QueryPlanNode::Join(node),
            }) => Some(node.schema.clone()),
            Self::CreateTable(plan) => None,
            Self::InsertTuple(plan) => None,
        }
    }
}

pub(crate) struct Optimizer;

impl Optimizer {
    pub fn run(plan: Plan) -> ExecutionPlan {
        match plan {
            Plan::CreateTable(CreateTablePlan {
                table_name,
                primary_key,
                schema_attributes,
            }) => ExecutionPlan::CreateTable(CreateTableExecutionPlan {
                table_name,
                primary_key,
                schema_attributes,
            }),
            Plan::InsertTuple(InsertTuplePlan { table_name, tuple }) => {
                ExecutionPlan::InsertTuple(InsertTupleExecutionPlan { table_name, tuple })
            }
            Plan::Query(QueryPlan { plan, .. }) => {
                ExecutionPlan::Query(QueryExecutionPlan { plan })
            }
        }
    }
}

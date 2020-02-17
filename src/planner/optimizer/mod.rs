mod create_table_execution_plan;
mod insert_tuple_execution_plan;
mod query_execution_plan;

pub(crate) use crate::planner::optimizer::create_table_execution_plan::CreateTableExecutionPlan;
pub(crate) use crate::planner::optimizer::insert_tuple_execution_plan::InsertTupleExecutionPlan;
pub(crate) use crate::planner::optimizer::query_execution_plan::QueryExecutionPlan;
pub(crate) use crate::planner::plan::create_plan::CreateTablePlan;
pub(crate) use crate::planner::plan::insert_plan::InsertTuplePlan;
pub(crate) use crate::planner::plan::query_plan::QueryPlan;
pub(crate) use crate::planner::plan::Plan;

pub(crate) enum ExecutionPlan {
    CreateTable(create_table_execution_plan::CreateTableExecutionPlan),
    InsertTuple(insert_tuple_execution_plan::InsertTupleExecutionPlan),
    Query(query_execution_plan::QueryExecutionPlan),
}

struct Optimizer;

impl Optimizer {
    fn run(plan: Plan) -> ExecutionPlan {
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

use crate::planner::plan::query_plan::QueryPlanNode;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct QueryExecutionPlan {
    pub plan: QueryPlanNode,
}

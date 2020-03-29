mod db;

use crate::execution::{
    CreateTableOperation, EmptyResult, Engine, FilterOperation, InnerJoinOperation,
    InsertTupleOperation, NextTuple, Operation, ProjectOperation, ScanOperation, SubQueryTuples,
    TupleResult,
};
use crate::planner::optimizer::{
    CreateTableExecutionPlan, InsertTupleExecutionPlan, QueryExecutionPlan,
};
use crate::planner::plan::query_plan::QueryPlanNode::Project;
use crate::planner::plan::query_plan::{
    FilterNode, JoinNode, ProjectNode, QueryPlan, QueryPlanNode, QueryResultSchema, ScanNode,
};
use crate::planner::ExecutionPlan;
use crate::storage::error::{Result as StorageResult, StorageError};
use crate::storage::storage_manager::{AttributeName, Schema, StorageManager};
use crate::storage::tuple::TupleRecord;
use crate::storage::tuple_serde::StorageTupleValue;
use std::collections::HashMap;

// Interface between optimizer and execution engine
pub(crate) struct Evaluation<'storage> {
    pub engine: Engine<'storage>,
}

pub(crate) struct EvaluationResult {
    schema: Option<QueryResultSchema>,
    input: Box<dyn NextTuple>,
}

impl EvaluationResult {
    pub fn next(&mut self) -> Option<StorageResult<Vec<(AttributeName, StorageTupleValue)>>> {
        self.input.next().map(|tuple| {
            tuple.and_then(|tuple| {
                Ok(tuple.to_values(self.schema.as_ref().unwrap().attributes.attributes_iter())?)
            })
        })
    }
}

impl From<EmptyResult> for EvaluationResult {
    fn from(result: EmptyResult) -> Self {
        struct EmptyResultIterator {
            result: Option<EmptyResult>, // Option to only return the result once.
        };
        impl NextTuple for EmptyResultIterator {
            fn next(&mut self) -> TupleResult {
                self.result.take().and_then(|result| match result {
                    Ok(_) => None,
                    Err(err) => Some(Err(err)),
                })
            }
        }

        EvaluationResult {
            schema: None,
            input: Box::new(EmptyResultIterator {
                result: Some(result),
            }),
        }
    }
}

impl<'storage> Evaluation<'storage> {
    pub fn evaluate(&mut self, plan: ExecutionPlan) -> EvaluationResult {
        let schema = plan.result_schema();
        match plan {
            ExecutionPlan::CreateTable(CreateTableExecutionPlan {
                table_name,
                primary_key,
                schema_attributes,
            }) => {
                let op = CreateTableOperation {
                    table_name,
                    primary_key,
                    schema_attributes,
                };

                EvaluationResult::from(self.engine.execute_create_table(op))
            }
            ExecutionPlan::InsertTuple(InsertTupleExecutionPlan { table_name, tuple }) => {
                EvaluationResult::from(
                    self.engine
                        .execute_insert_tuple(InsertTupleOperation { table_name, tuple }),
                )
            }
            ExecutionPlan::Query(QueryExecutionPlan { plan }) => {
                let schema = schema.expect("a query must have a schema.");
                let sub_query = self.create_query_plan(schema, plan);
                EvaluationResult {
                    schema: Some(sub_query.schema),
                    input: sub_query.tuples,
                }
            }
        }
    }

    fn evaluate_scan(&self, node: ScanNode) -> ScanOperation {
        let tuples = self
            .engine
            .storage_manager
            .get_table_store(&node.table_name)
            .expect("[scan operation] table storage no longer exists?")
            .scan()
            .map(|(_id, record)| record.clone())
            .collect();
        ScanOperation::new(tuples)
    }

    fn evaluate_filter(&mut self, node: FilterNode) -> FilterOperation {
        let FilterNode {
            predicate,
            schema,
            child,
        } = node;
        let sub_query = self.create_query_plan(child.result_schema, child.plan);
        FilterOperation::new(predicate, schema, sub_query.tuples)
    }

    fn evaluate_project(&mut self, node: ProjectNode) -> ProjectOperation {
        let ProjectNode {
            record_schema,
            attributes,
            child,
            schema: _,
        } = node;
        let sub_query = self.create_query_plan(child.result_schema, child.plan);
        ProjectOperation {
            record_schema,
            projected_attributes: attributes,
            input: sub_query.tuples,
        }
    }

    fn evaluate_join(&mut self, node: JoinNode) -> InnerJoinOperation {
        let JoinNode {
            join_type: _,
            predicate,
            schema,
            left,
            right,
        } = node;

        let left = self.create_query_plan(left.result_schema, left.plan);
        let right = self.create_query_plan(right.result_schema, right.plan);

        InnerJoinOperation::new(left, right, predicate)
    }

    fn create_query_plan(
        &mut self,
        schema: QueryResultSchema,
        node: QueryPlanNode,
    ) -> SubQueryTuples {
        let tuples: Box<dyn NextTuple> = match node {
            QueryPlanNode::Scan(node) => Box::new(self.evaluate_scan(node)),
            QueryPlanNode::Filter(node) => Box::new(self.evaluate_filter(node)),
            QueryPlanNode::Project(node) => Box::new(self.evaluate_project(node)),
            QueryPlanNode::Join(node) => Box::new(self.evaluate_join(node)),
        };

        SubQueryTuples { schema, tuples }
    }
}

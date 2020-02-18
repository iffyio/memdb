mod db;

use crate::execution::{
    CreateTableOperation, EmptyResult, Engine, FilterOperation, InsertTupleOperation, NextTuple,
    Operation, ProjectOperation, ScanOperation, TupleResult,
};
use crate::planner::optimizer::{
    CreateTableExecutionPlan, InsertTupleExecutionPlan, QueryExecutionPlan,
};
use crate::planner::plan::query_plan::QueryPlanNode::Project;
use crate::planner::plan::query_plan::{FilterNode, ProjectNode, QueryPlanNode, ScanNode};
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
    schema: Option<Schema>,
    input: Box<dyn NextTuple>,
}

impl EvaluationResult {
    pub fn next(&mut self) -> Option<StorageResult<Vec<(AttributeName, StorageTupleValue)>>> {
        self.input.next().map(|tuple| {
            tuple.and_then(|tuple| {
                Ok(tuple.to_values(self.schema.as_ref().unwrap().attributes_iter())?)
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
            ExecutionPlan::Query(QueryExecutionPlan {
                plan: QueryPlanNode::Scan(node),
            }) => EvaluationResult {
                schema,
                input: Box::new(self.evaluate_scan(node)),
            },
            ExecutionPlan::Query(QueryExecutionPlan {
                plan: QueryPlanNode::Filter(node),
            }) => EvaluationResult {
                schema,
                input: Box::new(self.evaluate_filter(node)),
            },
            ExecutionPlan::Query(QueryExecutionPlan {
                plan: QueryPlanNode::Project(node),
            }) => EvaluationResult {
                schema,
                input: Box::new(self.evaluate_project(node)),
            },
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

    fn evaluate_filter(&self, node: FilterNode) -> FilterOperation {
        let FilterNode {
            predicate,
            schema,
            child,
        } = node;

        match child.plan {
            QueryPlanNode::Scan(node) => {
                FilterOperation::new(predicate, schema, Box::new(self.evaluate_scan(node)))
            }
            QueryPlanNode::Filter(node) => {
                FilterOperation::new(predicate, schema, Box::new(self.evaluate_filter(node)))
            }
            QueryPlanNode::Project(node) => {
                FilterOperation::new(predicate, schema, Box::new(self.evaluate_project(node)))
            }
        }
    }

    fn evaluate_project(&self, node: ProjectNode) -> ProjectOperation {
        let ProjectNode {
            record_schema,
            attributes,
            child,
            ..
        } = node;

        match child.plan {
            QueryPlanNode::Scan(node) => ProjectOperation {
                record_schema,
                projected_attributes: attributes,
                input: Box::new(self.evaluate_scan(node)),
            },
            QueryPlanNode::Filter(node) => ProjectOperation {
                record_schema,
                projected_attributes: attributes,
                input: Box::new(self.evaluate_filter(node)),
            },
            QueryPlanNode::Project(node) => ProjectOperation {
                record_schema,
                projected_attributes: attributes,
                input: Box::new(self.evaluate_project(node)),
            },
        }
    }
}

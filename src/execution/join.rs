use crate::execution::expr_evaluation::evaluate_predicate_with_ctx;
use crate::execution::{FilterOperation, NextTuple, SubQueryTuples};
use crate::parser::ast::{AttributeValue, Expr};
use crate::planner::plan::query_plan::QueryResultSchema;
use crate::storage::error::StorageError;
use crate::storage::storage_manager::AttributeName;
use crate::storage::tuple::TupleRecord;
use crate::storage::tuple_serde::StorageTupleValue;
use std::collections::HashMap;

struct TupleWithColumnLookup {
    tuple: TupleRecord,
    columns: HashMap<AttributeName, StorageTupleValue>,
}

pub struct InnerJoinOperation {
    predicate: Expr,
    left: SubQueryTuples,
    right: SubQueryTuples,
    left_tuple_buffer: Vec<TupleWithColumnLookup>,
    joined_tuples_buffer: Vec<TupleRecord>,
    pre_fetched_left: bool,
}

impl InnerJoinOperation {
    pub fn new(left: SubQueryTuples, right: SubQueryTuples, predicate: Expr) -> Self {
        InnerJoinOperation {
            predicate,
            left,
            right,
            left_tuple_buffer: Vec::new(),
            joined_tuples_buffer: Vec::new(),
            pre_fetched_left: false,
        }
    }

    fn pre_fetch_left(&mut self) -> Result<(), StorageError> {
        self.pre_fetched_left = true;
        while let Some(result) = self.left.tuples.next() {
            let tuple = result?;
            let columns = tuple
                .to_values::<_, HashMap<_, _>>(self.left.schema.attributes.attributes_iter())?;
            self.left_tuple_buffer
                .push(TupleWithColumnLookup { tuple, columns });
        }
        Ok(())
    }

    fn join_next_tuple_from_right(&mut self) -> Result<(), StorageError> {
        if self.left_tuple_buffer.is_empty() && !self.joined_tuples_buffer.is_empty() {
            return Ok(());
        }

        while let Some(result) = self.right.tuples.next() {
            let right = result?;
            let right_columns = right
                .to_values::<_, HashMap<_, _>>(self.right.schema.attributes.attributes_iter())?;
            for left in &self.left_tuple_buffer {
                let join_ctx = left
                    .columns
                    .iter()
                    .chain(right_columns.iter())
                    .map(|(attr_name, attr_type)| (&attr_name.0, attr_type))
                    .collect::<HashMap<_, _>>();

                let join_match = evaluate_predicate_with_ctx(&self.predicate, &join_ctx);
                if join_match {
                    self.joined_tuples_buffer
                        .push(TupleRecord::concat(&left.tuple, &right));
                }
            }
            if !self.joined_tuples_buffer.is_empty() {
                break;
            }
        }
        Ok(())
    }
}

impl NextTuple for InnerJoinOperation {
    fn next(&mut self) -> Option<Result<TupleRecord, StorageError>> {
        if !self.pre_fetched_left {
            self.pre_fetch_left();
        }
        match self.join_next_tuple_from_right() {
            Ok(()) => (),
            Err(err) => return Some(Err(err)),
        }
        self.joined_tuples_buffer.pop().map(|t| Ok(t))
    }
}

#[cfg(test)]
mod test {
    use crate::execution::join::InnerJoinOperation;
    use crate::execution::{NextTuple, ScanOperation, SubQueryTuples};
    use crate::parser::ast::{BinaryExpr, BinaryOperation, Expr, LiteralExpr};
    use crate::planner::plan::query_plan::QueryResultSchema;
    use crate::storage::storage_manager::{AttributeName, Attributes};
    use crate::storage::tuple_serde::{deserialize_tuple, serialize_tuple, StorageTupleValue};
    use crate::storage::types::AttributeType;

    #[test]
    fn join() {
        let left_schema = QueryResultSchema::new(Attributes::new(vec![
            (AttributeName("name".to_owned()), AttributeType::Text),
            (AttributeName("age".to_owned()), AttributeType::Integer),
        ]));
        let right_schema = QueryResultSchema::new(Attributes::new(vec![
            (AttributeName("id".to_owned()), AttributeType::Text),
            (AttributeName("department".to_owned()), AttributeType::Text),
        ]));
        let join_schema = QueryResultSchema::new(Attributes::new(vec![
            (AttributeName("name".to_owned()), AttributeType::Text),
            (AttributeName("age".to_owned()), AttributeType::Integer),
            (AttributeName("id".to_owned()), AttributeType::Text),
            (AttributeName("department".to_owned()), AttributeType::Text),
        ]));

        let left_input = SubQueryTuples {
            schema: left_schema.clone().with_alias("person"),
            tuples: Box::new(ScanOperation::new(vec![
                serialize_tuple(vec![
                    StorageTupleValue::String("a".to_owned()),
                    StorageTupleValue::Integer(11),
                ]),
                serialize_tuple(vec![
                    StorageTupleValue::String("b".to_owned()),
                    StorageTupleValue::Integer(10),
                ]),
                serialize_tuple(vec![
                    StorageTupleValue::String("a".to_owned()),
                    StorageTupleValue::Integer(13),
                ]),
                serialize_tuple(vec![
                    StorageTupleValue::String("c".to_owned()),
                    StorageTupleValue::Integer(12),
                ]),
            ])),
        };
        let right_input = SubQueryTuples {
            schema: right_schema.clone().with_alias("employee"),
            tuples: Box::new(ScanOperation::new(vec![
                serialize_tuple(vec![
                    StorageTupleValue::String("a".to_owned()),
                    StorageTupleValue::String("sales".to_owned()),
                ]),
                serialize_tuple(vec![
                    StorageTupleValue::String("d".to_owned()),
                    StorageTupleValue::String("product".to_owned()),
                ]),
                serialize_tuple(vec![
                    StorageTupleValue::String("c".to_owned()),
                    StorageTupleValue::String("marketing".to_owned()),
                ]),
            ])),
        };
        let predicate = Expr::Binary(BinaryExpr {
            left: Box::new(Expr::Literal(LiteralExpr::Identifier(
                "person.name".to_owned(),
            ))),
            op: BinaryOperation::Equal,
            right: Box::new(Expr::Literal(LiteralExpr::Identifier(
                "employee.id".to_owned(),
            ))),
        });

        let mut j = InnerJoinOperation::new(left_input, right_input, predicate);
        let mut joined_tuples = Vec::new();
        while let Some(tuple) = j.next() {
            joined_tuples.push(tuple);
        }

        let mut joined_tuples = joined_tuples
            .into_iter()
            .map(|tuple| {
                tuple.map(|tuple| {
                    deserialize_tuple(
                        tuple,
                        join_schema
                            .clone()
                            .attributes
                            .attributes_iter()
                            .map(|(_, attr_type)| attr_type.clone())
                            .collect(),
                    )
                })
            })
            .collect::<Vec<_>>();
        joined_tuples.sort_by_key(|result| match result {
            Ok(tuples) => tuples.clone(),
            Err(_) => vec![],
        });
        assert_eq!(
            joined_tuples,
            vec![
                Ok(vec![
                    StorageTupleValue::String("a".to_owned()),
                    StorageTupleValue::Integer(11),
                    StorageTupleValue::String("a".to_owned()),
                    StorageTupleValue::String("sales".to_owned()),
                ]),
                Ok(vec![
                    StorageTupleValue::String("a".to_owned()),
                    StorageTupleValue::Integer(13),
                    StorageTupleValue::String("a".to_owned()),
                    StorageTupleValue::String("sales".to_owned()),
                ]),
                Ok(vec![
                    StorageTupleValue::String("c".to_owned()),
                    StorageTupleValue::Integer(12),
                    StorageTupleValue::String("c".to_owned()),
                    StorageTupleValue::String("marketing".to_owned()),
                ]),
            ]
        )
    }
}

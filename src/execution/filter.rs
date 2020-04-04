use crate::execution::expr_evaluation::evaluate_predicate_with_ctx;
use crate::execution::{NextTuple, TupleResult};
use crate::parser::ast::{BinaryOperation, Expr, LiteralExpr};
use crate::planner::plan::query_plan::QueryResultSchema;
use crate::storage::error::{Result as StorageResult, StorageError};
use crate::storage::storage_manager::{AttributeName, Schema, StorageManager};
use crate::storage::tuple::TupleRecord;
use crate::storage::tuple_serde::StorageTupleValue;
use std::collections::HashMap;

pub struct FilterOperation {
    pub predicate: Expr,
    pub schema: QueryResultSchema,
    pub input: Box<dyn NextTuple>,
}

impl NextTuple for FilterOperation {
    fn next(&mut self) -> TupleResult {
        loop {
            match self.input.next() {
                Some(Ok(record)) => {
                    match record
                        .to_values::<_, HashMap<_, _>>(self.schema.attributes.attributes_iter())
                    {
                        Ok(tuple_values) => {
                            let forward = evaluate_predicate_with_ctx(
                                &self.predicate,
                                &tuple_values
                                    .iter()
                                    .map(|(attr_name, attr_type)| (&attr_name.0, attr_type))
                                    .collect(),
                            );

                            if forward {
                                return Some(Ok(record));
                            }
                        }
                        Err(err) => return Some(Err(StorageError::from(err))),
                    }
                }
                other => return other,
            }
        }
    }
}

impl FilterOperation {
    pub fn new(predicate: Expr, schema: QueryResultSchema, input: Box<dyn NextTuple>) -> Self {
        FilterOperation {
            predicate,
            schema,
            input,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::execution::filter::FilterOperation;
    use crate::execution::{NextTuple, ScanOperation};
    use crate::parser::ast::Expr::{self, Binary};
    use crate::parser::ast::{BinaryExpr, BinaryOperation, LiteralExpr};
    use crate::planner::plan::query_plan::QueryResultSchema;
    use crate::storage::storage_manager::{AttributeName, Attributes, Schema};
    use crate::storage::tuple::StoreId;
    use crate::storage::tuple_serde::{deserialize_tuple, serialize_tuple, StorageTupleValue};
    use crate::storage::types::AttributeType;

    #[test]
    fn filter() {
        let schema = QueryResultSchema::new(Attributes::new(vec![
            (AttributeName("name".to_owned()), AttributeType::Text),
            (AttributeName("age".to_owned()), AttributeType::Integer),
        ]));

        let mut input = ScanOperation::new(vec![
            serialize_tuple(vec![
                StorageTupleValue::String("a".to_owned()),
                StorageTupleValue::Integer(11),
            ]),
            serialize_tuple(vec![
                StorageTupleValue::String("b".to_owned()),
                StorageTupleValue::Integer(10),
            ]),
            serialize_tuple(vec![
                StorageTupleValue::String("c".to_owned()),
                StorageTupleValue::Integer(12),
            ]),
            serialize_tuple(vec![
                StorageTupleValue::String("d".to_owned()),
                StorageTupleValue::Integer(9),
            ]),
        ]);
        let mut f = FilterOperation {
            predicate: Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Literal(LiteralExpr::Identifier("age".to_owned()))),
                op: BinaryOperation::LessThanOrEqual,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(10))),
            }),
            schema: schema.clone(),
            input: Box::new(input),
        };

        let mut filtered_tuples = Vec::new();
        while let Some(tuple) = f.next() {
            filtered_tuples.push(tuple)
        }
        assert_eq!(
            filtered_tuples
                .into_iter()
                .map(|tuple| tuple.map(|tuple| deserialize_tuple(
                    tuple,
                    schema
                        .clone()
                        .attributes
                        .attributes_iter()
                        .map(|(_, _type)| _type.clone())
                        .collect()
                )))
                .collect::<Vec<_>>(),
            vec![
                Ok(vec![
                    StorageTupleValue::String("b".to_owned()),
                    StorageTupleValue::Integer(10)
                ]),
                Ok(vec![
                    StorageTupleValue::String("d".to_owned()),
                    StorageTupleValue::Integer(9)
                ])
            ]
        );
    }
}

use crate::parser::ast::{AttributeType, BinaryOperation, Expr, LiteralExpr};
use crate::storage::error::Result as StorageResult;
use crate::storage::storage_manager::{AttributeName, Schema, StorageManager};
use crate::storage::tuple::TupleRecord;
use crate::storage::tuple_serde::StorageTupleValue;
use std::collections::HashMap;

#[derive(Debug, Eq, PartialEq)]
pub struct FilterOperation {
    pub predicate: Expr,
    pub schema: Schema,
}

impl FilterOperation {
    pub fn new(predicate: Expr, schema: Schema) -> Self {
        FilterOperation { predicate, schema }
    }

    pub fn execute<'a, T: 'a>(
        self,
        input: T,
    ) -> impl Iterator<Item = StorageResult<TupleRecord>> + 'a
    where
        T: Iterator<Item = StorageResult<TupleRecord>>,
    {
        let schema = self.schema.clone();
        let predicate = self.predicate;
        input
            .map(move |result| {
                result.and_then(|record| {
                    let tuple_values = record.to_values(schema.attributes_iter())?;
                    let yes = FilterOperation::evaluate_predicate_with_ctx(
                        &predicate,
                        tuple_values
                            .into_iter()
                            .map(|(attr_name, attr_type)| (attr_name.0, attr_type))
                            .collect(),
                    );
                    Ok((yes, record))
                })
            })
            .filter_map(|r| match r {
                Ok((false, _)) => None,
                Ok((true, inner)) => Some(Ok(inner)),
                Err(err) => Some(Err(err)),
            })
    }

    fn evaluate_predicate_with_ctx(
        predicate: &Expr,
        ctx: HashMap<String, StorageTupleValue>,
    ) -> bool {
        fn eval<'a>(
            attr: &String,
            ctx: &'a HashMap<String, StorageTupleValue>,
        ) -> &'a StorageTupleValue {
            ctx.get(attr)
                .expect("attribute doesn't exist in this context")
        }

        fn evaluate_expr(expr: &Expr, ctx: &HashMap<String, StorageTupleValue>) -> LiteralExpr {
            match expr {
                Expr::Binary(expr) => {
                    let left = evaluate_expr(&expr.left, ctx);
                    let right = evaluate_expr(&expr.right, ctx);
                    match left {
                        LiteralExpr::Boolean(left) => {
                            match right {
                                LiteralExpr::Boolean(right) => {
                                    match expr.op {
                                        BinaryOperation::Equal => LiteralExpr::Boolean(left == right),
                                        BinaryOperation::NotEqual => LiteralExpr::Boolean(left != right),
                                        BinaryOperation::LessThan => LiteralExpr::Boolean(left < right),
                                        BinaryOperation::LessThanOrEqual => LiteralExpr::Boolean(left <= right),
                                        BinaryOperation::GreaterThan => LiteralExpr::Boolean(left > right),
                                        BinaryOperation::GreaterThanOrEqual => LiteralExpr::Boolean(left >= right),
                                        _ => unreachable!("[validation] only equality operations are allowed between two booleans"),
                                    }
                                },
                                _ => unreachable!("[validation] incompatible op: left hand is bool but right hand isn't")
                            }
                        },
                        LiteralExpr::Integer(left) => {
                            match right {
                                LiteralExpr::Integer(right) => {
                                    match expr.op {
                                        BinaryOperation::Addition => LiteralExpr::Integer(left + right),
                                        BinaryOperation::Subtraction => LiteralExpr::Integer(left - right),
                                        BinaryOperation::Multiplication => LiteralExpr::Integer(left * right),
                                        BinaryOperation::Division => LiteralExpr::Integer(left / right),
                                        BinaryOperation::Equal => LiteralExpr::Boolean(left == right),
                                        BinaryOperation::NotEqual => LiteralExpr::Boolean(left != right),
                                        BinaryOperation::LessThan => LiteralExpr::Boolean(left < right),
                                        BinaryOperation::LessThanOrEqual => LiteralExpr::Boolean(left <= right),
                                        BinaryOperation::GreaterThan => LiteralExpr::Boolean(left > right),
                                        BinaryOperation::GreaterThanOrEqual => LiteralExpr::Boolean(left >= right),
                                    }
                                },
                                _ => unreachable!("[validation] incompatible op: left hand is a number but right hand isn't")
                            }
                        },
                        LiteralExpr::String(left) => {
                            match right {
                                LiteralExpr::String(right) => {
                                    match expr.op {
                                        BinaryOperation::Equal => LiteralExpr::Boolean(left == right),
                                        BinaryOperation::NotEqual => LiteralExpr::Boolean(left != right),
                                        _ => unreachable!("[validation] incompatible op: left hand is a string but right hand isn't")
                                    }
                                },
                                _ => unreachable!("[validation] only equality operations are allowed between two strings"),
                            }
                        },
                        LiteralExpr::Identifier(_) => unreachable!("identifier should have been evaluated to a concrete value.")
                    }
                }
                Expr::Literal(LiteralExpr::Identifier(id)) => match eval(id, ctx) {
                    StorageTupleValue::Boolean(value) => LiteralExpr::Boolean(*value),
                    StorageTupleValue::Integer(value) => LiteralExpr::Integer(*value),
                    StorageTupleValue::String(value) => LiteralExpr::String(value.clone()),
                },
                Expr::Literal(literal) => literal.clone(),
            }
        }

        match evaluate_expr(&predicate, &ctx) {
            LiteralExpr::Boolean(result) => result,
            unexpected => unreachable!(format!(
                "filter predicate is not an equality expression {:?}",
                unexpected
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::parser::ast::Expr::{self, Binary};
    use crate::parser::ast::{BinaryExpr, BinaryOperation, LiteralExpr};
    use crate::planner::operation::filter::FilterOperation;
    use crate::storage::storage_manager::{AttributeName, Schema};
    use crate::storage::tuple::StoreId;
    use crate::storage::tuple_serde::{deserialize_tuple, serialize_tuple, StorageTupleValue};
    use crate::storage::types::AttributeType;

    #[test]
    fn filter() {
        let schema = Schema::new(
            StoreId(0),
            AttributeName("name".to_owned()),
            vec![
                (AttributeName("name".to_owned()), AttributeType::Text),
                (AttributeName("age".to_owned()), AttributeType::Integer),
            ],
        );
        let f = FilterOperation {
            predicate: Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Literal(LiteralExpr::Identifier("age".to_owned()))),
                op: BinaryOperation::LessThanOrEqual,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(10))),
            }),
            schema: schema.clone(),
        };

        let filtered_tuples = f
            .execute(
                vec![
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
                ]
                .into_iter()
                .map(|t| Ok(t)),
            )
            .collect::<Vec<_>>();
        assert_eq!(
            filtered_tuples
                .into_iter()
                .map(|tuple| tuple.map(|tuple| deserialize_tuple(
                    tuple,
                    schema
                        .clone()
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

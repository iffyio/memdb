use crate::storage::error::Result as StorageResult;
use crate::storage::storage_manager::{AttributeName, Schema};
use crate::storage::tuple::TupleRecord;
use crate::storage::tuple_serde::serialize_tuple;

#[derive(Debug, Eq, PartialEq)]
pub struct ProjectOperation {
    pub record_schema: Schema,
    pub projected_attributes: Vec<AttributeName>,
}

impl ProjectOperation {
    pub fn execute<'a, T: 'a>(
        self,
        input: T,
    ) -> impl Iterator<Item = StorageResult<TupleRecord>> + 'a
    where
        T: Iterator<Item = StorageResult<TupleRecord>>,
    {
        let schema = self.record_schema;
        let attributes = self.projected_attributes;
        input.map(move |result| {
            result.and_then(|record| {
                let value_by_attr = record.to_values(schema.attributes_iter())?;
                let projected_values = attributes
                    .iter()
                    .map(|attr_name| {
                        value_by_attr
                            .get(attr_name)
                            .expect("we verified that the record has this attribute")
                            .clone()
                    })
                    .collect();
                let record = serialize_tuple(projected_values);
                Ok(record)
            })
        })
    }
}

#[cfg(test)]
mod test {
    use crate::parser::ast::Expr::{self, Binary};
    use crate::parser::ast::{BinaryExpr, BinaryOperation, LiteralExpr};
    use crate::planner::operation::filter::FilterOperation;
    use crate::planner::operation::ProjectOperation;
    use crate::storage::storage_manager::{AttributeName, Schema};
    use crate::storage::tuple::StoreId;
    use crate::storage::tuple_serde::{deserialize_tuple, serialize_tuple, StorageTupleValue};
    use crate::storage::types::AttributeType;

    #[test]
    fn project() {
        let schema = Schema::new(
            StoreId(0),
            AttributeName("name".to_owned()),
            vec![
                (AttributeName("name".to_owned()), AttributeType::Text),
                (AttributeName("age".to_owned()), AttributeType::Integer),
                (AttributeName("location".to_owned()), AttributeType::Text),
                (
                    AttributeName("is_member".to_owned()),
                    AttributeType::Boolean,
                ),
            ],
        );
        let projection_schema = Schema::new(
            StoreId(0),
            AttributeName("name".to_owned()),
            vec![
                (
                    AttributeName("is_member".to_owned()),
                    AttributeType::Boolean,
                ),
                (AttributeName("age".to_owned()), AttributeType::Integer),
            ],
        );
        let p = ProjectOperation {
            record_schema: schema.clone(),
            projected_attributes: vec![
                AttributeName("is_member".to_owned()),
                AttributeName("age".to_owned()),
            ],
        };

        let projected_tuples = p
            .execute(
                vec![
                    serialize_tuple(vec![
                        StorageTupleValue::String("a".to_owned()),
                        StorageTupleValue::Integer(11),
                        StorageTupleValue::String("locA".to_owned()),
                        StorageTupleValue::Boolean(true),
                    ]),
                    serialize_tuple(vec![
                        StorageTupleValue::String("b".to_owned()),
                        StorageTupleValue::Integer(10),
                        StorageTupleValue::String("locB".to_owned()),
                        StorageTupleValue::Boolean(false),
                    ]),
                    serialize_tuple(vec![
                        StorageTupleValue::String("c".to_owned()),
                        StorageTupleValue::Integer(12),
                        StorageTupleValue::String("locC".to_owned()),
                        StorageTupleValue::Boolean(false),
                    ]),
                    serialize_tuple(vec![
                        StorageTupleValue::String("d".to_owned()),
                        StorageTupleValue::Integer(9),
                        StorageTupleValue::String("locD".to_owned()),
                        StorageTupleValue::Boolean(true),
                    ]),
                ]
                .into_iter()
                .map(|t| Ok(t)),
            )
            .collect::<Vec<_>>();
        assert_eq!(
            projected_tuples
                .into_iter()
                .map(|tuple| tuple.map(|tuple| deserialize_tuple(
                    tuple,
                    projection_schema
                        .clone()
                        .attributes_iter()
                        .map(|(_, _type)| _type.clone())
                        .collect()
                )))
                .collect::<Vec<_>>(),
            vec![
                Ok(vec![
                    StorageTupleValue::Boolean(true),
                    StorageTupleValue::Integer(11),
                ]),
                Ok(vec![
                    StorageTupleValue::Boolean(false),
                    StorageTupleValue::Integer(10),
                ]),
                Ok(vec![
                    StorageTupleValue::Boolean(false),
                    StorageTupleValue::Integer(12),
                ]),
                Ok(vec![
                    StorageTupleValue::Boolean(true),
                    StorageTupleValue::Integer(9),
                ]),
            ]
        );
    }
}

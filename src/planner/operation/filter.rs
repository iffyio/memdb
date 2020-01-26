use crate::parser::ast::{AttributeType, Expr};
use crate::storage::error::Result as StorageResult;
use crate::storage::storage_manager::{AttributeName, Schema, StorageManager};
use crate::storage::tuple::TupleRecord;
use crate::storage::tuple_serde::StorageTupleValue;
use std::collections::HashMap;

pub struct FilterOperation {
    pub expr: Expr,
    pub schema: Schema,
}

impl FilterOperation {
    pub fn new(expr: Expr, schema: Schema) -> Self {
        FilterOperation { expr, schema }
    }

    pub fn execute<'a, T: 'a>(
        self,
        input: T,
    ) -> impl Iterator<Item = StorageResult<TupleRecord>> + 'a
    where
        T: Iterator<Item = StorageResult<TupleRecord>>,
    {
        let schema = self.schema.clone();
        let expr = self.expr;
        input.map(move |result| {
            result.and_then(|record| {
                let tuple_values = record.to_values(schema.attributes_iter())?;
                let _ = FilterOperation::evaluate_with_context(&expr, tuple_values);
                Ok(record)
            })
        })
    }

    fn evaluate_with_context(expr: &Expr, ctx: HashMap<AttributeName, StorageTupleValue>) -> bool {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn filter() {}
}

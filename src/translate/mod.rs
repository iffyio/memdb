mod error;

use crate::parser::ast::{
    AttributeDefinition, AttributeType as ParserAttributeType, AttributeType, AttributeValue,
    BinaryExpr, BinaryOperation, CreateTableStmt, Expr, InsertStmt, LiteralExpr, Stmt,
};
use crate::planner::operation::{CreateTableOperation, InsertOperation};
use crate::planner::plan::Plan;
use crate::storage::error::StorageError;
use crate::storage::storage_manager::{
    AttributeName, CreateTableRequest, Schema, StorageManager, TableName,
};
use crate::storage::tuple_serde::{serialize_tuple, StorageTupleValue};
use crate::storage::types::AttributeType as StorageAttributeType;
use crate::translate::error::TranslateError;
use error::Result;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub(crate) struct Translator {
    storage_manager: StorageManager,
}

impl Translator {
    pub fn translate(&mut self, stmt: Stmt) -> Result<Plan> {
        match stmt {
            Stmt::CreateTable(stmt) => self.translate_create_table(stmt),
            Stmt::Insert(stmt) => self.translate_insert(stmt),
            Stmt::Select(_) => unimplemented!(),
        }
    }

    fn translate_create_table(&mut self, stmt: CreateTableStmt) -> Result<Plan> {
        let CreateTableStmt {
            table_name,
            attribute_definitions,
        } = stmt;

        let table_name = TableName(table_name);

        if let Some(_) = self.storage_manager.get_schema(&table_name) {
            return Err(TranslateError::StorageError(Box::new(
                StorageError::AlreadyExists(format!("table {:?}", table_name.0)),
            )));
        }

        let primary_key = {
            let primary_keys = attribute_definitions
                .iter()
                .filter(|def| def.is_primary_key)
                .collect::<Vec<&AttributeDefinition>>();

            match primary_keys.len() {
                0 => return Err(TranslateError::PrimaryKeyRequired),
                1 => (),
                len => {
                    return Err(TranslateError::MultiplePrimaryKeys(
                        primary_keys.iter().map(|def| def.name.clone()).collect(),
                    ))
                }
            }

            AttributeName(primary_keys.iter().next().unwrap().name.clone())
        };
        let mut attributes = HashMap::new();

        for col in attribute_definitions {
            match attributes.entry(AttributeName(col.name)) {
                Entry::Vacant(entry) => {
                    entry.insert(Self::translate_attribute_type(col.attribute_type));
                }
                Entry::Occupied(entry) => {
                    return Err(TranslateError::DuplicateAttributeName(
                        entry.key().0.clone(),
                    ));
                }
            }
        }

        Ok(Plan::CreateTable(CreateTableOperation {
            table_name,
            attributes,
            primary_key,
        }))
    }

    fn translate_insert(&mut self, stmt: InsertStmt) -> Result<Plan> {
        let InsertStmt {
            table_name,
            attribute_names,
            attribute_values,
        } = stmt;

        let table_name = TableName(table_name);

        let schema = match self.storage_manager.get_schema(&table_name) {
            Some(meta) => meta,
            None => return Err(TranslateError::NoSuchTable(table_name.0)),
        };

        if attribute_names.len() != attribute_values.len() {
            return Err(TranslateError::InvalidArguments(format!(
                "attribute length mismatch: {:?} attributes specified, {} values provided, expected {}",
                attribute_names.len(),
                attribute_values.len(),
                schema.num_attributes(),
            )));
        }
        if attribute_values.is_empty() {
            return Err(TranslateError::InvalidArguments(
                "No attribute values provided".to_owned(),
            ));
        }

        let mut resolved_attribute_values = Vec::new();
        for attr in attribute_values {
            resolved_attribute_values.push(Self::resolve_attribute_value(attr)?);
        }

        let attribute_names = attribute_names
            .into_iter()
            .map(|name| AttributeName(name))
            .collect::<Vec<AttributeName>>();

        fn resolved_value_to_type(value: &StorageTupleValue) -> StorageAttributeType {
            match value {
                StorageTupleValue::Integer(_) => StorageAttributeType::Integer,
                StorageTupleValue::String(_) => StorageAttributeType::Text,
                StorageTupleValue::Boolean(_) => unimplemented!("no boolean upstream"),
            }
        }
        for (name, value) in attribute_names.iter().zip(resolved_attribute_values.iter()) {
            match schema.get_attribute_type(name) {
                Some(expected_type) if expected_type != resolved_value_to_type(value) => {
                    return Err(TranslateError::InvalidArguments(format!(
                        "type mismatch for attribute {:?} in table {:?}: expected {:?}, got {:?}",
                        name.0,
                        table_name.0,
                        expected_type,
                        resolved_value_to_type(value)
                    )))
                }
                None => {
                    return Err(TranslateError::InvalidArguments(format!(
                        "no such attribute {:?} in table {:?}",
                        name.0, table_name.0,
                    )))
                }
                _ => (), // types match so nothing to do.
            }
        }

        Ok(Plan::InsertTuple(InsertOperation {
            table_name,
            tuple: serialize_tuple(resolved_attribute_values),
        }))
    }

    fn translate_attribute_type(attribute_type: ParserAttributeType) -> StorageAttributeType {
        match attribute_type {
            ParserAttributeType::Integer => StorageAttributeType::Integer,
            ParserAttributeType::Text => StorageAttributeType::Text,
        }
    }

    fn resolve_attribute_value(attribute_value: AttributeValue) -> Result<StorageTupleValue> {
        fn resolve_literal_expr(expr: LiteralExpr) -> Result<StorageTupleValue> {
            match expr {
                LiteralExpr::String(s) => Ok(StorageTupleValue::String(s)),
                LiteralExpr::Boolean(b) => Ok(StorageTupleValue::Boolean(b)),
                LiteralExpr::Integer(i) => Ok(StorageTupleValue::Integer(i)),
            }
        }
        fn resolve_binary_expr(expr: BinaryExpr) -> Result<StorageTupleValue> {
            let left_type = resolve_expr(*expr.left)?;
            let right_type = resolve_expr(*expr.right)?;

            match left_type {
                StorageTupleValue::String(_) => Err(TranslateError::InvalidArguments(
                    "left operand of binary operations cannot be strings".to_owned(),
                )),
                StorageTupleValue::Integer(left) => match right_type {
                    StorageTupleValue::Integer(right) => match &expr.op {
                        BinaryOperation::Addition => Ok(StorageTupleValue::Integer(left + right)),
                        BinaryOperation::Subtraction => {
                            Ok(StorageTupleValue::Integer(left - right))
                        }
                        BinaryOperation::Multiplication => {
                            Ok(StorageTupleValue::Integer(left * right))
                        }
                        BinaryOperation::Division => Ok(StorageTupleValue::Integer(left / right)),
                        BinaryOperation::Equal => Ok(StorageTupleValue::Boolean(left == right)),
                        BinaryOperation::NotEqual => Ok(StorageTupleValue::Boolean(left != right)),
                        BinaryOperation::LessThan => Ok(StorageTupleValue::Boolean(left < right)),
                        BinaryOperation::LessThanOrEqual => {
                            Ok(StorageTupleValue::Boolean(left <= right))
                        }
                        BinaryOperation::GreaterThan => {
                            Ok(StorageTupleValue::Boolean(left > right))
                        }
                        BinaryOperation::GreaterThanOrEqual => {
                            Ok(StorageTupleValue::Boolean(left >= right))
                        }
                    },
                    invalid => Err(TranslateError::InvalidArguments(format!(
                        "Invalid right operand for arithmetic operation: {:?}",
                        invalid
                    ))),
                },
                StorageTupleValue::Boolean(left) => match right_type {
                    StorageTupleValue::Boolean(right) => match &expr.op {
                        BinaryOperation::Equal => Ok(StorageTupleValue::Boolean(left == right)),
                        BinaryOperation::NotEqual => Ok(StorageTupleValue::Boolean(left != right)),
                        op => Err(TranslateError::InvalidArguments(format!(
                            "Invalid operation {:?} with boolean operands",
                            op
                        ))),
                    },
                    invalid => Err(TranslateError::InvalidArguments(format!(
                        "Invalid right operand for arithmetic operation: {:?}",
                        invalid
                    ))),
                },
            }
        }
        fn resolve_expr(expr: Expr) -> Result<StorageTupleValue> {
            match expr {
                Expr::Binary(expr) => resolve_binary_expr(expr),
                Expr::Literal(expr) => resolve_literal_expr(expr),
            }
        }

        match attribute_value {
            AttributeValue::String(s) => Ok(StorageTupleValue::String(s)),
            AttributeValue::Expr(expr) => resolve_expr(expr),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Result;
    use crate::parser::ast::AttributeValue::Expr;
    use crate::parser::ast::Expr::Literal;
    use crate::parser::ast::{
        AttributeDefinition, AttributeType as ParserAttributeType, AttributeValue, CreateTableStmt,
        InsertStmt, LiteralExpr,
    };
    use crate::planner::operation::{CreateTableOperation, InsertOperation};
    use crate::planner::plan::Plan::{self, CreateTable};
    use crate::storage::storage_manager::{
        AttributeName, CreateTableRequest, StorageManager, TableName,
    };
    use crate::storage::tuple::{StoreId, TupleRecord};
    use crate::storage::types::AttributeType as StorageAttributeType;
    use crate::translate::Translator;
    use std::collections::HashMap;

    #[test]
    fn translate_create_table() -> Result<()> {
        let stmt = CreateTableStmt {
            table_name: "person".to_owned(),
            attribute_definitions: vec![
                AttributeDefinition {
                    name: "name".to_owned(),
                    attribute_type: ParserAttributeType::Text,
                    is_primary_key: true,
                },
                AttributeDefinition {
                    name: "age".to_owned(),
                    attribute_type: ParserAttributeType::Integer,
                    is_primary_key: false,
                },
            ],
        };

        let mut t = Translator {
            storage_manager: StorageManager::new(StoreId(0)),
        };

        let req = t.translate_create_table(stmt)?;

        let mut attributes = HashMap::new();
        attributes.insert(AttributeName("name".to_owned()), StorageAttributeType::Text);
        attributes.insert(
            AttributeName("age".to_owned()),
            StorageAttributeType::Integer,
        );
        assert_eq!(
            req,
            CreateTable(CreateTableOperation {
                table_name: TableName("person".to_owned()),
                primary_key: AttributeName("name".to_owned()),
                attributes,
            })
        );

        Ok(())
    }

    #[test]
    fn translate_insert() -> Result<()> {
        let stmt = InsertStmt {
            table_name: "person".to_owned(),
            attribute_names: vec!["name".to_owned(), "age".to_owned()],
            attribute_values: vec![
                AttributeValue::String("bob".to_owned()),
                AttributeValue::Expr(Literal(LiteralExpr::Integer(20))),
            ],
        };

        let mut attributes = HashMap::new();
        attributes.insert(AttributeName("name".to_owned()), StorageAttributeType::Text);
        attributes.insert(
            AttributeName("age".to_owned()),
            StorageAttributeType::Integer,
        );

        let mut storage_manager = StorageManager::new(StoreId(0));
        storage_manager.create_table(CreateTableRequest {
            table_name: TableName("person".to_owned()),
            primary_key: AttributeName("name".to_owned()),
            attributes,
        })?;
        let mut t = Translator { storage_manager };

        let plan = t.translate_insert(stmt)?;
        assert_eq!(
            plan,
            Plan::InsertTuple(InsertOperation {
                table_name: TableName("person".to_owned()),
                tuple: TupleRecord(vec![0, 0, 0, 3, 98, 111, 98, 0, 0, 0, 20])
            })
        );

        Ok(())
    }
}

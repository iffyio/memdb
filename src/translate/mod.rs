mod error;
mod type_check;

use crate::parser::ast::{
    AttributeDefinition, AttributeType as ParserAttributeType, AttributeType, AttributeValue,
    BinaryExpr, BinaryOperation, CreateTableStmt, Expr, FromClause, InsertStmt, LiteralExpr,
    SelectProperties, SelectStmt, Stmt, WhereClause,
};
use crate::planner::plan::create_plan::CreateTablePlan;
use crate::planner::plan::insert_plan::InsertTuplePlan;
use crate::planner::plan::query_plan::{
    FilterNode, ProjectNode, QueryPlan, QueryPlanNode, ScanNode,
};
use crate::planner::plan::Plan;
use crate::storage::error::StorageError;
use crate::storage::storage_manager::{
    AttributeName, CreateTableRequest, Schema, StorageManager, TableName,
};
use crate::storage::tuple_serde::{serialize_tuple, StorageTupleValue};
use crate::storage::types::AttributeType as StorageAttributeType;
use crate::translate::error::TranslateError;
use crate::translate::type_check::type_check_expr;
use error::Result;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

pub(crate) struct Translator {
    storage_manager: StorageManager,
}

impl Translator {
    pub fn translate(&mut self, stmt: Stmt) -> Result<Plan> {
        match stmt {
            Stmt::CreateTable(stmt) => self.translate_create_table(stmt),
            Stmt::Insert(stmt) => self.translate_insert(stmt),
            Stmt::Select(stmt) => self.translate_select(stmt),
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
        {
            let mut attributes = HashSet::new();
            for col in &attribute_definitions {
                if attributes.contains(&col.name) {
                    return Err(TranslateError::DuplicateAttributeName(col.name.clone()));
                }
                attributes.insert(&col.name);
            }
        }

        let schema_attributes = attribute_definitions
            .into_iter()
            .map(|attr| {
                (
                    AttributeName(attr.name),
                    Self::translate_attribute_type(attr.attribute_type),
                )
            })
            .collect();

        Ok(Plan::CreateTable(CreateTablePlan {
            table_name,
            primary_key,
            schema_attributes,
        }))
    }

    fn translate_insert(&mut self, stmt: InsertStmt) -> Result<Plan> {
        let InsertStmt {
            table_name,
            attribute_names,
            attribute_values,
        } = stmt;

        let table_name = TableName(table_name);
        let schema = self.get_table_schema(&table_name)?;

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

        Ok(Plan::InsertTuple(InsertTuplePlan {
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
                LiteralExpr::Identifier(s) => Err(TranslateError::InvalidArguments(format!(
                    "Identifiers cannot appear here: Found {:?}",
                    s
                ))),
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

    fn translate_select(&mut self, stmt: SelectStmt) -> Result<Plan> {
        let SelectStmt {
            properties,
            from_clause,
            where_clause,
        } = stmt;

        let child_plan = match from_clause {
            FromClause::Table(table_name) => {
                let table_name = TableName(table_name);
                let result_schema = self.get_table_schema(&table_name)?;
                QueryPlan {
                    result_schema,
                    plan: QueryPlanNode::Scan(ScanNode { table_name }),
                }
            }
            FromClause::Select(nested_select) => {
                let nested_table_plan = self.translate_select(*nested_select)?;
                match nested_table_plan {
                    Plan::Query(plan @ QueryPlan { .. }) => plan,
                    _ => unreachable!(), // TODO: Use traits for Plan instead to encode these invariants?
                }
            }
        };

        let plan = match where_clause {
            WhereClause::Expr(predicate) => {
                let ctx = child_plan.result_schema.as_lookup_table();
                type_check_expr(&predicate, &ctx)?;
                QueryPlan {
                    result_schema: child_plan.result_schema.clone(),
                    plan: QueryPlanNode::Filter(FilterNode {
                        schema: child_plan.result_schema.clone(),
                        predicate,
                        child: Box::new(child_plan),
                    }),
                }
            }
            WhereClause::None => child_plan,
        };

        let plan = match properties {
            SelectProperties::Identifiers(attr_names) => {
                let schema = plan.result_schema.as_lookup_table();
                match attr_names.iter().find(|attr| !schema.contains_key(attr)) {
                    Some(attr_name) => {
                        return Err(TranslateError::NoSuchAttribute(attr_name.clone()))
                    }
                    None => (),
                }
                let schema_attributes = attr_names
                    .iter()
                    .map(|attr_name| {
                        let attr_value = schema
                            .get(attr_name)
                            .expect("we've verified that the schema contains this attribute");
                        (AttributeName(attr_name.clone()), (*attr_value).clone())
                    })
                    .collect();
                let result_schema = Schema::new(
                    plan.result_schema.store_id.clone(),
                    plan.result_schema.primary_key.clone(),
                    schema_attributes,
                );
                QueryPlan {
                    result_schema: result_schema.clone(),
                    plan: QueryPlanNode::Project(ProjectNode {
                        record_schema: result_schema.clone(),
                        attributes: attr_names
                            .into_iter()
                            .map(|attr| AttributeName(attr))
                            .collect(),
                        child: Box::new(plan),
                    }),
                }
            }
            SelectProperties::Star => plan,
        };

        Ok(Plan::Query(plan))
    }

    fn get_table_schema(&self, table_name: &TableName) -> Result<Schema> {
        match self.storage_manager.get_schema(table_name) {
            Some(schema) => Ok(schema),
            None => Err(TranslateError::NoSuchTable(table_name.0.clone())),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Result;
    use crate::parser::ast::Expr::{self, Literal};
    use crate::parser::ast::{
        AttributeDefinition, AttributeType as ParserAttributeType, AttributeValue, BinaryExpr,
        BinaryOperation, CreateTableStmt, FromClause, InsertStmt, LiteralExpr, SelectProperties,
        SelectStmt, WhereClause,
    };
    use crate::planner::plan::create_plan::CreateTablePlan;
    use crate::planner::plan::insert_plan::InsertTuplePlan;
    use crate::planner::plan::query_plan::{
        FilterNode, ProjectNode, QueryPlan, QueryPlanNode, ScanNode,
    };
    use crate::planner::plan::Plan::{self, CreateTable};
    use crate::storage::storage_manager::{
        AttributeName, CreateTableRequest, Schema, StorageManager, TableName,
    };
    use crate::storage::tuple::{StoreId, TupleRecord};
    use crate::storage::types::{AttributeType as StorageAttributeType, AttributeType};
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
        assert_eq!(
            req,
            CreateTable(CreateTablePlan {
                table_name: TableName("person".to_owned()),
                primary_key: AttributeName("name".to_owned()),
                schema_attributes: vec![
                    (AttributeName("name".to_owned()), AttributeType::Text),
                    (AttributeName("age".to_owned()), AttributeType::Integer),
                ]
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

        let mut storage_manager = StorageManager::new(StoreId(0));
        storage_manager.create_table(CreateTableRequest {
            table_name: TableName("person".to_owned()),
            primary_key: AttributeName("name".to_owned()),
            schema_attributes: vec![
                (AttributeName("name".to_owned()), AttributeType::Text),
                (AttributeName("age".to_owned()), AttributeType::Integer),
            ],
        })?;
        let mut t = Translator { storage_manager };

        let plan = t.translate_insert(stmt)?;
        assert_eq!(
            plan,
            Plan::InsertTuple(InsertTuplePlan {
                table_name: TableName("person".to_owned()),
                tuple: TupleRecord(vec![0, 0, 0, 3, 98, 111, 98, 0, 0, 0, 20])
            })
        );

        Ok(())
    }

    #[test]
    fn translate_select_star() -> Result<()> {
        let predicate = Expr::Binary(BinaryExpr {
            left: Box::new(Expr::Literal(LiteralExpr::Identifier("age".to_owned()))),
            op: BinaryOperation::NotEqual,
            right: Box::new(Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Literal(LiteralExpr::Integer(8))),
                op: BinaryOperation::Addition,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(2))),
            })),
        });
        let stmt = SelectStmt {
            properties: SelectProperties::Star,
            from_clause: FromClause::Table("person".to_owned()),
            where_clause: WhereClause::Expr(predicate.clone()),
        };

        let schema_attributes = vec![
            (AttributeName("name".to_owned()), AttributeType::Text),
            (AttributeName("age".to_owned()), AttributeType::Integer),
        ];

        let mut storage_manager = StorageManager::new(StoreId(0));
        storage_manager.create_table(CreateTableRequest {
            table_name: TableName("person".to_owned()),
            primary_key: AttributeName("name".to_owned()),
            schema_attributes: schema_attributes.clone(),
        })?;

        let mut t = Translator { storage_manager };

        let plan = t.translate_select(stmt)?;

        let schema = Schema::new(
            StoreId(0),
            AttributeName("name".to_owned()),
            schema_attributes.clone(),
        );
        assert_eq!(
            plan,
            Plan::Query(QueryPlan {
                result_schema: schema.clone(),
                plan: QueryPlanNode::Filter(FilterNode {
                    predicate: predicate.clone(),
                    schema: schema.clone(),
                    child: Box::new(QueryPlan {
                        result_schema: schema.clone(),
                        plan: QueryPlanNode::Scan(ScanNode {
                            table_name: TableName("person".to_owned())
                        })
                    })
                })
            })
        );

        Ok(())
    }

    #[test]
    fn translate_projection() -> Result<()> {
        let predicate = Expr::Binary(BinaryExpr {
            left: Box::new(Expr::Literal(LiteralExpr::Identifier("age".to_owned()))),
            op: BinaryOperation::NotEqual,
            right: Box::new(Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Literal(LiteralExpr::Integer(8))),
                op: BinaryOperation::Addition,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(2))),
            })),
        });
        let stmt = SelectStmt {
            properties: SelectProperties::Identifiers(vec![
                "is_member".to_owned(),
                "age".to_owned(),
            ]),
            from_clause: FromClause::Table("person".to_owned()),
            where_clause: WhereClause::Expr(predicate.clone()),
        };

        let schema_attributes = vec![
            (AttributeName("name".to_owned()), AttributeType::Text),
            (AttributeName("age".to_owned()), AttributeType::Integer),
            (AttributeName("location".to_owned()), AttributeType::Text),
            (
                AttributeName("is_member".to_owned()),
                AttributeType::Boolean,
            ),
        ];

        let mut storage_manager = StorageManager::new(StoreId(0));
        storage_manager.create_table(CreateTableRequest {
            table_name: TableName("person".to_owned()),
            primary_key: AttributeName("name".to_owned()),
            schema_attributes: schema_attributes.clone(),
        })?;

        let mut t = Translator { storage_manager };

        let plan = t.translate_select(stmt)?;

        let schema = Schema::new(
            StoreId(0),
            AttributeName("name".to_owned()),
            schema_attributes.clone(),
        );
        let result_schema = Schema::new(
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
        assert_eq!(
            plan,
            Plan::Query(QueryPlan {
                result_schema: result_schema.clone(),
                plan: QueryPlanNode::Project(ProjectNode {
                    record_schema: result_schema.clone(),
                    attributes: vec![
                        AttributeName("is_member".to_owned()),
                        AttributeName("age".to_owned())
                    ],
                    child: Box::new(QueryPlan {
                        result_schema: schema.clone(),
                        plan: QueryPlanNode::Filter(FilterNode {
                            predicate: predicate.clone(),
                            schema: schema.clone(),
                            child: Box::new(QueryPlan {
                                result_schema: schema.clone(),
                                plan: QueryPlanNode::Scan(ScanNode {
                                    table_name: TableName("person".to_owned())
                                })
                            })
                        })
                    })
                })
            })
        );

        Ok(())
    }
}

mod error;

use crate::parser::ast::{
    AttributeDefinition, AttributeType as ParserAttributeType, CreateTableStmt, Stmt,
};
use crate::planner::plan::execution::CreateTablePlan;
use crate::planner::plan::Plan;
use crate::storage::error::StorageError;
use crate::storage::storage_manager::{
    AttributeName, CreateTableRequest, StorageManager, TableName,
};
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
            Stmt::CreateTable(_) => (),
            Stmt::Insert(_) => (),
            Stmt::Select(_) => (),
        }
        unimplemented!()
    }

    fn translate_create_table(&mut self, stmt: CreateTableStmt) -> Result<Plan> {
        let CreateTableStmt {
            table_name,
            attribute_definitions,
        } = stmt;

        let table_name = TableName(table_name);

        if let Some(_) = self.storage_manager.get_table_metadata(&table_name) {
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
                    entry.insert(self.translate_attribute_type(col.attribute_type));
                }
                Entry::Occupied(entry) => {
                    return Err(TranslateError::DuplicateAttributeName(
                        entry.key().0.clone(),
                    ));
                }
            }
        }

        Ok(Plan::CreateTable(CreateTablePlan {
            req: CreateTableRequest {
                table_name,
                attributes,
                primary_key,
            },
        }))
    }

    fn translate_attribute_type(
        &mut self,
        attribute_type: ParserAttributeType,
    ) -> StorageAttributeType {
        match attribute_type {
            ParserAttributeType::Integer => StorageAttributeType::Integer,
            ParserAttributeType::Text => StorageAttributeType::Text,
        }
    }
}

#[cfg(test)]
mod test {
    use super::Result;
    use crate::parser::ast::{
        AttributeDefinition, AttributeType as ParserAttributeType, CreateTableStmt,
    };
    use crate::planner::plan::execution::CreateTablePlan;
    use crate::planner::plan::Plan::CreateTable;
    use crate::storage::storage_manager::{
        AttributeName, CreateTableRequest, StorageManager, TableName,
    };
    use crate::storage::tuple::StoreId;
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
            CreateTable(CreateTablePlan {
                req: CreateTableRequest {
                    table_name: TableName("person".to_owned()),
                    primary_key: AttributeName("name".to_owned()),
                    attributes,
                }
            })
        );

        Ok(())
    }
}

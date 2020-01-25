use std::error::Error;
use std::fmt;

use crate::parser::ast::*;
use crate::parser::expr_parser;
use crate::parser::expr_parser::Parser as ExprParser;
use crate::parser::lexer::token::Token;
use crate::parser::parse::{Input, ParseError, ParseHelper, Result};

struct Parser {}

impl Parser {
    pub fn new() -> Self {
        Parser {}
    }

    pub fn create_table_stmt(&mut self, mut input: Input) -> Result<CreateTableStmt> {
        let _ = ParseHelper::match_token(Token::Create, input.next())?;
        let _ = ParseHelper::match_token(Token::Table, input.next())?;
        let table_name = ParseHelper::match_identifier(input.next())?;
        let _ = ParseHelper::match_token(Token::LeftParen, input.next())?;
        let attribute_definitions = self.attribute_definitions(&mut input)?;
        let _ = ParseHelper::match_token(Token::RightParen, input.next())?;

        Ok(CreateTableStmt {
            table_name,
            attribute_definitions,
        })
    }

    pub fn attribute_definitions(&mut self, input: &mut Input) -> Result<Vec<AttributeDefinition>> {
        let mut definitions = Vec::new();

        loop {
            let name = ParseHelper::match_identifier(input.next())?;
            let attribute_type = match input.next() {
                Some(Token::KeywordInteger) => AttributeType::Integer,
                Some(Token::KeywordVarchar) => AttributeType::Text,
                Some(got) => {
                    return Err(ParseError::token_mismatch(
                        Token::KeywordVarchar,
                        got.clone(),
                    ))
                }
                None => return Err(ParseError::unexpected_eof(Token::KeywordVarchar)),
            };
            let is_primary_key = match input.peek() {
                Some(&Token::KeywordPrimaryKey) => {
                    let _primary_key = input.next();
                    true
                }
                _ => false,
            };

            definitions.push(AttributeDefinition {
                name,
                attribute_type,
                is_primary_key,
            });

            match input.peek() {
                Some(&Token::Comma) => {
                    let _comma = input.next();
                }
                _ => return Ok(definitions),
            }
        }
    }

    pub fn insert_stmt(&mut self, mut input: Input) -> Result<InsertStmt> {
        let _ = ParseHelper::match_token(Token::Insert, input.next())?;
        let _ = ParseHelper::match_token(Token::KeywordInto, input.next())?;
        let table_name = ParseHelper::match_identifier(input.next())?;
        let _ = ParseHelper::match_token(Token::LeftParen, input.next())?;
        let attribute_names = self.identifiers(&mut input)?;
        let _ = ParseHelper::match_token(Token::RightParen, input.next())?;
        let _ = ParseHelper::match_token(Token::KeywordValues, input.next())?;
        let _ = ParseHelper::match_token(Token::LeftParen, input.next())?;
        let attribute_values = self.attribute_values(&mut input)?;
        let _ = ParseHelper::match_token(Token::RightParen, input.next())?;

        Ok(InsertStmt {
            table_name,
            attribute_names,
            attribute_values,
        })
    }

    pub fn identifiers(&mut self, input: &mut Input) -> Result<Vec<String>> {
        let mut identifiers = Vec::new();

        loop {
            let id = ParseHelper::match_identifier(input.next())?;
            identifiers.push(id);
            match input.peek() {
                Some(&Token::Comma) => {
                    let _comma = input.next();
                }
                _ => return Ok(identifiers),
            }
        }
    }

    pub fn attribute_values(&mut self, input: &mut Input) -> Result<Vec<AttributeValue>> {
        let mut values = Vec::new();

        loop {
            let v = match input.peek() {
                Some(&Token::StringLiteral(str)) => {
                    let _string = input.next();
                    AttributeValue::String(str.to_owned())
                }
                _ => AttributeValue::Expr(ExprParser::expr(input)?),
            };
            values.push(v);

            match input.peek() {
                Some(&Token::Comma) => {
                    let _comma = input.next();
                }
                _ => return Ok(values),
            }
        }
    }

    pub fn select_stmt(&mut self, mut input: Input) -> Result<SelectStmt> {
        let _ = ParseHelper::match_token(Token::Select, input.next())?;
        let properties = self.select_properties(&mut input)?;
        let from_clause = self.parse_from_clause(&mut input)?;
        let where_clause = self.where_clause(&mut input)?;

        Ok(SelectStmt {
            properties,
            from_clause,
            where_clause,
        })
    }

    fn select_properties(&self, input: &mut Input) -> Result<SelectProperties> {
        match input.next() {
            Some(Token::Star) => Ok(SelectProperties::Star),
            Some(Token::Identifier(id)) => {
                let mut ids = vec![id.clone()];
                while let Some(&Token::Comma) = input.peek() {
                    let _comma = input.next();
                    match input.next() {
                        Some(Token::Identifier(id)) => ids.push(id.clone()),
                        Some(unexpected) => {
                            return Err(ParseError::token_mismatch(
                                Token::Identifier("<attribute_name>".to_owned()),
                                unexpected.clone(),
                            ))
                        }
                        None => {
                            return Err(ParseError::unexpected_eof(Token::Identifier(
                                "<attribute_name>".to_owned(),
                            )))
                        }
                    }
                }
                Ok(SelectProperties::Identifiers(ids))
            }
            Some(got) => Err(ParseError::token_mismatch(Token::Star, got.clone())),
            None => Err(ParseError::unexpected_eof(Token::Star)),
        }
    }

    fn parse_from_clause(&self, input: &mut Input) -> Result<FromClause> {
        let _ = ParseHelper::match_token(Token::From, input.next())?;

        match input.next() {
            Some(Token::Identifier(id)) => Ok(FromClause::Table(id.to_owned())),
            Some(unexpected) => Err(ParseError::token_mismatch(
                Token::Identifier("<table>".to_owned()),
                unexpected.clone(),
            )),
            None => Err(ParseError::unexpected_eof(Token::Identifier(
                "<table>".to_owned(),
            ))),
        }
    }

    fn where_clause(&self, mut input: &mut Input) -> Result<WhereClause> {
        match input.peek() {
            Some(&Token::Where) => {
                let _where = input.next();
                Ok(WhereClause::Expr(ExprParser::expr(input)?))
            }
            _ => Ok(WhereClause::None),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_table() -> Result<()> {
        let mut p = Parser::new();
        let mut input = [
            Token::Create,
            Token::Table,
            Token::Identifier("person".to_owned()),
            Token::LeftParen,
            Token::Identifier("name".to_owned()),
            Token::KeywordVarchar,
            Token::KeywordPrimaryKey,
            Token::Comma,
            Token::Identifier("age".to_owned()),
            Token::KeywordInteger,
            Token::RightParen,
            Token::EOF,
        ];
        let mut input = input.iter().peekable();

        let create = p.create_table_stmt(&mut input)?;
        assert_eq!(
            create,
            CreateTableStmt {
                table_name: "person".to_owned(),
                attribute_definitions: vec![
                    AttributeDefinition {
                        name: "name".to_owned(),
                        attribute_type: AttributeType::Text,
                        is_primary_key: true,
                    },
                    AttributeDefinition {
                        name: "age".to_owned(),
                        attribute_type: AttributeType::Integer,
                        is_primary_key: false,
                    }
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn insert() -> Result<()> {
        let mut p = Parser::new();
        let mut input = [
            Token::Insert,
            Token::KeywordInto,
            Token::Identifier("person".to_owned()),
            Token::LeftParen,
            Token::Identifier("name".to_owned()),
            Token::Comma,
            Token::Identifier("age".to_owned()),
            Token::RightParen,
            Token::KeywordValues,
            Token::LeftParen,
            Token::StringLiteral("bob".to_owned()),
            Token::Comma,
            Token::Integer(10),
            Token::Plus,
            Token::Integer(20),
            Token::RightParen,
            Token::EOF,
        ];
        let mut input = input.iter().peekable();

        let insert = p.insert_stmt(&mut input)?;
        assert_eq!(
            insert,
            InsertStmt {
                table_name: "person".to_owned(),
                attribute_names: vec!["name".to_owned(), "age".to_owned()],
                attribute_values: vec![
                    AttributeValue::String("bob".to_owned()),
                    AttributeValue::Expr(Expr::Binary(BinaryExpr {
                        left: Box::new(Expr::Literal(LiteralExpr::Integer(10))),
                        op: BinaryOperation::Addition,
                        right: Box::new(Expr::Literal(LiteralExpr::Integer(20))),
                    }))
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn parse_select_star_from() -> Result<()> {
        let mut p = Parser::new();
        let mut input = [
            Token::Select,
            Token::Star,
            Token::From,
            Token::Identifier("person".to_string()),
            Token::EOF,
        ];
        let mut input = input.iter().peekable();

        let select = p.select_stmt(&mut input)?;
        assert_eq!(
            select,
            SelectStmt {
                properties: SelectProperties::Star,
                from_clause: FromClause::Table("person".to_string()),
                where_clause: WhereClause::None,
            }
        );

        Ok(())
    }

    #[test]
    fn parse_select_attributes_from() -> Result<()> {
        let mut p = Parser::new();
        let mut input = [
            Token::Select,
            Token::Identifier("name".to_string()),
            Token::Comma,
            Token::Identifier("age".to_string()),
            Token::From,
            Token::Identifier("person".to_string()),
            Token::EOF,
        ];
        let mut input = input.iter().peekable();

        let select = p.select_stmt(&mut input)?;
        assert_eq!(
            select,
            SelectStmt {
                properties: SelectProperties::Identifiers(vec![
                    "name".to_owned(),
                    "age".to_owned()
                ]),
                from_clause: FromClause::Table("person".to_string()),
                where_clause: WhereClause::None,
            }
        );

        Ok(())
    }

    #[test]
    fn parse_select_attributes_from_where() -> Result<()> {
        let mut p = Parser::new();
        let mut input = [
            Token::Select,
            Token::Identifier("name".to_string()),
            Token::Comma,
            Token::Identifier("age".to_string()),
            Token::From,
            Token::Identifier("person".to_string()),
            Token::Where,
            Token::True,
            Token::EOF,
        ];
        let mut input = input.iter().peekable();

        let select = p.select_stmt(&mut input)?;
        assert_eq!(
            select,
            SelectStmt {
                properties: SelectProperties::Identifiers(vec![
                    "name".to_owned(),
                    "age".to_owned()
                ]),
                from_clause: FromClause::Table("person".to_string()),
                where_clause: WhereClause::Expr(Expr::Literal(LiteralExpr::Boolean(true))),
            }
        );

        Ok(())
    }
}

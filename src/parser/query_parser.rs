use std::error::Error;
use std::fmt;

use crate::parser::ast::*;
use crate::parser::expr_parser;
use crate::parser::expr_parser::Parser as ExprParser;
use crate::parser::lexer::token::Token;
use crate::parser::lexer::token::Token::Where;
use crate::parser::parse::{Input, ParseError, ParseHelper, Result, TokenStream};

pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Parser {}
    }

    pub fn parse(&mut self, input: Input) -> std::result::Result<Stmt, ParseError> {
        match input.peek() {
            Some(&Token::Create) => Ok(Stmt::CreateTable(self.create_table_stmt(input)?.0)),
            Some(&Token::Insert) => Ok(Stmt::Insert(self.insert_stmt(input)?.0)),
            Some(&Token::Select) => Ok(Stmt::Select(self.select_stmt(input, true)?.0)),
            Some(token) => Err(ParseError {
                details: format!("invalid start of query {:?}", token),
            }),
            None => Err(ParseError {
                details: "empty query".to_owned(),
            }),
        }
    }

    pub fn create_table_stmt(&mut self, mut input: Input) -> Result<CreateTableStmt> {
        let _ = ParseHelper::match_token(Token::Create, input.next())?;
        let _ = ParseHelper::match_token(Token::Table, input.next())?;
        let table_name = ParseHelper::match_identifier(input.next())?;
        let _ = ParseHelper::match_token(Token::LeftParen, input.next())?;
        let (attribute_definitions, mut input) = self.attribute_definitions(input)?;
        let _ = ParseHelper::match_token(Token::RightParen, input.next())?;
        let _ = ParseHelper::match_token(Token::Semicolon, input.next())?;

        Ok((
            CreateTableStmt {
                table_name,
                attribute_definitions,
            },
            input,
        ))
    }

    pub fn attribute_definitions(&mut self, mut input: Input) -> Result<Vec<AttributeDefinition>> {
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
                _ => return Ok((definitions, input)),
            }
        }
    }

    pub fn insert_stmt(&mut self, mut input: Input) -> Result<InsertStmt> {
        let _ = ParseHelper::match_token(Token::Insert, input.next())?;
        let _ = ParseHelper::match_token(Token::KeywordInto, input.next())?;
        let table_name = ParseHelper::match_identifier(input.next())?;
        let _ = ParseHelper::match_token(Token::LeftParen, input.next())?;
        let (attribute_names, mut input) = self.identifiers(input)?;
        let _ = ParseHelper::match_token(Token::RightParen, input.next())?;
        let _ = ParseHelper::match_token(Token::KeywordValues, input.next())?;
        let _ = ParseHelper::match_token(Token::LeftParen, input.next())?;
        let (attribute_values, mut input) = self.attribute_values(input)?;
        let _ = ParseHelper::match_token(Token::RightParen, input.next())?;
        let _ = ParseHelper::match_token(Token::Semicolon, input.next())?;

        Ok((
            InsertStmt {
                table_name,
                attribute_names,
                attribute_values,
            },
            input,
        ))
    }

    pub fn identifiers(&mut self, mut input: Input) -> Result<Vec<String>> {
        let mut identifiers = Vec::new();

        loop {
            let id = ParseHelper::match_identifier(input.next())?;
            identifiers.push(id);
            match input.peek() {
                Some(&Token::Comma) => {
                    let _comma = input.next();
                }
                _ => return Ok((identifiers, input)),
            }
        }
    }

    pub fn attribute_values(&mut self, mut input: Input) -> Result<Vec<AttributeValue>> {
        let mut values = Vec::new();

        loop {
            let v = match input.peek() {
                Some(Token::StringLiteral(str)) => {
                    let result = AttributeValue::String(str.to_owned());
                    let _string = input.next();
                    result
                }
                _ => AttributeValue::Expr(ExprParser::expr(&mut input)?),
            };
            values.push(v);

            match input.peek() {
                Some(&Token::Comma) => {
                    let _comma = input.next();
                }
                _ => return Ok((values, input)),
            }
        }
    }

    pub fn select_stmt(&mut self, mut input: Input, is_stmt: bool) -> Result<SelectStmt> {
        let _ = ParseHelper::match_token(Token::Select, input.next())?;
        let (properties, mut input) = self.select_properties(input)?;

        let _ = ParseHelper::match_token(Token::From, input.next())?;
        let ((from_clause, alias), mut input) = self.parse_from_clause(input)?;

        fn from_clause_to_join_query(
            from_clause: FromClause,
            alias: Option<String>,
        ) -> SingleSelectStmt {
            SingleSelectStmt {
                properties: SelectProperties::Star,
                from_clause,
                where_clause: WhereClause::None,
                alias,
            }
        }

        let (rh_join, where_clause, mut input) = match input.peek() {
            Some(Token::KeywordInnerJoin) => {
                let _ = ParseHelper::match_token(Token::KeywordInnerJoin, input.next())?;
                let ((from_clause, alias), input) = self.parse_from_clause(input)?;

                // Wrap right hand side of join inside a select statement.
                (
                    Some(from_clause_to_join_query(from_clause, alias)),
                    None,
                    input,
                )
            }
            _ => {
                let (where_clause, input) = self.where_clause(input)?;
                (None, Some(where_clause), input)
            }
        };

        let (stmt, mut input) = match rh_join {
            Some(rh_join) => {
                let (where_clause, input) = self.join_predicate(input)?;
                (
                    SelectStmt::Join(JoinStmt {
                        join_type: JoinType::InnerJoin,
                        properties,
                        // Wrap left hand side of join inside a select statement.
                        left: from_clause_to_join_query(from_clause, alias),
                        right: rh_join,
                        predicate: where_clause,
                    }),
                    input,
                )
            }
            None => (
                SelectStmt::Select(SingleSelectStmt {
                    properties,
                    from_clause,
                    where_clause: where_clause.expect("we either have a join or where clause set."),
                    alias,
                }),
                input,
            ),
        };

        if is_stmt {
            let _ = ParseHelper::match_token(Token::Semicolon, input.next())?;
        }

        Ok((stmt, input))
    }

    fn select_properties(&self, mut input: Input) -> Result<SelectProperties> {
        match input.next() {
            Some(Token::Star) => Ok((SelectProperties::Star, input)),
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
                Ok((SelectProperties::Identifiers(ids), input))
            }
            Some(got) => Err(ParseError::token_mismatch(Token::Star, got.clone())),
            None => Err(ParseError::unexpected_eof(Token::Star)),
        }
    }

    fn parse_from_clause(&mut self, mut input: Input) -> Result<(FromClause, Option<String>)> {
        let has_parenthesis = input.peek() == Some(&&Token::LeftParen);
        if has_parenthesis {
            let _left_paren = input.next();
        }

        let res = match input.peek() {
            Some(Token::Identifier(_)) => Ok((
                FromClause::Table(ParseHelper::match_identifier(input.next())?),
                input,
            )),
            Some(Token::Select) => {
                let (select_stmt, mut input) = self.select_stmt(input, false)?;
                Ok((FromClause::Select(Box::new(select_stmt)), input))
            }
            Some(unexpected) => Err(ParseError::token_mismatch(
                Token::Identifier("<table>".to_owned()),
                unexpected.clone(),
            )),
            None => Err(ParseError::unexpected_eof(Token::Identifier(
                "<table>".to_owned(),
            ))),
        };

        res.and_then(|(from_clause, mut input)| {
            if has_parenthesis {
                let _ = ParseHelper::match_token(Token::RightParen, input.next())?;
            }
            let (alias, input) = self.match_alias(input)?;
            Ok(((from_clause, alias), input))
        })
    }

    fn where_clause(&self, mut input: Input) -> Result<WhereClause> {
        self.where_clause_with_prefix(Token::Where, input)
    }

    fn join_predicate(&self, mut input: Input) -> Result<WhereClause> {
        self.where_clause_with_prefix(Token::KeywordOn, input)
    }

    fn where_clause_with_prefix(&self, prefix: Token, mut input: Input) -> Result<WhereClause> {
        match input.peek() {
            Some(token) if token == &prefix => {
                let _prefix = input.next();
                Ok((WhereClause::Expr(ExprParser::expr(&mut input)?), input))
            }
            _ => Ok((WhereClause::None, input)),
        }
    }

    fn match_alias(&self, mut input: Input) -> Result<Option<String>> {
        match input.peek() {
            Some(&Token::KeywordAs) => {
                let _as = input.next();
                let alias = ParseHelper::match_identifier(input.next())?;
                Ok((Some(alias), input))
            }
            _ => Ok((None, input)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    type Result<T> = std::result::Result<T, ParseError>;

    #[test]
    fn create_table() -> Result<()> {
        let mut p = Parser::new();
        let mut input = Input::new(vec![
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
            Token::Semicolon,
            Token::EOF,
        ]);

        let (create, _) = p.create_table_stmt(input)?;
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
        let mut input = Input::new(vec![
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
            Token::Semicolon,
            Token::EOF,
        ]);

        let (insert, _) = p.insert_stmt(input)?;
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
        let mut input = Input::new(vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::Identifier("person".to_string()),
            Token::Semicolon,
            Token::EOF,
        ]);

        let (select, _) = p.select_stmt(input, true)?;
        assert_eq!(
            select,
            SelectStmt::Select(SingleSelectStmt {
                properties: SelectProperties::Star,
                from_clause: FromClause::Table("person".to_string()),
                where_clause: WhereClause::None,
                alias: None,
            })
        );

        Ok(())
    }

    #[test]
    fn parse_select_attributes_from() -> Result<()> {
        fn run_test(with_parenthesis: bool) -> Result<()> {
            let mut p = Parser::new();
            let mut input = Input::new(
                [
                    vec![
                        Token::Select,
                        Token::Identifier("name".to_string()),
                        Token::Comma,
                        Token::Identifier("age".to_string()),
                        Token::From,
                        Token::Identifier("person".to_string()),
                        Token::Semicolon,
                        Token::EOF,
                    ],
                    if with_parenthesis {
                        vec![Token::LeftParen]
                    } else {
                        vec![]
                    },
                    vec![Token::Identifier("person".to_string())],
                    if with_parenthesis {
                        vec![Token::RightParen]
                    } else {
                        vec![]
                    },
                    vec![Token::Semicolon, Token::EOF],
                ]
                .concat(),
            );

            let (select, _) = p.select_stmt(input, true)?;
            assert_eq!(
                select,
                SelectStmt::Select(SingleSelectStmt {
                    properties: SelectProperties::Identifiers(vec![
                        "name".to_owned(),
                        "age".to_owned()
                    ]),
                    from_clause: FromClause::Table("person".to_string()),
                    where_clause: WhereClause::None,
                    alias: None,
                })
            );

            Ok(())
        }

        run_test(true)?;
        run_test(false)
    }

    #[test]
    fn parse_select_attributes_from_where() -> Result<()> {
        let mut p = Parser::new();
        let mut input = Input::new(vec![
            Token::Select,
            Token::Identifier("name".to_string()),
            Token::Comma,
            Token::Identifier("age".to_string()),
            Token::From,
            Token::Identifier("person".to_string()),
            Token::Where,
            Token::True,
            Token::Semicolon,
            Token::EOF,
        ]);

        let (select, _) = p.select_stmt(input, true)?;
        assert_eq!(
            select,
            SelectStmt::Select(SingleSelectStmt {
                properties: SelectProperties::Identifiers(vec![
                    "name".to_owned(),
                    "age".to_owned()
                ]),
                from_clause: FromClause::Table("person".to_string()),
                where_clause: WhereClause::Expr(Expr::Literal(LiteralExpr::Boolean(true))),
                alias: None,
            })
        );

        Ok(())
    }

    #[test]
    fn parse_select_star_from_as() -> Result<()> {
        let mut p = Parser::new();
        let mut input = Input::new(vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::Identifier("person".to_string()),
            Token::KeywordAs,
            Token::Identifier("employee".to_string()),
            Token::Semicolon,
            Token::EOF,
        ]);

        let (select, _) = p.select_stmt(input, true)?;
        assert_eq!(
            select,
            SelectStmt::Select(SingleSelectStmt {
                properties: SelectProperties::Star,
                from_clause: FromClause::Table("person".to_string()),
                alias: Some("employee".to_string()),
                where_clause: WhereClause::None,
            })
        );

        Ok(())
    }

    #[test]
    fn parse_inner_join() -> Result<()> {
        let mut p = Parser::new();
        // select person.age, employee.id from foo as person
        //  inner join (select * from bar where false) as employee on true;
        let mut input = Input::new(vec![
            Token::Select,
            Token::Identifier("person.age".to_string()),
            Token::Comma,
            Token::Identifier("employee.id".to_string()),
            Token::From,
            Token::Identifier("foo".to_string()),
            Token::KeywordAs,
            Token::Identifier("person".to_string()),
            Token::KeywordInnerJoin,
            Token::LeftParen,
            Token::Select,
            Token::Star,
            Token::From,
            Token::Identifier("bar".to_string()),
            Token::Where,
            Token::False,
            Token::RightParen,
            Token::KeywordAs,
            Token::Identifier("employee".to_string()),
            Token::KeywordOn,
            Token::True,
            Token::Semicolon,
            Token::EOF,
        ]);

        let (select, _) = p.select_stmt(input, true)?;
        assert_eq!(
            select,
            SelectStmt::Join(JoinStmt {
                join_type: JoinType::InnerJoin,
                properties: SelectProperties::Identifiers(vec![
                    "person.age".to_owned(),
                    "employee.id".to_owned()
                ]),
                left: SingleSelectStmt {
                    properties: SelectProperties::Star,
                    from_clause: FromClause::Table("foo".to_owned()),
                    where_clause: WhereClause::None,
                    alias: Some("person".to_owned())
                },
                right: SingleSelectStmt {
                    properties: SelectProperties::Star,
                    from_clause: FromClause::Select(Box::new(SelectStmt::Select(
                        SingleSelectStmt {
                            properties: SelectProperties::Star,
                            from_clause: FromClause::Table("bar".to_owned()),
                            where_clause: WhereClause::Expr(Expr::Literal(LiteralExpr::Boolean(
                                false
                            ))),
                            alias: None
                        }
                    ))),
                    where_clause: WhereClause::None,
                    alias: Some("employee".to_owned())
                },
                predicate: WhereClause::Expr(Expr::Literal(LiteralExpr::Boolean(true)))
            })
        );

        Ok(())
    }
}

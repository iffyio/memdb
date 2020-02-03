use crate::parser::lexer::token::Token;

pub enum Stmt {
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
}

// Create Table
#[derive(Debug, Eq, PartialEq)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub attribute_definitions: Vec<AttributeDefinition>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct AttributeDefinition {
    pub name: String,
    pub attribute_type: AttributeType,
    pub is_primary_key: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub enum AttributeType {
    Integer,
    Text,
}

// Insert
#[derive(Debug, Eq, PartialEq)]
pub struct InsertStmt {
    pub table_name: String,
    pub attribute_names: Vec<String>,
    pub attribute_values: Vec<AttributeValue>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum AttributeValue {
    String(String),
    Expr(Expr),
}

// Select
#[derive(Debug, Eq, PartialEq)]
pub struct SelectStmt {
    pub properties: SelectProperties,
    pub from_clause: FromClause,
    pub where_clause: WhereClause,
}

#[derive(Debug, Eq, PartialEq)]
pub enum SelectProperties {
    Star,
    Identifiers(Vec<String>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum FromClause {
    Select(Box<SelectStmt>),
    Table(String),
}

#[derive(Debug, Eq, PartialEq)]
pub enum WhereClause {
    None,
    Expr(Expr),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum BinaryOperation {
    Addition,
    Subtraction,
    Multiplication,
    Division,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

impl From<Token> for BinaryOperation {
    fn from(t: Token) -> Self {
        match t {
            Token::Plus => Self::Addition,
            Token::Minus => Self::Subtraction,
            Token::Star => Self::Multiplication,
            Token::Slash => Self::Division,
            Token::Equal => Self::Equal,
            Token::LessThan => Self::LessThan,
            Token::GreaterThan => Self::GreaterThan,
            Token::LessThanOrEqual => Self::LessThanOrEqual,
            Token::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            _ => unreachable!(format!("[{}] is not a binary operation!", t)),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: BinaryOperation,
    pub right: Box<Expr>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum LiteralExpr {
    Integer(i32),
    Boolean(bool),
    String(String),
    Identifier(String),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Expr {
    Binary(BinaryExpr),
    Literal(LiteralExpr),
}

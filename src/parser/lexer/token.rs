use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Create,
    Table,
    Insert,
    Select,
    From,
    Where,
    KeywordInteger,
    KeywordVarchar,
    KeywordPrimaryKey,
    KeywordInto,
    KeywordValues,

    Identifier(String),
    StringLiteral(String),

    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Star,
    Plus,
    Minus,
    Slash,

    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,

    Integer(i32),

    True,
    False,

    EOF,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::borrow::Cow::{Borrowed, Owned};
        let s = match self {
            Self::Create => Borrowed("CREATE"),
            Self::Table => Borrowed("TABLE"),
            Self::Insert => Borrowed("INSERT"),
            Self::Select => Borrowed("SELECT"),
            Self::From => Borrowed("FROM"),
            Self::Where => Borrowed("WHERE"),
            Self::KeywordInteger => Borrowed("INTEGER"),
            Self::KeywordVarchar => Borrowed("VARCHAR"),
            Self::KeywordPrimaryKey => Borrowed("PRIMARY KEY"),
            Self::KeywordInto => Borrowed("INTO"),
            Self::KeywordValues => Borrowed("VALUES"),
            Self::Identifier(id) => Owned(format!("Identifier({})", id)),
            Self::StringLiteral(_) => Borrowed("\"<string>\""),
            Self::LeftParen => Borrowed("("),
            Self::RightParen => Borrowed(")"),
            Self::Comma => Borrowed(","),
            Self::Semicolon => Borrowed(";"),
            Self::Star => Borrowed("*"),
            Self::Plus => Borrowed("+"),
            Self::Minus => Borrowed("-"),
            Self::Slash => Borrowed("/"),
            Self::Equal => Borrowed("="),
            Self::NotEqual => Borrowed("!="),
            Self::LessThan => Borrowed(">"),
            Self::GreaterThan => Borrowed(">"),
            Self::LessThanOrEqual => Borrowed(">="),
            Self::GreaterThanOrEqual => Borrowed(">="),
            Self::Integer(_) => Borrowed("<integer>"),
            Self::True => Borrowed("true"),
            Self::False => Borrowed("false"),
            Self::EOF => Borrowed("EOF"),
        };

        write!(f, "{}", s)
    }
}

use crate::parser::lexer::token::Token;
use std::error::Error;
use std::fmt;
use std::iter::Peekable;
use std::slice::Iter;

#[derive(Debug)]
pub struct ParseError {
    details: String,
}

impl ParseError {
    pub fn token_mismatch(want: Token, got: Token) -> Self {
        ParseError {
            details: format!("Unexpected token [{}] expected [{}]", got, want),
        }
    }

    pub fn unexpected_eof(want: Token) -> Self {
        ParseError {
            details: format!("Unexpected eof wanted token [{}]", want),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for ParseError {
    fn description(&self) -> &str {
        &self.details
    }
}

pub type Input<'a> = &'a mut Peekable<Iter<'a, Token>>;
pub type Result<T> = std::result::Result<T, ParseError>;

pub struct ParseHelper {}

impl ParseHelper {
    pub fn match_token(want: Token, got: Option<&Token>) -> Result<()> {
        match got {
            Some(got) if want == *got => Ok(()),
            Some(got) => Err(ParseError::token_mismatch(Token::Select, got.clone())),
            None => Err(ParseError::unexpected_eof(want.clone())),
        }
    }

    pub fn match_identifier(got: Option<&Token>) -> Result<String> {
        match got {
            Some(Token::Identifier(id)) => Ok(id.to_owned()),
            Some(got) => Err(ParseError::token_mismatch(Token::Select, got.clone())),
            None => Err(ParseError::unexpected_eof(Token::Identifier("".to_owned()))),
        }
    }
}

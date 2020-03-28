use crate::parser::lexer::token::Token;
use std::error::Error;
use std::fmt;
use std::iter::Peekable;
use std::slice::Iter;

#[derive(Debug)]
pub struct ParseError {
    pub details: String,
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

pub struct TokenStream {
    curr_index: usize,
    tokens: Vec<Option<Token>>,
}

pub type Input = TokenStream;

pub type Result<T> = std::result::Result<(T, Input), ParseError>;

impl TokenStream {
    pub fn new(tokens: Vec<Token>) -> Self {
        TokenStream {
            curr_index: 0,
            tokens: tokens.into_iter().map(|token| Some(token)).collect(),
        }
    }

    pub fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.curr_index).and_then(|t| t.as_ref())
    }

    pub fn next(&mut self) -> Option<Token> {
        if self.curr_index < self.tokens.len() {
            self.curr_index += 1;
            self.tokens[self.curr_index - 1].take()
        } else {
            None
        }
    }
}

pub struct ParseHelper {}

impl ParseHelper {
    pub fn match_token(want: Token, got: Option<Token>) -> std::result::Result<(), ParseError> {
        match got {
            Some(got) if want == got => Ok(()),
            Some(got) => Err(ParseError::token_mismatch(Token::Select, got.clone())),
            None => Err(ParseError::unexpected_eof(want.clone())),
        }
    }

    pub fn match_identifier(got: Option<Token>) -> std::result::Result<String, ParseError> {
        match got {
            Some(Token::Identifier(id)) => Ok(id),
            Some(got) => Err(ParseError::token_mismatch(Token::Select, got.clone())),
            None => Err(ParseError::unexpected_eof(Token::Identifier("".to_owned()))),
        }
    }
}

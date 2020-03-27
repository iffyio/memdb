use crate::parser::lexer::token::Token;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

pub(crate) mod token;

#[derive(Debug)]
pub struct LexerError {
    pub details: String,
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for LexerError {
    fn description(&self) -> &str {
        &self.details
    }
}

type Result<T> = std::result::Result<T, LexerError>;

pub(crate) struct Lexer {
    keywords: HashMap<&'static str, Token>,
    double_word_keywords: HashMap<(&'static str, &'static str), Token>,
}

impl Lexer {
    pub fn new() -> Self {
        let mut keywords = HashMap::new();
        {
            keywords.insert("create", Token::Create);
            keywords.insert("table", Token::Table);
            keywords.insert("insert", Token::Insert);
            keywords.insert("select", Token::Select);
            keywords.insert("from", Token::From);
            keywords.insert("where", Token::Where);
            keywords.insert("integer", Token::KeywordInteger);
            keywords.insert("varchar", Token::KeywordVarchar);
            keywords.insert("into", Token::KeywordInto);
            keywords.insert("values", Token::KeywordValues);
            keywords.insert("as", Token::KeywordAs);
            keywords.insert("on", Token::KeywordOn);
            keywords.insert("inner join", Token::KeywordOn);
            keywords.insert("true", Token::True);
            keywords.insert("false", Token::False);
        }
        let mut double_word_keywords = HashMap::new();
        {
            double_word_keywords.insert(("primary", "key"), Token::KeywordPrimaryKey);
            double_word_keywords.insert(("inner", "join"), Token::KeywordInnerJoin);
        }
        Lexer {
            keywords,
            double_word_keywords,
        }
    }

    pub fn scan(&self, input: &str) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        let mut cur_pos = 0;

        while cur_pos != input.len() {
            let whitespace_count = Lexer::scan_whitespace(&input[cur_pos..]);
            if whitespace_count > 0 {
                cur_pos += whitespace_count;
                continue;
            }

            let (token, new_pos) = self.scan_token(&input[cur_pos..])?;
            tokens.push(token);
            cur_pos += new_pos;
        }

        tokens.push(Token::EOF);
        return Ok(tokens);
    }

    fn scan_token(&self, input: &str) -> Result<(Token, usize)> {
        let c = input.chars().next().ok_or(LexerError {
            details: "unexpected EOF".to_string(),
        })?;

        let one_char_token = match c {
            '(' => Some(Token::LeftParen),
            ')' => Some(Token::RightParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '*' => Some(Token::Star),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            '=' => Some(Token::Equal),
            '<' if input[1..].chars().peekable().peek() != Some(&'=') => Some(Token::LessThan),
            '>' if input[1..].chars().peekable().peek() != Some(&'=') => Some(Token::GreaterThan),
            _ => None,
        };
        if one_char_token.is_some() {
            return Ok((one_char_token.unwrap(), 1));
        }

        let two_char_token = match c {
            '<' => {
                Lexer::must('=', input[1..].chars().next())?;
                Some(Token::LessThanOrEqual)
            }
            '>' => {
                Lexer::must('=', input[1..].chars().next())?;
                Some(Token::GreaterThanOrEqual)
            }
            '!' => {
                Lexer::must('=', input[1..].chars().next())?;
                Some(Token::NotEqual)
            }
            _ => None,
        };
        if two_char_token.is_some() {
            return Ok((two_char_token.unwrap(), 2));
        }

        if c.is_alphabetic() {
            let identifier =
                Lexer::scan_identifier(&input).expect("id already has at least length 1");

            let suffix = if let Some('.') = input[identifier.len()..].chars().peekable().peek() {
                match Lexer::scan_identifier(&input[identifier.len() + 1..]) {
                    Some(suffix) => format!(".{}", suffix),
                    None => {
                        return Err(LexerError {
                            details: format!("no suffix provided for identifier {:?}.", identifier),
                        })
                    }
                }
            } else {
                "".to_owned()
            };

            let identifier = format!("{}{}", identifier, suffix);

            let length = identifier.len();

            // Match the suffix of a 2-part keyword e.g the ' JOIN' of an 'INNER JOIN'
            fn match_whitespace_and_keyword(input: &str, keyword: &str) -> Option<usize> {
                let whitespace_count = Lexer::scan_whitespace(input);
                if whitespace_count > 0
                    && Lexer::scan_identifier(&input[whitespace_count..])
                        .map(|id| id.to_lowercase())
                        == Some(keyword.to_string())
                {
                    return Some(whitespace_count + keyword.len());
                }
                None
            }

            // Is this a 2-part keyword e.g 'INNER JOIN'
            for ((prefix, suffix), keyword) in &self.double_word_keywords {
                if &identifier.to_lowercase().as_str() != prefix {
                    continue;
                }
                match match_whitespace_and_keyword(&input[length..], suffix) {
                    Some(matched_length) => return Ok((keyword.clone(), length + matched_length)),
                    None => (),
                }
            }

            return match self.keywords.get(identifier.to_ascii_lowercase().as_str()) {
                Some(token) => Ok((token.clone(), length)),
                None => Ok((Token::Identifier(identifier), length)),
            };
        }

        if c.is_numeric() {
            let digits = input
                .chars()
                .take_while(|ch| ch.is_numeric())
                .collect::<String>();
            let length = digits.len();
            let integer = digits.parse().expect("string consists only of digits");
            return Ok((Token::Integer(integer), length));
        }

        if c == '\'' {
            let mut chars = input.chars();
            chars.next(); // Discard the leading "'"
            let text = chars.take_while(|ch| ch != &'\'').collect::<String>();
            let length = text.len() + 2;
            let _ = Self::must('\'', input.chars().nth(length - 1))?;
            return Ok((Token::StringLiteral(text), length));
        }

        return Err(LexerError {
            details: format!("invalid character {:?}", c),
        });
    }

    fn must(want: char, got: Option<char>) -> Result<()> {
        match got {
            Some(got) if got == want => Ok(()),
            Some(got) => Err(LexerError {
                details: format!("wanted {:?}, got {:?}", want, got),
            }),
            None => Err(LexerError {
                details: format!("wanted {:?}, got EOF", want),
            }),
        }
    }

    fn scan_whitespace(input: &str) -> usize {
        input.chars().take_while(|ch| ch.is_whitespace()).count()
    }

    fn scan_identifier(input: &str) -> Option<String> {
        let str = input
            .chars()
            .take_while(|ch| ch.is_alphabetic())
            .collect::<String>();

        if !str.is_empty() {
            Some(str)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn one_char_token() -> Result<()> {
        let l = Lexer::new();
        let tokens = l.scan("(),;*+-/=<>")?;
        assert_eq!(
            tokens,
            vec![
                Token::LeftParen,
                Token::RightParen,
                Token::Comma,
                Token::Semicolon,
                Token::Star,
                Token::Plus,
                Token::Minus,
                Token::Slash,
                Token::Equal,
                Token::LessThan,
                Token::GreaterThan,
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn two_char_token() -> Result<()> {
        let l = Lexer::new();
        let tokens = l.scan("<=>=!=")?;
        assert_eq!(
            tokens,
            vec![
                Token::LessThanOrEqual,
                Token::GreaterThanOrEqual,
                Token::NotEqual,
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn identifiers() -> Result<()> {
        let l = Lexer::new();
        let tokens = l.scan("cat bat a rat foo.bar qux")?;
        assert_eq!(
            tokens,
            vec![
                Token::Identifier("cat".to_owned()),
                Token::Identifier("bat".to_owned()),
                Token::Identifier("a".to_owned()),
                Token::Identifier("rat".to_owned()),
                Token::Identifier("foo.bar".to_owned()),
                Token::Identifier("qux".to_owned()),
                Token::EOF,
            ]
        );

        assert!(l.scan("cat bat foo. bar").is_err());
        Ok(())
    }

    #[test]
    fn keywords() -> Result<()> {
        let l = Lexer::new();
        let tokens = l.scan("create insert INSERT table CREATE select from where integer varchar primary KEy into values as inner join on true false")?;
        assert_eq!(
            tokens,
            vec![
                Token::Create,
                Token::Insert,
                Token::Insert,
                Token::Table,
                Token::Create,
                Token::Select,
                Token::From,
                Token::Where,
                Token::KeywordInteger,
                Token::KeywordVarchar,
                Token::KeywordPrimaryKey,
                Token::KeywordInto,
                Token::KeywordValues,
                Token::KeywordAs,
                Token::KeywordInnerJoin,
                Token::KeywordOn,
                Token::True,
                Token::False,
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn numbers() -> Result<()> {
        let l = Lexer::new();
        let tokens = l.scan("1 2 34 5")?;
        assert_eq!(
            tokens,
            vec![
                Token::Integer(1),
                Token::Integer(2),
                Token::Integer(34),
                Token::Integer(5),
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn strings() -> Result<()> {
        let l = Lexer::new();
        let tokens = l.scan("id 'id' 'ab'")?;
        assert_eq!(
            tokens,
            vec![
                Token::Identifier("id".to_owned()),
                Token::StringLiteral("id".to_owned()),
                Token::StringLiteral("ab".to_owned()),
                Token::EOF,
            ]
        );
        Ok(())
    }
}

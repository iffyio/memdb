use crate::parser::ast::*;
use crate::parser::lexer::token::Token;
use crate::parser::parse::ParseHelper;
use crate::parser::parse::{Input, ParseError, Result};

pub struct Parser {}

impl Parser {
    pub fn expr(input: &mut Input) -> Result<Expr> {
        // l0 -> l0 == != l1 | l1
        // l1 -> l1 < > <= >= l2 | l2
        // l2 -> l2 +- l3 | l3
        // l3 -> l3 */ l4 | l4
        // l4 -> (l0) | identifier | number | true | false | -l0 | !l0
        Parser::l0_expr(input)
    }

    pub fn l0_expr(input: &mut Input) -> Result<Expr> {
        let mut curr = Parser::l1_expr(input)?;

        while let Some(&Token::Equal) | Some(&Token::NotEqual) = input.peek() {
            let tok = input.next().unwrap();
            curr = Expr::Binary(BinaryExpr {
                left: Box::new(curr),
                op: BinaryOperation::from(tok.clone()),
                right: Box::new(Parser::l1_expr(input)?),
            });
        }
        Ok(curr)
    }

    pub fn l1_expr(input: &mut Input) -> Result<Expr> {
        let mut curr = Parser::l2_expr(input)?;

        while let Some(&Token::LessThan)
        | Some(&Token::GreaterThan)
        | Some(&Token::LessThanOrEqual)
        | Some(&Token::GreaterThanOrEqual) = input.peek()
        {
            let tok = input.next().unwrap();
            curr = Expr::Binary(BinaryExpr {
                left: Box::new(curr),
                op: BinaryOperation::from(tok.clone()),
                right: Box::new(Parser::l2_expr(input)?),
            });
        }
        Ok(curr)
    }

    pub fn l2_expr(input: &mut Input) -> Result<Expr> {
        let mut curr = Parser::l3_expr(input)?;

        while let Some(&Token::Plus) | Some(&Token::Minus) = input.peek() {
            let tok = input.next().unwrap();
            curr = Expr::Binary(BinaryExpr {
                left: Box::new(curr),
                op: BinaryOperation::from(tok.clone()),
                right: Box::new(Parser::l3_expr(input)?),
            });
        }
        Ok(curr)
    }

    pub fn l3_expr(input: &mut Input) -> Result<Expr> {
        let mut curr = Parser::l4_expr(input)?;

        while let Some(&Token::Star) | Some(&Token::Slash) = input.peek() {
            let tok = input.next().unwrap();
            curr = Expr::Binary(BinaryExpr {
                left: Box::new(curr),
                op: BinaryOperation::from(tok.clone()),
                right: Box::new(Parser::l4_expr(input)?),
            });
        }
        Ok(curr)
    }

    pub fn l4_expr(input: &mut Input) -> Result<Expr> {
        match input.next() {
            Some(Token::Identifier(id)) => Ok(Expr::Literal(LiteralExpr::String(id.to_owned()))),
            Some(Token::Integer(num)) => Ok(Expr::Literal(LiteralExpr::Integer(*num))),
            Some(Token::True) => Ok(Expr::Literal(LiteralExpr::Boolean(true))),
            Some(Token::False) => Ok(Expr::Literal(LiteralExpr::Boolean(false))),
            Some(Token::LeftParen) => {
                let expr = Parser::l0_expr(input)?;
                let _ = ParseHelper::match_token(Token::RightParen, input.next());
                Ok(expr)
            }
            Some(unexpected) => Err(ParseError::token_mismatch(
                Token::Identifier("<expression>".to_owned()),
                unexpected.clone(),
            )),
            None => Err(ParseError::unexpected_eof(Token::Identifier(
                "<expression>".to_string(),
            ))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::parser::ast::Expr;

    #[test]
    fn parse_simple_addition() -> Result<()> {
        let mut input = [Token::Integer(1), Token::Plus, Token::Integer(2)];
        let mut input = input.iter().peekable();

        let e = Parser::expr(&mut &mut input)?;
        assert_eq!(
            e,
            Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Literal(LiteralExpr::Integer(1))),
                op: BinaryOperation::Addition,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(2))),
            })
        );

        Ok(())
    }

    #[test]
    fn parse_simple_multiplication() -> Result<()> {
        let mut input = [Token::Integer(1), Token::Slash, Token::Integer(2)];
        let mut input = input.iter().peekable();

        let e = Parser::expr(&mut &mut input)?;
        assert_eq!(
            e,
            Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Literal(LiteralExpr::Integer(1))),
                op: BinaryOperation::Division,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(2))),
            })
        );

        Ok(())
    }

    #[test]
    fn parse_precedence_arithmetic() -> Result<()> {
        let mut input = [
            Token::Integer(1),
            Token::Plus,
            Token::Integer(2),
            Token::Star,
            Token::Integer(3),
        ];
        let mut input = input.iter().peekable();

        let e = Parser::expr(&mut &mut input)?;
        assert_eq!(
            e,
            Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Literal(LiteralExpr::Integer(1))),
                op: BinaryOperation::Addition,
                right: Box::new(Expr::Binary(BinaryExpr {
                    left: Box::new(Expr::Literal(LiteralExpr::Integer(2))),
                    op: BinaryOperation::Multiplication,
                    right: Box::new(Expr::Literal(LiteralExpr::Integer(3))),
                }))
            })
        );

        Ok(())
    }

    #[test]
    fn parse_arithmetic_left_associativity() -> Result<()> {
        let mut input = [
            Token::Integer(1),
            Token::Plus,
            Token::Integer(2),
            Token::Plus,
            Token::Integer(3),
        ];
        let mut input = input.iter().peekable();

        let e = Parser::expr(&mut &mut input)?;
        assert_eq!(
            e,
            Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Binary(BinaryExpr {
                    left: Box::new(Expr::Literal(LiteralExpr::Integer(1))),
                    op: BinaryOperation::Addition,
                    right: Box::new(Expr::Literal(LiteralExpr::Integer(2))),
                })),
                op: BinaryOperation::Addition,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(3))),
            })
        );

        Ok(())
    }

    #[test]
    fn parse_equality_left_associativity() -> Result<()> {
        let mut input = [
            Token::Integer(1),
            Token::LessThan,
            Token::Integer(2),
            Token::GreaterThanOrEqual,
            Token::Integer(3),
        ];
        let mut input = input.iter().peekable();

        let e = Parser::expr(&mut &mut input)?;
        assert_eq!(
            e,
            Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Binary(BinaryExpr {
                    left: Box::new(Expr::Literal(LiteralExpr::Integer(1))),
                    op: BinaryOperation::LessThan,
                    right: Box::new(Expr::Literal(LiteralExpr::Integer(2))),
                })),
                op: BinaryOperation::GreaterThanOrEqual,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(3))),
            })
        );

        Ok(())
    }

    #[test]
    fn parse_parenthesis() -> Result<()> {
        let mut input = [
            Token::LeftParen,
            Token::Integer(1),
            Token::Plus,
            Token::Integer(2),
            Token::RightParen,
            Token::Star,
            Token::Integer(3),
        ];
        let mut input = input.iter().peekable();

        let e = Parser::expr(&mut &mut input)?;
        assert_eq!(
            e,
            Expr::Binary(BinaryExpr {
                left: Box::new(Expr::Binary(BinaryExpr {
                    left: Box::new(Expr::Literal(LiteralExpr::Integer(1))),
                    op: BinaryOperation::Addition,
                    right: Box::new(Expr::Literal(LiteralExpr::Integer(2))),
                })),
                op: BinaryOperation::Multiplication,
                right: Box::new(Expr::Literal(LiteralExpr::Integer(3))),
            })
        );

        Ok(())
    }
}

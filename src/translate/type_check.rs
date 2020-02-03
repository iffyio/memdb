use crate::parser::ast::{BinaryOperation, Expr, LiteralExpr};
use crate::storage::storage_manager::AttributeName;
use crate::storage::types::AttributeType;
use crate::translate::error::{Result, TranslateError};
use std::collections::HashMap;

pub fn type_check_expr(expr: &Expr, ctx: &HashMap<&String, &AttributeType>) -> Result<()> {
    fn eval(attr: &String, ctx: &HashMap<&String, &AttributeType>) -> Result<AttributeType> {
        ctx.get(attr).map(|t| (*t).clone()).ok_or_else(|| {
            TranslateError::InvalidArguments(format!("no such attribute {:?}", attr))
        })
    }

    fn type_check(expr: &Expr, ctx: &HashMap<&String, &AttributeType>) -> Result<AttributeType> {
        match expr {
            Expr::Binary(expr) => {
                let left = type_check(&expr.left, ctx)?;
                let right = type_check(&expr.right, ctx)?;
                if left != right {
                    return Err(TranslateError::TypeError(format!(
                        "For {:?} operation, left {:?} != right {:?}",
                        expr.op, left, right
                    )));
                }

                match left {
                    AttributeType::Text | AttributeType::Boolean => {
                        return match expr.op {
                            BinaryOperation::Equal | BinaryOperation::NotEqual => {
                                Ok(AttributeType::Boolean)
                            }
                            _ => Err(TranslateError::TypeError(format!(
                                "Arguments of type {:?} are not valid for operation {:?}",
                                left, expr.op
                            ))),
                        }
                    }
                    AttributeType::Integer => match expr.op {
                        BinaryOperation::Equal
                        | BinaryOperation::NotEqual
                        | BinaryOperation::LessThan
                        | BinaryOperation::LessThanOrEqual
                        | BinaryOperation::GreaterThan
                        | BinaryOperation::GreaterThanOrEqual => Ok(AttributeType::Boolean),
                        BinaryOperation::Addition
                        | BinaryOperation::Subtraction
                        | BinaryOperation::Multiplication
                        | BinaryOperation::Division => Ok(AttributeType::Integer),
                    },
                }
            }

            Expr::Literal(expr) => match expr {
                LiteralExpr::Integer(_) => Ok(AttributeType::Integer),
                LiteralExpr::Boolean(_) => Ok(AttributeType::Boolean),
                LiteralExpr::String(_) => Ok(AttributeType::Text),
                LiteralExpr::Identifier(attr) => eval(attr, ctx),
            },
        }
    }

    let _ = type_check(expr, ctx)?;
    Ok(())
}

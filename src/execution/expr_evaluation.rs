use crate::parser::ast::{BinaryOperation, Expr, LiteralExpr};
use crate::storage::tuple_serde::StorageTupleValue;
use std::collections::HashMap;

pub fn evaluate_predicate_with_ctx(
    predicate: &Expr,
    ctx: &HashMap<&String, &StorageTupleValue>,
) -> bool {
    fn eval<'a>(
        attr: &String,
        ctx: &'a HashMap<&String, &StorageTupleValue>,
    ) -> &'a StorageTupleValue {
        ctx.get(attr)
            .expect("[validation] attribute doesn't exist in this context")
    }

    fn evaluate_expr(expr: &Expr, ctx: &HashMap<&String, &StorageTupleValue>) -> LiteralExpr {
        match expr {
            Expr::Binary(expr) => {
                let left = evaluate_expr(&expr.left, ctx);
                let right = evaluate_expr(&expr.right, ctx);
                match left {
                    LiteralExpr::Boolean(left) => {
                        match right {
                            LiteralExpr::Boolean(right) => {
                                match expr.op {
                                    BinaryOperation::Equal => LiteralExpr::Boolean(left == right),
                                    BinaryOperation::NotEqual => LiteralExpr::Boolean(left != right),
                                    BinaryOperation::LessThan => LiteralExpr::Boolean(left < right),
                                    BinaryOperation::LessThanOrEqual => LiteralExpr::Boolean(left <= right),
                                    BinaryOperation::GreaterThan => LiteralExpr::Boolean(left > right),
                                    BinaryOperation::GreaterThanOrEqual => LiteralExpr::Boolean(left >= right),
                                    _ => unreachable!("[validation] only equality operations are allowed between two booleans"),
                                }
                            },
                            _ => unreachable!("[validation] incompatible op: left hand is bool but right hand isn't")
                        }
                    },
                    LiteralExpr::Integer(left) => {
                        match right {
                            LiteralExpr::Integer(right) => {
                                match expr.op {
                                    BinaryOperation::Addition => LiteralExpr::Integer(left + right),
                                    BinaryOperation::Subtraction => LiteralExpr::Integer(left - right),
                                    BinaryOperation::Multiplication => LiteralExpr::Integer(left * right),
                                    BinaryOperation::Division => LiteralExpr::Integer(left / right),
                                    BinaryOperation::Equal => LiteralExpr::Boolean(left == right),
                                    BinaryOperation::NotEqual => LiteralExpr::Boolean(left != right),
                                    BinaryOperation::LessThan => LiteralExpr::Boolean(left < right),
                                    BinaryOperation::LessThanOrEqual => LiteralExpr::Boolean(left <= right),
                                    BinaryOperation::GreaterThan => LiteralExpr::Boolean(left > right),
                                    BinaryOperation::GreaterThanOrEqual => LiteralExpr::Boolean(left >= right),
                                }
                            },
                            _ => unreachable!("[validation] incompatible op: left hand is a number but right hand isn't")
                        }
                    },
                    LiteralExpr::String(left) => {
                        match right {
                            LiteralExpr::String(right) => {
                                match expr.op {
                                    BinaryOperation::Equal => LiteralExpr::Boolean(left == right),
                                    BinaryOperation::NotEqual => LiteralExpr::Boolean(left != right),
                                    _ => unreachable!("[validation] incompatible op: left hand is a string but right hand isn't")
                                }
                            },
                            _ => unreachable!("[validation] only equality operations are allowed between two strings"),
                        }
                    },
                    LiteralExpr::Identifier(_) => unreachable!("identifier should have been evaluated to a concrete value.")
                }
            }
            Expr::Literal(LiteralExpr::Identifier(id)) => match eval(id, ctx) {
                StorageTupleValue::Boolean(value) => LiteralExpr::Boolean(*value),
                StorageTupleValue::Integer(value) => LiteralExpr::Integer(*value),
                StorageTupleValue::String(value) => LiteralExpr::String(value.clone()),
            },
            Expr::Literal(literal) => literal.clone(),
        }
    }

    match evaluate_expr(&predicate, ctx) {
        LiteralExpr::Boolean(result) => result,
        unexpected => unreachable!(format!(
            "[validation] predicate is not an equality expression {:?}",
            unexpected
        )),
    }
}

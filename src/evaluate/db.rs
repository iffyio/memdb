use crate::evaluate::Evaluation;
use crate::execution::Engine;
use crate::parser::Lexer;
use crate::parser::Parser;
use crate::planner::optimizer::Optimizer;
use crate::storage::storage_manager::{AttributeName, StorageManager};
use crate::storage::tuple::TupleRecord;
use crate::storage::tuple_serde::StorageTupleValue;
use crate::translate::Translator;
use std::collections::HashMap;
use std::error::Error;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

struct DB {
    storage_manager: StorageManager,
}

impl DB {
    pub fn new() -> Self {
        DB {
            storage_manager: StorageManager::new(),
        }
    }

    pub fn execute(&mut self, query: &str) -> Result<Vec<Vec<(AttributeName, StorageTupleValue)>>> {
        let lexer = Lexer::new();
        let mut parser = Parser::new();
        let mut tokens = lexer.scan(query)?;
        let stmt = parser.parse(&mut tokens.iter().peekable())?;

        let plan = {
            let mut translator = Translator {
                storage_manager: &self.storage_manager,
            };
            translator.translate(stmt)?
        };
        let plan = Optimizer::run(plan);

        let engine = Engine {
            storage_manager: &mut self.storage_manager,
        };

        let mut eval = Evaluation { engine };
        let mut r = eval.evaluate(plan);
        let mut tuples = Vec::new();
        loop {
            let result = r.next();
            match result {
                Some(Ok(values)) => tuples.push(values),
                Some(Err(err)) => return Err(Box::new(err)),
                None => break,
            }
        }
        return Ok(tuples);
    }
}

#[cfg(test)]
mod test {
    use super::DB;
    use crate::storage::storage_manager::AttributeName;
    use crate::storage::tuple_serde::StorageTupleValue;
    use crate::storage::tuple_serde::StorageTupleValue::Integer;
    use std::collections::HashMap;

    #[test]
    fn exec_query() {
        let mut db = DB::new();
        db.execute("create table person (name varchar primary key, age integer);")
            .unwrap();
        db.execute("insert into person (name, age) values ('a', 1);")
            .unwrap();
        db.execute("insert into person (name, age) values ('b', 2);")
            .unwrap();
        db.execute("insert into person (name, age) values ('c', 3);")
            .unwrap();
        db.execute("insert into person (name, age) values ('d', 4);")
            .unwrap();
        {
            let res = db
                .execute("select age, name from person where age <= 2;")
                .unwrap();
            assert_eq!(res.len(), 2);

            assert!(res.contains(&vec![
                (AttributeName("age".to_owned()), Integer(1)),
                (
                    AttributeName("name".to_owned()),
                    StorageTupleValue::String("a".to_owned())
                ),
            ]));
            assert!(res.contains(&vec![
                (AttributeName("age".to_owned()), Integer(2)),
                (
                    AttributeName("name".to_owned()),
                    StorageTupleValue::String("b".to_owned())
                ),
            ]));
        }
        {
            let mut res = db.execute("select * from person where age = 4;").unwrap();
            assert_eq!(res.len(), 1);

            for record in &mut res {
                record.sort_by(|(_a, _), (_b, _)| _a.0.cmp(&_b.0));
            }
            assert!(res.contains(&vec![
                (AttributeName("age".to_owned()), Integer(4)),
                (
                    AttributeName("name".to_owned()),
                    StorageTupleValue::String("d".to_owned())
                ),
            ]));
        }
    }
}

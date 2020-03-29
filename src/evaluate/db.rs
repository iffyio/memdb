use crate::evaluate::Evaluation;
use crate::execution::Engine;
use crate::parser::Parser;
use crate::parser::{Input, Lexer};
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
        let stmt = parser.parse(Input::new(tokens))?;

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

    fn execute_and_discard_result(db: &mut DB, stmt: Vec<&str>) {
        stmt.into_iter()
            .for_each(|stmt| db.execute(stmt).expect("invalid stmt").clear());
    }

    fn assert_tuples(
        mut expected: Vec<Vec<(AttributeName, StorageTupleValue)>>,
        mut actual: Vec<Vec<(AttributeName, StorageTupleValue)>>,
    ) {
        for mut tuples in vec![&mut expected, &mut actual] {
            tuples.iter_mut().for_each(|mut tuple| tuple.sort());
            tuples.sort();
        }
        assert_eq!(expected, actual);
    }

    #[test]
    fn exec_query() {
        let mut db = DB::new();
        execute_and_discard_result(
            &mut db,
            vec![
                "create table person (name varchar primary key, age integer);",
                "insert into person (name, age) values ('a', 1);",
                "insert into person (name, age) values ('b', 2);",
                "insert into person (name, age) values ('c', 3);",
                "insert into person (name, age) values ('d', 4);",
            ],
        );
        {
            let mut res = db
                .execute("select age, name from person where age <= 2;")
                .unwrap();

            assert_tuples(
                vec![
                    vec![
                        (AttributeName("age".to_owned()), Integer(1)),
                        (
                            AttributeName("name".to_owned()),
                            StorageTupleValue::String("a".to_owned()),
                        ),
                    ],
                    vec![
                        (AttributeName("age".to_owned()), Integer(2)),
                        (
                            AttributeName("name".to_owned()),
                            StorageTupleValue::String("b".to_owned()),
                        ),
                    ],
                ],
                res,
            );
        }
        {
            let mut res = db.execute("select * from person where age = 4;").unwrap();
            assert_tuples(
                vec![vec![
                    (AttributeName("age".to_owned()), Integer(4)),
                    (
                        AttributeName("name".to_owned()),
                        StorageTupleValue::String("d".to_owned()),
                    ),
                ]],
                res,
            );
        }
    }

    #[test]
    fn exec_inner_joins() {
        let mut db = DB::new();

        execute_and_discard_result(
            &mut db,
            vec![
                "create table person (name varchar primary key, age integer);",
                "insert into person (name, age) values ('a', 1);",
                "insert into person (name, age) values ('b', 2);",
                "insert into person (name, age) values ('c', 3);",
                "insert into person (name, age) values ('d', 4);",
                "create table employee (id varchar primary key, department varchar);",
                "insert into employee (id, department) values ('a', 'ac');",
                "insert into employee (id, department) values ('d', 'dc');",
            ],
        );
        {
            let res = db
                .execute("select name, department from person inner join employee on name = id;")
                .unwrap();

            assert_tuples(
                vec![
                    vec![
                        (
                            AttributeName("name".to_owned()),
                            StorageTupleValue::String("a".to_owned()),
                        ),
                        (
                            AttributeName("department".to_owned()),
                            StorageTupleValue::String("ac".to_owned()),
                        ),
                    ],
                    vec![
                        (
                            AttributeName("name".to_owned()),
                            StorageTupleValue::String("d".to_owned()),
                        ),
                        (
                            AttributeName("department".to_owned()),
                            StorageTupleValue::String("dc".to_owned()),
                        ),
                    ],
                ],
                res,
            );
        }
        {
            let res = db
                .execute("select al.name, department from (select * from person where age < 3) as al inner join employee on al.name = id;")
                .unwrap();

            assert_eq!(
                res,
                vec![vec![
                    (
                        AttributeName("al.name".to_owned()),
                        StorageTupleValue::String("a".to_owned())
                    ),
                    (
                        AttributeName("department".to_owned()),
                        StorageTupleValue::String("ac".to_owned())
                    ),
                ]]
            );
        }
    }
}

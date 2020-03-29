mod create;
mod engine;
mod expr_evaluation;
mod filter;
mod insert;
mod join;
mod project;
pub mod scan;

use crate::planner::plan::query_plan::QueryResultSchema;
use crate::storage::error::Result as StorageResult;
use crate::storage::tuple::TupleRecord;
pub(crate) use create::CreateTableOperation;
pub(crate) use engine::{Engine, Operation};
pub(crate) use filter::FilterOperation;
pub(crate) use insert::InsertTupleOperation;
pub(crate) use join::InnerJoinOperation;
pub(crate) use project::ProjectOperation;
pub(crate) use scan::{ScanOperation, Tuples};

pub(crate) type EmptyResult = StorageResult<()>;
pub(crate) type TupleResult = Option<StorageResult<TupleRecord>>;

pub trait NextTuple {
    fn next(&mut self) -> TupleResult;
}

pub struct SubQueryTuples {
    pub schema: QueryResultSchema,
    pub tuples: Box<dyn NextTuple>,
}

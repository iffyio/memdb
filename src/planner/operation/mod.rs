use crate::storage::error::Result as StorageResult;
use crate::storage::tuple::TupleRecord;

mod create;
mod filter;
mod insert;
mod scan;

pub(crate) use create::CreateTableOperation;
pub(crate) use insert::InsertOperation;

pub(crate) type ExecutionResult = StorageResult<Vec<TupleRecord>>;

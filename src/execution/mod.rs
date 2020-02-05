mod create;
mod filter;
mod insert;
mod project;
mod scan;

use crate::storage::error::Result as StorageResult;
use crate::storage::tuple::TupleRecord;
pub(crate) use create::CreateTableOperation;
pub(crate) use filter::FilterOperation;
pub(crate) use insert::InsertOperation;
pub(crate) use project::ProjectOperation;
pub(crate) use scan::ScanOperation;

pub(crate) type ExecutionResult = StorageResult<Vec<TupleRecord>>;

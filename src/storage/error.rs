use crate::storage::tuple::TupleId;
use crate::storage::tuple_serde::SerdeError;
use std::error::Error;

#[derive(Debug, Eq, PartialEq)]
pub enum StorageError {
    NoSuchTuple(TupleId),
    AlreadyExists(String),
    TupleSerdeError(String),
}

impl Error for StorageError {
    fn description(&self) -> &str {
        match self {
            Self::NoSuchTuple(_) => "The requested tuple does not exist",
            Self::AlreadyExists(_) => "The resource already exists",
            Self::TupleSerdeError(_) => "Error (de)serializing a tuple",
        }
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::NoSuchTuple(tid) => write!(f, "no such tuple {:?}", tid),
            Self::AlreadyExists(resource) => write!(f, "resource [{:?}] already exists", resource),
            Self::TupleSerdeError(msg) => write!(f, "{}", msg),
        }
    }
}

impl From<SerdeError> for StorageError {
    fn from(error: SerdeError) -> Self {
        Self::TupleSerdeError(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

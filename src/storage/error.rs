use crate::storage::tuple::TupleId;
use std::error::Error;

#[derive(Debug)]
pub enum StorageError {
    NoSuchTuple(TupleId),
    AlreadyExists(String),
}

impl Error for StorageError {
    fn description(&self) -> &str {
        match self {
            Self::NoSuchTuple(_) => "The requested tuple does not exist",
            Self::AlreadyExists(_) => "The resource already exists",
        }
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::NoSuchTuple(tid) => write!(f, "no such tuple {:?}", tid),
            Self::AlreadyExists(resource) => write!(f, "resource [{:?}] already exists", resource),
        }
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

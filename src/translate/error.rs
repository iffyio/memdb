use crate::storage::error::StorageError;
use std::error::Error;

#[derive(Debug)]
pub enum TranslateError {
    DuplicateAttributeName(String),
    PrimaryKeyRequired,
    MultiplePrimaryKeys(Vec<String>),
    StorageError(Box<dyn Error>),
}

impl Error for TranslateError {
    fn description(&self) -> &str {
        match self {
            Self::DuplicateAttributeName(_) => "An attribute with the same name already exists",
            Self::StorageError(_) => "An error occurred at the storage layer",
            Self::MultiplePrimaryKeys(_) => {
                "Only one primary key allowed but multiple keys were provided"
            }
            Self::PrimaryKeyRequired => "No primary key was provided",
        }
    }
}
impl std::fmt::Display for TranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::DuplicateAttributeName(name) => write!(f, "{:?}", name),
            Self::StorageError(err) => write!(f, "{}", err),
            Self::MultiplePrimaryKeys(keys) => write!(f, "{:?}", keys),
            Self::PrimaryKeyRequired => write!(f, "Primary key required"),
        }
    }
}

impl From<StorageError> for TranslateError {
    fn from(error: StorageError) -> Self {
        Self::StorageError(Box::new(error))
    }
}

pub type Result<T> = std::result::Result<T, TranslateError>;

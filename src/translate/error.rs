use crate::storage::error::StorageError;
use std::error::Error;

#[derive(Debug)]
pub enum TranslateError {
    DuplicateAttributeName(String),
    PrimaryKeyRequired,
    MultiplePrimaryKeys(Vec<String>),
    NoSuchTable(String),
    NoSuchAttribute(String),
    InvalidArguments(String),
    TypeError(String),
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
            Self::NoSuchTable(_) => "The table does not exist",
            Self::NoSuchAttribute(_) => "The attribute does not exist",
            Self::InvalidArguments(_) => "Invalid arguments were provided to an operation",
            Self::TypeError(_) => "Invalid types were provided to an operation",
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
            Self::NoSuchTable(name) => write!(f, "No such table [{:?}]", name),
            Self::NoSuchAttribute(name) => write!(f, "No such attribute [{:?}]", name),
            Self::InvalidArguments(reason) => write!(f, "{:?}", reason),
            Self::TypeError(reason) => write!(f, "{:?}", reason),
        }
    }
}

impl From<StorageError> for TranslateError {
    fn from(error: StorageError) -> Self {
        Self::StorageError(Box::new(error))
    }
}

pub type Result<T> = std::result::Result<T, TranslateError>;

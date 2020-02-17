use crate::execution::{EmptyResult, NextTuple, TupleResult};
use crate::storage::error::Result as StorageResult;
use crate::storage::storage_manager::{StorageManager, TableName};
use crate::storage::table_storage::Storage;
use crate::storage::tuple::{Tuple, TupleRecord};
use std::cell::RefMut;

pub struct TupleIterator<'storage> {
    inner: Box<dyn Iterator<Item = TupleRecord> + 'storage>,
}

impl<'storage> Iterator for TupleIterator<'storage> {
    type Item = StorageResult<TupleRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|record| Ok(record))
    }
}

pub struct Tuples<'storage> {
    storage: RefMut<'storage, Storage>,
}

impl<'storage> Tuples<'storage> {
    pub fn iter(&self) -> TupleIterator {
        TupleIterator {
            inner: Box::new(self.storage.scan().map(|(_id, record)| record.clone())),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ScanOperation {
    tuples: Vec<TupleRecord>,
    index: usize,
}

impl NextTuple for ScanOperation {
    fn next(&mut self) -> TupleResult {
        if self.index < self.tuples.len() {
            // TODO we don't clone since we don't need the tuple after returning it.
            let t = self.tuples[self.index].clone();
            self.index += 1;
            Some(Ok(t))
        } else {
            None
        }
    }
}

impl ScanOperation {
    pub fn new(tuples: Vec<TupleRecord>) -> Self {
        ScanOperation { tuples, index: 0 }
    }
}

#[cfg(test)]
mod test {
    use crate::execution::scan::Tuples;
    use crate::execution::{NextTuple, ScanOperation};
    use crate::storage::error::StorageError;
    use crate::storage::storage_manager::{
        AttributeName, CreateTableRequest, StorageManager, TableName,
    };
    use crate::storage::tuple::{StoreId, TupleRecord};
    use std::collections::HashMap;

    #[test]
    fn scan() -> Result<(), StorageError> {
        let mut scan = ScanOperation::new(vec![TupleRecord(vec![0xca, 0xfe])]);
        let mut items = Vec::new();
        while let Some(item) = scan.next() {
            items.push(item)
        }
        assert_eq!(items, vec![Ok(TupleRecord(vec![0xca, 0xfe]))]);

        Ok(())
    }
}

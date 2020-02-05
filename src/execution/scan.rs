use crate::execution::ExecutionResult;
use crate::storage::error::Result as StorageResult;
use crate::storage::storage_manager::{StorageManager, TableName};
use crate::storage::table_storage::Storage;
use crate::storage::tuple::TupleRecord;
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
    pub table_name: TableName,
}

impl ScanOperation {
    pub fn execute<'storage>(&self, storage_manager: &'storage StorageManager) -> Tuples<'storage> {
        let storage = storage_manager
            .get_table_store(&self.table_name)
            .expect("[scan operation] table storage no longer exists?");

        Tuples { storage }
    }
}

#[cfg(test)]
mod test {
    use crate::execution::scan::Tuples;
    use crate::execution::ScanOperation;
    use crate::storage::error::StorageError;
    use crate::storage::storage_manager::{
        AttributeName, CreateTableRequest, StorageManager, TableName,
    };
    use crate::storage::tuple::{StoreId, TupleRecord};
    use std::collections::HashMap;

    #[test]
    fn scan() -> Result<(), StorageError> {
        let mut storage_manager = StorageManager::new(StoreId(0));
        storage_manager.create_table(CreateTableRequest {
            table_name: TableName("person".to_owned()),
            primary_key: AttributeName("name".to_owned()),
            schema_attributes: Vec::new(),
        })?;

        {
            let mut store = storage_manager
                .get_table_store(&TableName("person".to_owned()))
                .unwrap();
            store.insert_tuple(TupleRecord(vec![0xca, 0xfe]));
        }

        let scan = ScanOperation {
            table_name: TableName("person".to_owned()),
        };
        let mut tuples = scan.execute(&storage_manager);
        let tuples = tuples.iter();
        let items = tuples.collect::<Vec<_>>();
        assert_eq!(items, vec![Ok(TupleRecord(vec![0xca, 0xfe]))]);

        Ok(())
    }
}

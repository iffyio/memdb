use crate::storage::tuple::{StoreId, TupleId, TupleIndex, TupleRecord};
use std::collections::HashMap;

pub struct Storage {
    next_index: TupleIndex,
    store_id: StoreId,
    tuple_store: HashMap<TupleId, TupleRecord>,
}

impl Storage {
    pub fn new(store_id: StoreId) -> Self {
        Storage {
            next_index: 0,
            store_id,
            tuple_store: HashMap::new(),
        }
    }

    pub fn insert_tuple(&mut self, tuple: TupleRecord) -> TupleId {
        let id = TupleId {
            store_id: self.store_id.clone(),
            slot_index: self.next_index,
        };
        self.next_index += 1;

        self.tuple_store.insert(id.clone(), tuple);
        id
    }

    pub fn get_tuple(&self, id: &TupleId) -> Option<TupleRecord> {
        self.tuple_store.get(id).map(|tuple| tuple.clone())
    }

    pub fn scan(&self) -> impl Iterator<Item = (&TupleId, &TupleRecord)> {
        self.tuple_store.iter()
    }
}

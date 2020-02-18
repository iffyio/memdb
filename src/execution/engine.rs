use crate::execution::{
    CreateTableOperation, EmptyResult, FilterOperation, InsertTupleOperation, ProjectOperation,
    ScanOperation,
};
use crate::storage::error::Result as StorageResult;
use crate::storage::storage_manager::StorageManager;
use crate::storage::tuple::TupleRecord;

pub enum Operation {
    CreateTable(CreateTableOperation),
    InsertTuple(InsertTupleOperation),
    Scan(ScanOperation),
    Filter(FilterOperation),
    Project(ProjectOperation),
}

pub struct Engine<'storage> {
    pub storage_manager: &'storage mut StorageManager,
}

impl<'storage> Engine<'storage> {
    pub fn execute_create_table(&mut self, op: CreateTableOperation) -> EmptyResult {
        op.execute(self.storage_manager)
    }

    pub fn execute_insert_tuple(&mut self, op: InsertTupleOperation) -> EmptyResult {
        op.execute(self.storage_manager)
    }
}

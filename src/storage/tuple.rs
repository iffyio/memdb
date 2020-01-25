#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct StoreId(pub u64);

pub type TupleIndex = u32;

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct TupleId {
    pub store_id: StoreId,
    pub slot_index: TupleIndex,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TupleRecord(Vec<u32>);
pub struct Tuple {
    id: TupleId,
    record: TupleRecord,
}

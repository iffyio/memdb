use crate::storage::storage_manager::AttributeName;
use crate::storage::tuple::TupleRecord;
use crate::storage::types::AttributeType;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::error::Error;

#[derive(Debug, Eq, PartialEq)]
pub enum SerdeError {
    EOF(String),
}

impl Error for SerdeError {
    fn description(&self) -> &str {
        match self {
            Self::EOF(_) => "Reached the end of file during deserialization",
        }
    }
}

impl std::fmt::Display for SerdeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::EOF(name) => write!(f, "unable to deserialize {:?}", name),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum StorageTupleValue {
    Integer(i32),
    Boolean(bool),
    String(String),
}

pub fn serialize_tuple(values: Vec<StorageTupleValue>) -> TupleRecord {
    let tuple_size = values.iter().fold(0, |size, value| {
        size + match value {
            StorageTupleValue::Integer(_) => 4,
            StorageTupleValue::Boolean(_) => 1,
            StorageTupleValue::String(s) => 4 + s.bytes().len(),
        }
    });
    let mut tuple = Vec::with_capacity(tuple_size);
    tuple.resize_with(tuple_size, || 0);

    let mut i = 0;
    for value in values {
        match value {
            StorageTupleValue::Integer(value) => {
                (&mut tuple[i..i + 4])
                    .write_i32::<BigEndian>(value)
                    .unwrap();
                i += 4;
            }
            StorageTupleValue::Boolean(value) => {
                (&mut tuple[i..i + 1])
                    .write_u8(if value { 0x1 } else { 0x0 })
                    .unwrap();
                i += 1;
            }
            StorageTupleValue::String(value) => {
                let value = value.as_bytes();
                (&mut tuple[i..i + 4])
                    .write_u32::<BigEndian>(value.len() as u32)
                    .unwrap();
                i += 4;
                for byte in value {
                    tuple[i] = *byte;
                    i += 1;
                }
            }
        }
    }

    TupleRecord(tuple)
}

pub fn deserialize_tuple(tuple: TupleRecord, schema: Vec<AttributeType>) -> Vec<StorageTupleValue> {
    let tuple = tuple.0;
    let mut values = Vec::new();
    let mut i = 0;
    for attr_type in schema {
        let (read_bytes, value) = match attr_type {
            AttributeType::Integer => TupleRecord::read_integer(&tuple[i..]).expect("Invalid data"),
            AttributeType::Boolean => TupleRecord::read_boolean(&tuple[i..]).expect("Invalid data"),
            AttributeType::Text => TupleRecord::read_text(&tuple[i..]).expect("Invalid data"),
        };
        i += read_bytes;
        values.push(value);
    }

    values
}

impl TupleRecord {
    pub fn to_values<'schema, S, V>(&self, schema: S) -> Result<V, SerdeError>
    where
        S: Iterator<Item = &'schema (AttributeName, AttributeType)>,
        V: Default + Extend<(AttributeName, StorageTupleValue)>,
    {
        let mut values = V::default();

        let mut index = 0;
        for (attr_name, attr_type) in schema {
            let (read_bytes, value) = match attr_type {
                AttributeType::Integer => Self::read_integer(&self.0[index..])?,
                AttributeType::Text => Self::read_text(&self.0[index..])?,
                AttributeType::Boolean => Self::read_boolean(&self.0[index..])?,
            };
            values.extend(vec![(attr_name.clone(), value)]);
            index += read_bytes;
        }

        assert_eq!(index, self.0.len(), "There should be no unread bytes");

        Ok(values)
    }

    fn read_integer(tuple: &[u8]) -> Result<(usize, StorageTupleValue), SerdeError> {
        let value = (&tuple[..4])
            .read_i32::<BigEndian>()
            .expect("Invalid tuple - tried to read integer");
        Ok((4, StorageTupleValue::Integer(value)))
    }

    fn read_boolean(tuple: &[u8]) -> Result<(usize, StorageTupleValue), SerdeError> {
        let value = (&tuple[..1])
            .read_u8()
            .expect("Invalid tuple - tried to read boolean");
        Ok((1, StorageTupleValue::Boolean(value != 0x0)))
    }

    fn read_text(tuple: &[u8]) -> Result<(usize, StorageTupleValue), SerdeError> {
        let text_size = (&tuple[..4])
            .read_u32::<BigEndian>()
            .expect("Invalid tuple - tried to read text size");
        let start = 4;
        let mut text = Vec::with_capacity(text_size as usize);
        let end = start + text_size as usize;
        for byte in &tuple[start..end as usize] {
            text.push(*byte);
        }
        let value = String::from_utf8(text).expect("Invalid tuple - failed to read text");

        Ok((end, StorageTupleValue::String(value)))
    }
}

#[cfg(test)]
mod test {
    use crate::storage::tuple_serde::{deserialize_tuple, serialize_tuple, StorageTupleValue};
    use crate::storage::types::AttributeType;

    #[test]
    fn serde_tuple() {
        let values = vec![
            StorageTupleValue::Integer(3),
            StorageTupleValue::Boolean(false),
            StorageTupleValue::Integer(-4),
            StorageTupleValue::Boolean(true),
            StorageTupleValue::String("hello".to_owned()),
        ];
        let schema = vec![
            AttributeType::Integer,
            AttributeType::Boolean,
            AttributeType::Integer,
            AttributeType::Boolean,
            AttributeType::Text,
        ];

        assert_eq!(
            values.clone(),
            deserialize_tuple(serialize_tuple(values), schema)
        )
    }
}

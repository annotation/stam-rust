use std::fmt;
use serde::{Serialize,Deserialize};
use serde::ser::{Serializer, SerializeStruct};
//use serde_json::Result;


#[derive(Serialize,Deserialize,Debug,PartialEq)]
#[serde(tag = "@type", content="value")]
pub enum DataValue {
    ///No value
    Null,
    String(String),
    Bool(bool),
    Int(isize),
    Float(f64),
    //Datetime(chrono::DateTime), //TODO

    /// Value is an unordered set
    //Set(HashSet<DataValue>),

    //Value is an ordered list
    List(Vec<DataValue>)
}

impl From<&str> for DataValue {
    fn from(item: &str) -> Self {
        Self::String(item.to_string())
    }
}


impl From<String> for DataValue {
    fn from(item: String) -> Self {
        Self::String(item)
    }
}

impl From<f64> for DataValue {
    fn from(item: f64) -> Self {
        Self::Float(item)
    }
}

impl From<f32> for DataValue {
    fn from(item: f32) -> Self {
        Self::Float(item as f64)
    }
}

impl From<isize> for DataValue {
    fn from(item: isize) -> Self {
        Self::Int(item)
    }
}

impl From<i64> for DataValue {
    fn from(item: i64) -> Self {
        Self::Int(item as isize)
    }
}

impl From<i32> for DataValue {
    fn from(item: i32) -> Self {
        Self::Int(item as isize)
    }
}

impl From<i16> for DataValue {
    fn from(item: i16) -> Self {
        Self::Int(item as isize)
    }
}

impl From<i8> for DataValue {
    fn from(item: i8) -> Self {
        Self::Int(item as isize)
    }
}

impl From<usize> for DataValue {
    fn from(item: usize) -> Self {
        Self::Int(item.try_into().expect("integer out of bounds (u64 -> i64 failed)"))
    }
}

impl From<u64> for DataValue {
    fn from(item: u64) -> Self {
        Self::Int(item.try_into().expect("integer out of bounds (u64 -> i64 failed)"))
    }
}

impl From<u32> for DataValue {
    fn from(item: u32) -> Self {
        Self::Int(item.try_into().unwrap())
    }
}

impl From<u16> for DataValue {
    fn from(item: u16) -> Self {
        Self::Int(item.try_into().unwrap())
    }
}

impl From<u8> for DataValue {
    fn from(item: u8) -> Self {
        Self::Int(item.try_into().unwrap())
    }
}

impl From<bool> for DataValue {
    fn from(item: bool) -> Self {
        Self::Bool(item)
    }
}

impl From<Vec<DataValue>> for DataValue {
    fn from(item: Vec<DataValue>) -> Self {
        Self::List(item)
    }
}


// These PartialEq implementation allow for more direct comparisons 

impl PartialEq<str> for DataValue {
    fn eq(&self, other: &str) -> bool {
        match self {
            Self::String(v) => v == other,
            _ => false
        }
    }
}

impl PartialEq<DataValue> for str {
    fn eq(&self, other: &DataValue) -> bool {
        match other {
            DataValue::String(v) => v.as_str() == self,
            _ => false
        }
    }
}

impl PartialEq<f64> for DataValue {
    fn eq(&self, other: &f64) -> bool {
        match self {
            Self::Float(v) => v == other,
            _ => false
        }
    }
}

impl PartialEq<DataValue> for f64 {
    fn eq(&self, other: &DataValue) -> bool {
        match other {
            DataValue::Float(v) => v == self,
            _ => false
        }
    }
}

impl PartialEq<isize> for DataValue {
    fn eq(&self, other: &isize) -> bool {
        match self {
            Self::Int(v) => v == other,
            _ => false
        }
    }
}

impl PartialEq<DataValue> for isize {
    fn eq(&self, other: &DataValue) -> bool {
        match other {
            DataValue::Int(v) => v == self,
            _ => false
        }
    }
}


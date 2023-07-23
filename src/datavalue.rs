use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::StamError;
use crate::types::*;
use datasize::{data_size, DataSize};
use sealed::sealed;

#[sealed]
impl TypeInfo for DataValue {
    fn typeinfo() -> Type {
        Type::DataValue
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Encode, Decode)]
#[serde(tag = "@type", content = "value")]
pub enum DataValue {
    /// No value
    #[n(0)]
    Null,

    #[n(1)]
    String(#[n(0)] String),
    #[n(2)]
    Bool(#[n(0)] bool),
    #[n(3)]
    Int(#[n(0)] isize),
    #[n(4)]
    Float(#[n(0)] f64),
    //Datetime(chrono::DateTime), //TODO
    /// Value is an unordered set
    //Set(HashSet<DataValue>),

    /// Value is an ordered list
    #[n(5)]
    List(#[n(0)] Vec<DataValue>),
}

impl DataSize for DataValue {
    // `MyType` contains a `Vec` and a `String`, so `IS_DYNAMIC` is set to true.
    const IS_DYNAMIC: bool = true;
    const STATIC_HEAP_SIZE: usize = 8; //the descriminator/tag of the enum (worst case estimate)

    #[inline]
    fn estimate_heap_size(&self) -> usize {
        match self {
            Self::Null => 8, //discriminator base size only
            Self::Bool(v) => 8 + data_size(v),
            Self::String(v) => 8 + data_size(v),
            Self::Int(v) => 8 + data_size(v),
            Self::Float(v) => 8 + data_size(v),
            Self::List(v) => 8 + data_size(v),
        }
    }
}

#[derive(Clone)]
pub enum DataOperator<'a> {
    Null,
    Any,
    Equals(&'a str),
    EqualsInt(isize),
    EqualsFloat(f64),
    True,
    False,
    GreaterThan(isize),
    GreaterThanOrEqual(isize),
    GreaterThanFloat(f64),
    GreaterThanOrEqualFloat(f64),
    LessThan(isize),
    LessThanOrEqual(isize),
    LessThanFloat(f64),
    LessThanOrEqualFloat(f64),
    HasElement(&'a str),
    HasElementInt(isize),
    HasElementFloat(f64),
    Not(Box<DataOperator<'a>>),
    And(Vec<DataOperator<'a>>),
    Or(Vec<DataOperator<'a>>),
}

impl<'a> DataValue {
    pub fn test(&self, operator: &DataOperator<'a>) -> bool {
        match (self, operator) {
            (_, DataOperator::Any) => true,
            (Self::Null, DataOperator::Null) => true,
            (Self::Bool(true), DataOperator::True) => true,
            (Self::Bool(false), DataOperator::False) => true,
            (Self::String(s), DataOperator::Equals(s2)) => &s.as_str() == s2,
            (Self::Int(n), DataOperator::EqualsInt(n2)) => *n == *n2,
            (Self::Int(n), DataOperator::GreaterThan(n2)) => *n > *n2,
            (Self::Int(n), DataOperator::GreaterThanOrEqual(n2)) => *n >= *n2,
            (Self::Int(n), DataOperator::LessThan(n2)) => *n < *n2,
            (Self::Int(n), DataOperator::LessThanOrEqual(n2)) => *n <= *n2,
            (Self::Float(n), DataOperator::EqualsFloat(n2)) => *n == *n2,
            (Self::Float(n), DataOperator::GreaterThanFloat(n2)) => *n > *n2,
            (Self::Float(n), DataOperator::GreaterThanOrEqualFloat(n2)) => *n >= *n2,
            (Self::Float(n), DataOperator::LessThanFloat(n2)) => *n < *n2,
            (Self::Float(n), DataOperator::LessThanOrEqualFloat(n2)) => *n <= *n2,
            (Self::List(v), DataOperator::HasElement(s)) => {
                v.iter().any(|e| e.test(&DataOperator::Equals(s)))
            }
            (Self::List(v), DataOperator::HasElementInt(n)) => {
                v.iter().any(|e| e.test(&DataOperator::EqualsInt(*n)))
            }
            (Self::List(v), DataOperator::HasElementFloat(f)) => {
                v.iter().any(|e| e.test(&DataOperator::EqualsFloat(*f)))
            }
            (value, DataOperator::Not(operator)) => !value.test(operator),
            (value, DataOperator::And(operators)) => {
                operators.iter().all(|operator| value.test(operator))
            }
            (value, DataOperator::Or(operators)) => {
                operators.iter().any(|operator| value.test(operator))
            }
            _ => false,
        }
    }

    /// Writes a datavalue to one STAM JSON string, with appropriate formatting
    pub fn to_json(&self) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        serde_json::to_string_pretty(&self).map_err(|e| {
            StamError::SerializationError(format!("Writing datavalue to string: {}", e))
        })
    }

    /// Writes a datavalue to one STAM JSON string, without any indentation
    pub fn to_json_compact(&self) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        serde_json::to_string(&self).map_err(|e| {
            StamError::SerializationError(format!("Writing datavalue to string: {}", e))
        })
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Null => write!(f, ""),
            Self::String(v) => write!(f, "{}", v),
            Self::Bool(v) => write!(f, "{}", v),
            Self::Int(v) => write!(f, "{}", v),
            Self::Float(v) => write!(f, "{}", v),
            Self::List(v) => {
                for (i, item) in v.iter().enumerate() {
                    if i < v.len() - 1 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                Ok(())
            }
        }
    }
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
        Self::Int(
            item.try_into()
                .expect("integer out of bounds (u64 -> i64 failed)"),
        )
    }
}

impl From<u64> for DataValue {
    fn from(item: u64) -> Self {
        Self::Int(
            item.try_into()
                .expect("integer out of bounds (u64 -> i64 failed)"),
        )
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
            _ => false,
        }
    }
}

impl PartialEq<&str> for DataValue {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Self::String(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<DataValue> for str {
    fn eq(&self, other: &DataValue) -> bool {
        match other {
            DataValue::String(v) => v.as_str() == self,
            _ => false,
        }
    }
}

impl PartialEq<DataValue> for &str {
    fn eq(&self, other: &DataValue) -> bool {
        match other {
            DataValue::String(v) => v.as_str() == *self,
            _ => false,
        }
    }
}

impl PartialEq<f64> for DataValue {
    fn eq(&self, other: &f64) -> bool {
        match self {
            Self::Float(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<DataValue> for f64 {
    fn eq(&self, other: &DataValue) -> bool {
        match other {
            DataValue::Float(v) => v == self,
            _ => false,
        }
    }
}

impl PartialEq<isize> for DataValue {
    fn eq(&self, other: &isize) -> bool {
        match self {
            Self::Int(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<DataValue> for isize {
    fn eq(&self, other: &DataValue) -> bool {
        match other {
            DataValue::Int(v) => v == self,
            _ => false,
        }
    }
}

impl<'a> TryFrom<DataOperator<'a>> for DataValue {
    type Error = StamError;

    fn try_from(operator: DataOperator<'a>) -> Result<Self, Self::Error> {
        match operator {
            DataOperator::Null => Ok(Self::Null),
            DataOperator::Equals(s) => Ok(Self::String(s.to_string())),
            DataOperator::EqualsFloat(f) => Ok(Self::Float(f)),
            DataOperator::EqualsInt(i) => Ok(Self::Int(i)),
            DataOperator::True => Ok(Self::Bool(true)),
            DataOperator::False => Ok(Self::Bool(false)),
            _ => Err(StamError::OtherError(
                "Data operator can not be converted to a single DataValue",
            )),
        }
    }
}

impl<'a> From<&DataValue> for DataOperator<'a> {
    fn from(v: &DataValue) -> Self {
        match v {
            DataValue::Null => DataOperator::Null,
            DataValue::String(s) => DataOperator::Equals(s.as_str()),
            DataValue::Int(v) => DataOperator::EqualsInt(*v),
            DataValue::Float(v) => DataOperator::EqualsFloat(*v),
            DataValue::Bool(true) => DataOperator::True,
            DataValue::Bool(false) => DataOperator::False,
            DataValue::List(v) => {
                eprintln!("STAM warning: Automatic conversion from list values to operators is not supported!");
                DataOperator::Null
            }
        }
    }
}

impl<'a> From<&'a str> for DataOperator<'a> {
    fn from(s: &str) -> Self {
        DataOperator::Equals(s)
    }
}

impl<'a> From<String> for DataOperator<'a> {
    fn from(s: String) -> Self {
        DataOperator::Equals(s.as_str())
    }
}

impl<'a> From<isize> for DataOperator<'a> {
    fn from(v: isize) -> Self {
        DataOperator::EqualsInt(v)
    }
}

impl<'a> From<usize> for DataOperator<'a> {
    fn from(v: usize) -> Self {
        DataOperator::EqualsInt(v as isize)
    }
}

impl<'a> From<f64> for DataOperator<'a> {
    fn from(v: f64) -> Self {
        DataOperator::EqualsFloat(v)
    }
}

impl<'a> From<bool> for DataOperator<'a> {
    fn from(v: bool) -> Self {
        if v {
            DataOperator::True
        } else {
            DataOperator::False
        }
    }
}

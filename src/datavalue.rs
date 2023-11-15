/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the API for [`DataValue`]. It defines and implements the
//! struct, the handle, and things like serialisation, deserialisation to STAM JSON.
//! It also implements the [`DataOperator`].
//! This API is used both on high and low levels.

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
/// This type encapsulates a value and its type.
/// It is held by [`AnnotationData`](crate::AnnotationData) alongside a reference to a [`DataKey`](crate::DataKey), resulting in a key/value pair.
pub enum DataValue {
    /// No value
    #[n(0)]
    Null,

    /// A string value
    #[n(1)]
    String(#[n(0)] String),

    /// A boolean value
    #[n(2)]
    Bool(#[n(0)] bool),

    /// A numeric integer value
    #[n(3)]
    Int(#[n(0)] isize),

    /// A numeric floating-point value
    #[n(4)]
    Float(#[n(0)] f64),

    //Datetime(chrono::DateTime), //TODO
    /// Value is an unordered set
    //Set(HashSet<DataValue>),

    /// The value is an ordered list
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

#[derive(Clone, Debug, PartialEq)]
/// This type defines a test that can be done on a [`DataValue`] (via [`DataValue::test()`]).
/// The operator does not merely consist of the operator-part, but also holds the value that is tested against, which may
/// be one of various types, hence the many variants of this type.
///
/// [`DataOperator::Any`] is a special variant of this operator that will always pass.
pub enum DataOperator<'a> {
    Null,
    Any,
    /// Tests against a string
    Equals(&'a str),
    /// Tests against a numeric integer
    EqualsInt(isize),
    /// Tests against a numeric floating-point value
    EqualsFloat(f64),
    True,
    False,
    /// The datavalue must be numeric and greater than the value with the operator
    GreaterThan(isize),
    /// The datavalue must be numeric and greater than or equal to the value with the operator
    GreaterThanOrEqual(isize),
    /// The datavalue must be numeric and greater than the value with the operator
    GreaterThanFloat(f64),
    /// The datavalue must be numeric and greater than or equal to the value with the operator
    GreaterThanOrEqualFloat(f64),
    /// The datavalue must be numeric and less than the value with the operator
    LessThan(isize),
    /// The datavalue must be numeric and less than or equal to the value with the operator
    LessThanOrEqual(isize),
    /// The datavalue must be numeric and less than or equal to the value with the operator
    LessThanFloat(f64),
    /// The datavalue must be numeric and less than or equal to the value with the operator
    LessThanOrEqualFloat(f64),
    HasElement(&'a str),
    HasElementInt(isize),
    HasElementFloat(f64),
    /// Logical negation, reverses the operator
    Not(Box<DataOperator<'a>>),
    /// Logical AND operator (conjunction) to combine multiple operators into one
    And(Vec<DataOperator<'a>>),
    /// Logical OR operator (disjunction) to combine multiple operators into one
    Or(Vec<DataOperator<'a>>),
}

impl<'a> DataValue {
    /// This applies a [`DataOperator`] to the data value, and returns a boolean if the values passes the constraints posed by the operator.
    /// Note that the [`DataOperator`] itself holds the value that is tested against.
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

impl<'a> From<&'a DataValue> for DataOperator<'a> {
    fn from(v: &'a DataValue) -> Self {
        match v {
            DataValue::Null => DataOperator::Null,
            DataValue::String(s) => DataOperator::Equals(s.as_str()),
            DataValue::Int(v) => DataOperator::EqualsInt(*v),
            DataValue::Float(v) => DataOperator::EqualsFloat(*v),
            DataValue::Bool(true) => DataOperator::True,
            DataValue::Bool(false) => DataOperator::False,
            DataValue::List(_) => {
                eprintln!("STAM warning: Automatic conversion from list values to operators is not supported!");
                DataOperator::Null
            }
        }
    }
}

impl<'a> From<&'a str> for DataOperator<'a> {
    fn from(s: &'a str) -> Self {
        DataOperator::Equals(s)
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

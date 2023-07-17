use datasize::DataSize;
use sealed::sealed;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::error::StamError;

/// A cursor points to a specific point in a text. I
/// Used to select offsets. Units are unicode codepoints (not bytes!)
/// and are 0-indexed.
///
/// The cursor can be either begin-aligned or end-aligned. Where BeginAlignedCursor(0)
/// is the first unicode codepoint in a referenced text, and EndAlignedCursor(0) the last one.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, DataSize)]
#[serde(tag = "@type", content = "value")]
pub enum Cursor {
    /// Cursor relative to the start of a text. Has a value of 0 or higher
    #[serde(rename = "BeginAlignedCursor")]
    BeginAligned(usize),
    /// Cursor relative to the end of a text. Has a value of 0 or lower. The last character of a text begins at EndAlignedCursor(-1) and ends at EndAlignedCursor(0)
    #[serde(rename = "EndAlignedCursor")]
    EndAligned(isize),
}

impl From<usize> for Cursor {
    fn from(cursor: usize) -> Self {
        Self::BeginAligned(cursor)
    }
}

impl TryFrom<isize> for Cursor {
    type Error = StamError;
    fn try_from(cursor: isize) -> Result<Self, Self::Error> {
        if cursor > 0 {
            Err(StamError::InvalidCursor(format!("{}", cursor), "Cursor is a signed integer and converts to EndAlignedCursor, expected a value <= 0. Convert from an unsigned integer for a normal BeginAlignedCursor"))
        } else {
            Ok(Self::EndAligned(cursor))
        }
    }
}

impl TryFrom<&str> for Cursor {
    type Error = StamError;
    fn try_from(cursor: &str) -> Result<Self, Self::Error> {
        if cursor.starts_with('-') {
            //EndAligned
            let cursor: isize = isize::from_str_radix(cursor, 10).map_err(|_e| {
                StamError::InvalidCursor(cursor.to_owned(), "Invalid EndAlignedCursor")
            })?;
            Cursor::try_from(cursor)
        } else {
            //BeginAligned
            let cursor: usize = usize::from_str_radix(cursor, 10).map_err(|_e| {
                StamError::InvalidCursor(cursor.to_owned(), "Invalid BeginAlignedCursor")
            })?;
            Ok(Cursor::from(cursor))
        }
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::EndAligned(0) => write!(f, "-0"), //add sign
            Self::BeginAligned(x) => write!(f, "{}", x),
            Self::EndAligned(x) => write!(f, "{}", x), //sign already included
        }
    }
}

/// The handle trait is implemented on various handle types. They have in common that refer to the internal id
/// of a [`crate::store::Storable`] item in a struct implementing [`crate::store::StoreFor`] by index. Types implementing this are lightweight and do not borrow anything, they can be passed and copied freely.
// To get an actual reference to the item from a handle type, call the `get()`` method on the store that holds it.
/// This is a sealed trait, not implementable outside this crate.
#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait Handle:
    Clone + Copy + core::fmt::Debug + PartialEq + Eq + PartialOrd + Ord + Hash + DataSize
{
    /// Create a new handle for an internal ID. You shouldn't need to use this as handles will always be generated for you by higher-level functions.
    fn new(intid: usize) -> Self;
    /// Returns the internal index for this handle
    fn unwrap(&self) -> usize;
}

#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait TypeInfo {
    fn typeinfo() -> Type;
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub enum Type {
    AnnotationStore,
    Annotation,
    AnnotationDataSet,
    AnnotationData,
    DataKey,
    DataValue,
    TextResource,
    TextSelection,
    TextSelectionSet,
    Config,
}

impl TryFrom<&str> for Type {
    type Error = StamError;
    fn try_from(val: &str) -> Result<Self, Self::Error> {
        let val_lower = val.to_lowercase();
        match val_lower.as_str() {
            "annotationstore" | "store" => Ok(Self::AnnotationStore),
            "annotation" | "annotations" => Ok(Self::Annotation),
            "annotationdataset" | "dataset" | "annotationset" | "annotationdatasets"
            | "datasets" | "annotationsets" => Ok(Self::AnnotationDataSet),
            "data" | "annotationdata" => Ok(Self::AnnotationData),
            "datakey" | "datakeys" | "key" | "keys" => Ok(Self::DataKey),
            "datavalue" | "value" | "values" => Ok(Self::DataValue),
            "resource" | "textresource" | "resources" | "textresources" => Ok(Self::TextResource),
            "textselection" | "textselections" => Ok(Self::TextSelection),
            "textselectionset" => Ok(Self::TextSelectionSet),
            "config" | "configuration" => Ok(Self::Config),
            _ => Err(StamError::OtherError("Unknown type supplied")),
        }
    }
}

impl Type {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Annotation => "Annotation",
            Self::AnnotationData => "AnnotationData",
            Self::AnnotationDataSet => "AnnotationDataSet",
            Self::AnnotationStore => "AnnotationStore",
            Self::DataKey => "DataKey",
            Self::DataValue => "DataValue",
            Self::TextResource => "TextResource",
            Self::TextSelection => "TextSelection",
            Self::TextSelectionSet => "TextSelectionSet",
            Self::Config => "Config",
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
pub enum DataFormat {
    Json {
        compact: bool,
    },

    #[cfg(feature = "csv")]
    Csv,
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Json { .. } => write!(f, "json"),

            #[cfg(feature = "csv")]
            Self::Csv => write!(f, "csv"),
        }
    }
}

impl TryFrom<&str> for DataFormat {
    type Error = StamError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "json" | "Json" | "JSON" => Ok(Self::Json { compact: false }),
            "json-compact" | "Json-compact" | "JSON-compact" => Ok(Self::Json { compact: true }),

            #[cfg(feature = "csv")]
            "csv" | "Csv" | "CSV" => Ok(Self::Csv),

            _ => Err(StamError::OtherError("Invalid value for DataFormat")),
        }
    }
}

pub(crate) fn debug<F>(config: &Config, message_func: F)
where
    F: FnOnce() -> String,
{
    if config.debug {
        eprintln!("[STAM DEBUG] {}", message_func());
    }
}

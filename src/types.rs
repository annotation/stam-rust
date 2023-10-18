/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module implements some common types that are found throughout the API, both low and high-level.

use datasize::DataSize;
use sealed::sealed;
use std::hash::Hash;

use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::error::StamError;

/// A cursor points to a specific point in a text. I
/// Used to select offsets. Units are unicode codepoints (not bytes!)
/// and are 0-indexed.
///
/// The cursor can be either begin-aligned or end-aligned. Where BeginAlignedCursor(0)
/// is the first unicode codepoint in a referenced text, and EndAlignedCursor(0) the last one.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, DataSize, Encode, Decode)]
#[serde(tag = "@type", content = "value")]
pub enum Cursor {
    /// Cursor relative to the start of a text. Has a value of 0 or higher
    #[serde(rename = "BeginAlignedCursor")]
    #[n(0)] //these macros are field index numbers for cbor binary (de)serialisation
    BeginAligned(#[n(0)] usize),

    /// Cursor relative to the end of a text. Has a value of 0 or lower. The last character of a text begins at `Cursor::EndAligned(-1)` and ends at `Cursor::EndAligned(0)`
    #[serde(rename = "EndAlignedCursor")]
    #[n(1)]
    EndAligned(#[n(0)] isize),
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

/// The handle trait is implemented for various handle types. They have in common that refer to the internal id
/// of a [`Storable`](crate::store::Storable) item in a struct implementing [`StoreFor`](crate::store::StoreFor) by index. Types implementing this are lightweight and do not borrow anything, they can be passed and copied freely.
// To get an actual reference to the item from a handle type, call the [`get()`](StoreFor<T>::get()) method on the store that holds it.
/// This is a sealed trait, not implementable outside this crate.
#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait Handle:
    Clone + Copy + core::fmt::Debug + PartialEq + Eq + PartialOrd + Ord + Hash + DataSize
{
    /// Create a new handle for an internal ID. You shouldn't need to use this as handles will always be generated for you by higher-level functions.
    /// In fact, creating them yourself like this should be considered dangerous!
    fn new(intid: usize) -> Self;

    /// Returns the internal index for this handle.
    fn as_usize(&self) -> usize;

    /// Low-level method to compute a new handle based on a list of gaps, used by reindexers. There is usually no reason to call this yourself.
    fn reindex(&self, gaps: &[(Self, isize)]) -> Self {
        let mut delta = 0;
        for (gaphandle, gapdelta) in gaps.iter() {
            if gaphandle.as_usize() < self.as_usize() {
                delta += gapdelta;
            } else {
                break;
            }
        }
        Self::new((self.as_usize() as isize + delta) as usize)
    }
}

/// This trait provides some introspection on STAM data types. It is a sealed trait that can not be implemented.
#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait TypeInfo {
    /// Return the type (introspection).
    fn typeinfo() -> Type;

    /// Return the prefix for temporary identifiers of this type
    fn temp_id_prefix() -> &'static str {
        match Self::typeinfo() {
            Type::AnnotationStore => "!Z",
            Type::Annotation => "!A",
            Type::AnnotationDataSet => "!S",
            Type::AnnotationData => "!D",
            Type::DataKey => "!K",
            Type::DataValue => "!V",
            Type::TextResource => "!R",
            Type::TextSelection => "!T",
            Type::TextSelectionSet => "!X",
            Type::Config => "!C",
        }
    }
}

/// An enumeration of STAM data types. This is used for introspection via [`TypeInfo`].
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

/// Data formats for serialisation and deserialisation supported by the library.
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Decode, Encode)]
pub enum DataFormat {
    /// STAM JSON, see the [specification](https://github.com/annotation/stam/blob/master/README.md#stam-json)
    /// The canonical extension used by the library is `.stam.json`.
    #[n(0)]
    Json {
        #[n(0)]
        compact: bool,
    },

    /// Concise Binary Object Representation, a binary format suitable for quick loading and saving, as it also
    /// holds all indices (unlike STAM JSON/CSV). This should be used for caching only and not as a data interchange
    /// storage format as the format changes per version of this library (and may even differ based on compile-time options).
    ///
    /// The canonical extension used by the library is `.stam.cbor`.
    #[n(1)]
    CBOR,

    /// STAM CSV, see the [specification](https://github.com/annotation/stam/tree/master/extensions/stam-csv)
    ///
    /// The canonical extension used by the library is `.stam.csv`.
    #[cfg(feature = "csv")]
    #[n(2)]
    Csv,
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Json { .. } => write!(f, "json"),

            Self::CBOR { .. } => write!(f, "cbor"),

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
            "cbor" => Ok(Self::CBOR),

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

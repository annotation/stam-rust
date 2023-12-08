/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! ## Introduction
//!
//! STAM is a data model for stand-off text annotation. This is a software library to work with the
//! model from Rust, and is the primary library/reference implementation for STAM. It aims to
//! implement the full model as per the [STAM specification](https://github.com/annotation/stam) and most of the
//! extensions.
//!
//! **What can you do with this library?**
//!
//! * Keep, build and manipulate an efficient in-memory store of texts and annotations on texts
//! * Search in annotations, data and text:
//!    * Search annotations by data, textual content, relations between text fragments (overlap, embedding, adjacency, etc).
//!    * Search in text (incl. via regular expressions) and find annotations targeting found text selections.
//!    * Elementary text operations with regard for text offsets (splitting text on a delimiter, stripping text).
//!    * Search in data (set,key,value) and find annotations that use the data.
//!    * Convert between different kind of offsets (absolute, relative to other structures, UTF-8 bytes vs unicode codepoints, etc)
//! * Read and write resources and annotations from/to STAM JSON, STAM CSV, or an optimised binary (CBOR) representation.
//!     * The underlying [STAM model](https://github.com/annotation/stam) aims to be clear and simple. It is flexible and
//!       does not commit to any vocabulary or annotation paradigm other than stand-off annotation.
//!
//! This STAM library is intended as a foundation upon which further applications
//! can be built that deal with stand-off annotations on text. We implement all the
//! low-level logic in dealing this so you no longer have to and can focus on your
//! actual application. The library is written with performance in mind.
//!
//! This is the root module for the STAM library. The STAM library consists of two APIs, a
//! low-level API and a high-level API, the latter is of most interest to end users and is
//! implemented in `api/*.rs`.
//!
//! High-level API (or mixed low/high):
//! * [`AnnotationStore`]
//! * [`ResultItem<Annotation>`](struct.ResultItem.html#impl-ResultItem<'store,+Annotation>)
//! * [`ResultItem<AnnotationDataSet>`](struct.ResultItem.html#impl-ResultItem<'store,+AnnotationDataSet>)
//! * [`ResultItem<AnnotationData>`](struct.ResultItem.html#impl-ResultItem<'store,+AnnotationData>)
//! * [`ResultItem<DataKey>`](struct.ResultItem.html#impl-ResultItem<'store,+DataKey>)
//! * [`DataValue`]
//! * [`DataOperator`]
//! * [`ResultItem<TextResource>`](struct.ResultItem.html#impl-ResultItem<'store,+TextResource>)
//! * [`ResultTextSelection`]
//! * [`TextSelectionOperator`]
//! * [`Annotations`] - collection
//! * [`AnnotationsIter`] - iterator
//! * [`Data`] - collection
//! * [`DataIter`] - iterator
//! * [`TextSelectionsIter`] - iterator
//! * [`Cursor`]
//! * [`Offset`]
//!
//! Low-level API:
//! * [`Annotation`]
//! * [`AnnotationDataSet`]
//! * [`AnnotationData`]
//! * [`TextSelection`]
//! * [`TextResource`]

mod annotation;
mod annotationdata;
mod annotationdataset;
mod annotationstore;
mod api;
mod cbor;
mod config;
mod datakey;
mod datavalue;
mod error;
mod file;
mod json;
mod resources;
mod selector;
mod store;
mod text;
mod textselection;
mod types;

#[cfg(feature = "csv")]
mod csv;

// Our internal crate structure is not very relevant to the outside world,
// expose all structs and traits in the root namespace, and be explicit about it:

#[cfg(feature = "csv")]
pub use crate::csv::{FromCsv, ToCsv};
pub use annotation::{Annotation, AnnotationBuilder, AnnotationHandle};
pub use annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
pub use annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
pub use annotationstore::AnnotationStore;
pub use api::*;
pub use config::{Config, Configurable};
pub use datakey::{DataKey, DataKeyHandle};
pub use datavalue::{DataOperator, DataValue};
pub use error::StamError;
pub use file::*;
pub use json::{FromJson, ToJson};
pub use resources::{
    PositionMode, TextResource, TextResourceBuilder, TextResourceHandle, TextSelectionIter,
};
pub use selector::{
    Offset, OffsetMode, Selector, SelectorBuilder, SelectorIter, SelectorKind, SelfSelector,
};
pub use store::{BuildItem, Request, ResultItem, StamResult, Storable, Store, StoreFor};
pub use text::Text;
pub use textselection::{
    ResultTextSelection, TestTextSelection, TextSelection, TextSelectionHandle,
    TextSelectionOperator, TextSelectionSet, TextSelectionSetIntoIter, TextSelectionSetIter,
};
pub use types::*;

pub use regex::{Regex, RegexSet};

mod tests;

mod annotation;
mod annotationdata;
mod annotationdataset;
mod annotationstore;
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
pub use annotation::{Annotation, AnnotationBuilder, AnnotationHandle, TargetIter, TargetIterItem};
pub use annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
pub use annotationdataset::{AnnotationDataSet, AnnotationDataSetBuilder, AnnotationDataSetHandle};
pub use annotationstore::{AnnotationStore, AnnotationStoreBuilder};
pub use config::{Config, Configurable};
pub use datakey::{DataKey, DataKeyHandle};
pub use datavalue::DataValue;
pub use error::StamError;
pub use file::*;
pub use json::{FromJson, ToJson};
pub use resources::{
    PositionMode, TextResource, TextResourceBuilder, TextResourceHandle, TextSelectionIter,
};
pub use selector::{
    Offset, Selector, SelectorBuilder, SelectorIter, SelectorIterItem, SelectorKind, SelfSelector,
};
pub use store::*;
pub use text::{FindRegexIter, FindRegexMatch, Text};
pub use textselection::{TextSelection, TextSelectionOperator, TextSelectionSet};
pub use types::*;

pub use regex::{Regex, RegexSet};

pub type TextSelectionRef<'a> = WrappedItem<'a, TextSelection>;
pub type AnnotationRef<'a> = WrappedItem<'a, Annotation>;
pub type AnnotationDataRef<'a> = WrappedItem<'a, AnnotationData>;
pub type AnnotationDataSetRef<'a> = WrappedItem<'a, AnnotationDataSet>;

mod tests;

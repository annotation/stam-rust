mod annotation;
mod annotationdata;
mod annotationdataset;
mod annotationstore;
mod datakey;
mod datavalue;
mod error;
mod resources;
mod selector;
mod textselection;
mod types;

// Our internal crate structure is not very relevant to the outside world,
// expose all structs and traits in the root namespace, and be explicit about it:

pub use annotation::{Annotation, AnnotationBuilder, AnnotationHandle};
pub use annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
pub use annotationdataset::{AnnotationDataSet, AnnotationDataSetBuilder, AnnotationDataSetHandle};
pub use annotationstore::{AnnotationStore, AnnotationStoreBuilder, TargetIter, TargetIterItem};
pub use datakey::{DataKey, DataKeyHandle};
pub use datavalue::DataValue;
pub use error::StamError;
pub use resources::{TextResource, TextResourceBuilder, TextResourceHandle, TextSelectionIter};
pub use selector::{
    Offset, Selector, SelectorBuilder, SelectorIter, SelectorIterItem, SelectorKind, SelfSelector,
};
pub use textselection::{TextSelection, TextSelectionOperator};
pub use types::*;

mod tests;

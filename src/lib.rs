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
mod search;
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
    Offset, OffsetMode, Selector, SelectorBuilder, SelectorIter, SelectorIterItem, SelectorKind,
    SelfSelector,
};
pub use store::{BuildItem, Request, ResultItem, Storable, Store, StoreFor};
pub use text::Text;
pub use textselection::{
    ResultTextSelection, TestTextSelection, TextSelection, TextSelectionOperator, TextSelectionSet,
    TextSelectionSetIntoIter, TextSelectionSetIter,
};
pub use types::*;

pub use regex::{Regex, RegexSet};

mod tests;

use std::borrow::Cow;

// Lazy iterator computing an intersection
pub(crate) struct IntersectionIter<'a, T>
where
    T: Ord,
    [T]: ToOwned<Owned = Vec<T>>,
{
    left: Cow<'a, [T]>,
    left_sorted: bool,
    left_cursor: usize,
    right: Cow<'a, [T]>,
    right_sorted: bool,
    right_cursor: usize,
}

impl<'a, T> IntersectionIter<'a, T>
where
    T: Ord,
    [T]: ToOwned<Owned = Vec<T>>,
{
    pub(crate) fn new(
        left: Cow<'a, [T]>,
        left_sorted: bool,
        right: Cow<'a, [T]>,
        right_sorted: bool,
    ) -> Self {
        if (!right_sorted && left_sorted)
            || (right_sorted && left_sorted && right.len() < left.len())
        {
            //swap positions according to these conditions:
            //  - always prefer sorted in right position
            //  - if both are sorted, prefer shortest one in left position
            Self {
                left: right,
                right: left,
                left_sorted: right_sorted,
                right_sorted: left_sorted,
                left_cursor: 0,
                right_cursor: 0,
            }
        } else {
            Self {
                left,
                right,
                left_sorted,
                right_sorted,
                left_cursor: 0,
                right_cursor: 0,
            }
        }
    }
}

impl<'a, T> Iterator for IntersectionIter<'a, T>
where
    T: Ord + Copy,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.left.get(self.left_cursor) {
                if self.right_sorted {
                    if let Ok(index) = &self.right[self.right_cursor..].binary_search(&item) {
                        self.left_cursor += 1;
                        if self.left_sorted {
                            // the left is sorted so we can all discount 'lesser than' items from the right side
                            // in subsequent iterations by moving the cursor ahead
                            self.right_cursor = *index;
                        }
                        return Some(*item);
                    }
                } else {
                    if self.right.contains(&item) {
                        return Some(*item);
                    }
                }
                self.left_cursor += 1;
            } else {
                return None;
            }
        }
    }
}

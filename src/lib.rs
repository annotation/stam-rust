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

use smallvec::SmallVec;
use std::borrow::Cow;

// Lazy iterator computing an intersection
pub(crate) struct IntersectionIter<'a, T>
where
    T: Ord,
    [T]: ToOwned<Owned = Vec<T>>,
{
    sources: SmallVec<[IntersectionSource<'a, T>; 2]>,
    cursors: SmallVec<[usize; 2]>,
}

pub(crate) struct IntersectionSource<'a, T>
where
    T: Ord,
    [T]: ToOwned<Owned = Vec<T>>,
{
    data: Cow<'a, [T]>,
    sorted: bool,
}

impl<'a, T> IntersectionIter<'a, T>
where
    T: Ord,
    [T]: ToOwned<Owned = Vec<T>>,
{
    pub(crate) fn new(source: Cow<'a, [T]>, sorted: bool) -> Self {
        let mut iter = Self {
            sources: SmallVec::new(),
            cursors: SmallVec::new(),
        };
        iter.with(source, sorted)
    }

    pub(crate) fn with(mut self, data: Cow<'a, [T]>, sorted: bool) -> Self {
        let source = IntersectionSource { data, sorted };
        if self.sources.is_empty() {
            self.sources.push(source);
            self.cursors.push(0);
        } else {
            //find insertion point
            let mut pos = 0;
            for refsource in self.sources.iter() {
                if (source.sorted == refsource.sorted) && (refsource.data.len() > source.data.len())
                {
                    //shorter before longer (allows us to discard quicker)
                    break;
                } else if !source.sorted && refsource.sorted {
                    //unsorted before sorted (it is more efficient to have sorted in the right hand side as we can do binary search there)
                    break;
                }
                pos += 1;
            }
            if pos == self.sources.len() {
                self.sources.push(source);
                self.cursors.push(0);
            } else {
                self.sources.insert(pos, source);
                self.cursors.insert(pos, 0);
            }
        }
        self
    }
}

impl<'a, T> Iterator for IntersectionIter<'a, T>
where
    T: Ord + Copy,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let left = self.sources.get(0).unwrap();
            if let Some(item) = left.data.get(self.cursors[0]).copied() {
                if self.sources[1..].is_empty() {
                    //only one source
                    self.cursors[0] += 1;
                    return Some(item);
                } else {
                    let mut found = true; //falsify
                    for (i, right) in self.sources.iter().enumerate() {
                        if i == 0 {
                            //the first item is the left iter
                            continue;
                        };
                        if right.sorted {
                            //binary search
                            if let Ok(index) = right.data[self.cursors[i]..].binary_search(&item) {
                                if left.sorted {
                                    // the left is sorted so we can discount 'lesser than' items from the right side
                                    // in subsequent iterations by moving the cursor ahead
                                    // this is an extra optimisation
                                    self.cursors[i] = index;
                                }
                                //item found! continue with next source to ensure item is also in there
                                continue;
                            } else {
                                found = false;
                                break;
                            }
                        } else {
                            //linear search
                            if !right.data.contains(&item) {
                                found = false;
                                break;
                            }
                        }
                    }
                    if found {
                        self.cursors[0] += 1;
                        return Some(item);
                    }
                }
                //no results found, continue with next item
                self.cursors[0] += 1;
            } else {
                return None;
            }
        }
    }
}

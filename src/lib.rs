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
//mod search;
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
pub use store::{BuildItem, Request, ResultItem, Storable, Store, StoreFor};
pub use text::Text;
pub use textselection::{
    ResultTextSelection, TestTextSelection, TextSelection, TextSelectionOperator, TextSelectionSet,
    TextSelectionSetIntoIter, TextSelectionSetIter,
};
pub use types::*;

pub use regex::{Regex, RegexSet};

mod tests;

use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;

// Lazy iterator computing an intersection
pub(crate) struct IntersectionIter<'a, T>
where
    T: Ord,
    T: Copy,
    [T]: ToOwned<Owned = Vec<T>>,
{
    sources: SmallVec<[IntersectionSource<'a, T>; 2]>,
    cursors: SmallVec<[usize; 2]>,

    /// indicates that no results can be expected anymore
    abort: bool,
}

pub(crate) struct IntersectionSource<'a, T>
where
    T: Ord,
    T: Copy,
    [T]: ToOwned<Owned = Vec<T>>,
{
    iter: Option<Box<dyn Iterator<Item = T> + 'a>>,
    array: Option<Cow<'a, [T]>>,
    sorted: bool,
}

/*
impl<'a, T> Clone for IntersectionSource<'a, T>
where
    T: Ord,
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn clone(&self) -> Self {
        if self.iter.is_some() {
            Self {
                iter: None,
                array: Some(Cow::Owned(self.iter.unwrap().collect())),
                sorted: self.sorted,
            }
        } else {
            Self {
                iter: None,
                array: self.array.clone(),
                sorted: self.sorted,
            }
        }
    }
}
*/

impl<'a, T> IntersectionSource<'a, T>
where
    T: Ord,
    T: Copy,
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn len(&self) -> Option<usize> {
        if let Some(array) = &self.array {
            return Some(array.len());
        }
        None
    }

    fn take_item(&mut self, index: usize) -> Option<T> {
        if let Some(array) = &self.array {
            return array.get(index).map(|x| *x);
        } else if let Some(iter) = self.iter.as_mut() {
            return iter.next();
        }
        return None;
    }
}

impl<'a, T> IntersectionIter<'a, T>
where
    T: Ord,
    T: Copy,
    [T]: ToOwned<Owned = Vec<T>>,
{
    pub(crate) fn new(source: Cow<'a, [T]>, sorted: bool) -> Self {
        let iter = Self {
            sources: SmallVec::new(),
            cursors: SmallVec::new(),
            abort: false,
        };
        iter.with(source, sorted)
    }

    pub(crate) fn new_with_iterator(iter: Box<dyn Iterator<Item = T> + 'a>, sorted: bool) -> Self {
        let source = IntersectionSource {
            array: None,
            iter: Some(iter),
            sorted,
        };
        Self {
            cursors: smallvec!(0),
            sources: smallvec!(source),
            abort: false,
        }
    }

    pub(crate) fn with(mut self, data: Cow<'a, [T]>, sorted: bool) -> Self {
        if data.is_empty() {
            //don't bother, empty data invalidates the whole iterator
            self.abort = true;
            return self;
        }
        let source = IntersectionSource {
            array: Some(data),
            iter: None,
            sorted,
        };
        if self.sources.is_empty() {
            self.sources.push(source);
            self.cursors.push(0);
        } else {
            //find insertion point
            let mut pos = 0;
            for refsource in self.sources.iter() {
                if refsource.array.is_none() {
                    //pass: arrays go at the end
                } else if (source.sorted == refsource.sorted) && (refsource.len() > source.len()) {
                    //shorter before longer (allows us to discard quicker)
                    break;
                } else if pos > 0 && !source.sorted && refsource.sorted {
                    //unsorted before sorted (it is more efficient to have sorted in the right hand side as we can do binary search there)
                    //however, we do not do this if the first item is already sorted, because that determines the sorting of the entire result
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

    /// Merges another IntersectionIter into the current one. This effectively
    /// iterates over the intersection of both.
    pub(crate) fn merge(mut self, other: IntersectionIter<'a, T>) -> Self {
        for source in other.sources {
            if source.array.is_some() {
                self = self.with(source.array.unwrap(), source.sorted);
            } else if let Some(iter) = source.iter {
                let data = iter.collect(); //consume the iterator, we can only have an iterator in the first position
                self = self.with(Cow::Owned(data), source.sorted);
            }
        }
        self
    }

    /// Extends this iterator with another one. This is a *union* and not an *intersection*.
    /// However, all additional constraints on either iterator are preserved (and those are intersections).
    pub(crate) fn extend(mut self, mut other: IntersectionIter<'a, T>) -> Self {
        //edge-cases first:
        if self.sources.is_empty() {
            return other;
        } else if other.sources.is_empty() {
            //no-op
            return self;
        }

        if self.sources[0].iter.is_some() && other.sources[0].iter.is_some() {
            // We have two iterators, we can solve the whole problem lazily by just chaining them! \o/
            let iter = self.sources[0].iter.take().unwrap();
            let iter2 = other.sources[0].iter.take().unwrap();
            self.sources[0].iter = Some(Box::new(iter.chain(iter2)));
            self.sources[0].sorted = false; //we can no longer guarantee any sorting
        } else {
            if let Some(Cow::Borrowed(_)) = self.sources[0].array {
                //we have take ownership (= make a clone) because we'll be extending this array later
                self.sources[0]
                    .array
                    .as_mut()
                    .map(|array| *array = Cow::Owned(array.to_vec()));
            } else if self.sources[0].iter.is_some() {
                //similar as above: we have to consume our iterator first and turn it into an owned collection, otherwise we can't extend it
                let iter = self.sources[0].iter.take().unwrap();
                self.sources[0].array = Some(Cow::Owned(iter.collect()));
            }
            if let Some(Cow::Owned(array)) = self.sources[0].array.as_mut() {
                if other.sources[0].iter.is_some() {
                    let iter2 = other.sources[0].iter.take().unwrap();
                    array.extend(iter2);
                } else if let Some(array2) = other.sources[0].array.as_ref() {
                    array.extend(array2.iter().map(|x| *x));
                }
            }
            self.sources[0].sorted = false; //we can no longer guarantee any sorting
        }

        //copy any further constraints on the other iterator
        for (i, source) in other.sources.into_iter().enumerate() {
            if i > 0 && source.array.is_some() {
                self = self.with(source.array.unwrap(), source.sorted);
            }
        }
        self
    }
}

impl<'a, T> IntersectionIter<'a, T>
where
    T: Ord,
    T: Copy,
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn test_sources(&mut self, item: T, left_sorted: bool) -> Option<T> {
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
                if let Some(data) = &right.array {
                    if right.sorted {
                        //binary search
                        match data[self.cursors[i]..].binary_search(&item) {
                            Ok(index) => {
                                if left_sorted {
                                    // the left is sorted so we can discount 'lesser than' items from the right side
                                    // in subsequent iterations by moving the cursor ahead
                                    // this is an extra optimisation
                                    self.cursors[i] = index;
                                }
                                //item found! continue with next source to ensure item is also in there
                                continue;
                            }
                            Err(index) => {
                                found = false;
                                if left_sorted && index == data.len() {
                                    // because the left hand side is sorted and we're at the end of this right array,
                                    // we can abort the whole IntersectionIter, no more results will be found
                                    // this is an extra optimisation
                                    self.abort = true;
                                }
                                break;
                            }
                        }
                    } else {
                        //linear search
                        if !data.contains(&item) {
                            found = false;
                            break;
                        }
                    }
                } else {
                    unreachable!("IntersectionIter: only the first source may be an interator");
                }
            }
            if found {
                self.cursors[0] += 1;
                return Some(item);
            }
        }
        None
    }
}

impl<'a, T> Iterator for IntersectionIter<'a, T>
where
    T: Ord + Copy,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.abort || self.sources.is_empty() {
                return None;
            } else if let Some(item) = self.sources.get_mut(0).unwrap().take_item(self.cursors[0]) {
                //                                                                ^-- (cursor may or may not be used depending on source type)
                let result = self.test_sources(item, self.sources.get(0).unwrap().sorted);
                if result.is_some() {
                    return result;
                }
                //no results found, continue with next item (cursor may or may not be used)
                self.cursors[0] += 1;
            } else {
                return None;
            }
        }
    }
}

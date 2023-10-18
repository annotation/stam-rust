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
//! What can you do with this library?
//!
//! * Keep, build and manipulate an efficient in-memory store of texts and annotations on texts
//!    * Search annotations by data, textual content, relations between text fragments (overlap, embedding, adjacency, etc),
//!    * Search in text (incl. via regular expressions) and find annotations
//!    * Elementary text operations with regard for text offsets (splitting text on a delimiter, stripping text)
//!    * Search in data (set,key,value) and find annotations
//!    * Convert between different kind of offsets (absolute, relative to other structures, UTF-8 bytes vs unicode codepoints, etc)
//! * Read and write resources and annotations from/to STAM JSON, STAM CSV, or an optimised binary (CBOR) representation
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
//! * [`ResultItem<Annotation>`](Annotation)
//! * [`ResultItem<AnnotationDataSet>`](AnnotationDataSet)
//! * [`ResultItem<AnnotationData>`](AnnotationData)
//! * [`ResultItem<DataKey>`](DataKey)
//! * [`DataValue`]
//! * [`DataOperator`]
//! * [`ResultItem<TextResource>`](TextResource)
//! * [`ResultTextSelection`](TextSelection)
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
pub use store::{BuildItem, Request, ResultItem, StamResult, Storable, Store, StoreFor};
pub use text::Text;
pub use textselection::{
    ResultTextSelection, TestTextSelection, TextSelection, TextSelectionHandle,
    TextSelectionOperator, TextSelectionSet, TextSelectionSetIntoIter, TextSelectionSetIter,
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
    singleton: Option<T>,
    sorted: bool,
}

impl<'a, T> IntersectionSource<'a, T>
where
    T: Ord,
    T: Copy,
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn len(&self) -> Option<usize> {
        if let Some(array) = &self.array {
            return Some(array.len());
        } else if self.singleton.is_some() {
            return Some(1);
        }
        None
    }

    fn take_item(&mut self, index: usize) -> Option<T> {
        if let Some(array) = &self.array {
            return array.get(index).map(|x| *x);
        } else if let Some(iter) = self.iter.as_mut() {
            return iter.next();
        } else if self.singleton.is_some() {
            return self.singleton.take();
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
            singleton: None,
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
            singleton: None,
            sorted,
        };
        self.insert_source(source);
        self
    }

    pub(crate) fn with_singleton(mut self, data: T) -> Self {
        let source = IntersectionSource {
            array: None,
            iter: None,
            singleton: Some(data),
            sorted: true,
        };
        self.insert_source(source);
        self
    }

    #[inline]
    pub(crate) fn insert_source(&mut self, source: IntersectionSource<'a, T>) {
        if self.sources.is_empty() {
            self.sources.push(source);
            self.cursors.push(0);
        } else {
            //find insertion point
            let mut pos = 0;
            for refsource in self.sources.iter() {
                if source.len() == Some(1) {
                    //singletons before everything else
                    break;
                } else if refsource.array.is_none() {
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
            } else if let Some(singleton) = source.singleton {
                self = self.with_singleton(singleton)
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

    pub(crate) fn returns_sorted(&self) -> bool {
        if let Some(source) = self.sources.get(0) {
            source.sorted
        } else {
            true //empty iterators can be considered sorted
        }
    }
}

impl<'a, T> IntersectionIter<'a, T>
where
    T: Ord,
    T: Copy,
    [T]: ToOwned<Owned = Vec<T>>,
{
    #[inline]
    fn test_sources(&mut self, item: T, left_sorted: bool) -> Option<T> {
        if self.sources[1..].is_empty() {
            //only one source
            self.cursors[0] += 1;
            return Some(item);
        } else {
            let mut found = true; //falsify
            for (i, right) in self.sources.iter_mut().enumerate() {
                if i == 0 {
                    //the first item is the left iter
                    continue;
                };
                if let Some(data) = &right.singleton {
                    found = item == *data;
                    break;
                } else if let Some(data) = &right.array {
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
                } else if let Some(iter) = right.iter.as_mut() {
                    //iterators normally don't occur on the right hand side (because they can't be reused), EXCEPT when the left hand side is a singleton or array with length 1
                    for x in iter {
                        if x == item {
                            break;
                        }
                        if right.sorted && x > item {
                            found = false;
                            break;
                        }
                    }
                } else {
                    unreachable!("IntersectionIter: unknown type");
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

mod test {
    #![allow(unused_imports)]
    use super::IntersectionIter;
    use std::borrow::Cow;

    /*
    fn setup_example() -> Result<AnnotationStore, StamError> {
        let n = 100;
        let mut store = AnnotationStore::new(Config::default()).with_id("test");

        //artificial text with 100 Xs
        let mut text = String::with_capacity(n);
        for _ in 0..n {
            text.push('X');
        }
        store = store
            .add(TextResource::from_string(
                "testres",
                text,
                Config::default(),
            ))
            .unwrap()
            .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))
            .unwrap();

        for i in 0..n {
            store.annotate(
                AnnotationBuilder::new()
                    .with_target(SelectorBuilder::textselector(
                        "testres",
                        Offset::simple(i, i + 1),
                    ))
                    .with_id(format!("A{}", i))
                    .with_data("testdataset", "type", "X")
                    .with_data("testdataset", "n", i),
            )?;
        }

        Ok(store)
    }
    */

    #[test]
    fn test_intersectioniter_1() {
        let iter = IntersectionIter::new(Cow::Owned(vec![1, 2, 3, 4, 5]), true);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_intersectioniter_2() {
        let mut iter = IntersectionIter::new(Cow::Owned(vec![1, 2, 3, 4, 5]), true);
        iter = iter.with(Cow::Owned(vec![1, 3, 5, 7]), true);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![1, 3, 5]);
    }

    #[test]
    fn test_intersectioniter_3() {
        let mut iter = IntersectionIter::new(Cow::Owned(vec![1, 2, 3, 4, 5]), true);
        iter = iter.with(Cow::Owned(vec![1, 3, 5, 7]), true);
        iter = iter.with(Cow::Owned(vec![5]), true);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![5]);
    }

    #[test]
    fn test_intersectioniter_singleton() {
        let mut iter = IntersectionIter::new(Cow::Owned(vec![1, 2, 3, 4, 5]), true);
        iter = iter.with(Cow::Owned(vec![1, 3, 5, 7]), true);
        iter = iter.with_singleton(5);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![5]);
    }

    #[test]
    fn test_intersectioniter_unsorted() {
        let mut iter = IntersectionIter::new(Cow::Owned(vec![1, 2, 2, 4, 5]), false);
        iter = iter.with(Cow::Owned(vec![3, 1, 5, 7]), false);
        iter = iter.with(Cow::Owned(vec![5]), false);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![5]);
    }

    #[test]
    fn test_intersectioniter_borrow() {
        let v = vec![1, 2, 3, 4, 5];
        let v2 = vec![1, 3, 5];
        let mut iter = IntersectionIter::new(Cow::Borrowed(&v), true);
        iter = iter.with(Cow::Borrowed(&v2), true);
        iter = iter.with(Cow::Owned(vec![5]), true);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![5]);
    }

    #[test]
    fn test_intersectioniter_iter() {
        let v = vec![1, 2, 3, 4, 5];
        let mut iter = IntersectionIter::new_with_iterator(Box::new(v.iter().copied()), true);
        iter = iter.with(Cow::Owned(vec![1, 3, 5, 7]), true);
        iter = iter.with(Cow::Owned(vec![5]), true);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![5]);
    }

    #[test]
    fn test_intersectioniter_emptyresult() {
        let mut iter = IntersectionIter::new(Cow::Owned(vec![1, 2, 3, 4, 5]), true);
        iter = iter.with(Cow::Owned(vec![1, 3, 5, 7]), true);
        iter = iter.with(Cow::Owned(vec![2, 4]), true);
        let v: Vec<_> = iter.collect();
        let v2: Vec<u32> = Vec::new();
        assert_eq!(v, v2);
    }

    #[test]
    fn test_intersectioniter_merge() {
        let mut iter = IntersectionIter::new(Cow::Owned(vec![1, 2, 3, 4, 5]), true);
        iter = iter.with(Cow::Owned(vec![1, 3, 5]), true);
        let iter2 = IntersectionIter::new(Cow::Owned(vec![5, 6]), true);
        iter = iter.merge(iter2);
        iter = iter.with(Cow::Owned(vec![5]), true);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![5]);
    }

    #[test]
    fn test_intersectioniter_extend() {
        let mut iter = IntersectionIter::new(Cow::Owned(vec![1, 2, 3, 4, 5]), true);
        let iter2 = IntersectionIter::new(Cow::Owned(vec![6, 7]), true);
        iter = iter.extend(iter2);
        let v: Vec<_> = iter.collect();
        assert_eq!(v, vec![1, 2, 3, 4, 5, 6, 7]);
    }
}

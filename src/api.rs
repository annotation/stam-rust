/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This is the root module for the high-level API. See the different submodules for further
//! documentation regarding each STAM object.

mod annotation;
mod annotationdata;
mod annotationdataset;
mod annotationstore;
mod datakey;
mod query;
mod resources;
mod text;
mod textselection;

pub use annotation::*;
pub use annotationdata::*;
pub use annotationdataset::*;
pub use annotationstore::*;
pub use datakey::*;
pub use query::*;
pub use resources::*;
pub use text::*;
pub use textselection::*;

use crate::annotation::AnnotationHandle;
use crate::annotationdata::AnnotationDataHandle;
use crate::annotationdataset::AnnotationDataSetHandle;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKeyHandle;
use crate::datavalue::DataOperator;
use crate::resources::TextResourceHandle;
use crate::textselection::TextSelectionOperator;

use crate::store::*;
use crate::types::*;

use smallvec::SmallVec;
use std::borrow::Cow;
use std::fmt::Debug;

/// Holds a collection of items by handle. The collection may be either
/// owned or borrowed from the store (usually from a reverse index).
/// The items in the collection by definition refer to the [`AnnotationStore`], as
/// internally the collection only keeps handles and a reference to the store.
///
/// This structure is produced by calling a [`to_collection()`]. method
#[derive(Clone)]
pub struct Handles<'store, T>
where
    T: Storable,
{
    array: Cow<'store, [T::FullHandleType]>,
    /// Sorted by handle? (i.e. chronologically)
    sorted: bool,
    store: &'store AnnotationStore,
}

impl<'store, T> Debug for Handles<'store, T>
where
    T: Storable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = format!("Collection<{}>", T::typeinfo());
        f.debug_struct(s.as_str())
            .field("array", &self.array)
            .field("sorted", &self.sorted)
            .finish()
    }
}

pub type HandlesIter<'a, T> =
    std::iter::Copied<std::slice::Iter<'a, <T as Storable>::FullHandleType>>;

impl<'store, T> Handles<'store, T>
where
    T: Storable,
{
    pub fn returns_sorted(&self) -> bool {
        self.sorted
    }

    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }

    /// Low-level method to instantiate annotations from an existing vector of handles (either owned or borrowed from the store).
    /// Warning: Use of this function is dangerous and discouraged in most cases as there is no validity check on the handles you pass!
    pub fn new(
        array: Cow<'store, [T::FullHandleType]>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            array,
            sorted,
            store,
        }
    }

    pub fn from_iter(
        iter: impl Iterator<Item = T::FullHandleType>,
        store: &'store AnnotationStore,
    ) -> Self {
        let mut sorted = true;
        let mut v = Vec::new();
        let mut prev: Option<T::FullHandleType> = None;
        for item in iter {
            if let Some(p) = prev {
                if p > item {
                    sorted = false;
                }
            }
            v.push(item);
            prev = Some(item);
        }
        Self {
            array: Cow::Owned(v),
            sorted,
            store,
        }
    }

    /// Low-level method to take out the underlying vector of handles
    pub fn take(self) -> Cow<'store, [T::FullHandleType]>
    where
        Self: Sized,
    {
        self.array
    }

    /// Returns the number of items in this collection.
    pub fn len(&self) -> usize {
        self.array.len()
    }

    /// Returns the number of items in this collection.
    pub fn get(&self, index: usize) -> Option<T::FullHandleType> {
        self.array.get(index).copied()
    }

    /// Returns a boolean indicating whether the collection is empty or not.
    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    /// Tests if the collection contains a specific element
    pub fn contains(&self, handle: &T::FullHandleType) -> bool {
        if self.sorted {
            match self.array.binary_search(&handle) {
                Ok(_) => true,
                Err(_) => false,
            }
        } else {
            self.array.contains(&handle)
        }
    }

    /// Tests if the collection contains a specific element and returns the index
    pub fn position(&self, handle: &T::FullHandleType) -> Option<usize> {
        if self.sorted {
            match self.array.binary_search(&handle) {
                Ok(index) => Some(index),
                Err(_) => None,
            }
        } else {
            self.array.iter().position(|x| x == handle)
        }
    }

    /// Returns an iterator over the low-level handles in this collection
    pub fn iter<'a>(&'a self) -> HandlesIter<'a, T> {
        self.array.iter().copied()
    }

    /// Computes the union between two collections, retains order and ensures there are no duplicates
    /// Modifies the collection in-place to accommodate the other
    pub fn union(&mut self, other: &Self) {
        match other.len() {
            0 => return,                                    //edge-case
            1 => self.add(other.handles().next().unwrap()), //edge-case
            _ => {
                let mut updated = false;
                let mut offset = 0;
                for item in other.handles() {
                    if self.sorted && other.sorted {
                        //optimisation if both are sorted
                        match self.array[offset..].binary_search(&item) {
                            Ok(index) => offset = index + 1,
                            Err(index) => {
                                offset = index + 1;
                                updated = true;
                                self.add_unchecked(item);
                            }
                        }
                    } else {
                        if !self.contains(&item) {
                            //will do either binary or linear search
                            updated = true;
                            self.add_unchecked(item);
                        }
                    }
                }
                if self.sorted && updated {
                    //resort
                    self.array.to_mut().sort_unstable();
                }
            }
        }
    }

    /// Computes the intersection between two collections, retains order
    /// Modifies the collection in-place to match the other
    pub fn intersection(&mut self, other: &Self) {
        match (self.len(), other.len()) {
            (0, _) | (_, 0) =>
            //edge-case: empty result because one of the collections is empty
            {
                self.array.to_mut().clear();
                return;
            }
            (len, otherlen) => {
                if len == otherlen && self.sorted && other.sorted {
                    //edge-case: number of elements are equal, check if all elements are equal
                    if self.handles().zip(other.handles()).all(|(x, y)| x == y) {
                        return;
                    }
                } else if otherlen < len {
                    //check if we need to modify the vector in place or if we can just copy the other
                    if self.contains_subset(other) {
                        self.array = other.array.clone(); //may be cheap if borrowed, expensive if owned
                        return;
                    }
                } else if len < otherlen {
                    //check if we need to modify the vector in place or if we can just copy the other
                    if other.contains_subset(self) {
                        return; //nothing to do
                    }
                }
            }
        }
        let mut offset = 0;
        // this takes ownership and will clone the array if it was borrowed
        self.array.to_mut().retain(|x| {
            if self.sorted && other.sorted {
                //optimisation if both are sorted
                match other.array[offset..].binary_search(x) {
                    Ok(index) => {
                        offset = index + 1;
                        true
                    }
                    Err(index) => {
                        offset = index + 1;
                        false
                    }
                }
            } else {
                other.contains(x) //will do either binary or linear search
            }
        });
    }

    /// Checks if the collections contains another (need not be contingent)
    pub fn contains_subset(&self, subset: &Self) -> bool {
        for handle in subset.handles() {
            if !self.contains(&handle) {
                return false;
            }
        }
        true
    }

    /// Sorts the collection in chronological order (i.e. the handles are sorted)
    /// Note that this is *NOT* the same as textual order.
    pub fn sort(&mut self) {
        if !self.sorted {
            self.array.to_mut().sort_unstable();
            self.sorted = true;
        }
    }

    /// Adds an item to the collection, this does *NOT* check for duplicates and MAY invalidate any existing sorting
    pub(crate) fn add_unchecked(&mut self, item: T::FullHandleType) {
        self.array.to_mut().push(item);
    }

    /// Adds an item to the collection, this checks for duplicates and respects existing sorting (if any),
    /// so this comes with performance overhead. Use [`Self.union()`] to add multiple items at once more efficiently.
    pub fn add(&mut self, item: T::FullHandleType) {
        if self.sorted {
            if let Err(pos) = self.array.binary_search(&item) {
                self.array.to_mut().insert(pos, item)
            }
        } else {
            if !self.contains(&item) {
                self.array.to_mut().push(item);
            }
        }
    }
}

pub trait TestableIterator: Iterator
where
    Self: Sized,
{
    /// Returns true if the iterator has items, false otherwise
    fn test(mut self) -> bool {
        self.next().is_some()
    }
}

pub trait AbortableIterator: Iterator
where
    Self: Sized,
{
    /// Set the iterator to abort, no further results will be returned
    fn abort(&mut self);
}

// Auxiliary data structures the API relies on internally:

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub(crate) enum FilterMode {
    Any,
    All,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub(crate) enum TextMode {
    Exact,
    Lowercase,
}

#[derive(Debug)]
/// A filter that is evaluated lazily, applied on [`AnnotationsIter`], [`DataIter`],[`TextSelectionsIter`]
pub(crate) enum Filter<'a> {
    AnnotationData(AnnotationDataSetHandle, AnnotationDataHandle),
    AnnotationDataSet(AnnotationDataSetHandle),
    DataKey(AnnotationDataSetHandle, DataKeyHandle),
    Annotation(AnnotationHandle),
    TextResource(TextResourceHandle),
    DataOperator(DataOperator<'a>),
    TextSelectionOperator(TextSelectionOperator),
    Annotations(Annotations<'a>),
    Data(Data<'a>, FilterMode),
    Text(String, TextMode, &'a str), //the last string represents the delimiter for joining text

    //these have the advantage the collections are external references
    BorrowedAnnotations(&'a Annotations<'a>),
    BorrowedData(&'a Data<'a>, FilterMode),
    BorrowedText(&'a str, TextMode, &'a str), //the last string represents the delimiter for joining text
}

pub(crate) enum ResultItemIterSource<'store, T>
where
    T: Storable,
{
    None,
    HandlesArray(Handles<'store, T>), //internally, the array of handles is either owned or borrowed from the store
    HandlesIter(Box<dyn Iterator<Item = T::FullHandleType> + 'store>), //MAYBE TODO: this lifetime bound may technically be not exactly right
}

/// Iterator over the items that can be turned into ResultItem
pub struct ResultItemIter<'store, T>
where
    T: Storable,
{
    source: ResultItemIterSource<'store, T>,
    store: &'store AnnotationStore,
    sorted: bool,
}

impl<'store, T> Iterator for ResultItemIter<'store, T>
where
    T: Storable,
{
    type Item = ResultItem<'store, T>;

    fn next(&mut self) -> Option<Self::Item> {
        if let ResultItemIterSource::HandlesArray(handles) = self.source {
            let cursor = self.cursor;
            self.cursor += 1;
            handles.get(cursor)
        } else if let ResultItemIterSource::HandlesIter(iter) = &mut self.source {
            iter.next()
        }
    }
}

impl<'store, T: Storable> ResultItemIter<'store, T> {
    pub fn from_collection(collection: Handles<'store, T>) -> Self {
        collection.into()
    }

    /// Borrows from a collection held by the AnnotationStore
    pub(crate) fn borrow_from(
        array: &'store [T],
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self {
        Self::from_collection(Handles {
            array: Cow::Borrowed(array),
            sorted,
            store,
        })
    }

    ///Returns a dummy iterator
    pub fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            source: ResultItemIterSource::None,
            sorted: false,
            store,
        }
    }
}

impl<'store, T: Storable> From<Handles<'store, T>> for ResultItemIterSource<'store, T> {
    fn from(value: Handles<'store, T>) -> Self {
        Self {
            store: value.store(),
            sorted: value.returns_sorted(),
            source: ResultItemIterSource::Handles(value),
        }
    }
}

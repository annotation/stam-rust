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

#[cfg(feature = "webanno")]
mod webanno;

#[cfg(feature = "transpose")]
mod transpose;

pub use annotation::*;
pub use annotationdata::*;
pub use datakey::*;
pub use query::*;
pub use resources::*;
pub use text::*;
pub use textselection::*;

#[cfg(feature = "webanno")]
pub use webanno::*;

#[cfg(feature = "transpose")]
pub use transpose::*;

use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::AnnotationDataSetHandle;
use crate::annotationstore::AnnotationStore;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::DataOperator;
use crate::resources::{TextResource, TextResourceHandle};
use crate::textselection::{TextSelection, TextSelectionOperator};

use crate::{store::*, TextSelectionHandle};

use regex::Regex;
use std::borrow::Cow;
use std::fmt::Debug;
use std::marker::PhantomData;

/// Holds a collection of items. The collection may be either
/// owned or borrowed from the store (usually from a reverse index).
///
/// The items in the collection by definition refer to the [`AnnotationStore`], as
/// internally the collection only keeps *fully qualified* handles and a reference to the store.
///
/// This structure is produced via the [`ToHandles`] trait that is implemented
/// for all iterators over [`ResultItem<T>`].
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

/// Iterator over the handles in a [`Handles<T>`] collection.
pub type HandlesIter<'a, T> =
    std::iter::Copied<std::slice::Iter<'a, <T as Storable>::FullHandleType>>;

impl<'store, T> Handles<'store, T>
where
    T: Storable,
{
    /// Are the items in this collection sorted (chronologically, i.e. by handle) or not?
    pub fn returns_sorted(&self) -> bool {
        self.sorted
    }

    /// Returns a reference to the underlying [`AnnotationStore`].
    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }

    /// Low-level method to instantiate annotations from an existing vector of handles (either owned or borrowed from the store).
    /// Warning: Use of this function is dangerous and discouraged in most cases as there is no validity check on the handles you pass!
    ///
    /// The safe option is to convert from an iterator of [`ResultItem<T>`] to [`Handles<T>`], then use [`ToHandles::to_handles()`] on that
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

    /// Returns a new empty handles collection
    pub fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            array: Cow::Owned(Vec::new()),
            sorted: false,
            store,
        }
    }

    /// Create a new handles collection from an iterator handles
    /// Warning: Use of this function is dangerous and discouraged in most cases as there is no validity check on the handles you pass!
    ///
    /// If you want to convert from an iterator of [`ResultItem<T>`] to [`Handles<T>`], then use [`ToHandles::to_handles()`] on that
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
    /// If you want to iterate over the actual items ([`ResultItem<T>`]), then use [`Self::items()`] instead.
    pub fn iter<'a>(&'a self) -> HandlesIter<'a, T> {
        self.array.iter().copied()
    }

    /// Returns an iterator over the high-level items in this collection
    /// If you want to iterate over the low-level handles, then use [`Self::iter()`] instead.
    pub fn items<'a>(&'a self) -> FromHandles<'store, T, HandlesIter<'store, T>>
    where
        'a: 'store,
    {
        FromHandles::new(self.iter(), self.store())
    }

    /// Computes the union between two collections, retains order and ensures there are no duplicates
    /// Modifies the collection in-place to accommodate the other
    pub fn union(&mut self, other: &Self) {
        match other.len() {
            0 => return,                                 //edge-case
            1 => self.add(other.iter().next().unwrap()), //edge-case
            _ => {
                let mut updated = false;
                let mut offset = 0;
                for item in other.iter() {
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
                    if self.iter().zip(other.iter()).all(|(x, y)| x == y) {
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
        for handle in subset.iter() {
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

pub struct OwnedHandlesIter<'store, T>
where
    T: Storable,
{
    handles: Handles<'store, T>,
    cursor: usize,
}

impl<'store, T> Iterator for OwnedHandlesIter<'store, T>
where
    T: Storable,
{
    type Item = T::FullHandleType;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(handle) = self.handles.get(self.cursor) {
            self.cursor += 1;
            Some(handle)
        } else {
            None
        }
    }
}

impl<'store, T> IntoIterator for Handles<'store, T>
where
    T: Storable,
{
    type Item = T::FullHandleType;
    type IntoIter = OwnedHandlesIter<'store, T>;
    fn into_iter(self) -> Self::IntoIter {
        OwnedHandlesIter {
            handles: self,
            cursor: 0,
        }
    }
}

/// This internal trait is implemented for various forms of [`FromHandles<'store,T>`]
pub(crate) trait FullHandleToResultItem<'store, T>
where
    T: Storable,
{
    fn get_item(&self, handle: T::FullHandleType) -> Option<ResultItem<'store, T>>;
}

/// Iterator that turns iterators over full handles into [`ResultItem<T>`], holds a reference to the [`AnnotationStore`]
pub struct FromHandles<'store, T, I>
where
    T: Storable + 'store,
    I: Iterator<Item = T::FullHandleType>,
{
    inner: I,
    store: &'store AnnotationStore,
    _marker: PhantomData<T>, //zero-size, only needed to bind generic T
}

impl<'store, T, I> FromHandles<'store, T, I>
where
    T: Storable + 'store,
    I: Iterator<Item = T::FullHandleType>,
{
    pub fn new(inner: I, store: &'store AnnotationStore) -> Self {
        Self {
            inner,
            store,
            _marker: PhantomData,
        }
    }
}

impl<'store, T, I> Iterator for FromHandles<'store, T, I>
where
    T: Storable + 'store,
    I: Iterator<Item = T::FullHandleType>,
    Self: FullHandleToResultItem<'store, T>,
{
    type Item = ResultItem<'store, T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(full_handle) = self.inner.next() {
                let item = self.get_item(full_handle);
                if item.is_none() {
                    continue; //invalid handles are ignored
                } else {
                    return item;
                }
            } else {
                return None;
            }
        }
    }
}

/// This trait is implemented on iterators over [`ResultItem<T>`] and turns effectively collects
/// these items, by only their handles and a reference to a store, as [`Handles<T>`].
/// It is implemented alongside traits like [`AnnotationIterator`], [`DataIterator`], etc...
pub trait ToHandles<'store, T>
where
    T: Storable,
{
    /// Convert an iterator over [`ResultItem<T>`] to [`Handles<T>`].
    fn to_handles(&mut self, store: &'store AnnotationStore) -> Handles<'store, T>;
}

impl<'store, T, I> ToHandles<'store, T> for I
where
    T: Storable + 'store,
    I: Iterator<Item = ResultItem<'store, T>>,
    ResultItem<'store, T>: FullHandle<T>,
{
    fn to_handles(&mut self, store: &'store AnnotationStore) -> Handles<'store, T> {
        Handles::from_iter(self.map(|item| item.fullhandle()), store)
    }
}

/// This iterator implements a simple `.test()` method that just checks whether an iterator is
/// empty or yields results. It is implemented alongside traits like [`AnnotationIterator`],
/// [`DataIterator`], etc...
pub trait TestableIterator: Iterator
where
    Self: Sized,
{
    /// Returns true if the iterator has items, false otherwise
    fn test(mut self) -> bool {
        self.next().is_some()
    }
}

impl<I> TestableIterator for I where I: Iterator {} //blanket implementation

/// An iterator that may be sorted or not and knows a-priori whether it is or not.
pub trait MaybeSortedIterator: Iterator {
    /// Does this iterator return items in sorted order?
    fn returns_sorted(&self) -> bool;
}

/// An iterator that may be sorted or not and knows a-priori whether it is or not, it may also be a completely empty iterator.
pub struct ResultIter<I>
where
    I: Iterator,
{
    inner: Option<I>,
    sorted: bool,
}

impl<I: Iterator> MaybeSortedIterator for ResultIter<I> {
    fn returns_sorted(&self) -> bool {
        self.sorted
    }
}

impl<I: Iterator> ResultIter<I> {
    pub(crate) fn new(inner: I, sorted: bool) -> Self {
        Self {
            inner: Some(inner),
            sorted,
        }
    }

    /// This does no sorting, it just tells that the iterator passed is sorted
    pub(crate) fn new_sorted(inner: I) -> Self {
        Self {
            inner: Some(inner),
            sorted: true,
        }
    }

    /// This tells that the iterator passed is not sorted
    pub(crate) fn new_unsorted(inner: I) -> Self {
        Self {
            inner: Some(inner),
            sorted: false,
        }
    }

    /// Creates a dummy iterator
    pub(crate) fn new_empty() -> Self {
        Self {
            inner: None,
            sorted: true,
        }
    }
}

impl<I: Iterator> Iterator for ResultIter<I> {
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = self.inner.as_mut() {
            inner.next()
        } else {
            None
        }
    }
}

pub struct FilterAllIter<'store, T, I>
where
    T: Storable + 'store,
    I: Iterator<Item = ResultItem<'store, T>>,
{
    inner: I,
    filter: Handles<'store, T>,
    store: &'store AnnotationStore,
    buffer: Option<Handles<'store, T>>,
    cursor: usize,
}

impl<'store, T, I> FilterAllIter<'store, T, I>
where
    T: Storable + 'store,
    I: Iterator<Item = ResultItem<'store, T>>,
    I: ToHandles<'store, T>,
{
    pub fn new(inner: I, filter: Handles<'store, T>, store: &'store AnnotationStore) -> Self {
        Self {
            inner,
            filter,
            store,
            buffer: None,
            cursor: 0,
        }
    }
}

impl<'store, T, I> Iterator for FilterAllIter<'store, T, I>
where
    T: Storable + 'store,
    I: Iterator<Item = ResultItem<'store, T>>,
    Self: FullHandleToResultItem<'store, T>,
    I: ToHandles<'store, T>,
{
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_none() {
            let buffer = self.inner.to_handles(self.store);
            if !self.filter.iter().all(|h| buffer.contains(&h)) {
                //constraint not satisfied, iterator yields nothing:
                //all filter items must be in the buffer
                return None;
            }
            self.buffer = Some(buffer);
        }
        //drain the buffer
        let buffer = self
            .buffer
            .as_mut()
            .expect("buffer must exist at this point");

        if let Some(handle) = buffer.get(self.cursor) {
            self.cursor += 1;
            self.get_item(handle)
        } else {
            None
        }
    }
}

// Auxiliary data structures the API relies on internally:

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
/// This determines how a filter is applied when there the filter is provided with multiple
/// reference instances to match against. It determines if the filter requires a match with
/// any of the instances (default), or with all of them.
pub enum FilterMode {
    /// The filter succeeds if any match is found.
    Any,

    /// The filter only succeeds if all reference instances are matched.
    /// Note: This might not make sense in some contexts!
    All,
}

impl Default for FilterMode {
    fn default() -> Self {
        Self::Any
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
/// Determines whether a text search is exact (case sensitive) or case insensitive.
pub enum TextMode {
    Exact,
    CaseInsensitive,
}

impl Default for TextMode {
    fn default() -> Self {
        Self::Exact
    }
}

#[derive(Debug)]
/// This is a low-level data structure that holds filter states for the iterators [`FilteredAnnotations`], [`FilteredData`], [`FilteredResources`],[`FilteredTextSelections`].
/// You likely do not need this and should use the appropriate `filter_*` methods on the iterators instead.
/// The only possible use from outside is in programmatically setting direct query constraints via [`Constraint::Filter`].
pub(crate) enum Filter<'store> {
    AnnotationData(
        AnnotationDataSetHandle,
        AnnotationDataHandle,
        SelectionQualifier,
    ),
    AnnotationDataSet(AnnotationDataSetHandle, SelectionQualifier),
    DataKey(AnnotationDataSetHandle, DataKeyHandle, SelectionQualifier),
    DataKeyAndOperator(
        AnnotationDataSetHandle,
        DataKeyHandle,
        DataOperator<'store>,
        SelectionQualifier,
    ),

    Annotation(AnnotationHandle, SelectionQualifier, AnnotationDepth),

    TextResource(TextResourceHandle, SelectionQualifier),

    DataOperator(DataOperator<'store>, SelectionQualifier),
    TextSelectionOperator(TextSelectionOperator, SelectionQualifier),

    Annotations(
        Handles<'store, Annotation>,
        FilterMode,
        SelectionQualifier,
        AnnotationDepth,
    ),
    Resources(
        Handles<'store, TextResource>,
        FilterMode,
        SelectionQualifier,
    ),
    Data(
        Handles<'store, AnnotationData>,
        FilterMode,
        SelectionQualifier,
    ),
    Keys(Handles<'store, DataKey>, FilterMode, SelectionQualifier),
    Text(String, TextMode, &'store str), //the last string represents the delimiter for joining text
    Regex(Regex, &'store str),           //the last string represents the delimiter for joining text
    TextSelection(TextResourceHandle, TextSelectionHandle),
    TextSelections(Handles<'store, TextSelection>, FilterMode),

    //these have the advantage the collections are external references
    BorrowedAnnotations(
        &'store Annotations<'store>,
        FilterMode,
        SelectionQualifier,
        AnnotationDepth,
    ),
    BorrowedData(
        &'store Handles<'store, AnnotationData>,
        FilterMode,
        SelectionQualifier,
    ),
    BorrowedKeys(
        &'store Handles<'store, DataKey>,
        FilterMode,
        SelectionQualifier,
    ),
    BorrowedText(&'store str, TextMode, &'store str), //the last string represents the delimiter for joining text
    BorrowedResources(
        &'store Handles<'store, TextResource>,
        FilterMode,
        SelectionQualifier,
    ),
}

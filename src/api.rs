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

use std::borrow::Cow;
use std::fmt::Debug;

/// Holds a collection of items. The collection may be either
/// owned or borrowed from the store (usually from a reverse index).
/// The items in the collection by definition refer to the [`AnnotationStore`], as
/// internally the collection only keeps handles and a reference to the store.
///
/// This structure is produced by calling a [`to_collection()`]. method
#[derive(Clone)]
pub struct Collection<'store, T>
where
    T: Storable,
{
    array: Cow<'store, [T::FullHandleType]>,
    /// Sorted by handle? (i.e. chronologically)
    sorted: bool,
    store: &'store AnnotationStore,
}

impl<'store, T> Debug for Collection<'store, T>
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

impl<'store, T> Collection<'store, T>
where
    T: Storable,
{
    pub fn returns_sorted(&self) -> bool {
        self.sorted
    }

    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }

    /// Low-level method to instantiate annotations from an existing vector of handles (either owned or borrowed).
    /// Warning: Use of this function is dangerous and discouraged in most cases as there is no validity check on the handles you pass!
    pub fn from_handles(
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

    /// Returns a boolean indicating whether the collection is empty or not.
    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    /// Tests if the collection contains a specific element
    pub fn contains(&self, handle: &T::FullHandleType) -> bool {
        if self.returns_sorted() {
            match self.array.binary_search(&handle) {
                Ok(_) => true,
                Err(_) => false,
            }
        } else {
            self.array.contains(&handle)
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

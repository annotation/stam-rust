/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`DataKey`]. This API is implemented on
//! [`ResultItem<DataKey>`].

use std::collections::BTreeSet;

use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::api::*;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::resources::TextResource;
use crate::store::*;

impl<'store> FullHandle<DataKey> for ResultItem<'store, DataKey> {
    fn fullhandle(&self) -> <DataKey as Storable>::FullHandleType {
        (self.set().handle(), self.handle())
    }
}

/// This is the implementation of the high-level API for [`DataKey`].
impl<'store> ResultItem<'store, DataKey> {
    /// Returns a reference to the dataset that holds this key
    pub fn set(&self) -> ResultItem<'store, AnnotationDataSet> {
        let rootstore = self.rootstore();
        self.store().as_resultitem(rootstore, rootstore)
    }

    /// Returns the public identifier that identifies the key
    pub fn as_str(&self) -> &'store str {
        self.as_ref().as_str()
    }

    /// Returns an iterator over all data ([`crate::AnnotationData`]) that makes use of this key.
    /// Use methods on this iterator like [`DataIter.filter_value()`] to further constrain the results.
    pub fn data(&self) -> MaybeIter<impl Iterator<Item = ResultItem<'store, AnnotationData>>> {
        let store = self.store();
        if let Some(vec) = store.data_by_key(self.handle()) {
            let iter = vec
                .iter()
                .map(|datahandle| (store.handle().unwrap(), *datahandle));
            MaybeIter::new_sorted(FromHandles::new(iter, self.rootstore()))
        } else {
            MaybeIter::new_empty()
        }
    }

    /// Returns an iterator over all annotations ([`crate::Annotation`]) that make use of this key.
    pub fn annotations(&self) -> MaybeIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let set_handle = self.store().handle().expect("set must have handle");
        let annotationstore = self.rootstore();
        let annotations: Vec<_> = annotationstore.annotations_by_key(set_handle, self.handle()); //MAYBE TODO: extra reverse index so we can borrow directly?
        MaybeIter::new_sorted(FromHandles::new(annotations.into_iter(), self.rootstore()))
    }

    /// Returns the number of annotations that make use of this key.
    /// Note: this method has suffix `_count` instead of `_len` because it is not O(1) but does actual counting (O(n) at worst).
    /// (This function internally allocates a temporary buffer to ensure no duplicates are returned)
    pub fn annotations_count(&self) -> usize {
        self.rootstore()
            .annotations_by_key(
                self.store().handle().expect("set must have handle"),
                self.handle(),
            )
            .len()
    }

    /// Tests whether two DataKeys are the same
    pub fn test(&self, other: impl Request<DataKey>) -> bool {
        if other.any() {
            true
        } else {
            self.handle() == other.to_handle(self.store()).expect("key must have handle")
        }
    }

    /// Returns an set of all text resources ([`crate::TextResource`]) that make use of this key via annotations (either as metadata or on text)
    pub fn resources(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources().map(|resource| resource.clone()))
            .flatten()
            .collect()
    }

    /// Returns a set of all text resources ([`crate::TextResource`]) that make use of this key as metadata (via annotations with a ResourceSelector)
    pub fn resources_as_metadata(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources_as_metadata())
            .flatten()
            .collect()
    }

    /// Returns a set of all text resources ([`crate::TextResource`]) that make use of this key via annotations via a ResourceSelector (i.e. as metadata)
    pub fn resources_on_text(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources_on_text())
            .flatten()
            .collect()
    }

    /// Returns a set of all data sets ([`crate::AnnotationDataSet`]) that annotations using this key reference via a DataSetSelector (i.e. metadata)
    pub fn datasets(&self) -> BTreeSet<ResultItem<'store, AnnotationDataSet>> {
        self.annotations()
            .map(|annotation| annotation.datasets().map(|dataset| dataset.clone()))
            .flatten()
            .collect()
    }
}

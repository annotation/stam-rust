/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`DataKey`]. This API is implemented on
//! [`ResultItem<DataKey>`].

use rayon::iter::IntoParallelIterator;
use std::collections::BTreeSet;
use std::ops::Deref;

use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::api::*;
use crate::datakey::DataKey;
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
    pub fn data(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, AnnotationData>>> {
        let store = self.store();
        if let Some(vec) = store.data_by_key(self.handle()) {
            let iter = vec
                .iter()
                .map(|datahandle| (store.handle().unwrap(), *datahandle));
            ResultIter::new_sorted(FromHandles::new(iter, self.rootstore()))
        } else {
            ResultIter::new_empty()
        }
    }

    /// Returns an iterator over all annotations ([`crate::Annotation`]) that make use of this key.
    pub fn annotations(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let set_handle = self.store().handle().expect("set must have handle");
        let annotationstore = self.rootstore();
        let annotations: Vec<_> = annotationstore.annotations_by_key(set_handle, self.handle()); //MAYBE TODO: extra reverse index so we can borrow directly? (the reversee index has been reserved in the struct but not in use yet)
        ResultIter::new_sorted(FromHandles::new(annotations.into_iter(), self.rootstore()))
    }

    /// Returns an iterator over all annotations about this datakey, i.e. Annotations with a DataKeySelector.
    /// Such annotations can be considered metadata.
    pub fn annotations_as_metadata(
        &self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        if let Some(annotations) = self
            .rootstore()
            .annotations_by_key_metadata(self.set().handle(), self.handle())
        {
            ResultIter::new_sorted(FromHandles::new(
                annotations.iter().copied(),
                self.rootstore(),
            ))
        } else {
            ResultIter::new_empty()
        }
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

    /// Returns a set of all text resources ([`crate::TextResource`]) that make use of this key as metadata (via annotations with a ResourceSelector)
    pub fn resources_as_metadata(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources_as_metadata())
            .flatten()
            .collect()
    }

    /// Returns a set of all text resources ([`crate::TextResource`]) that make use of this key via annotations via a ResourceSelector (i.e. as metadata)
    pub fn resources(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources())
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

/// Holds a collection of [`DataKey`] (by reference to an [`AnnotationStore`] and handles). This structure is produced by calling
/// [`ToHandles::to_handles()`], which is available on all iterators over keys.
pub type Keys<'store> = Handles<'store, DataKey>;

impl<'store, I> FullHandleToResultItem<'store, DataKey> for FromHandles<'store, DataKey, I>
where
    I: Iterator<Item = (AnnotationDataSetHandle, DataKeyHandle)>,
{
    fn get_item(
        &self,
        handle: (AnnotationDataSetHandle, DataKeyHandle),
    ) -> Option<ResultItem<'store, DataKey>> {
        if let Some(dataset) = self.store.dataset(handle.0) {
            dataset.key(handle.1)
        } else {
            None
        }
    }
}

impl<'store, I> FullHandleToResultItem<'store, DataKey> for FilterAllIter<'store, DataKey, I>
where
    I: Iterator<Item = ResultItem<'store, DataKey>>,
{
    fn get_item(
        &self,
        handle: (AnnotationDataSetHandle, DataKeyHandle),
    ) -> Option<ResultItem<'store, DataKey>> {
        if let Some(dataset) = self.store.dataset(handle.0) {
            dataset.key(handle.1)
        } else {
            None
        }
    }
}

pub trait KeyIterator<'store>: Iterator<Item = ResultItem<'store, DataKey>>
where
    Self: Sized,
{
    fn parallel(self) -> rayon::vec::IntoIter<ResultItem<'store, DataKey>> {
        let annotations: Vec<_> = self.collect();
        annotations.into_par_iter()
    }

    /// Iterate over the annotations that make use of data in this iterator.
    /// Annotations will be returned chronologically (add `.textual_order()` to sort textually) and contain no duplicates.
    fn annotations(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self.map(|key| key.annotations()).flatten().collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultIter::new_sorted(annotations.into_iter())
    }

    /// Iterates over all the annotations for all keys in this iterator.
    /// This only returns annotations that target the key via a DataKeySelector, i.e.
    /// the annotations provide metadata for the keys.
    ///
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    fn annotations_as_metadata(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|key| key.annotations_as_metadata())
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultIter::new_sorted(annotations.into_iter())
    }

    fn filter_handle(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredKeys<'store, Self> {
        FilteredKeys {
            inner: self,
            filter: Filter::DataKey(set, key, SelectionQualifier::Normal),
        }
    }

    fn filter_key(self, key: &ResultItem<'store, DataKey>) -> FilteredKeys<'store, Self> {
        FilteredKeys {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_set(self, set: &ResultItem<'store, AnnotationDataSet>) -> FilteredKeys<'store, Self> {
        FilteredKeys {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_set_handle(self, set: AnnotationDataSetHandle) -> FilteredKeys<'store, Self> {
        FilteredKeys {
            inner: self,
            filter: Filter::AnnotationDataSet(set, SelectionQualifier::Normal),
        }
    }

    fn filter_any(self, keys: Keys<'store>) -> FilteredKeys<'store, Self> {
        FilteredKeys {
            inner: self,
            filter: Filter::Keys(keys, FilterMode::Any, SelectionQualifier::Normal),
        }
    }

    fn filter_any_byref(self, keys: &'store Keys<'store>) -> FilteredKeys<'store, Self> {
        FilteredKeys {
            inner: self,
            filter: Filter::BorrowedKeys(keys, FilterMode::Any, SelectionQualifier::Normal),
        }
    }

    /// Constrain this iterator to filter on *all* of the mentioned keys, that is.
    /// If not all of the items in the parameter exist in the iterator, the iterator returns nothing.
    fn filter_all(
        self,
        keys: Keys<'store>,
        store: &'store AnnotationStore,
    ) -> FilterAllIter<'store, DataKey, Self> {
        FilterAllIter::new(self, keys, store)
    }

    fn filter_one(self, data: &ResultItem<'store, AnnotationData>) -> FilteredKeys<'store, Self> {
        FilteredKeys {
            inner: self,
            filter: Filter::AnnotationData(
                data.set().handle(),
                data.handle(),
                SelectionQualifier::Normal,
            ),
        }
    }

    fn filter_annotation(
        self,
        annotation: &ResultItem<'store, Annotation>,
    ) -> FilteredKeys<'store, Self> {
        self.filter_annotation_handle(annotation.handle())
    }

    fn filter_annotation_handle(self, annotation: AnnotationHandle) -> FilteredKeys<'store, Self> {
        FilteredKeys {
            inner: self,
            filter: Filter::Annotation(
                annotation,
                SelectionQualifier::Normal,
                AnnotationDepth::default(),
            ),
        }
    }
}

impl<'store, I> KeyIterator<'store> for I
where
    I: Iterator<Item = ResultItem<'store, DataKey>>,
{
    //blanket implementation
}

pub struct FilteredKeys<'store, I>
where
    I: Iterator<Item = ResultItem<'store, DataKey>>,
{
    inner: I,
    filter: Filter<'store>,
}

impl<'store, I> Iterator for FilteredKeys<'store, I>
where
    I: Iterator<Item = ResultItem<'store, DataKey>>,
{
    type Item = ResultItem<'store, DataKey>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.inner.next() {
                if self.test_filter(&item) {
                    return Some(item);
                }
            } else {
                return None;
            }
        }
    }
}

impl<'store, I> FilteredKeys<'store, I>
where
    I: Iterator<Item = ResultItem<'store, DataKey>>,
{
    #[allow(suspicious_double_ref_op)]
    fn test_filter(&self, key: &ResultItem<'store, DataKey>) -> bool {
        match &self.filter {
            Filter::DataKey(set_handle, key_handle, _) => {
                key.handle() == *key_handle && key.set().handle() == *set_handle
            }
            Filter::AnnotationDataSet(set_handle, _) => key.set().handle() == *set_handle,
            Filter::Annotations(annotations, FilterMode::Any, SelectionQualifier::Normal, _) => {
                key.annotations().filter_any_byref(annotations).test()
            }
            Filter::Annotations(annotations, FilterMode::All, SelectionQualifier::Normal, _) => key
                .annotations()
                .filter_all(annotations.clone(), key.rootstore())
                .test(),
            Filter::BorrowedAnnotations(
                annotations,
                FilterMode::Any,
                SelectionQualifier::Normal,
                _,
            ) => key.annotations().filter_any_byref(annotations).test(),
            Filter::BorrowedAnnotations(
                annotations,
                FilterMode::All,
                SelectionQualifier::Normal,
                _,
            ) => key
                .annotations()
                .filter_all(annotations.deref().clone(), key.rootstore())
                .test(),
            Filter::Keys(_, FilterMode::All, _) => {
                unreachable!("not handled by this iterator but by FilterAllIter")
            }
            Filter::BorrowedKeys(_, FilterMode::All, _) => {
                unreachable!("not handled by this iterator but by FilterAllIter")
            }
            _ => unreachable!("Filter {:?} not implemented for FilteredKeys", self.filter),
        }
    }
}

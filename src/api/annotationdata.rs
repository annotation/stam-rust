/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`AnnotationData`]. This API is implemented on
//! [`ResultItem<AnnotationData>`]. Moreover, it defines and implements the [`DataIter`] iterator to iterate over annotations,
//! which also exposes a rich API. Last, it defines and implements [`Data`], which is a simple collection of AnnotationData,
//! and can be iterated over.

use crate::annotation::Annotation;
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::api::*;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::resources::TextResource;
use crate::store::*;
use crate::Filter;
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::ops::Deref;

impl<'store> FullHandle<AnnotationData> for ResultItem<'store, AnnotationData> {
    fn fullhandle(&self) -> <AnnotationData as Storable>::FullHandleType {
        (self.set().handle(), self.handle())
    }
}

/// This is the implementation of the high-level API for [`AnnotationData`].
impl<'store> ResultItem<'store, AnnotationData> {
    /// Return a reference to the dataset that holds this data
    pub fn set(&self) -> ResultItem<'store, AnnotationDataSet> {
        let rootstore = self.rootstore();
        self.store().as_resultitem(rootstore, rootstore)
    }

    /// Return a reference to the data value
    pub fn value(&self) -> &'store DataValue {
        self.as_ref().value()
    }

    /// Return a reference to the key for this data
    pub fn key(&self) -> ResultItem<'store, DataKey> {
        self.store()
            .key(self.as_ref().key())
            .expect("AnnotationData must always have a key at this point")
            .as_resultitem(self.store(), self.rootstore())
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that makes use of this data.
    /// The iterator returns the annotations as [`ResultItem<Annotation>`].
    /// Especially useful in combination with a call to  [`ResultItem<AnnotationDataSet>.find_data()`] or [`AnnotationDataSet.annotationdata()`] first.
    pub fn annotations(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let set_handle = self.store().handle().expect("set must have handle");
        if let Some(annotations) = self
            .rootstore()
            .annotations_by_data_indexlookup(set_handle, self.handle())
        {
            ResultIter::new_sorted(FromHandles::new(
                annotations.iter().copied(),
                self.rootstore(),
            ))
        } else {
            ResultIter::new_empty()
        }
    }

    /// Returns the number of annotations ([`Annotation`]) that make use of this data.
    pub fn annotations_len(&self) -> usize {
        let annotationstore = self.rootstore();
        if let Some(vec) = annotationstore.annotations_by_data_indexlookup(
            self.store().handle().expect("set must have handle"),
            self.handle(),
        ) {
            vec.len()
        } else {
            0
        }
    }

    pub fn test(&self, key: impl Request<DataKey>, operator: &DataOperator) -> bool {
        if key.any() || self.key().test(key) {
            self.as_ref().value().test(operator)
        } else {
            false
        }
    }

    /// Returns an set of all text resources that make use of this data via annotations via a ResourceSelector (i.e. as metadata)
    pub fn resources_as_metadata(
        &self,
    ) -> <BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter {
        self.annotations()
            .map(|annotation| annotation.resources_as_metadata())
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
    }

    /// Returns an iterator over all text resources that make use of this data via annotations via a TextSelector (i.e. on text)
    pub fn resources(
        &self,
    ) -> <BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter {
        self.annotations()
            .map(|annotation| annotation.resources())
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
    }

    /// Returns an iterator over all data sets that annotations using this data reference via a DataSetSelector (i.e. as metadata)
    pub fn datasets(
        &self,
    ) -> <BTreeSet<ResultItem<'store, AnnotationDataSet>> as IntoIterator>::IntoIter {
        self.annotations()
            .map(|annotation| annotation.datasets())
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
    }
}

pub type Data<'store> = Handles<'store, AnnotationData>;

impl<'store, I> FullHandleToResultItem<'store, AnnotationData>
    for FromHandles<'store, AnnotationData, I>
where
    I: Iterator<Item = (AnnotationDataSetHandle, AnnotationDataHandle)>,
{
    fn get_item(
        &self,
        handle: (AnnotationDataSetHandle, AnnotationDataHandle),
    ) -> Option<ResultItem<'store, AnnotationData>> {
        if let Some(dataset) = self.store.dataset(handle.0) {
            dataset.annotationdata(handle.1)
        } else {
            None
        }
    }
}

impl<'store, I> FullHandleToResultItem<'store, AnnotationData>
    for FilterAllIter<'store, AnnotationData, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationData>>,
{
    fn get_item(
        &self,
        handle: (AnnotationDataSetHandle, AnnotationDataHandle),
    ) -> Option<ResultItem<'store, AnnotationData>> {
        if let Some(dataset) = self.store.dataset(handle.0) {
            dataset.annotationdata(handle.1)
        } else {
            None
        }
    }
}

pub trait DataIterator<'store>: Iterator<Item = ResultItem<'store, AnnotationData>>
where
    Self: Sized,
{
    fn parallel(self) -> rayon::vec::IntoIter<ResultItem<'store, AnnotationData>> {
        let annotations: Vec<_> = self.collect();
        annotations.into_par_iter()
    }

    /// Iterate over the annotations that make use of data in this iterator.
    /// Annotations will be returned chronologically (add `.textual_order()` to sort textually) and contain no duplicates.
    fn annotations(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self.map(|data| data.annotations()).flatten().collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultIter::new_sorted(annotations.into_iter())
    }

    fn filter_handle(
        self,
        set: AnnotationDataSetHandle,
        data: AnnotationDataHandle,
    ) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationData(set, data, SelectionQualifier::Normal),
        }
    }

    fn filter_key(self, key: &ResultItem<'store, DataKey>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_key_handle(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::DataKey(set, key, SelectionQualifier::Normal),
        }
    }

    fn filter_set(self, set: &ResultItem<'store, AnnotationDataSet>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_set_handle(self, set: AnnotationDataSetHandle) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationDataSet(set, SelectionQualifier::Normal),
        }
    }

    fn filter_key_handle_value(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
        value: DataOperator<'store>,
    ) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::DataKeyAndOperator(set, key, value, SelectionQualifier::Normal),
        }
    }

    fn filter_value(self, operator: DataOperator<'store>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::DataOperator(operator, SelectionQualifier::Normal),
        }
    }

    fn filter_any(self, data: Data<'store>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::Data(data, FilterMode::Any, SelectionQualifier::Normal),
        }
    }

    fn filter_any_byref(self, data: &'store Data<'store>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::BorrowedData(data, FilterMode::Any, SelectionQualifier::Normal),
        }
    }

    /// Constrain this iterator to filter on *all* of the mentioned data, that is.
    /// If not all of the items in the parameter exist in the iterator, the iterator returns nothing.
    fn filter_all(
        self,
        data: Data<'store>,
        store: &'store AnnotationStore,
    ) -> FilterAllIter<'store, AnnotationData, Self> {
        FilterAllIter::new(self, data, store)
    }

    fn filter_one(self, data: &ResultItem<'store, AnnotationData>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationData(
                data.set().handle(),
                data.handle(),
                SelectionQualifier::Normal,
            ),
        }
    }

    fn filter_data_handle(
        self,
        set: AnnotationDataSetHandle,
        data: AnnotationDataHandle,
    ) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationData(set, data, SelectionQualifier::Normal),
        }
    }

    fn filter_annotation(
        self,
        annotation: &ResultItem<'store, Annotation>,
    ) -> FilteredData<'store, Self> {
        self.filter_annotation_handle(annotation.handle())
    }

    fn filter_annotation_handle(self, annotation: AnnotationHandle) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::Annotation(
                annotation,
                SelectionQualifier::Normal,
                AnnotationDepth::default(),
            ),
        }
    }
}

impl<'store, I> DataIterator<'store> for I
where
    I: Iterator<Item = ResultItem<'store, AnnotationData>>,
{
    //blanket implementation
}

pub struct FilteredData<'store, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationData>>,
{
    inner: I,
    filter: Filter<'store>,
}

impl<'store, I> Iterator for FilteredData<'store, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationData>>,
{
    type Item = ResultItem<'store, AnnotationData>;
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

impl<'store, I> FilteredData<'store, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationData>>,
{
    fn test_filter(&self, data: &ResultItem<'store, AnnotationData>) -> bool {
        match &self.filter {
            Filter::AnnotationData(set_handle, data_handle, _) => {
                data.handle() == *data_handle && data.set().handle() == *set_handle
            }
            Filter::Data(v, FilterMode::Any, _) => v.contains(&data.fullhandle()),
            Filter::BorrowedData(v, FilterMode::Any, _) => v.contains(&data.fullhandle()),
            Filter::AnnotationDataSet(set_handle, _) => data.set().handle() == *set_handle,
            Filter::DataKey(set_handle, key_handle, _) => {
                data.key().handle() == *key_handle && data.set().handle() == *set_handle
            }
            Filter::DataOperator(operator, _) => data.test(false, &operator),
            Filter::DataKeyAndOperator(set_handle, key_handle, operator, _) => {
                data.key().handle() == *key_handle
                    && data.set().handle() == *set_handle
                    && data.test(false, &operator)
            }
            Filter::Annotations(annotations, FilterMode::Any, SelectionQualifier::Normal, _) => {
                data.annotations().filter_any_byref(annotations).test()
            }
            Filter::Annotations(annotations, FilterMode::All, SelectionQualifier::Normal, _) => {
                data.annotations()
                    .filter_all(annotations.clone(), data.rootstore())
                    .test()
            }
            Filter::BorrowedAnnotations(
                annotations,
                FilterMode::Any,
                SelectionQualifier::Normal,
                _,
            ) => data.annotations().filter_any_byref(annotations).test(),
            Filter::BorrowedAnnotations(
                annotations,
                FilterMode::All,
                SelectionQualifier::Normal,
                _,
            ) => data
                .annotations()
                .filter_all(annotations.deref().clone(), data.rootstore())
                .test(),
            Filter::Data(_, FilterMode::All, _) => {
                unreachable!("not handled by this iterator but by FilterAllIter")
            }
            Filter::BorrowedData(_, FilterMode::All, _) => {
                unreachable!("not handled by this iterator but by FilterAllIter")
            }
            _ => unreachable!("Filter {:?} not implemented for FilteredData", self.filter),
        }
    }
}

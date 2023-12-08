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
use crate::annotationstore::AnnotationStore;
use crate::api::*;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::resources::TextResource;
use crate::store::*;
use crate::{Filter, IntersectionIter};
use rayon::prelude::*;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt::Debug;

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
    pub fn annotations(&self) -> MaybeIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let set_handle = self.store().handle().expect("set must have handle");
        if let Some(annotations) = self
            .rootstore()
            .annotations_by_data_indexlookup(set_handle, self.handle())
        {
            MaybeIter::new_sorted(FromHandles::new(
                annotations.iter().copied(),
                self.rootstore(),
            ))
        } else {
            MaybeIter::new_empty()
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

    /// Returns an iterator over all text resources that make use of this data via annotations (either as metadata or on text)
    pub fn resources(
        &self,
    ) -> <BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter {
        self.annotations()
            .map(|annotation| annotation.resources())
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
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
    pub fn resources_on_text(
        &self,
    ) -> <BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter {
        self.annotations()
            .map(|annotation| annotation.resources_on_text())
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

//pub type DataIter<'store> = ResultItemIter<'store, AnnotationData>;

/*
impl<'store> DataIter<'store> {
    /// Transform the iterator into a parallel iterator; subsequent iterator methods like `filter` and `map` will run in parallel.
    /// This first consumes the sequential iterator into a newly allocated buffer.
    ///
    /// Note: It does not parallelize the operation of DataIter itself!
    pub fn parallel(
        self,
    ) -> impl ParallelIterator<Item = ResultItem<'store, AnnotationData>> + 'store {
        self.collect::<Vec<_>>().into_par_iter()
    }

    /// Constrain the iterator to return only the data used by the specified annotation
    pub fn filter_annotation(mut self, annotation: &ResultItem<'store, Annotation>) -> Self {
        let annotation_data = annotation.as_ref().raw_data();
        self.filter_data(Handles::new(
            Cow::Borrowed(annotation_data),
            false,
            self.store,
        ))
    }

    /// Constrain the iterator to return only the data used by the specified annotations
    /// Shortcut for `self.filter_data(annotations.data())`.
    pub fn filter_annotations(self, annotations: AnnotationsIter<'store>) -> Self {
        self.filter_data(annotations.data())
    }

    /// Constrain this iterator by a vector of handles (intersection).
    /// You can use [`Self::to_collection()`] on a DataIter and then later reload it with this method.
    pub fn filter_from(self, data: &Data<'store>) -> Self {
        self.filter_data(data.iter())
    }

    /// Constrain the iterator to return only the data that uses the specified key
    /// This method can only be called once.
    pub fn filter_key(self, key: &ResultItem<'store, DataKey>) -> Self {
        self.filter_key_handle(key.set().handle(), key.handle())
    }

    /// Constrain the iterator to return only the data that is also in the other iterator (intersection)
    ///
    /// You can cast any existing iterator that produces [`ResultItem<AnnotationData>`] to a [`DataIter`] using [`DataIter::from_iter()`].
    pub fn filter_data(self, data: &Data<'store>) -> Self {
        self.intersection(data)
    }

    /// Find and filter data. Returns an iterator over the data.
    /// See [`AnnotationStore::find_data()`] for further documentation.
    pub fn find_data<'a>(
        self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> DataIter<'store>
    where
        'a: 'store,
    {
        let store = self.store;
        match store.find_data_request_resolver(set, key) {
            Some((Some(set_handle), Some(key_handle))) => self
                .filter_key_handle(set_handle, key_handle)
                .filter_value(value),
            Some((Some(set_handle), None)) => {
                self.filter_set_handle(set_handle).filter_value(value)
            }
            Some((None, None)) => self.filter_value(value),
            Some((None, Some(_))) => {
                eprintln!(
                    "STAM warning: find_data() can not be used without a set if you specify a key!"
                );
                DataIter::new_empty(store)
            }
            None => DataIter::new_empty(store),
        }
    }

    /// Constrain this iterator to only a single annotationdata
    /// If you're looking for a lower-level variant, use [`Self::filter_handle()`] instead.
    pub fn filter_annotationdata(self, annotationdata: &ResultItem<AnnotationData>) -> Self {
        self.filter_handle(annotationdata.set().handle(), annotationdata.handle())
    }

    /// Constrain this iterator to only a single annotationdata, by handle
    /// You will like want to use the higher-level method [`Self::filter_annotationdata()`] instead.
    pub fn filter_handle(
        mut self,
        set_handle: AnnotationDataSetHandle,
        data_handle: AnnotationDataHandle,
    ) -> Self {
        if self.iter.is_some() {
            self.iter = Some(self.iter.unwrap().with_singleton((set_handle, data_handle)));
        }
        self
    }

    /// Select data by testing the value (mediated by a [`DataOperator`]).
    /// Consumes the iterator and returns an iterator over AnnotationData instances
    ///
    /// Note: You can only use this method once (it will overwrite earlier value filters, use [`DataOperator::Or`] and [`DataOperator::And`] to test against multiple values)
    pub fn filter_value(mut self, operator: DataOperator<'store>) -> Self {
        if let DataOperator::Any = operator {
            //don't bother actually adding the operator, this is a no-op
            return self;
        }
        self.filters.push(Filter::DataOperator(operator));
        self
    }

    /// Filter for sets
    /// This can only be used once.
    pub(crate) fn filter_set_handle(mut self, set_handle: AnnotationDataSetHandle) -> Self {
        self.filters.push(Filter::AnnotationDataSet(set_handle));
        self
    }

    /// Filter for keys. This is a low-level function, use [`Self::filter_key()`] instead.
    /// This can only be used once.
    pub fn filter_key_handle(
        mut self,
        set_handle: AnnotationDataSetHandle,
        key_handle: DataKeyHandle,
    ) -> Self {
        self.filters.push(Filter::DataKey(set_handle, key_handle));
        self
    }

    /// Iterate over the annotations that make use of data in this iterator.
    /// Annotations will be returned chronologically (add `.textual_order()` to sort textually) and contain no duplicates.
    /// If you want to keep track of what data has what annotations, then use [`Self::zip_annotations()`] instead.
    pub fn annotations(self) -> AnnotationsIter<'store> {
        let store = self.store;
        let mut annotations: Vec<_> = self
            .filter_map(|data| {
                if let Some(annotations) = data
                    .rootstore()
                    .annotations_by_data_indexlookup(data.set().handle(), data.handle())
                {
                    Some(annotations.iter().copied())
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        AnnotationsIter::new(IntersectionIter::new(Cow::Owned(annotations), true), store)
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that makes use of this data.
    /// Unlike [`Self::annotations()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant)
    pub fn annotations_unchecked(self) -> AnnotationsIter<'store> {
        let store = self.store;
        AnnotationsIter::new(
            IntersectionIter::new_with_iterator(
                Box::new(
                    self.filter_map(|data| {
                        if let Some(annotations) = data
                            .rootstore()
                            .annotations_by_data_indexlookup(data.set().handle(), data.handle())
                        {
                            Some(annotations.iter().copied())
                        } else {
                            None
                        }
                    })
                    .flatten(),
                ),
                false,
            ),
            store,
        )
    }

    /// Returns data along with annotations, either may occur multiple times!
    /// Consumes the iterator.
    pub fn zip_annotations(
        self,
    ) -> impl Iterator<
        Item = (
            ResultItem<'store, AnnotationData>,
            ResultItem<'store, Annotation>,
        ),
    > + 'store {
        self.map(|data| {
            data.annotations()
                .map(move |annotation| (data.clone(), annotation))
        })
        .flatten()
    }
}
*/

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

/*
impl<'store> ResultItemIterator<'store, AnnotationData> for ResultItemIter<'store, AnnotationData> {
    /// See if the filters match for the annotation data
    /// This does not include any filters directly on annotationdata, as those are handled already by the underlying IntersectionsIter
    fn test_filters(&self, data: &ResultItem<'store, AnnotationData>) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        for filter in self.filters.iter() {
            match filter {
                Filter::AnnotationDataSet(set_handle) => {
                    if *set_handle != data.set().handle() {
                        return false;
                    }
                }
                Filter::DataKey(set_handle, key_handle) => {
                    if *set_handle != data.set().handle() || *key_handle != data.key().handle() {
                        return false;
                    }
                }
                Filter::DataOperator(op) => {
                    if !data.test(false, op) {
                        return false;
                    }
                }
                _ => unimplemented!("Filter {:?} not implemented for DataIter", filter),
            }
        }
        true
    }
}
*/

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
    ) -> MaybeIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self.map(|data| data.annotations()).flatten().collect();
        annotations.sort_unstable();
        annotations.dedup();
        MaybeIter::new_sorted(annotations.into_iter())
    }

    fn filter_key(self, key: &ResultItem<'store, DataKey>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle()),
        }
    }

    fn filter_key_handle(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::DataKey(set, key),
        }
    }

    fn filter_set(self, set: &ResultItem<'store, AnnotationDataSet>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle()),
        }
    }

    fn filter_set_handle(self, set: AnnotationDataSetHandle) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationDataSet(set),
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
            filter: Filter::DataKeyAndOperator(set, key, value),
        }
    }

    fn filter_value(self, operator: DataOperator<'store>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::DataOperator(operator),
        }
    }

    fn filter_data(self, data: Data<'store>) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::Data(data, FilterMode::Any),
        }
    }

    fn filter_annotationdata(
        self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationData(data.set().handle(), data.handle()),
        }
    }

    fn filter_data_handle(
        self,
        set: AnnotationDataSetHandle,
        data: AnnotationDataHandle,
    ) -> FilteredData<'store, Self> {
        FilteredData {
            inner: self,
            filter: Filter::AnnotationData(set, data),
        }
    }

    /*
    /// Find and filter data. Returns an iterator over the data.
    /// See [`AnnotationStore::find_data()`] for further documentation.
    fn find_data<'a>(
        self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>>>
    where
        'a: 'store,
    {
        let store = self.store();
        match store.find_data_request_resolver(set, key) {
            Some((Some(set_handle), Some(key_handle))) => {
                Box::new(self.filter_key_handle_value(set_handle, key_handle, value))
            }
            Some((Some(set_handle), None)) => {
                Box::new(self.filter_set_handle(set_handle).filter_value(value))
            }
            Some((None, None)) => Box::new(self.filter_value(value)),
            Some((None, Some(_))) => {
                eprintln!(
                    "STAM warning: find_data() can not be used without a set if you specify a key!"
                );
                Box::new(std::iter::empty())
            }
            None => Box::new(std::iter::empty()),
        }
    }
    */

    /*
    fn apply_filters<F: FromIterator<Filter<'store>>>(
        self,
        filters: F,
    ) -> Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>>> {
        let mut iter: Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>>> = Box::new(self);
        for filter in filters {
            std::mem::replace(
                &mut iter,
                FilteredData {
                    inner: *iter,
                    filter,
                },
            );
        }
        iter
    }
    */
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
            Filter::AnnotationData(set_handle, data_handle) => {
                data.handle() == *data_handle && data.set().handle() == *set_handle
            }
            Filter::Data(v, _) => v.contains(&data.fullhandle()),
            Filter::AnnotationDataSet(set_handle) => data.set().handle() == *set_handle,
            Filter::DataKey(set_handle, key_handle) => {
                data.key().handle() == *key_handle && data.set().handle() == *set_handle
            }
            Filter::DataOperator(operator) => data.test(false, &operator),
            Filter::DataKeyAndOperator(set_handle, key_handle, operator) => {
                data.key().handle() == *key_handle
                    && data.set().handle() == *set_handle
                    && data.test(false, &operator)
            }
            _ => unreachable!("Filter {:?} not implemented for FilteredData", self.filter),
        }
    }
}

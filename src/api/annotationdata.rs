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
use crate::api::annotation::AnnotationsIter;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::resources::TextResource;
use crate::store::*;
use crate::IntersectionIter;
use rayon::prelude::*;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt::Debug;

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
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        let set_handle = self.store().handle().expect("set must have handle");
        if let Some(annotations) = self
            .rootstore()
            .annotations_by_data_indexlookup(set_handle, self.handle())
        {
            AnnotationsIter::new(
                IntersectionIter::new(Cow::Borrowed(annotations), true),
                self.rootstore(),
            )
        } else {
            AnnotationsIter::new_empty(self.rootstore())
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

    /// Returns a set of all text resources that make use of this data via annotations (either as metadata or on text)
    pub fn resources(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources().map(|resource| resource.clone()))
            .flatten()
            .collect()
    }

    /// Returns an set of all text resources that make use of this data via annotations via a ResourceSelector (i.e. as metadata)
    pub fn resources_as_metadata(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources_as_metadata())
            .flatten()
            .collect()
    }

    /// Returns an iterator over all text resources that make use of this data via annotations via a TextSelector (i.e. on text)
    pub fn resources_on_text(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources_on_text())
            .flatten()
            .collect()
    }

    /// Returns an iterator over all data sets that annotations using this data reference via a DataSetSelector (i.e. as metadata)
    pub fn datasets(&self) -> BTreeSet<ResultItem<'store, AnnotationDataSet>> {
        self.annotations()
            .map(|annotation| annotation.datasets().map(|dataset| dataset.clone()))
            .flatten()
            .collect()
    }
}

#[derive(Clone)]
/// Holds a collection of annotation data.
/// This structure is produced by calling [`DataIter.to_cache()`].
/// Use [`Self.iter()`] to iterate over the collection.
pub struct Data<'store> {
    pub(crate) array: Cow<'store, [(AnnotationDataSetHandle, AnnotationDataHandle)]>,
    pub(crate) store: &'store AnnotationStore,
    pub(crate) sorted: bool,
}

impl<'store> Debug for Data<'store> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Data")
            .field("array", &self.array)
            .field("sorted", &self.sorted)
            .finish()
    }
}

impl<'a> Data<'a> {
    /// Returns an iterator over the data in this collection, the iterator exposes further high-level API methods.
    pub fn iter(&self) -> DataIter<'a> {
        DataIter::new(
            IntersectionIter::new(self.array.clone(), self.sorted),
            self.store,
        )
    }

    /// Returns the number of data items in this collection.
    pub fn len(&self) -> usize {
        self.array.len()
    }

    /// Returns a boolean indicating whether the collection is empty or not.
    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    /// Low-level method to instantiate data from an existing collection
    /// Warning: Use of this function is dangerous and discouraged in most cases as there is no validity check on the handles you pass!
    pub fn from_handles(
        array: Cow<'a, [(AnnotationDataSetHandle, AnnotationDataHandle)]>,
        sorted: bool,
        store: &'a AnnotationStore,
    ) -> Self {
        Self {
            array,
            sorted,
            store,
        }
    }

    /// Low-level method to take the underlying vector of handles
    pub fn take(mut self) -> Vec<(AnnotationDataSetHandle, AnnotationDataHandle)> {
        self.array.to_mut().to_vec()
    }
}

/// `DataIter` iterates over annotation data, it returns `ResultItem<AnnotationData>` instances.
/// The iterator offers a various high-level API methods that operate on a collection of annotation data, and
/// allow to further filter or map annotations.
///
/// The iterator is produced by calling the `data()` method that is implemented for several objects, such as [`Annotation.data()`]
pub struct DataIter<'store> {
    iter: Option<IntersectionIter<'store, (AnnotationDataSetHandle, AnnotationDataHandle)>>,
    store: &'store AnnotationStore,

    operator: Option<DataOperator<'store>>,
    set_test: Option<AnnotationDataSetHandle>,
    key_test: Option<DataKeyHandle>,

    //for optimisations:
    last_set_handle: Option<AnnotationDataSetHandle>,
    last_set: Option<ResultItem<'store, AnnotationDataSet>>,
}

impl<'store> DataIter<'store> {
    pub(crate) fn new(
        iter: IntersectionIter<'store, (AnnotationDataSetHandle, AnnotationDataHandle)>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            iter: Some(iter),
            store,
            last_set_handle: None,
            last_set: None,
            operator: None,
            set_test: None,
            key_test: None,
        }
    }

    pub(crate) fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            iter: None,
            store,
            last_set_handle: None,
            last_set: None,
            operator: None,
            set_test: None,
            key_test: None,
        }
    }

    /// Builds a new data iterator from any other iterator of annotationdata
    /// Eagerly consumes the iterator.
    pub fn from_iter(
        iter: impl Iterator<Item = ResultItem<'store, AnnotationData>>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self {
        let data: Vec<_> = iter
            .map(|data| (data.set().handle(), data.handle()))
            .collect();
        Self {
            iter: Some(IntersectionIter::new(Cow::Owned(data), sorted)),
            last_set_handle: None,
            last_set: None,
            operator: None,
            set_test: None,
            key_test: None,
            store,
        }
    }

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
        if self.iter.is_some() {
            self.iter = Some(
                self.iter
                    .unwrap()
                    .with(Cow::Borrowed(annotation_data), true),
            );
        }
        self
    }

    /// Constrain the iterator to return only the data used by the specified annotations
    /// Shortcut for `self.filter_data(annotations.data())`.
    pub fn filter_annotations(self, annotations: AnnotationsIter<'store>) -> Self {
        self.filter_data(annotations.data())
    }

    /// Constrain this iterator by a vector of handles (intersection).
    /// You can use [`Self.to_cache()`] on a DataIter and then later reload it with this method.
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
    /// You can cast any existing iterator that produces `ResultItem<AnnotationData>` to a [`DataIter`] using [`DataIter::from_iter()`].
    pub fn filter_data(self, data: DataIter<'store>) -> Self {
        self.merge(data)
    }

    /// Find and filter data. Returns an iterator over the data.
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
    /// If you're looking for a lower-level variant, use [`Self.filter_handle()`] instead.
    pub fn filter_annotationdata(self, annotationdata: &ResultItem<AnnotationData>) -> Self {
        self.filter_handle(annotationdata.set().handle(), annotationdata.handle())
    }

    /// Constrain this iterator to only a single annotationdata, by handle
    /// You will like want to use the higher-level method [`Self.filter_annotationdata()`] instead.
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
        self.operator = Some(operator);
        self
    }

    /// Filter for sets
    /// This can only be used once.
    pub(crate) fn filter_set_handle(mut self, set_handle: AnnotationDataSetHandle) -> Self {
        self.set_test = Some(set_handle);
        self
    }

    /// Filter for keys. This is a low-level function, use [`Self.filter_key()`] instead.
    /// This can only be used once.
    pub fn filter_key_handle(
        mut self,
        set_handle: AnnotationDataSetHandle,
        key_handle: DataKeyHandle,
    ) -> Self {
        self.set_test = Some(set_handle);
        self.key_test = Some(key_handle);
        self
    }

    /// Iterate over the annotations that make use of data in this iterator.
    /// Annotations will be returned chronologically (add `.textual_order()` to sort textually) and contain no duplicates.
    /// If you want to keep track of what data has what annotations, then use [`self.zip_annotations()`] instead.
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
    /// Unlike [`self.annotations()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant)
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

    /// Produces the union between two data iterators
    /// Any constraints on either iterator remain valid!
    pub fn extend(mut self, other: DataIter<'store>) -> DataIter<'store> {
        if self.iter.is_some() && other.iter.is_some() {
            self.iter = Some(self.iter.unwrap().extend(other.iter.unwrap()));
        } else if self.iter.is_none() {
            return other;
        }
        self
    }

    /// Produces the intersection between two data iterators
    /// Any constraints on either iterator remain valid!
    pub fn merge(mut self, other: DataIter<'store>) -> DataIter<'store> {
        if self.iter.is_some() && other.iter.is_some() {
            self.iter = Some(self.iter.unwrap().merge(other.iter.unwrap()));
        } else if self.iter.is_none() {
            return other;
        }
        self
    }

    /// Extract a low-level vector of handles from this iterator.
    /// This is different than running `collect()`, which produces high-level objects.
    ///
    /// An extracted vector can be easily turned back into a DataIter again with [`Self.load()`] or
    /// used directly as a filter using [`Self.filter_from()`].
    pub fn to_cache(self) -> Data<'store> {
        let store = self.store;
        let sorted = self.returns_sorted();
        if self.operator.is_none() && self.set_test.is_none() && self.key_test.is_none() {
            //we can be a bit more performant if we have no operator:
            if let Some(iter) = self.iter {
                Data {
                    array: Cow::Owned(iter.collect()),
                    sorted,
                    store,
                }
            } else {
                Data {
                    array: Cow::Owned(Vec::new()),
                    sorted,
                    store,
                }
            }
        } else {
            // go to higher-level (needed to deal with the operator) and back to handles
            Data {
                array: Cow::Owned(
                    self.map(|annotationdata| {
                        (annotationdata.set().handle(), annotationdata.handle())
                    })
                    .collect(),
                ),
                sorted,
                store,
            }
        }
    }

    /// Exports the iterator to a low-level vector that can be reused at will by invoking `.iter()`.
    /// This consumes the iterator but takes only the first n elements up to the specified limit.
    pub fn to_cache_limit(self, limit: usize) -> Data<'store> {
        let store = self.store;
        let sorted = self.returns_sorted();
        Data {
            array: Cow::Owned(
                self.take(limit)
                    .map(|annotationdata| (annotationdata.set().handle(), annotationdata.handle()))
                    .collect(),
            ),
            store,
            sorted,
        }
    }

    /// Returns true if the iterator has items, false otherwise
    pub fn test(mut self) -> bool {
        self.next().is_some()
    }

    /// Does this iterator return items in sorted order?
    pub fn returns_sorted(&self) -> bool {
        if let Some(iter) = self.iter.as_ref() {
            iter.returns_sorted()
        } else {
            true //empty iterators can be considered sorted
        }
    }
}

impl<'store> Iterator for DataIter<'store> {
    type Item = ResultItem<'store, AnnotationData>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iter.as_mut() {
                if let Some((set_handle, data_handle)) = iter.next() {
                    if let Some(test_set) = self.set_test {
                        if set_handle != test_set {
                            continue;
                        }
                    }
                    //optimisation so we don't have to grab the same set over and over:
                    let dataset = if Some(set_handle) == self.last_set_handle {
                        self.last_set.as_ref().unwrap()
                    } else {
                        self.last_set_handle = Some(set_handle);
                        self.last_set =
                            Some(self.store.dataset(set_handle).expect("set must exist"));
                        self.last_set.as_ref().unwrap()
                    };
                    let data = dataset
                        .annotationdata(data_handle)
                        .expect("data must exist");
                    if let Some(test_key) = self.key_test {
                        if test_key != data.key().handle() {
                            continue;
                        }
                    }
                    if let Some(operator) = self.operator.as_ref() {
                        if !data.test(false, operator) {
                            //data does not pass the value test
                            continue;
                        }
                    }
                    return Some(data);
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        None
    }
}

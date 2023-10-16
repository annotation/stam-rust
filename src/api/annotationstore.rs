/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for the [`AnnotationStore`]. This API is implemented on
//! The high-level API is characterised by returning items as [`ResultItem<T>`], upon which further
//! object-specific API methods are implemented.

use crate::annotation::Annotation;
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::annotation::AnnotationsIter;
use crate::api::annotationdata::DataIter;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::DataOperator;
use crate::resources::TextResource;
use crate::store::*;
use crate::IntersectionIter;

impl AnnotationStore {
    /// Requests a specific [`TextResource`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a `&str`, `String` or [`crate::TextResourceHandle`].
    ///
    /// The item is returned as a fat pointer [`ResultItem<TextResource>`]) in an Option.
    /// Returns `None` if it does not exist.
    pub fn resource(
        &self,
        request: impl Request<TextResource>,
    ) -> Option<ResultItem<TextResource>> {
        self.get(request).map(|x| x.as_resultitem(self, self)).ok()
    }

    /// Requests a specific [`AnnotationDataSet`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a `&str`, [`String`] or [`AnnotationDataSetHandle`].
    pub fn dataset(
        &self,
        request: impl Request<AnnotationDataSet>,
    ) -> Option<ResultItem<AnnotationDataSet>> {
        self.get(request).map(|x| x.as_resultitem(self, self)).ok()
    }

    /// Requests a specific [`Annotation`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a `&str`,[`String`] or [`crate::AnnotationHandle`].
    ///
    /// The item is returned as a fat pointer [`ResultItem<Annotation>`]) in an Option.
    /// Returns `None` if it does not exist.
    pub fn annotation(&self, request: impl Request<Annotation>) -> Option<ResultItem<Annotation>> {
        self.get(request).map(|x| x.as_resultitem(self, self)).ok()
    }

    /// Returns an iterator over all text resources ([`TextResource`] instances) in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>`]) .
    pub fn resources(&self) -> impl Iterator<Item = ResultItem<TextResource>> {
        self.iter()
            .map(|item: &TextResource| item.as_resultitem(self, self))
    }

    /// Returns an iterator over all [`AnnotationDataSet`] instances in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>`]) .
    pub fn datasets<'a>(&'a self) -> impl Iterator<Item = ResultItem<AnnotationDataSet>> {
        self.iter()
            .map(|item: &AnnotationDataSet| item.as_resultitem(self, self))
    }

    /// Returns an iterator over all annotations ([`Annotation`] instances) in the store.
    pub fn annotations<'a>(&'a self) -> AnnotationsIter<'a> {
        AnnotationsIter::new(
            IntersectionIter::new_with_iterator(
                Box::new(
                    self.iter()
                        .map(|a: &'a Annotation| a.handle().expect("annotation must have handle")),
                ),
                true,
            ),
            self,
        )
    }

    /// internal helper method
    pub(crate) fn find_data_request_resolver<'store>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
    ) -> Option<(Option<AnnotationDataSetHandle>, Option<DataKeyHandle>)> {
        let mut test_set_handle: Option<AnnotationDataSetHandle> = None; //None means 'any' in this context
        let mut test_key_handle: Option<DataKeyHandle> = None; //idem

        if !set.any() {
            if let Ok(set) = self.get(set) {
                test_set_handle = Some(set.handle().expect("set must have handle"));
                if !key.any() {
                    test_key_handle = key.to_handle(set);
                    if test_key_handle.is_none() {
                        //requested key doesn't exist, bail out early, we won't find anything at all
                        return None;
                    }
                }
            } else {
                //requested set doesn't exist, bail out early, we won't find anything at all
                return None;
            }
        } else if !key.any() {
            // Not the most elegant solution but it'll have to do, I don't want to wrap this in Result<>, and I don't
            // want to be entirely silent about this error either:
            eprintln!("STAM warning: Providing a key without a set in data searches is invalid! Key will be ignored!");
        }

        Some((test_set_handle, test_key_handle))
    }

    /// Finds [`AnnotationData`] using data search criteria.
    /// This returns an iterator over all matches.
    ///
    /// If you are not interested in returning the results but merely testing the presence of particular data,
    /// then use [`self.test_data()`] instead..
    ///
    /// You can pass a boolean (true/false, doesn't matter) or empty string literal for `set` or `key` to represent *any* set/key.
    /// To search for any value, `value` must be explicitly set to [`DataOperator::Any`] to return all values.
    ///
    /// Value is a DataOperator that can apply a data test to the value. Use [`DataOperator::Equals`] to search
    /// for an exact value. As a shortcut, you can pass `"value".into()` to automatically convert various data types into
    /// [`DataOperator::Equals`].
    ///
    /// Example call to retrieve all data indiscriminately: `annotation.find_data(false,false, DataOperator::Any)`
    ///  .. or just use the alias function `data()`.
    ///
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored!! You can not
    ///       search for keys if you don't know their set!
    pub fn find_data<'store, 'q>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'q>,
    ) -> DataIter<'store>
    where
        'q: 'store,
    {
        if !set.any() {
            if let Some(dataset) = self.dataset(set) {
                //delegate to the dataset
                return dataset.find_data(key, value);
            } else {
                return DataIter::new_empty(self);
            }
        }

        //all datasets
        if let Some((_, key_handle)) = self.find_data_request_resolver(set, key) {
            //iterate over all datasets
            let iter: Box<
                dyn Iterator<Item = (AnnotationDataSetHandle, AnnotationDataHandle)> + 'store,
            > = Box::new(
                self.datasets()
                    .map(move |dataset| {
                        dataset.as_ref().data().filter_map(move |annotationdata| {
                            if key_handle.is_none() || key_handle.unwrap() == annotationdata.key() {
                                Some((
                                    dataset.handle(),
                                    annotationdata.handle().expect("data must have handle"),
                                ))
                            } else {
                                None
                            }
                        })
                    })
                    .flatten(),
            );
            if let DataOperator::Any = value {
                DataIter::new(IntersectionIter::new_with_iterator(iter, true), self)
            } else {
                DataIter::new(IntersectionIter::new_with_iterator(iter, true), self)
                    .filter_value(value)
            }
        } else {
            DataIter::new_empty(self)
        }
    }

    /// Returns an iterator over all data in all sets.
    /// If possible, use a more constrained method (on [`AnnotationDataSet`] or a [`DataKey`]), it will have better performance.
    pub fn data<'store>(&'store self) -> DataIter<'store> {
        self.find_data(false, false, DataOperator::Any)
    }

    /// Tests if certain annotation data exists, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// You can pass a boolean (true/false, doesn't matter) or empty string literal for `set` or `key` to represent *any* set/key.
    /// To search for any value, `value` must be explicitly set to `DataOperator::Any` to return all values.
    ///
    /// Note: This gives no guarantee that data, although it exists, is actually used by annotations.
    pub fn test_data<'store, 'a>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> bool
    where
        'a: 'store,
    {
        self.find_data(set, key, value).next().is_some()
    }

    /// Searches for resources by metadata.
    /// Returns an iterator returning both the annotation, as well the annotation data
    ///
    /// This may return the same resource multiple times if different matching data references it!
    ///
    /// If you already have a `ResultItem<AnnotationData>` instance, just use `ResultItem<AnnotationData>.resources_as_metadata()` instead, it'll be much more efficient.
    ///
    /// See [`Self.find_data()`] for further parameter explanation.
    pub fn resources_by_metadata<'store, 'a>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> impl Iterator<
        Item = (
            ResultItem<'store, TextResource>,
            ResultItem<'store, AnnotationData>,
        ),
    >
    where
        'a: 'store,
    {
        self.find_data(set, key, value)
            .map(|data| {
                data.resources_as_metadata()
                    .into_iter()
                    .map(move |resource| (resource, data.clone()))
            })
            .into_iter()
            .flatten()
    }

    /// Searches for datasets by metadata.
    /// Returns an iterator returning both the annotation, as well the annotation data
    ///
    /// This may return the same resource multiple times if different matching data references it!
    ///
    /// If you already have a `ResultItem<AnnotationData>` instance, just use `ResultItem<AnnotationData>.resources_as_metadata()` instead, it'll be much more efficient.
    ///
    /// See [`Self.find_data()`] for further parameter explanation.
    pub fn datasets_by_metadata<'store, 'a>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> impl Iterator<
        Item = (
            ResultItem<'store, AnnotationDataSet>,
            ResultItem<'store, AnnotationData>,
        ),
    >
    where
        'a: 'store,
    {
        self.find_data(set, key, value)
            .map(|data| {
                data.datasets()
                    .into_iter()
                    .map(move |dataset| (dataset, data.clone()))
            })
            .into_iter()
            .flatten()
    }
}

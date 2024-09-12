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
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::DataOperator;
use crate::resources::TextResource;
use crate::store::*;
use crate::ResultTextSelection;
use crate::{api::*, AnnotationSubStore};

use std::collections::BTreeSet;

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

    /// Requests a specific [`DataKey`] (pertaining to an [`AnnotationDataSet`]) to be returned by reference.
    pub fn key(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
    ) -> Option<ResultItem<DataKey>> {
        if let Some(dataset) = self.dataset(set) {
            dataset.key(key)
        } else {
            None
        }
    }

    /// Requests a specific [`AnnotationData`] (pertaining to an [`AnnotationDataSet`]) to be returned by reference.
    pub fn annotationdata(
        &self,
        set: impl Request<AnnotationDataSet>,
        data: impl Request<AnnotationData>,
    ) -> Option<ResultItem<AnnotationData>> {
        if let Some(dataset) = self.dataset(set) {
            dataset.annotationdata(data)
        } else {
            None
        }
    }

    /// Requests a specific [`TextSelection`] by handle (pertaining to an [`AnnotationDataSet`]) to be returned by reference.
    pub fn textselection(
        &self,
        resource: impl Request<TextResource>,
        handle: TextSelectionHandle,
    ) -> Option<ResultTextSelection> {
        if let Some(resource) = self.resource(resource) {
            resource.textselection_by_handle(handle).ok()
        } else {
            None
        }
    }

    /// Requests a specific [`Annotation`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a `&str`,[`String`] or [`AnnotationHandle`](crate::AnnotationHandle).
    ///
    /// The item is returned as a fat pointer [`ResultItem<Annotation>`]),
    /// which exposes the high-level API, in an Option.
    /// Returns `None` if it does not exist.
    pub fn annotation(&self, request: impl Request<Annotation>) -> Option<ResultItem<Annotation>> {
        self.get(request).map(|x| x.as_resultitem(self, self)).ok()
    }

    /// Requests a specific [`AnnotationSubStore`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a `&str`, [`String`] or [`AnnotationSubStoreHandle`].
    pub fn substore(
        &self,
        request: impl Request<AnnotationSubStore>,
    ) -> Option<ResultItem<AnnotationSubStore>> {
        self.get(request).map(|x| x.as_resultitem(self, self)).ok()
    }

    /// Returns an iterator over all text resources ([`TextResource`] instances) in the store.
    /// Items are returned as a fat pointer [`ResultItem<TextResource>`]),
    /// which exposes the high-level API.
    pub fn resources<'a>(&'a self) -> ResultIter<impl Iterator<Item = ResultItem<TextResource>>> {
        ResultIter::new_sorted(
            self.iter()
                .map(|item: &TextResource| item.as_resultitem(self, self)),
        )
    }

    /// Returns an iterator over all [`AnnotationDataSet`] instances in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>`]),
    /// which exposes the high-level API.
    pub fn datasets<'a>(
        &'a self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<AnnotationDataSet>>> {
        ResultIter::new_sorted(
            self.iter()
                .map(|item: &AnnotationDataSet| item.as_resultitem(self, self)),
        )
    }

    /// Returns an iterator over all annotations ([`Annotation`] instances) in the store.
    /// The resulting iterator yields items as a fat pointer [`ResultItem<Annotation>`]),
    /// which exposes the high-level API.
    /// Note that this will include all annotations from all substores, if you want only
    /// annnotations pertaining to root store, then use [`annotations_no_substores()`] instead.
    pub fn annotations<'a>(
        &'a self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'a, Annotation>>> {
        ResultIter::new_sorted(
            self.iter()
                .map(|a: &'a Annotation| a.as_resultitem(self, self)),
        )
    }

    /// Returns an iterator over all annotations ([`Annotation`] instances) in the root store.
    /// The resulting iterator yields items as a fat pointer [`ResultItem<Annotation>`]),
    /// which exposes the high-level API.
    /// Unlike [`annotations()`], this will not return annotations from substores.
    pub fn annotations_no_substores<'a>(
        &'a self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'a, Annotation>>> {
        ResultIter::new_sorted(
            self.iter()
                .map(|a: &'a Annotation| a.as_resultitem(self, self))
                .filter(|a| self.annotation_substore_map.get(a.handle()).is_some()),
        )
    }

    /// Returns an iterator over all substores ([`AnnotationSubStore`] instances) in the store.
    /// The resulting iterator yields items as a fat pointer [`ResultItem<AnnotationSubStore>`]),
    /// which exposes the high-level API. Note that each substore may itself consist of substores!
    /// If you want a flattened representation, use `substores_flatten()` instead
    pub fn substores<'a>(
        &'a self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'a, AnnotationSubStore>>> {
        ResultIter::new_sorted(self.iter().filter_map(|a: &'a AnnotationSubStore| {
            if a.parent.is_none() {
                Some(a.as_resultitem(self, self))
            } else {
                None
            }
        }))
    }

    /// Returns an iterator over all substores ([`AnnotationSubStore`] instances) in the store, including substores that are nested in others.
    /// The resulting iterator yields items as a fat pointer [`ResultItem<AnnotationSubStore>`]),
    /// which exposes the high-level API.
    pub fn substores_flatten<'a>(
        &'a self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'a, AnnotationSubStore>>> {
        ResultIter::new_sorted(
            self.iter()
                .map(|a: &'a AnnotationSubStore| a.as_resultitem(self, self)),
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
    /// then use [`Self::test_data()`] instead..
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
    ///
    /// ## Example
    ///
    /// ```
    /// # use stam::*;
    /// # fn main() -> Result<(),StamError> {
    /// # let store = AnnotationStore::default()
    /// #   .with_id("example")
    /// #   .add(TextResource::from_string(
    /// #       "myresource",
    /// #       "Hello world",
    /// #       Config::default(),
    /// #   ))?
    /// #   .add(AnnotationDataSet::new(Config::default()).with_id("mydataset"))?
    /// #   .with_annotation(
    /// #       AnnotationBuilder::new()
    /// #           .with_id("A1")
    /// #           .with_target(SelectorBuilder::textselector(
    /// #               "myresource",
    /// #               Offset::simple(6, 11),
    /// #           ))
    /// #           .with_data_with_id("mydataset", "part-of-speech", "noun", "D1"),
    /// #   )?;
    /// //in this store we have a single annotation, and single annotation data with key 'part-of-speech' and value 'noun':
    /// for annotationdata in store.find_data("mydataset", "part-of-speech", DataOperator::Equals("noun")) {
    ///     assert_eq!(annotationdata.id(), Some("D1"));
    ///     assert_eq!(annotationdata.value(), "noun");
    /// }
    /// #    Ok(())
    /// # }
    /// ```
    ///
    pub fn find_data<'store, 'q>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'q>,
    ) -> Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'q: 'store,
    {
        if !set.any() {
            if let Some(dataset) = self.dataset(set) {
                //delegate to the dataset
                return dataset.find_data(key, value);
            } else {
                return Box::new(std::iter::empty());
            }
        }

        //all datasets
        if let Some((_, key_handle)) = self.find_data_request_resolver(set, key) {
            //iterate over all datasets
            let rootstore = self;
            let iter: Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>>> = Box::new(
                self.datasets()
                    .map(move |dataset| {
                        dataset.as_ref().data().filter_map(move |annotationdata| {
                            if key_handle.is_none() || key_handle.unwrap() == annotationdata.key() {
                                Some(annotationdata.as_resultitem(dataset.as_ref(), rootstore))
                            } else {
                                None
                            }
                        })
                    })
                    .flatten(),
            );
            if let DataOperator::Any = value {
                iter
            } else {
                Box::new(iter.filter_value(value))
            }
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Returns an iterator over all data in all sets.
    /// If possible, use a more constrained method (on [`AnnotationDataSet`] or a [`DataKey`]), it will have better performance.
    pub fn data<'store>(
        &'store self,
    ) -> Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store> {
        self.find_data(false, false, DataOperator::Any)
    }

    /// Returns an iterator over all keys in all sets.
    /// If possible, use a more constrained method (on [`AnnotationDataSet`]), it will have better performance.
    pub fn keys<'store>(
        &'store self,
    ) -> <BTreeSet<ResultItem<'store, DataKey>> as IntoIterator>::IntoIter {
        let keys: BTreeSet<_> = self
            .datasets()
            .map(|dataset| dataset.keys())
            .flatten()
            .collect();
        keys.into_iter()
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
        self.find_data(set, key, value).test()
    }

    /// Searches for resources by metadata.
    /// Returns an iterator returning both the annotation, as well the annotation data
    ///
    /// This may return the same resource multiple times if different matching data references it!
    ///
    /// If you already have a `ResultItem<AnnotationData>` instance, just use `ResultItem<AnnotationData>.resources_as_metadata()` instead, it'll be much more efficient.
    ///
    /// See [`Self::find_data()`] for further parameter explanation.
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
    /// See [`Self::find_data()`] for further parameter explanation.
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

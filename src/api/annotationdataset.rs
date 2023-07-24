use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::store::*;

impl<'store> ResultItem<'store, AnnotationDataSet> {
    /// Returns an iterator over all data in this set
    pub fn data(&self) -> impl Iterator<Item = ResultItem<AnnotationData>> {
        self.as_ref()
            .data()
            .map(|item| item.as_resultitem(self.as_ref()))
    }

    /// Returns an iterator over all keys in this set
    pub fn keys(&self) -> impl Iterator<Item = ResultItem<DataKey>> {
        self.as_ref()
            .keys()
            .map(|item| item.as_resultitem(self.as_ref()))
    }

    /// Retrieve a key in this set
    pub fn key(&self, key: impl Request<DataKey>) -> Option<ResultItem<DataKey>> {
        self.as_ref()
            .get(key)
            .map(|x| x.as_resultitem(self.as_ref()))
            .ok()
    }

    /// Retrieve a single [`AnnotationData`] in this set
    ///
    /// Returns a reference to [`AnnotationData`] that is wrapped in a fat pointer
    /// ([`WrappedItem<AnnotationData>`]) that also contains reference to the store and which is
    /// immediately implements various methods for working with the type. If you need a more
    /// performant low-level method, use `StoreFor<T>::get()` instead.
    pub fn annotationdata<'a>(
        &'a self,
        annotationdata: impl Request<AnnotationData>,
    ) -> Option<ResultItem<'a, AnnotationData>> {
        self.as_ref()
            .get(annotationdata)
            .map(|x| x.as_resultitem(self.as_ref()))
            .ok()
    }

    /// Returns an iterator over annotations that directly point at the dataset, i.e. are metadata for it.
    /// If you want to iterator over all annotations that reference data from this set, use [`annotations()`] instead.
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_dataset_metadata(self.handle())
            .into_iter()
            .map(|v| v.iter())
            .flatten()
            .filter_map(|a_handle| store.annotation(*a_handle))
    }

    /// Finds the [`AnnotationData'] in the annotation dataset. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use `test_data` instead.
    ///
    /// If you pass an empty string literal or boolean to `key`, all keys will be searched.
    ///
    /// If you already have a `ResultItem<DataKey>` , use `ResultItem<DataKey>.find_data()` instead, it'll be much more efficient.
    ///
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    pub fn find_data<'a>(
        &self,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'a: 'store,
    {
        let mut key_handle: Option<DataKeyHandle> = None; //this means 'any' in this context
        if !key.any() {
            key_handle = key.to_handle(self.as_ref());
            if key_handle.is_none() {
                //requested key doesn't exist, bail out early, we won't find anything at all
                return None;
            }
        };
        let store = self.as_ref();
        Some(store.data().filter_map(move |annotationdata| {
            if (key_handle.is_none() || key_handle.unwrap() == annotationdata.key())
                && annotationdata.value().test(&value)
            {
                Some(annotationdata.as_resultitem(store))
            } else {
                None
            }
        }))
    }

    /// Tests if the dataset has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored.
    pub fn test_data<'a>(&self, key: impl Request<DataKey>, value: &'a DataOperator<'a>) -> bool {
        match self.find_data(key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Tests whether two AnnotationDataSets are the same
    pub fn test(&self, other: impl Request<AnnotationDataSet>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }

    /// Searches for annotations by data, invariant over all keys.
    /// If you want to search by a key, use `ResultItem<DataKey>.annotations_by_data()` instead, it'll be much more efficient.
    /// This returns an iterator returning both the annotation and the matching data item
    ///
    /// This may return the same annotation multiple times if different keys or annotationdata (e.g. multiple values) reference it!
    ///
    /// If you already have a `ResultItem<AnnotationData>` instance, just use `ResultItem<AnnotationData>.annotations()` instead, it'll be much more efficient.
    ///
    /// See `find_data()` for further parameter explanation.
    pub fn annotations_by_data<'a>(
        &self,
        value: &'a DataOperator<'a>,
    ) -> impl Iterator<
        Item = (
            ResultItem<'store, Annotation>,
            ResultItem<'store, AnnotationData>,
        ),
    >
    where
        'a: 'store,
    {
        let set_handle = self.handle();
        self.annotations()
            .map(move |annotation| {
                annotation
                    .find_data(set_handle, false, value)
                    .into_iter()
                    .flatten()
                    .map(move |data| (annotation.clone(), data))
            })
            .flatten()
    }

    /// Search for data *about* this text, i.e. data on annotations that refer to this set as metadata.
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn find_data_about<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<
        impl Iterator<
                Item = (
                    ResultItem<'store, AnnotationData>,
                    ResultItem<'store, Annotation>,
                ),
            > + 'store,
    >
    where
        'a: 'store,
    {
        let store = self.store();
        if let Some((test_set_handle, test_key_handle)) = store.find_data_request_resolver(set, key)
        {
            Some(
                self.annotations()
                    .map(move |annotation| {
                        annotation
                            .find_data(test_set_handle, test_key_handle, value)
                            .into_iter()
                            .flatten()
                            .map(move |data| (data, annotation.clone()))
                    })
                    .flatten(),
            )
        } else {
            None
        }
    }

    /// Test data *about* this dataset, i.e. data on annotations that refer to this dataset as metadata
    pub fn test_data_about<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> bool
    where
        'a: 'store,
    {
        match self.find_data_about(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }
}

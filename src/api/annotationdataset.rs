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

    /// Returns an iterator over annotations that directly point at the resource, i.e. are metadata for it.
    /// If you want to iterator over all annotations that reference data from this set, use [`annotations()`] instead.
    pub fn annotations_as_metadata(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_dataset_metadata(self.handle())
            .into_iter()
            .map(|v| v.iter())
            .flatten()
            .filter_map(|a_handle| store.annotation(*a_handle))
    }

    /// Returns an iterator over annotations that directly point at the resource, i.e. are metadata for it.
    /// If you want to iterator over all annotations that reference data from this set, use [`annotations()`] instead.
    pub fn annotations_using_set(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_dataset(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| store.annotation(a_handle))
    }

    /// Returns an iterator over all annotations that reference this dataset, both annotations that can be considered metadata as well
    /// annotations that make use of this set. The former are always returned before the latter.
    /// Use `annotations_as_metadata()` or `annotations_using_set()` instead if you want to differentiate the two.
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.annotations_as_metadata()
            .chain(self.annotations_using_set())
    }

    /// Returns a single [`AnnotationData'] in the annotation dataset that matches they key and value.
    /// Returns a single match, use `Self::find_data()` for a more extensive search.
    pub fn data_by_value(
        &self,
        key: impl Request<DataKey>,
        value: &DataValue,
    ) -> Option<ResultItem<'store, AnnotationData>> {
        self.as_ref()
            .data_by_value(key, value)
            .map(|annotationdata| annotationdata.as_resultitem(self.as_ref()))
    }

    /// Finds the [`AnnotationData'] in the annotation dataset. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use `test_data` instead.
    ///
    /// Provide `key`  as an Options, if set to `None`, all keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    pub fn find_data<'a>(
        &self,
        key: Option<impl Request<DataKey>>,
        value: DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'a: 'store,
    {
        let mut key_handle: Option<DataKeyHandle> = None; //this means 'any' in this context
        if let Some(key) = key {
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
    pub fn test_data<'a>(
        &self,
        key: Option<impl Request<DataKey>>,
        value: DataOperator<'a>,
    ) -> bool {
        match self.find_data(key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Tests whether two AnnotationDataSets are the same
    pub fn test(&self, other: impl Request<AnnotationDataSet>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }
}

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::api::annotation::AnnotationsIter;
use crate::api::annotationdata::DataIter;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::DataOperator;
use crate::{store::*, IntersectionIter};

use std::borrow::Cow;

impl<'store> ResultItem<'store, AnnotationDataSet> {
    /// Returns an iterator over all data in this set
    pub fn data(&self) -> DataIter<'store> {
        let set_handle = self.handle();
        let iter = self
            .as_ref()
            .data()
            .map(move |data| (set_handle, data.handle().expect("data must have handle")));
        DataIter::new(
            IntersectionIter::new_with_iterator(Box::new(iter), true),
            self.rootstore(),
        )
    }

    /// Returns an iterator over all keys in this set
    pub fn keys(&self) -> impl Iterator<Item = ResultItem<DataKey>> {
        self.as_ref()
            .keys()
            .map(|item| item.as_resultitem(self.as_ref(), self.rootstore()))
    }

    /// Retrieve a key in this set
    pub fn key(&self, key: impl Request<DataKey>) -> Option<ResultItem<'store, DataKey>> {
        self.as_ref()
            .get(key)
            .map(|x| x.as_resultitem(self.as_ref(), self.rootstore()))
            .ok()
    }

    /// Retrieve a single [`AnnotationData`] in this set
    ///
    /// Returns a reference to [`AnnotationData`] that is wrapped in a fat pointer
    /// ([`ResultItem<AnnotationData>`]) that also contains reference to the store and which is
    /// immediately implements various methods for working with the type. If you need a more
    /// performant low-level method, use `StoreFor<T>::get()` instead.
    pub fn annotationdata(
        &self,
        annotationdata: impl Request<AnnotationData>,
    ) -> Option<ResultItem<'store, AnnotationData>> {
        self.as_ref()
            .get(annotationdata)
            .map(|x| x.as_resultitem(self.as_ref(), self.rootstore()))
            .ok()
    }

    /// Returns an iterator over annotations that directly point at the dataset, i.e. are metadata for it (via a DataSetSelector).
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        let store = self.store();
        if let Some(annotations) = store.annotations_by_dataset_metadata(self.handle()) {
            AnnotationsIter::new(
                IntersectionIter::new(Cow::Borrowed(annotations), true),
                self.store(),
            )
        } else {
            AnnotationsIter::new_empty(self.store())
        }
    }

    /// Finds the [`AnnotationData'] in the annotation dataset. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use `test_data` instead.
    ///
    /// If you pass an empty string literal or boolean to `key`, all keys will be searched.
    ///
    /// If you already have a `ResultItem<DataKey>` , use `ResultItem<DataKey>.find_data()` instead, it'll be much more efficient.
    ///
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    pub fn find_data<'q>(
        &self,
        key: impl Request<DataKey>,
        value: DataOperator<'q>,
    ) -> DataIter<'store>
    where
        'q: 'store,
    {
        if !key.any() {
            if let Some(key) = self.key(key) {
                if let DataOperator::Any = value {
                    return key.data();
                } else {
                    return key.data().filter_value(value);
                }
            } else {
                //requested key doesn't exist, bail out early, we won't find anything at all
                return DataIter::new_empty(self.rootstore());
            }
        };

        //any key
        if let DataOperator::Any = value {
            self.data()
        } else {
            self.data().filter_value(value)
        }
    }

    /// Tests if the dataset has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored.
    pub fn test_data<'a>(&self, key: impl Request<DataKey>, value: DataOperator<'a>) -> bool {
        self.find_data(key, value).next().is_some()
    }

    /// Tests whether two AnnotationDataSets are the same
    pub fn test(&self, other: impl Request<AnnotationDataSet>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }
}

use crate::annotation::Annotation;
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::annotation::AnnotationsIter;
use crate::datakey::DataKey;
use crate::datavalue::{DataOperator, DataValue};
use crate::resources::TextResource;
use crate::selector::Selector;
use crate::store::*;
use crate::IntersectionIter;
use std::borrow::Cow;
use std::collections::BTreeSet;

impl<'store> ResultItem<'store, AnnotationData> {
    /// Method to return a reference to the dataset that holds this data
    pub fn set(&self) -> ResultItem<'store, AnnotationDataSet> {
        let rootstore = self.rootstore();
        self.store().as_resultitem(rootstore, rootstore)
    }

    /// Return a reference to data value
    pub fn value(&self) -> &'store DataValue {
        self.as_ref().value()
    }

    pub fn key(&self) -> ResultItem<'store, DataKey> {
        self.store()
            .key(self.as_ref().key())
            .expect("AnnotationData must always have a key at this point")
            .as_resultitem(self.store(), self.rootstore())
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that makes use of this data.
    /// The iterator returns the annoations as [`WrappedItem<Annotation>`].
    /// Especially useful in combination with a call to  [`WrappedItem<AnnotationDataSet>.find_data()`] or [`AnnotationDataSet.annotationdata()`] first.
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let set_handle = self.store().handle().expect("set must have handle");
        let annotationstore = self.rootstore();
        annotationstore
            .annotations_by_data_indexlookup(set_handle, self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| annotationstore.annotation(*a_handle))
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

pub struct DataIter<'store> {
    iter: Option<IntersectionIter<'store, (AnnotationDataSetHandle, AnnotationDataHandle)>>,
    cursor: usize,
    store: &'store AnnotationStore,

    operator: Option<DataOperator<'store>>,

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
            cursor: 0,
            iter: Some(iter),
            store,
            last_set_handle: None,
            last_set: None,
            operator: None,
        }
    }

    pub(crate) fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            cursor: 0,
            iter: None,
            store,
            last_set_handle: None,
            last_set: None,
            operator: None,
        }
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

    /// Constrain the iterator to return only the data that uses the specified key
    /// Shortcut for `self.filter_data(key.data())`.
    pub fn filter_key(mut self, key: &ResultItem<'store, DataKey>) -> Self {
        self.filter_data(key.data())
    }

    pub fn filter_data(self, data: DataIter<'store>) -> Self {
        self.merge(data)
    }

    /// Select data by testing the value (mediated by a [`DataOperator']).
    /// Consumes the iterator and returns an iterator over AnnotationData instances
    ///
    /// Note: You can only use this method once (it will overwrite earlier value filters, use DataOperator::Or and DataOperator::And to test against multiple values)
    pub fn filter_value(mut self, operator: DataOperator<'store>) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Iterate over the annotations that make use of data in this iterator.
    /// Annotations will be returnted chronologically (add `.textual_order()` to sort textually) and contain no duplicates.
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
            self.iter = Some(self.iter.unwrap().extend(other.iter.unwrap()));
        } else if self.iter.is_none() {
            return other;
        }
        self
    }

    /// Extract a low-level vector of handles from this iterator.
    /// This is different than running `collect()`, which produces high-level objects.
    ///
    /// An extracted vector can be easily turned back into a DataIter again with [`Self.from_vec()`]
    pub fn to_vec(mut self) -> Vec<(AnnotationDataSetHandle, AnnotationDataHandle)> {
        if self.operator.is_none() {
            //we can be a bit more performant if we have no operator:
            let mut results: Vec<(AnnotationDataSetHandle, AnnotationDataHandle)> = Vec::new();
            loop {
                if self.iter.is_some() {
                    if let Some((set_handle, data_handle)) = self.iter.as_mut().unwrap().next() {
                        results.push((set_handle, data_handle));
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            results
        } else {
            // go to higher-level (needed to deal with the operator) and back to handles
            self.map(|annotationdata| (annotationdata.set().handle(), annotationdata.handle()))
                .collect()
        }
    }

    /// Turns a low-level vector into a DataIter. Borrows the underlying vec, use [`Self::from_vec_owned()`] if you need an owned variant.
    /// A low-level vector can be produced from any DataIter with [`Self.to_vec()`]
    /// The `sorted` parameter expressed whether the underlying vector is sorted (you need to set it, it does not do anything itself)
    pub fn from_vec<'a>(
        vec: &'a Vec<(AnnotationDataSetHandle, AnnotationDataHandle)>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self
    where
        'a: 'store,
    {
        Self::new(IntersectionIter::new(Cow::Borrowed(vec), sorted), store)
    }

    /// Turns a low-level vector into a DataIter. Owns the underlying vec, use [`Self::from_vec()`] if you want a borrowed variant.
    /// A low-level vector can be produced from any DataIter with [`Self.to_vec()`]
    /// The `sorted` parameter expressed whether the underlying vector is sorted (you need to set it, it does not do anything itself)
    pub fn from_vec_owned(
        vec: Vec<(AnnotationDataSetHandle, AnnotationDataHandle)>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self {
        Self::new(IntersectionIter::new(Cow::Owned(vec), sorted), store)
    }
}

impl<'store> Iterator for DataIter<'store> {
    type Item = ResultItem<'store, AnnotationData>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iter.as_mut() {
                if let Some((set_handle, data_handle)) = iter.next() {
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

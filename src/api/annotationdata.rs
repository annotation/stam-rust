use crate::annotation::Annotation;
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::annotation::AnnotationsIter;
use crate::datakey::DataKey;
use crate::datavalue::{DataOperator, DataValue};
use crate::resources::TextResource;
use crate::selector::SelectorKind;
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
            .map(|annotation| {
                annotation.resources().filter_map(|resource| {
                    if resource.selector().kind() == SelectorKind::ResourceSelector {
                        Some(resource.clone())
                    } else {
                        None
                    }
                })
            })
            .flatten()
            .collect()
    }

    /// Returns an iterator over all text resources that make use of this data via annotations via a TextSelector (i.e. on text)
    pub fn resources_on_text(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| {
                annotation
                    .resources()
                    .filter_map(|resource| match resource.selector().kind() {
                        SelectorKind::TextSelector | SelectorKind::ResourceSelector => {
                            Some(resource.clone())
                        }
                        _ => None,
                    })
            })
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
        }
    }

    pub(crate) fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            cursor: 0,
            iter: None,
            store,
            last_set_handle: None,
            last_set: None,
        }
    }

    /// Constrain the iterator to return only the data that used by the specified annotation
    pub fn filter_annotation(mut self, annotation: &ResultItem<'store, Annotation>) -> Self {
        let annotation_data = annotation.as_ref().raw_data();
        if let Some(iter) = self.iter.as_mut() {
            *iter = iter.with(Cow::Borrowed(annotation_data), true);
        }
        self
    }

    pub fn filter_annotations(mut self, annotations: AnnotationsIter<'store>) -> Self {
        todo!("implement");
    }

    pub fn filter_data(mut self, data: DataIter<'store>) -> Self {
        self.iter.merge(data.iter);
        self
    }

    /// Iterate over the annotations that make use of data in this iterator
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        todo!("implement");
    }
}

impl<'store> Iterator for DataIter<'store> {
    type Item = ResultItem<'store, AnnotationData>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.iter.as_mut() {
            if let Some((set_handle, data_handle)) = iter.next() {
                //optimisation so we don't have to grab the same set over and over:
                let dataset = if Some(set_handle) == self.last_set_handle {
                    self.last_set.unwrap()
                } else {
                    self.last_set_handle = Some(set_handle);
                    self.last_set = Some(self.store.dataset(set_handle).expect("set must exist"));
                    self.last_set.unwrap()
                };
                let data = dataset
                    .annotationdata(data_handle)
                    .expect("data must exist");
                return Some(data);
            }
        }
        None
    }
}

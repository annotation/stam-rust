use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::datavalue::{DataOperator, DataValue};
use crate::resources::TextResource;
use crate::selector::SelectorKind;
use crate::store::*;
use std::collections::BTreeSet;

impl<'store> ResultItem<'store, AnnotationData> {
    /// Method to return a reference to the dataset that holds this data
    pub fn set(&self, store: &'store AnnotationStore) -> ResultItem<'store, AnnotationDataSet> {
        self.store().as_resultitem(store)
    }

    /// Return a reference to data value
    pub fn value(&self) -> &'store DataValue {
        self.as_ref().value()
    }

    pub fn key(&self) -> ResultItem<'store, DataKey> {
        self.store()
            .key(self.as_ref().key())
            .expect("AnnotationData must always have a key at this point")
            .as_resultitem(self.store())
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that makes use of this data.
    /// The iterator returns the annoations as [`WrappedItem<Annotation>`].
    /// Especially useful in combination with a call to  [`WrappedItem<AnnotationDataSet>.find_data()`] or [`AnnotationDataSet.annotationdata()`] first.
    pub fn annotations(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let set_handle = self.store().handle().expect("set must have handle");
        annotationstore
            .annotations_by_data_indexlookup(set_handle, self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| annotationstore.annotation(*a_handle))
    }

    /// Returns the number of annotations ([`Annotation`]) that make use of this data.
    pub fn annotations_len(&self, annotationstore: &'store AnnotationStore) -> usize {
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
    pub fn resources(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations(annotationstore)
            .map(|annotation| annotation.resources().map(|resource| resource.clone()))
            .flatten()
            .collect()
    }

    /// Returns an set of all text resources that make use of this data via annotations via a ResourceSelector (i.e. as metadata)
    pub fn resources_as_metadata(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations(annotationstore)
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
    pub fn resources_on_text(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations(annotationstore)
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

    /// Returns an iterator over all data sets that annotations using this data reference via a DataSetSelector (i.e. metadata)
    pub fn datasets(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> BTreeSet<ResultItem<'store, AnnotationDataSet>> {
        self.annotations(annotationstore)
            .map(|annotation| annotation.datasets().map(|dataset| dataset.clone()))
            .flatten()
            .collect()
    }
}

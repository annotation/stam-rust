use std::borrow::Cow;
use std::collections::BTreeSet;

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::api::annotation::AnnotationsIter;
use crate::api::annotationdata::DataIter;
use crate::datakey::DataKey;
use crate::resources::TextResource;
use crate::selector::SelectorKind;
use crate::store::*;
use crate::IntersectionIter;

impl<'store> ResultItem<'store, DataKey> {
    /// Method to return a reference to the dataset that holds this key
    pub fn set(&self) -> ResultItem<'store, AnnotationDataSet> {
        let rootstore = self.rootstore();
        self.store().as_resultitem(rootstore, rootstore)
    }

    /// Returns the public identifier that identifies the key
    pub fn as_str(&self) -> &'store str {
        self.as_ref().as_str()
    }

    /// Returns an iterator over all data ([`AnnotationData`]) that makes use of this key.
    /// Use methods on this iterator like `DataIter.filter_value()` to further constrain the results.
    pub fn data(&self) -> DataIter<'store> {
        let rootstore = self.rootstore();
        let store = self.store();
        if let Some(vec) = store.data_by_key(self.handle()) {
            let iter = vec
                .iter()
                .map(|datahandle| (store.handle().unwrap(), *datahandle));
            DataIter::new(
                IntersectionIter::new_with_iterator(Box::new(iter), true),
                self.rootstore(),
            )
        } else {
            DataIter::new_empty(self.rootstore())
        }
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that make use of this key.
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        let set_handle = self.store().handle().expect("set must have handle");
        let annotationstore = self.rootstore();
        let annotations: Vec<_> = annotationstore.annotations_by_key(set_handle, self.handle()); //MAYBE TODO: extra reverse index so we can borrow directly?
        AnnotationsIter::new(
            IntersectionIter::new(Cow::Owned(annotations), true),
            self.rootstore(),
        )
    }

    /// Returns the number of annotations that make use of this key.
    ///  Note: this method has suffix `_count` instead of `_len` because it is not O(1) but does actual counting (O(n) at worst).
    /// (This function internally allocates a temporary buffer to ensure no duplicates are returned)
    pub fn annotations_count(&self) -> usize {
        self.rootstore()
            .annotations_by_key(
                self.store().handle().expect("set must have handle"),
                self.handle(),
            )
            .len()
    }

    /// Tests whether two DataKeys are the same
    pub fn test(&self, other: impl Request<DataKey>) -> bool {
        if other.any() {
            true
        } else {
            self.handle() == other.to_handle(self.store()).expect("key must have handle")
        }
    }

    /// Returns an iterator over all text resources that make use of this key via annotations (either as metadata or on text)
    /// (This function allocates a temporary buffer to ensure no duplicates are returned)
    pub fn resources(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources().map(|resource| resource.clone()))
            .flatten()
            .collect()
    }

    /// Returns resources that make use of this key as metadata (via annotation with a ResourceSelector)
    pub fn resources_as_metadata(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources_as_metadata())
            .flatten()
            .collect()
    }

    /// Returns a set of all text resources that make use of this key via annotations via a ResourceSelector (i.e. as metadata)
    pub fn resources_on_text(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.annotations()
            .map(|annotation| annotation.resources_on_text())
            .flatten()
            .collect()
    }

    /// Returns a set of all data sets that annotations using this key reference via a DataSetSelector (i.e. metadata)
    pub fn datasets(&self) -> BTreeSet<ResultItem<'store, AnnotationDataSet>> {
        self.annotations()
            .map(|annotation| annotation.datasets().map(|dataset| dataset.clone()))
            .flatten()
            .collect()
    }
}

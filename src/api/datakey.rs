use std::collections::BTreeSet;

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::datakey::DataKey;
use crate::resources::TextResource;
use crate::selector::SelectorKind;
use crate::{store::*, DataOperator};

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
    pub fn data(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store {
        let rootstore = self.rootstore();
        let store = self.store();
        store
            .data_by_key(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|data_handle| {
                store
                    .annotationdata(*data_handle)
                    .map(|d| d.as_resultitem(store, rootstore))
            })
    }

    /// Searches for data for this data key
    pub fn find_data<'a>(
        &self,
        value: &'a DataOperator<'a>,
    ) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store
    where
        'a: 'store,
    {
        self.data().filter_map(|data| {
            if data.test(false, value) {
                Some(data)
            } else {
                None
            }
        })
    }

    /// Tests for the presence of data for this data key
    pub fn test_data<'a>(&self, value: &'a DataOperator<'a>) -> bool {
        self.data().any(|data| data.test(false, value))
    }

    /// Searches for annotations by data.
    /// Returns an iterator returning both the annotation and the matching data item
    ///
    /// This may return the same annotation multiple times if different annotationdata (e.g. multiple values) reference it!
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
        let set_handle = self.store().handle().expect("set must have handle");
        let key_handle = self.handle();
        self.annotations()
            .map(move |annotation| {
                annotation
                    .find_data(set_handle, key_handle, value)
                    .into_iter()
                    .flatten()
                    .map(move |data| (annotation.clone(), data))
            })
            .flatten()
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that make use of this key.
    /// Especially useful in combination with a call to  [`AnnotationDataSet.key()`] first.
    ///
    /// (This function internally allocates a temporary buffer to ensure no duplicates are returned)
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let set_handle = self.store().handle().expect("set must have handle");
        let annotationstore = self.rootstore();
        annotationstore
            .annotations_by_key(set_handle, self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| annotationstore.annotation(*a_handle))
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
            .map(|x| x.len())
            .unwrap_or(0)
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

    /// Returns a set of all text resources that make use of this key via annotations via a ResourceSelector (i.e. as metadata)
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

    /// Returns a set of all data sets that annotations using this key reference via a DataSetSelector (i.e. metadata)
    pub fn datasets(&self) -> BTreeSet<ResultItem<'store, AnnotationDataSet>> {
        self.annotations()
            .map(|annotation| annotation.datasets().map(|dataset| dataset.clone()))
            .flatten()
            .collect()
    }
}

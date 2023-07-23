use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::store::*;

//TODO: implement reference to rootstore so we don't need to pass AnnotationStore to various methods
impl<'store> ResultItem<'store, DataKey> {
    /// Method to return a reference to the dataset that holds this key
    pub fn set(&self, store: &'store AnnotationStore) -> ResultItem<'store, AnnotationDataSet> {
        self.store().as_resultitem(store)
    }

    /// Returns the public identifier that identifies the key
    pub fn as_str(&self) -> &'store str {
        self.as_ref().as_str()
    }

    /// Returns an iterator over all data ([`AnnotationData`]) that makes use of this key.
    pub fn data(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store {
        let store = self.store();
        store
            .data_by_key(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|data_handle| {
                store
                    .annotationdata(*data_handle)
                    .map(|d| d.as_resultitem(store))
            })
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that make use of this key.
    /// Especially useful in combination with a call to  [`AnnotationDataSet.key()`] first.
    pub fn annotations(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + 'store> {
        if let Some(iter) = annotationstore.annotations_by_key(
            self.store().handle().expect("set must have handle"),
            self.handle(),
        ) {
            Some(iter.filter_map(|a_handle| annotationstore.annotation(a_handle)))
        } else {
            None
        }
    }

    /// Returns the number of annotations that make use of this key.
    ///  Note: this method has suffix `_count` instead of `_len` because it is not O(1) but does actual counting (O(n) at worst).
    pub fn annotations_count(&self, annotationstore: &'store AnnotationStore) -> usize {
        if let Some(iter) = annotationstore.annotations_by_key(
            self.store().handle().expect("set must have handle"),
            self.handle(),
        ) {
            iter.count()
        } else {
            0
        }
    }

    /// Tests whether two DataKeys are the same
    pub fn test(&self, other: &BuildItem<DataKey>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }
}

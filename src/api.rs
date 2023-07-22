use std::marker::PhantomData;

use crate::annotation::Annotation;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::resources::TextResource;
use crate::store::*;
use crate::types::*;

impl AnnotationStore {
    /// Requests a specific [`TextResource`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a &str/String or handle.
    ///
    /// The item is returned as a fat pointer [`ResultItem<TextResource>']) in an Option.
    /// Returns `None` if it does not exist.
    pub fn resource(
        &self,
        request: impl Request<TextResource>,
    ) -> Option<ResultItem<TextResource>> {
        self.get(request).map(|x| x.as_resultitem(self)).ok()
    }

    /// Requests a specific [`AnnotationDataSet`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a &str,String or handle.
    ///
    /// The item is returned as a fat pointer [`ResultItem<AnnotationDataSet>']) in an Option.
    /// Returns `None` if it does not exist.
    pub fn dataset(
        &self,
        request: impl Request<AnnotationDataSet>,
    ) -> Option<ResultItem<AnnotationDataSet>> {
        self.get(request).map(|x| x.as_resultitem(self)).ok()
    }

    /// Requests a specific [`Annotation`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a &str,String or handle.
    ///
    /// The item is returned as a fat pointer [`ResultItem<Annotation>']) in an Option.
    /// Returns `None` if it does not exist.
    pub fn annotation(&self, request: impl Request<Annotation>) -> Option<ResultItem<Annotation>> {
        self.get(request).map(|x| x.as_resultitem(self)).ok()
    }

    /// Returns an iterator over all text resources ([`TextResource`] instances) in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>']) .
    pub fn resources(&self) -> impl Iterator<Item = ResultItem<TextResource>> {
        self.iter()
    }

    /// Returns an iterator over all [`AnnotationDataSet`] instances in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>']) .
    pub fn datasets<'a>(&'a self) -> impl Iterator<Item = ResultItem<AnnotationDataSet>> {
        self.iter()
    }

    /// Returns an iterator over all annotations ([`Annotation`] instances) in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>']) .
    pub fn annotations<'a>(&'a self) -> impl Iterator<Item = ResultItem<Annotation>> {
        self.iter()
    }
}

impl<'store> ResultItem<'store, TextResource> {
    /// Iterates over all annotations about this resource as a whole, i.e. Annotations with a ResourceSelector.
    /// Such annotations can be considered metadata.
    pub fn annotations_about_metadata(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_resource_metadata(self.handle())
            .into_iter()
            .map(|v| v.iter())
            .flatten()
            .filter_map(|a_handle| store.annotation(*a_handle))
    }

    /// Iterates over all annotations about this text in this resource i.e. Annotations with a TextSelector.
    pub fn annotations_about_text(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_resource(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| store.annotation(a_handle))
    }

    /// Iterates over all annotations about this resource, both annotations that can be considered metadata as well
    /// annotations that reference a portion of the text. The former are always returned before the latter.
    pub fn annotations_about(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.annotations_about_metadata()
            .chain(self.annotations_about_text())
    }
}

/*
pub struct AnnotationsIter<T, I> {
    iter: I,
    _phantomdata: PhantomData<T>,
}

impl<'store, I> Iterator for AnnotationsIter<TextResource, I>
where
    I: Iterator<Item = ResultItem<'store, TextResource>>,
{
    type Item = ResultItem<'store, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {
    }
}

impl<'store, I> Iterator for AnnotationsIter<Annotation, I>
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    type Item = ResultItem<'store, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {}
}
*/

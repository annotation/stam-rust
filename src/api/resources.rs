use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::TextResource;
use crate::store::*;
use crate::textselection::{
    ResultTextSelection, TextSelection, TextSelectionOperator, TextSelectionSet,
};

impl<'store> ResultItem<'store, TextResource> {
    /// Returns an iterator over all annotations about this resource as a whole, i.e. Annotations with a ResourceSelector.
    /// Such annotations can be considered metadata.
    pub fn annotations_as_metadata(
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

    /// Returns an iterator over all annotations about any text in this resource i.e. Annotations with a TextSelector.
    pub fn annotations_on_text(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_resource(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| store.annotation(a_handle))
    }

    /// Returns an iterator over all annotations that reference this resource, both annotations that can be considered metadata as well
    /// annotations that reference a portion of the text. The former are always returned before the latter.
    /// Use `annotations_as_metadata()` or `annotations_on_text()` instead if you want to differentiate the two.
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.annotations_as_metadata()
            .chain(self.annotations_on_text())
    }

    /// Returns an iterator over all text selections that are marked in this resource (i.e. there are one or more annotations on it).
    /// They are returned in textual order, but this does not come with any significant performance overhead. If you want an unsorted version use [`self.as_ref().textselections_unsorted()`] instead.
    /// This is a double-ended iterator that can be traversed in both directions.
    pub fn textselections(&self) -> impl DoubleEndedIterator<Item = ResultTextSelection<'store>> {
        let resource = self.as_ref();
        let annotationstore = self.rootstore();
        resource
            .iter()
            .map(|item| item.as_resultitem(resource, annotationstore).into())
    }

    /// Returns a sorted double-ended iterator over a range of all textselections and returns all
    /// textselections that either start or end in this range (depending on the direction you're
    /// iterating in)
    pub fn textselections_in_range(
        &self,
        begin: usize,
        end: usize,
    ) -> impl DoubleEndedIterator<Item = ResultTextSelection<'store>> {
        let resource = self.as_ref();
        let annotationstore = self.rootstore();
        resource
            .range(begin, end)
            .map(|item| item.as_resultitem(resource, annotationstore).into())
    }

    /// Returns the number of textselections that are marked in this resource (i.e. there are one or more annotations on it).
    pub fn textselections_len(&self) -> usize {
        self.as_ref().textselections_len()
    }

    /// Find textselections by applying a text selection operator ([`TextSelectionOperator`]) to a
    /// one or more querying textselections. Returns an iterator over all matching
    /// text selections in the resource.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
        refset: impl Into<TextSelectionSet>,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let resource = self.as_ref();
        let annotationstore = self.rootstore();
        resource
            .textselections_by_operator(operator, refset.into())
            .map(|ts_handle| {
                let textselection: &'store TextSelection = resource
                    .get(ts_handle)
                    .expect("textselection handle must be valid");
                textselection
                    .as_resultitem(resource, annotationstore)
                    .into()
            })
    }

    /*
    /// Find textselections by applying a text selection operator ([`TextSelectionOperator`]) to a
    /// one or more querying textselections (in an [`TextSelectionSet']). Returns an iterator over all matching
    /// text selections in the resource, as [`ResultItem<TextSelection>`].
    pub fn find_textselections_ref<'q>(
        &self,
        operator: TextSelectionOperator,
        refset: &'q TextSelectionSet,
    ) -> impl Iterator<Item = ResultItem<'store, TextSelection>> + 'q
    where
        'store: 'q, //store lives at least as long as 'q
    {
        self.as_ref()
            .textselections_by_operator_ref(operator, reset)
            .map(|ts_handle| {
                let textselection: &'store TextSelection = self
                    .as_ref()
                    .get(ts_handle)
                    .expect("textselection handle must be valid");
                textselection.as_resultitem(self.as_ref())
            })
    }
    */

    /// Search for data *about* this text, i.e. data on annotations that refer to this resource in any way.
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

    /// Search for data *about* this text, i.e. data on annotations that refer to this resource as metadata.
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn find_metadata_about<'a>(
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
                self.annotations_as_metadata()
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

    /// Search for data *about* this text, i.e. data on annotations that refer to text in this resource.
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn find_data_about_text<'a>(
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
                self.annotations_on_text()
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

    /// Test data *about* this resource, i.e. data on annotations that refer to this resource in any way
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

    /// Test data *about* this resource, i.e. data on metadata annotations that refer to this resource
    pub fn test_metadata_about<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> bool
    where
        'a: 'store,
    {
        match self.find_metadata_about(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Test data *about* this resource, i.e. data on annotations that refer to text in this resource
    pub fn test_data_about_text<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> bool
    where
        'a: 'store,
    {
        match self.find_data_about_text(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }
}

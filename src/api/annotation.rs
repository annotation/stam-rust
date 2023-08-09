use std::marker::PhantomData;

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::api::{FindText, TargetIter};
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::error::StamError;
use crate::resources::TextResource;
use crate::selector::{Selector, SelectorIter};
use crate::store::*;
use crate::text::Text;
use crate::textselection::{
    ResultTextSelection, TextSelection, TextSelectionOperator, TextSelectionSet,
};

use crate::api::textselection::SortTextualOrder;

impl<'store> ResultItem<'store, Annotation> {
    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns no duplicates even if a resource is referenced multiple times.
    /// If you want to distinguish between resources references as metadata and on text, check  `selector().kind()` on the return values.
    pub fn resources(&self) -> TargetIter<'store, TextResource> {
        let selector_iter: SelectorIter<'store> =
            self.as_ref().target().iter(self.store(), true, false);
        //                                            ^--- recurse
        TargetIter::new(self.store(), selector_iter, false)
    }

    /// Returns an iterator over the datasets that this annotation (by its target selector) references
    /// This returns no duplicates even if a dataset is referenced multiple times.
    pub fn datasets(&self) -> TargetIter<'store, AnnotationDataSet> {
        let selector_iter: SelectorIter<'store> =
            self.as_ref().target().iter(self.store(), true, false);
        //                                            ^--- recurse
        TargetIter::new(self.store(), selector_iter, false)
    }

    /// Iterates over all the annotations this annotation targets (i.e. via a [`Selector::AnnotationSelector'])
    /// Use [`Self.annotations()'] if you want to find the annotations that reference this one (the reverse).
    ///
    /// This does no sorting, if you want results in textual order, add `.textual_order()`. Duplicates are already handled.
    pub fn annotations_in_targets(
        &self,
        recursive: bool,
        track_ancestors: bool,
    ) -> TargetIter<'store, Annotation> {
        let selector_iter: SelectorIter<'store> =
            self.as_ref()
                .target()
                .iter(self.store(), recursive, track_ancestors);
        TargetIter::new(self.store(), selector_iter, false)
    }

    /// Iterates over all the annotations that reference this annotation, if any
    /// If you want to find the annotations this annotation targets, then use [`Self::annotations_in_targets()`] instead.
    ///
    /// This does no sorting nor deduplication, if you want results in textual order, add `.textual_order()`
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        self.store()
            .annotations_by_annotation_reverse(self.handle())
            .into_iter()
            .flatten()
            .map(|a_handle| {
                store
                    .annotation(*a_handle)
                    .expect("annotation handle must be valid")
            })
    }

    /// Iterate over all text selections this annotation references (i.e. via [`Selector::TextSelector`])
    /// They are returned in the exact order as they were selected.
    pub fn textselections(&self) -> impl Iterator<Item = ResultTextSelection<'store>> + 'store {
        let store = self.store();
        store
            .textselections_by_selector(self.as_ref().target())
            .into_iter()
            .map(|(resource, textselection)| {
                ResultTextSelection::Bound(textselection.as_resultitem(resource, store))
            })
    }

    /// Iterates over all text slices this annotation refers to
    /// They are returned in the exact order as they were selected.
    pub fn text(&self) -> impl Iterator<Item = &'store str> {
        self.textselections()
            .map(|textselection| textselection.text())
    }

    /// If this annotation refers to a single simple text slice,
    /// this returns it. If not contains no text or multiple text references, it returns None.
    pub fn text_simple(&self) -> Option<&'store str> {
        let mut iter = self.text();
        let text = iter.next();
        if let None = iter.next() {
            return text;
        } else {
            None
        }
    }

    /// Returns all underlying text for this annotation concatenated
    pub fn text_join(&self, delimiter: &str) -> String {
        let mut s = String::new();
        for slice in self.text() {
            if !s.is_empty() {
                s += delimiter;
            }
            s += slice;
        }
        s
    }

    /// Returns the (single!) resource the annotation points to. Only works for TextSelector,
    /// ResourceSelector and AnnotationSelector, and not for complex selectors.
    pub fn resource(&self) -> Option<ResultItem<'store, TextResource>> {
        match self.as_ref().target() {
            Selector::TextSelector(res_id, _, _)
            | Selector::ResourceSelector(res_id)
            | Selector::AnnotationSelector(_, Some((res_id, _, _))) => {
                self.store().resource(*res_id)
            }
            Selector::AnnotationSelector(a_id, _) => {
                //still needed for targeted annotations with a ResourceSelector rather than a textselector
                if let Some(annotation) = self.store().annotation(*a_id) {
                    annotation.resource()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Finds [`AnnotationData'] pertaining directly to this annotation, using data search criteria.
    /// This returns an iterator over all matches.
    ///
    /// If you are not interested in returning the results but merely testing the presence of particular data,
    /// then use `test_data` instead..
    ///
    /// You can pass a boolean (true/false, doesn't matter) or empty string literal for set or key to represent any set/key.
    /// To search for any value, `value` can must be explicitly set to `DataOperator::Any` to return all values.
    ///
    /// Value is a DataOperator that can apply a data test to the value. Use `DataOperator::Equals` to search
    /// for an exact value. As a shortcut, you can pass `"value".into()`  to the automatically convert into an equality
    /// DataOperator.
    ///
    /// Example call to retrieve all data indiscriminately: `annotation.data(false,false, DataOperator::Any)`
    ///
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored!! You can not
    ///       search for keys if you don't know their set.
    pub fn find_data<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'a: 'store,
    {
        let store = self.store();
        if let Some((test_set_handle, test_key_handle)) = store.find_data_request_resolver(set, key)
        {
            Some(
                self.as_ref()
                    .data()
                    .filter_map(move |(dataset_handle, data_handle)| {
                        if test_set_handle.is_none() || test_set_handle.unwrap() == *dataset_handle
                        {
                            Some(
                                store
                                    .get(*dataset_handle)
                                    .map(|set| {
                                        set.annotationdata(*data_handle)
                                            .map(|data| data.as_resultitem(set, store))
                                            .expect("data must exist")
                                    })
                                    .expect("set must exist"),
                            )
                        } else {
                            None
                        }
                    })
                    .filter_map(move |annotationdata| {
                        if (test_key_handle.is_none()
                            || test_key_handle.unwrap() == annotationdata.key().handle())
                            && annotationdata.as_ref().value().test(&value)
                        {
                            Some(annotationdata)
                        } else {
                            None
                        }
                    }),
            )
        } else {
            None
        }
    }

    /// Shortcut method to get all data
    pub fn data(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store {
        self.find_data(false, false, &DataOperator::Any)
            .expect("must return an iterator")
    }

    /// Tests if the annotation has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `data()` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    ///
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored!! You can not
    ///       search for keys if you don't know their set.
    pub fn test_data<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> bool {
        match self.find_data(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Search for data *about* this annotation, i.e. data on other annotation that refer to this one.
    /// Do not confuse this with the data this annotation holds, which can be searched with [`Self.find_data()`].
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

    /// Search for data in annotations targeted by this one (i.e. via an AnnotationSelector).
    /// Do not confuse this with the data this annotation holds, which can be searched with [`Self.find_data()`],
    /// or with annotations that target the instance in question, which can be searched with [`Self.find_data_about()`].
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn find_data_in_targets<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
        recursive: bool,
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
                self.annotations_in_targets(recursive, false)
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

    /// Search for data in annotations targeted this one (i.e. via an AnnotationSelector).
    /// Do not confuse this with the data this annotation holds, which can be searched with [`Self.find_data()`],
    /// or with annotations that target the instance in question, which can be searched with [`Self.find_data_about()`].
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn test_data_in_targets<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
        recursive: bool,
    ) -> bool {
        let store = self.store();
        if let Some((test_set_handle, test_key_handle)) = store.find_data_request_resolver(set, key)
        {
            self.annotations_in_targets(recursive, false)
                .any(move |annotation| {
                    annotation.test_data(test_set_handle, test_key_handle, value)
                })
        } else {
            false
        }
    }

    /// Shortcut method to get all data *about* this annotation, i.e. data on other annotation that refer to this one.
    /// Do not confuse this with the data this annotation holds, which can be obtained via [`Self.data()`].
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn data_about(
        &self,
    ) -> Option<
        impl Iterator<
                Item = (
                    ResultItem<'store, AnnotationData>,
                    ResultItem<'store, Annotation>,
                ),
            > + 'store,
    > {
        self.find_data_about(false, false, &DataOperator::Any)
    }

    /// Test data *about* this annotation, i.e. data on other annotation that refer to this one.
    /// Do not confuse this with the data this annotation holds, which can be tested via [`Self.test_data()`].
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

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the text relations pertaining to the annotations. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultTextSelection`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.annotations_by_related_text()`] instead.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        //first we gather all textselections for this annotation in a set, as the chosen operator may apply to them jointly
        let tset: TextSelectionSet = self.textselections().collect();
        tset.as_resultset(self.store()).related_text(operator)
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the text selections of the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultTextSelection`]).
    /// If you are interested in the text selections only, use [`Self.related_text`] instead.
    ///
    /// Note: this may return the current annotation again if it is referenced by related text!
    pub fn annotations_by_related_text(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.related_text(operator)
            .filter_map(|tsel| tsel.annotations())
            .flatten()
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the current one *and* that match specific data. Returns a set of annotations.
    /// If you also want to filter based on the data, use [`Self.annotations_by_related_text_matching_data()`]
    /// If you are interested in the text selections only, use [`Self.related_text()`] instead.
    /// The annotations are returned in textual order.
    pub fn annotations_by_related_text_and_data<'a>(
        &self,
        operator: TextSelectionOperator,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Vec<ResultItem<'store, Annotation>>
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) =
            self.rootstore().find_data_request_resolver(set, key)
        {
            self.related_text(operator)
                .map(move |tsel| {
                    tsel.find_data_about(test_set_handle, test_key_handle, value)
                        .into_iter()
                        .flatten()
                        .map(|(_data, annotation)| annotation)
                })
                .flatten()
                .textual_order()
        } else {
            Vec::new()
        }
    }

    /// This selects text in a specific relation to the text of the current annotation, where that has text has certain data describing it.
    /// It returns both the matching text and for each also the matching annotation data and matching annotation
    /// If you do not wish to return the data, but merely test for it, then use [`Self.related_text_test_data()`] instead.
    /// It effectively combines `related_text()` with `find_data_about()` on its results, into a single method.
    /// See these methods for further parameter explanation.
    pub fn related_text_with_data<'a>(
        &self,
        operator: TextSelectionOperator,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<
        impl Iterator<
            Item = (
                ResultTextSelection<'store>,
                Vec<(
                    ResultItem<'store, AnnotationData>,
                    ResultItem<'store, Annotation>,
                )>,
            ),
        >,
    >
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) =
            self.store().find_data_request_resolver(set, key)
        {
            Some(self.related_text(operator).filter_map(move |tsel| {
                if let Some(iter) = tsel.find_data_about(test_set_handle, test_key_handle, value) {
                    let data: Vec<_> = iter.collect();
                    if data.is_empty() {
                        None
                    } else {
                        Some((tsel.clone(), data))
                    }
                } else {
                    None
                }
            }))
        } else {
            None
        }
    }

    /// This selects text in a specific relation to the text of the current annotation, where that has text has certain data describing it.
    /// This returns the matching text, not the data.
    /// It effectively combines `related_text()` with `test_data_about()` on its results, into a single method.
    /// See these methods for further parameter explanation.
    pub fn related_text_test_data<'a>(
        &self,
        operator: TextSelectionOperator,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultTextSelection<'store>>>
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) =
            self.store().find_data_request_resolver(set, key)
        {
            Some(self.related_text(operator).filter_map(move |tsel| {
                if tsel.test_data_about(test_set_handle, test_key_handle, value) {
                    Some(tsel)
                } else {
                    None
                }
            }))
        } else {
            None
        }
    }
}

use rayon::prelude::*;
use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::marker::PhantomData;

use crate::annotation::{Annotation, AnnotationHandle, TargetIter};
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::api::annotationdata::DataIter;
use crate::api::textselection::TextSelectionsIter;
use crate::api::FindText;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::error::StamError;
use crate::resources::TextResource;
use crate::selector::{Selector, SelectorIter, SelectorKind};
use crate::store::*;
use crate::text::Text;
use crate::textselection::{
    ResultTextSelection, ResultTextSelectionSet, TextSelection, TextSelectionOperator,
    TextSelectionSet,
};
use crate::IntersectionIter;

use crate::api::textselection::SortTextualOrder;

impl<'store> ResultItem<'store, Annotation> {
    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns no duplicates even if a resource is referenced multiple times.
    /// If you want to distinguish between resources references as metadata and on text, check  `selector().kind()` on the return values.
    pub fn resources(&self) -> impl Iterator<Item = ResultItem<'store, TextResource>> + 'store {
        let selector = self.as_ref().target();
        let iter: TargetIter<TextResource> = TargetIter::new(selector.iter(self.store(), true));
        //                                                                               ^--- recurse
        let store = self.store();
        iter.map(|handle| store.resource(handle).unwrap())
    }

    /// Returns an iterator over the datasets that this annotation (by its target selector) references
    /// This returns no duplicates even if a dataset is referenced multiple times.
    pub fn datasets(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store {
        let selector = self.as_ref().target();
        let iter: TargetIter<AnnotationDataSet> =
            TargetIter::new(selector.iter(self.store(), false));
        let store = self.store();
        iter.map(|handle| store.dataset(handle).unwrap())
    }

    /// Iterates over all the annotations this annotation targets (i.e. via a [`Selector::AnnotationSelector'])
    /// Use [`Self.annotations()'] if you want to find the annotations that reference this one (the reverse).
    /// Results will be in textual order unless recursive is set or a DirectionalSelector is involved.
    pub fn annotations_in_targets(&self, recursive: bool) -> AnnotationsIter<'store> {
        let selector = self.as_ref().target();
        let iter: TargetIter<Annotation> = TargetIter::new(selector.iter(self.store(), recursive));
        let sorted = !recursive && selector.kind() != SelectorKind::DirectionalSelector;
        AnnotationsIter::new(
            IntersectionIter::new_with_iterator(Box::new(iter), sorted),
            self.store(),
        )
    }

    /// Iterates over all the annotations that reference this annotation, if any
    /// If you want to find the annotations this annotation targets, then use [`Self::annotations_in_targets()`] instead.
    ///
    /// Note: This does no sorting nor deduplication, if you want results in textual order without duplicates, add `.textual_order()`
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        let annotations = self
            .store()
            .annotations_by_annotation_reverse(self.handle());
        if let Some(annotations) = annotations {
            AnnotationsIter::new(
                IntersectionIter::new(Cow::Borrowed(annotations), true),
                self.store(),
            )
        } else {
            //useless iter that won't yield anything, used only to have a simpler return type and save wrapping the whole thing in an Option
            AnnotationsIter::new_empty(self.store())
        }
    }

    /// Iterates over all the annotations that reference this annotation, if any, in parallel.
    ///
    /// Note: This does no sorting nor deduplication!
    pub fn annotations_par(
        &self,
    ) -> impl ParallelIterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        self.store()
            .annotations_by_annotation_reverse(self.handle())
            .into_par_iter()
            .flatten()
            .map(|a_handle| {
                store
                    .annotation(*a_handle)
                    .expect("annotation handle must be valid")
            })
    }

    /// Iterate over all text selections this annotation references (i.e. via [`Selector::TextSelector`])
    /// They are returned in textual order, or in case if a DirectionSelector is involved, in the exact order as they were selected.
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
    /// They are returned in textual order, or in case if a DirectionSelector is involved, in the exact order as they were selected.
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
    /// If you already have an AnnotationData instance, use `has_data()` instead, it is much more efficient.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `set` and `key` , if set to a boolean (false or true), all sets and keys will be searched.
    /// Value is a DataOperator. It can be set to `DataOperator::Any` to return all values.
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

    /// Tests if the annotation has certain data, returns a boolean.
    /// If you don't have a data instance yet, use `test_data()` instead.
    /// If you do, this method is much more efficient than `test_data()`.
    pub fn has_data(&self, data: &ResultItem<AnnotationData>) -> bool {
        self.as_ref().has_data(data.set().handle(), data.handle())
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

    /// Search for annotations *about* this annotation, satisfying certain exact data that is already known.
    /// For a higher-level variant, see `find_data_about`, but that method is less efficient than this one.
    pub fn annotations_by_data_about(
        &self,
        data: ResultItem<'store, AnnotationData>,
    ) -> AnnotationsIter<'store> {
        let annotations = self
            .store()
            .annotations_by_annotation_reverse(self.handle());
        let data_annotations = self
            .store()
            .annotations_by_data_indexlookup(data.set().handle(), data.handle());
        if let (Some(annotations), Some(data_annotations)) = (annotations, data_annotations) {
            AnnotationsIter {
                store: self.store(),
                iter: Some(
                    IntersectionIter::new(Cow::Borrowed(data_annotations), true)
                        .with(Cow::Borrowed(annotations), true),
                ),
                cursor: 0,
            }
        } else {
            //useless iter that won't yield anything, used only to have a simpler return type and save wrapping the whole thing in an Option
            AnnotationsIter {
                store: self.store(),
                iter: None,
                cursor: 0,
            }
        }
    }

    /// Search for annotations *about* this annotation, satisfying multiple exact data that are already known.
    /// For a higher-level variant, see `find_data_about`, but that method is less efficient than this one.
    /*
    pub fn annotations_by_data_about_from_iter(
        &self,
        data: impl Iterator<Item = ResultItem<'store, AnnotationData>>,
    ) -> AnnotationsIter<'store> {
        let annotations = self
            .store()
            .annotations_by_annotation_reverse(self.handle());
        let mut data_annotations: Vec<_> = Vec::new();
        for data in data {
            data_annotations.extend(
                self.store()
                    .annotations_by_data_indexlookup(data.set().handle(), data.handle())
                    .map(|v| v.iter().copied())
                    .into_iter()
                    .flatten(),
            );
        }
        data_annotations.sort_unstable();
        data_annotations.dedup();

        AnnotationsIter {
            store: self.store(),
            iter: if let Some(annotations) = annotations {
                Some(IntersectionIter::new(
                    Cow::Owned(data_annotations),
                    true,
                    Cow::Borrowed(annotations),
                    true,
                ))
            } else {
                None
            },
            cursor: 0,
        }
    }
    */

    /// Tests if the annotation has certain data in other annotatations that reference this one, returns a boolean.
    /// If you don't have a data instance yet, use `test_data_about()` instead.
    /// This method is much more efficient than `test_data_about()`.
    pub fn has_data_about(&self, data: ResultItem<'store, AnnotationData>) -> bool {
        self.annotations_by_data_about(data).next().is_some()
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
                self.annotations_in_targets(recursive)
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

    /// Tests for data in annotations targeted by this one (i.e. via an AnnotationSelector).
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
            self.annotations_in_targets(recursive)
                .any(move |annotation| {
                    annotation.test_data(test_set_handle, test_key_handle, value)
                })
        } else {
            false
        }
    }

    /// Tests for data in annotations targeted by this one (i.e. via an AnnotationSelector).
    /// Do not confuse this with the data this annotation holds, which can be searched with [`Self.find_data()`],
    /// or with annotations that target the instance in question, which can be searched with [`Self.find_data_about()`].
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn has_data_in_targets<'a>(
        &self,
        data: &ResultItem<'store, AnnotationData>,
        recursive: bool,
    ) -> bool {
        self.annotations_in_targets(recursive)
            .any(move |annotation| {
                annotation
                    .as_ref()
                    .has_data(data.set().handle(), data.handle())
            })
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
    /// are in a specific relation with the text relations pertaining to the annotations. Returns an iterator over the [`TextSelection`] instances, in textual order.
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
    /// are in a specific relation with the text selections of the current one. Returns an iterator over the [`Annotation`] instances, in textual order.
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
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + 'store>
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) =
            self.rootstore().find_data_request_resolver(set, key)
        {
            Some(
                self.related_text(operator)
                    .map(move |tsel| {
                        tsel.find_data_about(test_set_handle, test_key_handle, value)
                            .into_iter()
                            .flatten()
                            .map(|(_data, annotation)| annotation)
                    })
                    .flatten(),
            )
        } else {
            None
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

pub struct AnnotationsIter<'store> {
    iter: Option<IntersectionIter<'store, AnnotationHandle>>,
    cursor: usize,
    store: &'store AnnotationStore,
}

impl<'store> AnnotationsIter<'store> {
    pub(crate) fn new(
        iter: IntersectionIter<'store, AnnotationHandle>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            cursor: 0,
            iter: Some(iter),
            store,
        }
    }

    pub(crate) fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            cursor: 0,
            iter: None,
            store,
        }
    }

    /// Maps annotations to data, consuming the iterator. Returns a new iterator over the data in
    /// all the annotations. This returns data without annotations (sorted chronologically and
    /// without duplicates), use [`Self.with_data()`] instead if you want to know which annotations
    /// have which data.
    pub fn data(mut self) -> DataIter<'store> {
        let mut data: Vec<_> = self
            .map(|annotation| annotation.as_ref().data().copied())
            .flatten()
            .collect();
        data.sort_unstable();
        data.dedup();
        DataIter::new(IntersectionIter::new(Cow::Owned(data), true), self.store)
    }

    /// Constrain the iterator to return only the annotations that have this exact data item
    /// To filter by multiple data instances, use [`Self.filter_data()`] instead.
    pub fn filter_annotationdata(mut self, data: &ResultItem<'store, AnnotationData>) -> Self {
        let data_annotations = self
            .store
            .annotations_by_data_indexlookup(data.set().handle(), data.handle());
        if let Some(iter) = self.iter.as_mut() {
            if let Some(data_annotations) = data_annotations {
                *iter = iter.with(Cow::Borrowed(data_annotations), true);
            } else {
                iter.abort = true; //data is not used, invalidate the iterator
            }
        }
        self
    }

    /// Constrain the iterator to only returns annotations that have data that occurs in the passed data iterator.
    /// If you have a single AnnotationData instance, use [`Self.filter_annotationdata()`] instead.
    pub fn filter_data(mut self, data: DataIter<'store>) -> Self {
        self.filter_annotations(data.annotations())
    }

    pub fn filter_find_data<'a>(
        mut self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Self {
        self.filter_data(self.store.find_data(set, key, value))
    }

    /// Returns annotations along with matching data, either may occur multiple times!
    /// Consumes the iterator.
    pub fn zip_data(
        self,
    ) -> impl Iterator<
        Item = (
            ResultItem<'store, Annotation>,
            ResultItem<'store, AnnotationData>,
        ),
    > + 'store {
        self.map(|annotation| {
            annotation
                .data()
                .map(move |data| (annotation.clone(), data))
        })
        .flatten()
    }

    pub fn zip_find_data<'a>(
        self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<
        impl Iterator<
                Item = (
                    ResultItem<'store, Annotation>,
                    ResultItem<'store, AnnotationData>,
                ),
            > + 'store,
    >
    where
        'a: 'store,
    {
        if let Some((set_handle, key_handle)) = self.store.find_data_request_resolver(set, key) {
            Some(
                self.map(move |annotation| {
                    annotation.data().filter_map(move |data| {
                        if (set_handle.is_none() || data.set().handle() == set_handle.unwrap())
                            && (key_handle.is_none() || data.key().handle() == key_handle.unwrap())
                            && data.value().test(value)
                        {
                            Some((annotation.clone(), data))
                        } else {
                            None
                        }
                    })
                })
                .flatten(),
            )
        } else {
            None
        }
    }

    /// Constrain this iterator by another (intersection)
    pub fn filter_annotations(mut self, annotations: AnnotationsIter<'store>) -> Self {
        self.iter.merge(annotations.iter);
        self
    }

    /// Find all text selections that are related to any text selections in this iterator, the operator
    /// determines the type of the relation. Shortcut method for `.textselections().related_text(operator)`.
    pub fn related_text(&self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        self.textselections().related_text(operator)
    }

    /// Maps annotations to textselections. Results will be returned in textual order.
    pub fn textselections(self) -> TextSelectionsIter<'store> {
        TextSelectionsIter {
            data: self
                .map(|annotation| annotation.textselections())
                .flatten()
                .textual_order(),
            cursor: 0,
            store: self.store,
        }
    }

    /// Returns annotations along with matching text selections, either may occur multiple times!
    /// Consumes the iterator. Results are not sorted in textual order.
    pub fn zip_textselections(
        self,
    ) -> impl Iterator<
        Item = (
            ResultItem<'store, Annotation>,
            ResultTextSelectionSet<'store>,
        ),
    > + 'store {
        let store = self.store;
        self.map(move |annotation| {
            let tset: TextSelectionSet = annotation.textselections().collect();
            (annotation, tset.as_resultset(store))
        })
    }

    pub fn filter_textselection(mut self, textselection: &ResultTextSelection<'store>) -> Self {
        self.filter_annotations(textselection.annotations())
    }

    pub fn filter_textselections(mut self, textselections: TextSelectionsIter<'store>) -> Self {
        self.filter_annotations(textselections.annotations())
    }

    pub fn zip_related_text(
        self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = (ResultItem<'store, Annotation>, TextSelectionsIter<'store>)> + 'store
    {
        let store = self.store;
        self.map(move |annotation| {
            let tset: TextSelectionSet = annotation.textselections().collect();
            (
                annotation,
                TextSelectionsIter::new(
                    tset.as_resultset(store)
                        .related_text(operator.clone())
                        .collect(),
                    store,
                ),
            )
        })
    }
}

impl<'store> Iterator for AnnotationsIter<'store> {
    type Item = ResultItem<'store, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.iter.as_mut() {
            if let Some(item) = iter.next() {
                return Some(self.store.annotation(item).expect("annotation must exist"));
            }
        }
        None
    }
}

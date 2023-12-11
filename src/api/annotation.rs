/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`Annotation`]. This API is implemented on
//! [`ResultItem<Annotation>`]. Moreover, it defines and implements the [`AnnotationsIter`] iterator to iterate over annotations,
//! which also exposes a rich API. Last, it defines and implements [`Annotations`], which is a simple collection of annotations,
//! and can be iterated over.

use rayon::prelude::*;
use std::borrow::Cow;
use std::collections::BTreeSet;

use crate::annotation::{Annotation, AnnotationHandle, TargetIter};
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::api::*;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::{TextResource, TextResourceHandle};
use crate::selector::{Selector, SelectorKind};
use crate::textselection::{ResultTextSelection, TextSelectionOperator, TextSelectionSet};
use crate::{Filter, FilterMode, TextMode};

impl<'store> FullHandle<Annotation> for ResultItem<'store, Annotation> {
    fn fullhandle(&self) -> <Annotation as Storable>::FullHandleType {
        self.handle()
    }
}

/// This is the implementation of the high-level API for [`Annotation`].
impl<'store> ResultItem<'store, Annotation> {
    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns no duplicates even if a resource is referenced multiple times.
    /// If you want to distinguish between resources references as metadata and on text, use [`Self::resources_as_metadata()`] or [`Self::resources_on_text()` ] instead.
    pub fn resources(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, TextResource>>> {
        let selector = self.as_ref().target();
        let iter: TargetIter<TextResource> = TargetIter::new(selector.iter(self.store(), true));
        //                                                                               ^--- recurse, targetiter prevents duplicates
        ResultIter::new_sorted(FromHandles::new(iter, self.store()))
    }

    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns only resources that are targeted via a [`Selector::ResourceSelector`] and
    /// returns no duplicates even if a resource is referenced multiple times.
    pub fn resources_as_metadata(
        &self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, TextResource>>> {
        let collection: BTreeSet<TextResourceHandle> = self
            .as_ref()
            .target()
            .iter(self.store(), true)
            .filter_map(|selector| {
                if let Selector::ResourceSelector(res_handle) = selector.as_ref() {
                    Some(*res_handle)
                } else {
                    None
                }
            })
            .collect();
        ResultIter::new_sorted(FromHandles::new(collection.into_iter(), self.store()))
    }

    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns only resources that are targeted via a [`Selector::TextSelector`] and
    /// returns no duplicates even if a resource is referenced multiple times.
    pub fn resources_on_text(
        &self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, TextResource>>> {
        let collection: BTreeSet<TextResourceHandle> = self
            .as_ref()
            .target()
            .iter(self.store(), true)
            .filter_map(|selector| {
                if let Selector::TextSelector(res_handle, ..) = selector.as_ref() {
                    Some(*res_handle)
                } else {
                    None
                }
            })
            .collect();
        ResultIter::new_sorted(FromHandles::new(collection.into_iter(), self.store()))
    }

    /// Returns an iterator over the datasets that this annotation (by its target selector) references via a [`Selector::DataSetSelector`].
    /// This returns no duplicates even if a dataset is referenced multiple times.
    pub fn datasets(
        &self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store> {
        let selector = self.as_ref().target();
        let iter: TargetIter<AnnotationDataSet> =
            TargetIter::new(selector.iter(self.store(), false));
        ResultIter::new_sorted(FromHandles::new(iter, self.store()))
    }

    /// Iterates over all the annotations this annotation targets (i.e. via a [`Selector::AnnotationSelector`])
    /// Use [`Self::annotations()`] if you want to find the annotations that reference this one (the reverse operation).
    ///
    /// Results will be in textual order unless `recursive` is set or a [`Selector::DirectionalSelector`] is involved, then they are in the exact order as they were selected.
    pub fn annotations_in_targets(
        &self,
        recursive: bool,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let selector = self.as_ref().target();
        let iter: TargetIter<Annotation> = TargetIter::new(selector.iter(self.store(), recursive));
        let sorted = !recursive && selector.kind() != SelectorKind::DirectionalSelector;
        ResultIter::new(FromHandles::new(iter, self.store()), sorted)
    }

    /// Returns an iterator over all annotations that reference this annotation, if any
    /// If you want to find the annotations this annotation targets, then use [`Self::annotations_in_targets()`] instead.
    ///
    /// Results will be in chronological order and without duplicates, if you want results in textual order, add `.iter().textual_order()`
    pub fn annotations(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        if let Some(annotations) = self.store().annotations_by_annotation(self.handle()) {
            ResultIter::new_sorted(FromHandles::new(annotations.iter().copied(), self.store()))
        } else {
            ResultIter::new_empty()
        }
    }

    /// Returns a borrowed collection of all the annotations that reference this annotation, if any
    /// If you want to find the annotations this annotation targets, then use [`Self::annotations_in_targets()`] instead.
    ///
    /// Results will be in chronological order and without duplicates, if you want results in textual order, add `.iter().textual_order()`
    pub fn annotations_handles(&self) -> Annotations<'store> {
        if let Some(annotations) = self.store().annotations_by_annotation(self.handle()) {
            Handles::new(Cow::Borrowed(&annotations), true, self.store())
        } else {
            Handles::new_empty(self.store())
        }
    }

    /// Iterate over all text selections this annotation references (i.e. via [`Selector::TextSelector`])
    /// They are returned in textual order, except in case a [`Selector::DirectionalSelector`] is involved, then they are in the exact order as they were selected.
    pub fn textselections(&self) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let textselections = self
            .store()
            .textselections_by_selector(self.as_ref().target());
        ResultTextSelections::new(ResultIter::new_unsorted(
            //textual order is not chronological order
            FromHandles::new(textselections.into_iter(), self.store()),
        ))
    }

    /// Iterates over all text slices this annotation refers to
    /// They are returned in textual order, or in case a [`Selector::DirectionalSelector`] is involved, in the exact order as they were selected.
    pub fn text(&self) -> impl Iterator<Item = &'store str> {
        self.textselections().text()
    }

    /// If this annotation refers to a single simple text slice,
    /// this returns it. If not contains no text or multiple text references, it returns None.
    pub fn text_simple(&self) -> Option<&'store str> {
        self.textselections().text_simple()
    }

    /// Returns all underlying text for this annotation concatenatedhttps://en.wikipedia.org/wiki/TempleOS
    /// Shortcut for `.textselections().text_join()`
    pub fn text_join(&self, delimiter: &str) -> String {
        self.textselections().text_join(delimiter)
    }

    /// Returns the (single!) resource the annotation points to. Only works if this annotation targets using a [`Selector::TextSelector`],
    /// [`Selector::ResourceSelector`] or [`Selector::AnnotationSelector`], and not for complex selectors.
    /// Use [`Self::resources()`] if want to iterate over all resources instead.
    /// AnnotationSelectors are followed recursively if needed.
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

    /// Get an iterator over all data ([`AnnotationData`]) for this annotation.
    pub fn data(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, AnnotationData>>> {
        ResultIter::new_unsorted(FromHandles::new(
            self.as_ref().raw_data().iter().copied(),
            self.store(),
        ))
    }

    /// Tests if the annotation has certain data, returns a boolean.
    /// This will be a bit quicker than using `.data().filter_annotationdata()`.
    pub fn has_data(&self, data: &ResultItem<AnnotationData>) -> bool {
        self.as_ref().has_data(data.set().handle(), data.handle())
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the text selected by this annotation. Returns an iterator over the [`crate::TextSelection`] instances, in textual order.
    /// (as [`ResultTextSelection`]).
    ///
    /// This method is slightly different from `.textselections().related_text()`. This method
    /// will consider multiple textselections pertaining to this annotation as a single set, the
    /// other method treats each textselection separately.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        //first we gather all textselections for this annotation in a set, as the chosen operator may apply to them jointly
        let tset: TextSelectionSet = self.textselections().collect();
        tset.as_resultset(self.store()).related_text(operator)
    }
}

/// Holds a collection of annotations.
/// This structure is produced by calling [`AnnotationsIter::to_collection()`].
/// Use [`Annotations::iter()`] to iterate over the collection.
pub type Annotations<'store> = Handles<'store, Annotation>;

impl<'store, I> FullHandleToResultItem<'store, Annotation> for FromHandles<'store, Annotation, I>
where
    I: Iterator<Item = AnnotationHandle>,
{
    fn get_item(&self, handle: AnnotationHandle) -> Option<ResultItem<'store, Annotation>> {
        self.store.annotation(handle)
    }
}

pub trait AnnotationIterator<'store>: Iterator<Item = ResultItem<'store, Annotation>>
where
    Self: Sized,
{
    fn parallel(self) -> rayon::vec::IntoIter<ResultItem<'store, Annotation>> {
        let annotations: Vec<_> = self.collect();
        annotations.into_par_iter()
    }

    /// Iterates over all the annotations that reference any annotations (i.e. via a [`Selector::AnnotationSelector`]) in this iterator.
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    ///
    /// If you want annotations unsorted and with possible duplicates, then just do:  `.map(|a| a.annotations()).flatten()`
    fn annotations(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|annotation| annotation.annotations())
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultIter::new_sorted(annotations.into_iter())
    }

    /// Iterates over all the annotations that reference any annotations (i.e. via a [`Selector::AnnotationSelector`]) in this iterator.
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned unsorted and returned with possible duplicates
    fn annotations_unchecked(
        self,
    ) -> Box<dyn Iterator<Item = ResultItem<'store, Annotation>> + 'store>
    where
        Self: 'store,
    {
        Box::new(ResultIter::new_unsorted(
            self.map(|annotation| annotation.annotations()).flatten(),
        ))
    }

    /// Iterates over all the annotations targeted by the annotation in this iterator (i.e. via a [`Selector::AnnotationSelector`])
    /// Use [`Self::annotations()`] if you want to find the annotations that reference these ones (the reverse).
    /// Annotations will be returned sorted chronologically, without duplicates
    fn annotations_in_targets(
        self,
        recursive: bool,
    ) -> ResultIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|annotation| annotation.annotations_in_targets(recursive))
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultIter::new_sorted(annotations.into_iter())
    }

    /// Maps annotations to data, consuming the iterator. Returns a new iterator over the AnnotationData in
    /// all the annotations. This returns data sorted chronologically and
    /// without duplicates. It does not include the annotations.
    fn data(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, AnnotationData>> as IntoIterator>::IntoIter> {
        let mut data: Vec<_> = self.map(|annotation| annotation.data()).flatten().collect();
        data.sort_unstable();
        data.dedup();
        ResultIter::new_sorted(data.into_iter())
    }

    /// Maps annotations to data, consuming the iterator. Returns a new iterator over the AnnotationData in
    /// all the annotations. This returns data unsorted and
    /// with possible duplicates.
    fn data_unchecked(self) -> Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        Self: 'store,
    {
        Box::new(ResultIter::new_unsorted(
            self.map(|annotation| annotation.data()).flatten(),
        ))
    }

    /// Shortcut for `.textselections().text()`
    fn text(
        self,
    ) -> TextIter<'store, <Vec<ResultTextSelection<'store>> as IntoIterator>::IntoIter> {
        self.textselections().text()
    }

    /// Maps annotations to resources, consuming the iterator. Will return in chronological order without duplicates.
    fn resources(
        self,
    ) -> ResultIter<<BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter> {
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources())
            .flatten()
            .collect();
        ResultIter::new_sorted(collection.into_iter())
    }

    /// Maps annotations to resources, consuming the iterator. This only covers resources targeted via a ResourceSelector (i.e. annotations as metadata)
    /// Will return in chronological order without duplicates.
    fn resources_as_metadata(
        self,
    ) -> ResultIter<<BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter> {
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources_as_metadata())
            .flatten()
            .collect();
        ResultIter::new_sorted(collection.into_iter())
    }

    /// Maps annotations to resources, consuming the iterator. This only covers resources targeted via a TextSelector (i.e. annotations on the text)
    /// Will return in chronological order without duplicates.
    fn resources_on_text(
        self,
    ) -> ResultIter<<BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter> {
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources_on_text())
            .flatten()
            .collect();
        ResultIter::new_sorted(collection.into_iter())
    }

    /// Maps annotations to textselections, consuming the iterator. Results will be returned in textual order.
    fn textselections(self) -> <Vec<ResultTextSelection<'store>> as IntoIterator>::IntoIter {
        self.map(|annotation| annotation.textselections())
            .flatten()
            .textual_order()
            .into_iter()
    }

    /// Maps annotations to related text selections, as specified by the operator. The iterator will be consumed. Results will be returned in textual order.
    ///
    /// This method is slightly different from `.textselections().related_text()`. This method
    /// will consider multiple textselections pertaining to an annotation as a single set, the
    /// other method treats each textselection separately.
    fn related_text(
        self,
        operator: TextSelectionOperator,
    ) -> <Vec<ResultTextSelection<'store>> as IntoIterator>::IntoIter {
        self.map(|annotation| annotation.related_text(operator))
            .flatten()
            .textual_order()
            .into_iter()
    }

    /// Constrain this iterator to only a single annotation
    /// This method can only be used once! Use [`Self::filter_annotations()`] to filter on multiple annotations (disjunction).
    fn filter_annotation(
        self,
        annotation: &ResultItem<Annotation>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotation(annotation.handle()),
        }
    }

    /// Constrain this iterator to filter on one of the mentioned annotations
    fn filter_annotations(
        self,
        annotations: Annotations<'store>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotations(annotations),
        }
    }

    /// Constrain this iterator to filter on one of the mentioned annotations
    fn filter_annotations_byref(
        self,
        annotations: &'store Annotations<'store>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::BorrowedAnnotations(annotations),
        }
    }

    /// Constrain this iterator to filter only a single annotation (by handle). This is a lower-level method, use [`Self::filter_annotation()`] instead.
    /// This method can only be used once! Use [`Self::filter_annotations()`] to filter on multiple annotations (disjunction).
    fn filter_handle(self, handle: AnnotationHandle) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotation(handle),
        }
    }

    /// Constrain the iterator to only return annotations that have data that corresponds with any of the items in the passed data.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    // /// If you have a borrowed reference, use [`Self::filter_data_byref()`] instead.
    /// If you want to check whether multiple data are ALL found in a single annotation, then use [`Self::filter_data_all()`].
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    // /// If you want eager evaluation, use [`Self::filter_annotations()`] as follows: `annotation.filter_annotations(&data.annotations().into())`.
    fn filter_data(self, data: Data<'store>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Data(data, FilterMode::Any),
        }
    }

    fn filter_data_byref(self, data: &'store Data<'store>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::BorrowedData(data, FilterMode::Any),
        }
    }

    /// Constrain the iterator to only return annotations that, in a single annotation, has data that corresponds with *ALL* of the items in the passed data.
    /// All items have to be found or none will be returned.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    // /// If you have a borrowed reference, use [`Self::filter_data_byref_multi()`] instead.
    /// If you want to check for *ANY* match rather than requiring multiple matches in a single annotation, then use [`Self::filter_data()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    fn filter_data_all(self, data: Data<'store>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Data(data, FilterMode::All),
        }
    }

    fn filter_data_all_byref(
        self,
        data: &'store Data<'store>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::BorrowedData(data, FilterMode::All),
        }
    }

    fn filter_resource(
        self,
        resource: &ResultItem<'store, TextResource>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::TextResource(resource.handle()),
        }
    }

    /// Constrain the iterator to return only the annotations that have this exact data item
    /// To filter by multiple data instances (union/disjunction), use [`Self::filter_data()`] or (intersection/conjunction) [`Self::filter_data_all()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    fn filter_annotationdata(
        self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::AnnotationData(data.set().handle(), data.handle()),
        }
    }

    fn filter_key_value(
        self,
        key: &ResultItem<'store, DataKey>,
        value: DataOperator<'store>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKeyAndOperator(key.set().handle(), key.handle(), value),
        }
    }

    fn filter_key(self, key: &ResultItem<'store, DataKey>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle()),
        }
    }

    fn filter_key_handle(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKey(set, key),
        }
    }

    fn filter_value(self, value: DataOperator<'store>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataOperator(value),
        }
    }

    fn filter_key_handle_value(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
        value: DataOperator<'store>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKeyAndOperator(set, key, value),
        }
    }

    fn filter_set(
        self,
        set: &ResultItem<'store, AnnotationDataSet>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle()),
        }
    }

    fn filter_set_handle(self, set: AnnotationDataSetHandle) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::AnnotationDataSet(set),
        }
    }

    /// Constrain the iterator to only return annotations that have text matching the specified text
    ///
    /// If you have a borrowed reference, use [`Self::filter_text_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    ///
    /// The `delimiter` parameter determines how multiple possible non-contiguous text selections are joined prior to comparison, you most likely want to set it to either a space or an empty string.
    fn filter_text(
        self,
        text: String,
        case_sensitive: bool,
        delimiter: &'store str,
    ) -> FilteredAnnotations<'store, Self> {
        if case_sensitive {
            FilteredAnnotations {
                inner: self,
                filter: Filter::Text(text, TextMode::Exact, delimiter),
            }
        } else {
            FilteredAnnotations {
                inner: self,
                filter: Filter::Text(text.to_lowercase(), TextMode::Lowercase, delimiter),
            }
        }
    }

    /// Constrain the iterator to only return annotations that have text matching the specified text
    ///
    /// If you set `case_sensitive` to `false`, then `text` *MUST* be a lower-cased &str!
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    ///
    /// The `delimiter` parameter determines how multiple possible non-contiguous text selections are joined prior to comparison, you most likely want to set it to either a space or an empty string.
    fn filter_text_byref(
        self,
        text: &'store str,
        case_sensitive: bool,
        delimiter: &'store str,
    ) -> FilteredAnnotations<'store, Self> {
        if case_sensitive {
            FilteredAnnotations {
                inner: self,
                filter: Filter::BorrowedText(text, TextMode::Exact, delimiter),
            }
        } else {
            FilteredAnnotations {
                inner: self,
                filter: Filter::BorrowedText(text, TextMode::Lowercase, delimiter),
            }
        }
    }

    /// Find only annotations whose text selections are related to any text selections of annotations in this iterator, the operator
    /// determines the type of the relation.
    fn filter_related_text(
        self,
        operator: TextSelectionOperator,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::TextSelectionOperator(operator),
        }
    }
}

impl<'store, I> AnnotationIterator<'store> for I
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    //blanket implementation
}

pub struct FilteredAnnotations<'store, I>
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    inner: I,
    filter: Filter<'store>,
}

impl<'store, I> Iterator for FilteredAnnotations<'store, I>
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    type Item = ResultItem<'store, Annotation>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(annotation) = self.inner.next() {
                if self.test_filter(&annotation) {
                    return Some(annotation);
                }
            } else {
                return None;
            }
        }
    }
}

impl<'store, I> FilteredAnnotations<'store, I>
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    fn test_filter(&self, annotation: &ResultItem<'store, Annotation>) -> bool {
        match &self.filter {
            Filter::Annotation(handle) => annotation.handle() == *handle,
            Filter::Annotations(handles) => handles.contains(&annotation.fullhandle()),
            Filter::BorrowedAnnotations(handles) => handles.contains(&annotation.fullhandle()),
            Filter::Data(data, FilterMode::Any) => {
                annotation.data().filter_data_byref(&data).test()
            }
            Filter::Data(data, FilterMode::All) => {
                annotation.data().filter_data_byref(&data).count() >= data.len()
            }
            Filter::BorrowedData(data, FilterMode::Any) => {
                annotation.data().filter_data_byref(data).test()
            }
            Filter::BorrowedData(data, FilterMode::All) => {
                annotation.data().filter_data_byref(data).count() >= data.len()
            }
            Filter::DataKey(set, key) => annotation.data().filter_key_handle(*set, *key).test(),
            Filter::DataKeyAndOperator(set, key, value) => annotation
                .data()
                .filter_key_handle_value(*set, *key, value.clone())
                .test(),
            Filter::DataOperator(value) => annotation.data().filter_value(value.clone()).test(),
            Filter::AnnotationDataSet(set) => annotation.data().filter_set_handle(*set).test(),
            Filter::AnnotationData(set, data) => {
                annotation.data().filter_handle(*set, *data).test()
            }
            Filter::TextResource(res_handle) => annotation
                .resources()
                .any(|res| res.handle() == *res_handle),
            Filter::Text(reftext, textmode, delimiter) => {
                if let Some(text) = annotation.text_simple() {
                    match textmode {
                        TextMode::Exact => text == reftext.as_str(),
                        TextMode::Lowercase => text.to_lowercase() == reftext.as_str(),
                    }
                } else {
                    let mut text = annotation.text_join(delimiter);
                    if *textmode == TextMode::Lowercase {
                        text = text.to_lowercase();
                    }
                    text == reftext.as_str()
                }
            }
            Filter::BorrowedText(reftext, textmode, delimiter) => {
                if let Some(text) = annotation.text_simple() {
                    match textmode {
                        TextMode::Exact => text == *reftext,
                        TextMode::Lowercase => text.to_lowercase() == *reftext,
                    }
                } else {
                    let mut text = annotation.text_join(delimiter);
                    if *textmode == TextMode::Lowercase {
                        text = text.to_lowercase();
                    }
                    text == *reftext
                }
            }
            Filter::TextSelectionOperator(operator) => annotation.related_text(*operator).test(),
            _ => unreachable!(
                "Filter {:?} not implemented for FilteredAnnotations",
                self.filter
            ),
        }
    }
}

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
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt::Debug;

use crate::annotation::{Annotation, AnnotationHandle, TargetIter};
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::*;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::{TextResource, TextResourceHandle};
use crate::selector::{Selector, SelectorKind};
use crate::textselection::{
    ResultTextSelection, ResultTextSelectionSet, TextSelectionOperator, TextSelectionSet,
};
use crate::*;
use crate::{Filter, FilterMode, IntersectionIter, TextMode};

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
    pub fn resources(&self) -> impl Iterator<Item = ResultItem<'store, TextResource>> {
        let selector = self.as_ref().target();
        let iter: TargetIter<TextResource> = TargetIter::new(selector.iter(self.store(), true));
        //                                                                               ^--- recurse, targetiter prevents duplicates
        MaybeIter::new_sorted(FromHandles::new(iter, self.store()))
    }

    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns only resources that are targeted via a [`Selector::ResourceSelector`] and
    /// returns no duplicates even if a resource is referenced multiple times.
    pub fn resources_as_metadata(&self) -> impl Iterator<Item = ResultItem<'store, TextResource>> {
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
        MaybeIter::new_sorted(FromHandles::new(collection.into_iter(), self.store()))
    }

    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns only resources that are targeted via a [`Selector::TextSelector`] and
    /// returns no duplicates even if a resource is referenced multiple times.
    pub fn resources_on_text(&self) -> impl Iterator<Item = ResultItem<'store, TextResource>> {
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
        MaybeIter::new_sorted(FromHandles::new(collection.into_iter(), self.store()))
    }

    /// Returns an iterator over the datasets that this annotation (by its target selector) references via a [`Selector::DataSetSelector`].
    /// This returns no duplicates even if a dataset is referenced multiple times.
    pub fn datasets(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store {
        let selector = self.as_ref().target();
        let iter: TargetIter<AnnotationDataSet> =
            TargetIter::new(selector.iter(self.store(), false));
        MaybeIter::new_sorted(FromHandles::new(iter, self.store()))
    }

    /// Iterates over all the annotations this annotation targets (i.e. via a [`Selector::AnnotationSelector`])
    /// Use [`Self::annotations()`] if you want to find the annotations that reference this one (the reverse operation).
    ///
    /// Results will be in textual order unless `recursive` is set or a [`Selector::DirectionalSelector`] is involved, then they are in the exact order as they were selected.
    pub fn annotations_in_targets(
        &self,
        recursive: bool,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> {
        let selector = self.as_ref().target();
        let iter: TargetIter<Annotation> = TargetIter::new(selector.iter(self.store(), recursive));
        let sorted = !recursive && selector.kind() != SelectorKind::DirectionalSelector;
        MaybeIter::new(FromHandles::new(iter, self.store()), sorted)
    }

    /// Returns an iterator over all annotations that reference this annotation, if any
    /// If you want to find the annotations this annotation targets, then use [`Self::annotations_in_targets()`] instead.
    ///
    /// Results will be in chronological order and without duplicates, if you want results in textual order, add `.iter().textual_order()`
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> {
        if let Some(annotations) = self.store().annotations_by_annotation(self.handle()) {
            MaybeIter::new_sorted(FromHandles::new(annotations.iter().copied(), self.store()))
        } else {
            MaybeIter::new_empty()
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
    pub fn textselections(&self) -> impl Iterator<Item = ResultItem<'store, TextSelection>> {
        let textselections = self
            .store()
            .textselections_by_selector(self.as_ref().target());
        MaybeIter::new_unsorted(
            //textual order is not chronological order
            FromHandles::new(textselections.into_iter(), self.store()),
        )
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
    pub fn data(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> {
        MaybeIter::new_unsorted(FromHandles::new(
            self.as_ref().raw_data().iter().copied(),
            self.store(),
        ))
    }

    /// Find data ([`AnnotationData`]) amongst the data for this annotation. Returns an iterator over the data.
    /// If you have a particular annotation data instance and want to test if the annotation uses it, then use [`Self::has_data()`] instead.
    pub fn find_data<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> impl Iterator<Item = ResultItem<'store, AnnotationData>>
    where
        'a: 'store,
    {
        self.data().find_data(set, key, value)
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
    /// This method is slight different from `.textselections().related_text()`. This method
    /// will consider multiple textselections pertaining to this annotation as a single set, the
    /// other method treats each textselection separately.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultItem<'store, TextSelection>> {
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

/*
impl<'store> AnnotationsIter<'store> {
    /// Transform the iterator into a parallel iterator; subsequent iterator methods like `filter` and `map` will run in parallel.
    /// This first consumes the sequential iterator into a newly allocated buffer.
    ///
    /// Note: It does not parallelize the operation of AnnotationsIter itself.
    pub fn parallel(self) -> impl ParallelIterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.collect::<Vec<_>>().into_par_iter()
    }

    /// Iterates over all the annotations targeted by the annotation in this iterator (i.e. via a [`Selector::AnnotationSelector`])
    /// Use [`Self::annotations()`] if you want to find the annotations that reference these ones (the reverse).
    /// Annotations will be returned sorted chronologically, without duplicates
    pub fn annotations_in_targets(self, recursive: bool) -> AnnotationsIter<'store> {
        let store = self.store;
        let mut annotations: Vec<_> = self
            .map(|annotation| {
                annotation
                    .annotations_in_targets(recursive)
                    .map(|a| a.handle())
            })
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultItemIter::from_vec(annotations, true, store)
    }

    /// Iterates over all the annotations targeted by the annotation in this iterator (i.e. via a [`Selector::AnnotationSelector`])
    /// Use [`Self::annotations()`] if you want to find the annotations that reference these ones (the reverse).
    /// Unlike [`Self::annotations_in_targets()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant).
    pub fn annotations_in_targets_unchecked(self, recursive: bool) -> AnnotationsIter<'store> {
        let store = self.store;
        ResultItemIter::from_iterator(
            Box::new(
                self.map(move |annotation| annotation.annotations_in_targets(recursive))
                    .flatten(),
            ),
            true,
            store,
        )
    }

    /// Iterates over all the annotations that reference any annotations in this iterator (i.e. via a [`Selector::AnnotationSelector`])
    /// Annotations will be returned sorted chronologically, without duplicates
    pub fn annotations(self) -> AnnotationsIter<'store> {
        let store = self.store;
        let mut annotations: Vec<_> = self
            .map(|annotation| annotation.annotations().map(|a| a.handle()))
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultItemIter::from_vec(annotations, true, store)
    }

    /// Iterates over all the annotations that reference any annotations in this iterator (i.e. via a [`Selector::AnnotationSelector`])
    /// Unlike [`Self::annotations()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant).
    pub fn annotations_unchecked(self) -> AnnotationsIter<'store> {
        let store = self.store;
        ResultItemIter::from_iterator(
            Box::new(self.map(|annotation| annotation.annotations()).flatten()),
            false,
            store,
        )
    }

    /// Maps annotations to data, consuming the iterator. Returns a new iterator over the data in
    /// all the annotations. This returns data without annotations (sorted chronologically and
    /// without duplicates), use [`Self::iter_with_data()`] instead if you want to know which annotations
    /// have which data.
    pub fn data(self) -> DataIter<'store> {
        let store = self.store;
        let mut data: Vec<_> = self
            .map(|annotation| annotation.as_ref().data().copied())
            .flatten()
            .collect();
        data.sort_unstable();
        data.dedup();
        DataIter::new(IntersectionIter::new(Cow::Owned(data), true), store)
    }

    /// Maps annotations to data, consuming the iterator. Returns a new iterator over the data in all the annotations.
    /// Unlike [`Self::data()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant).
    pub fn data_unchecked(self) -> DataIter<'store> {
        let store = self.store;
        DataIter::new(
            IntersectionIter::new_with_iterator(
                Box::new(
                    self.map(|annotation| annotation.as_ref().data().copied())
                        .flatten(),
                ),
                false,
            ),
            store,
        )
    }

    /// Find data for the annotations in this iterator. Returns an iterator over the data (losing the information about annotations).
    /// If you want specifically know what annotation has what data, use [`Self::iter_with_data()`] instead.
    /// If you want to constrain annotations by a data search, use [`Self::filter_find_data()`] instead.
    pub fn find_data<'a>(
        self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> DataIter<'store>
    where
        'a: 'store,
    {
        self.data().find_data(set, key, value)
    }

    /// Constrain the iterator to return only the annotations that have this exact data item
    /// This method can only be used once, to filter by multiple data instances, use [`Self::filter_data()`] or [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    pub fn filter_annotationdata(mut self, data: &ResultItem<'store, AnnotationData>) -> Self {
        self.filters
            .push(Filter::AnnotationData(data.set().handle(), data.handle()));
        self
    }

    /// Constrain the iterator to return only the annotations that have this exact data item. This is a lower-level method that takes handles, use [`Self::filter_annotationdata()`] instead.
    /// This method can only be used once, to filter by multiple data instances, use [`Self::filter_data()`] or [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    pub fn filter_annotationdata_handle(
        mut self,
        set_handle: AnnotationDataSetHandle,
        data_handle: AnnotationDataHandle,
    ) -> Self {
        self.filters
            .push(Filter::AnnotationData(set_handle, data_handle));
        self
    }

    /// Constrain the iterator to only return annotations that have data that corresponds with any of the items in the passed data.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a borrowed reference, use [`Self::filter_data_byref()`] instead.
    /// If you want to check whether multiple data are ALL found in a single annotation, then use [`Self::filter_data_multi()`].
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    /// If you want eager evaluation, use [`Self::filter_annotations()`] as follows: `annotation.filter_annotations(&data.annotations().into())`.
    pub fn filter_data(mut self, data: Data<'store>) -> Self {
        self.filters.push(Filter::Data(data, FilterMode::Any));
        self
    }

    /// Constrain the iterator to only return annotations that have data that corresponds with any of the items in the passed data.
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a owned data, use [`Self::filter_data()`] instead.
    /// If you want to check whether multiple data are ALL found in a single annotation, then use [`Self::filter_data_byref_multi()`].
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    /// If you want eager evaluation, use [`Self::filter_annotations()`] as follows: `annotation.filter_annotations(&data.annotations().into())`.
    pub fn filter_data_byref(mut self, data: &'store Data<'store>) -> Self {
        self.filters
            .push(Filter::BorrowedData(data, FilterMode::Any));
        self
    }

    /// Constrain the iterator to only return annotations that, in a single annotation, has data that corresponds with *ALL* of the items in the passed data.
    /// All items have to be found or none will be returned.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a borrowed reference, use [`Self::filter_data_byref_multi()`] instead.
    /// If you want to check for *ANY* match rather than requiring multiple matches in a single annotation, then use [`Self::filter_data()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    pub fn filter_data_multi(mut self, data: Data<'store>) -> Self {
        self.filters.push(Filter::Data(data, FilterMode::All));
        self
    }

    /// Constrain the iterator to only return annotations that, in a single annotation, has data that corresponds with *ALL* of the items in the passed data.
    /// All items have to be found or none will be returned.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have owned data, use [`Self::filter_data_multi()`] instead.
    /// If you want to check for *ANY* match rather than requiring multiple matches in a single annotation, then use [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    pub fn filter_data_byref_multi(mut self, data: &'store Data<'store>) -> Self {
        self.filters
            .push(Filter::BorrowedData(data, FilterMode::All));
        self
    }

    /// Constrain the iterator to only return annotations that have data matching the search parameters.
    /// This is a just shortcut method for `self.filter_data( store.find_data(..).to_collection() )`
    ///
    ///
    /// Note: This filter is evaluated lazily, it will obtain and check the data for each annotation.
    ////      Do not call this method in a loop, it will be very inefficient! Compute it once before and cache it (`let data = store.find_data(..).to_collection()`), then
    ///       pass the result to [`Self::filter_data(data.clone())`], the clone will be cheap.
    pub fn filter_find_data<'a>(
        self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> Self
    where
        'a: 'store,
    {
        let store = self.store;
        self.filter_data(store.find_data(set, key, value).to_collection())
    }

    /// Constrain the iterator to only return annotations that have text matching the specified text
    ///
    /// If you have a borrowed reference, use [`Self::filter_text_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    ///
    /// The `delimiter` parameter determines how multiple possible non-contiguous text selections are joined prior to comparison, you most likely want to set it to either a space or an empty string.
    pub fn filter_text(
        mut self,
        text: String,
        case_sensitive: bool,
        delimiter: &'store str,
    ) -> Self {
        if case_sensitive {
            self.filters
                .push(Filter::Text(text, TextMode::Exact, delimiter));
        } else {
            self.filters.push(Filter::Text(
                text.to_lowercase(),
                TextMode::Lowercase,
                delimiter,
            ));
        }
        self
    }

    /// Constrain the iterator to only return annotations that have text matching the specified text
    ///
    /// Important note: If you set `case_sensitive` to false, then YOU must ensure the passed reference is lowercased! Use [`Self.filter_text()`] instead if you can't guarantee this.
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    ///
    /// The `delimiter` parameter determines how multiple possible non-contiguous text selections are joined prior to comparison, you most likely want to set it to either a space or an empty string.
    pub fn filter_text_byref(
        mut self,
        text: &'store str,
        case_sensitive: bool,
        delimiter: &'store str,
    ) -> Self {
        self.filters.push(Filter::BorrowedText(
            text,
            if case_sensitive {
                TextMode::Lowercase
            } else {
                TextMode::Exact
            },
            delimiter,
        ));
        self
    }

    /// Returns an iterator over annotations along with matching data as requested
    /// via [`Self::filter_data()`], [`Self::filter_find_data()`] or [`Self::filter_annotationdata()`]).
    /// Implicit filters on data via e.g. `filter_annotations(data.annotations())` will **NOT** be included.
    /// This consumes the iterator.
    pub fn iter_with_data(self) -> AnnotationsWithDataIter<'store> {
        AnnotationsWithDataIter(self)
    }

    /// Constrain this iterator by another (intersection)
    /// This method can be called multiple times
    ///
    /// You can cast any existing iterator that produces `ResultItem<Annotation>` to an [`AnnotationsIter`] using [`AnnotationsIter::from_iter()`]
    ///
    /// Note: this filter is evaluated immediately prior to any other filters you add!
    pub fn filter_annotations(mut self, annotations: &Annotations<'store>) -> Self {
        self.intersection(annotations)
    }

    /// Constrain this iterator to only a single annotation
    /// This method can only be used once! Use [`Self::filter_annotations()`] to filter on multiple annotations (disjunction).
    pub fn filter_annotation(self, annotation: &ResultItem<Annotation>) -> Self {
        self.filter_handle(annotation.handle())
    }

    /// Constrain this iterator to filter only a single annotation (by handle). This is a lower-level method, use [`Self::filter_annotation()`] instead.
    /// This method can only be used once! Use [`Self::filter_annotations()`] to filter on multiple annotations (disjunction).
    pub fn filter_handle(mut self, handle: AnnotationHandle) -> Self {
        self.filters.push(Filter::Annotation(handle));
        self
    }

    /// Constrain this iterator to only return annotations that reference a particular resource
    pub fn filter_resource(self, resource: &ResultItem<TextResource>) -> Self {
        self.filter_resource_handle(resource.handle())
    }

    /// Constrain this iterator to only return annotations that reference a particular resource
    pub fn filter_resource_handle(mut self, handle: TextResourceHandle) -> Self {
        self.filters.push(Filter::TextResource(handle));
        self
    }

    /// Find all text selections that are related to any text selections of annotations in this iterator, the operator
    /// determines the type of the relation. Shortcut method for `.textselections().related_text(operator)`.
    pub fn related_text(self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        self.textselections().related_text(operator)
    }

    /// Find only annotations whose text selections are related to any text selections of annotations in this iterator, the operator
    /// determines the type of the relation.
    pub fn filter_related_text(mut self, operator: TextSelectionOperator) -> Self {
        self.filters.push(Filter::TextSelectionOperator(operator));
        self
    }

    /// Maps annotations to textselections, consuming the iterator. Results will be returned in textual order.
    pub fn textselections(self) -> TextSelectionsIter<'store> {
        let store = self.store;
        TextSelectionsIter::new(
            self.map(|annotation| annotation.textselections())
                .flatten()
                .textual_order(),
            store,
        )
    }

    /// Maps annotations to textselections, consuming the iterator. Results will be returned in textual order.
    /// Unlike [`Self::textselections()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant).
    pub fn textselections_unchecked(self) -> TextSelectionsIter<'store> {
        let store = self.store;
        TextSelectionsIter::new_with_iterator(
            Box::new(self.map(|annotation| annotation.textselections()).flatten()),
            store,
        )
    }

    /// Returns annotations along with matching text selections, either may occur multiple times!
    /// If an annotation references multiple text selections, they are returned as a set.
    /// Note that results are in chronological annotation order, not textual order.
    pub fn zip_textselections(
        //TODO: refactor this into iter_with_textselections() (like iter_with_data()
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

    /// Shortcut for `.textselections().text()`
    pub fn text(self) -> impl Iterator<Item = &'store str> {
        self.textselections().text()
    }

    /// Constrain the iterator to only return annotations that reference the specified text selection
    /// This is a just shortcut method for `.filter_annotations( textselection.annotations(..) )`
    pub fn filter_textselection(self, textselection: &ResultTextSelection<'store>) -> Self {
        self.filter_annotations(&textselection.annotations().into())
    }

    /// Constrain the iterator to only return annotations that reference any of the specified text selections
    /// This is a just shortcut method for `.filter_annotations( textselections.annotations(..) )`
    pub fn filter_textselections(self, textselections: TextSelectionsIter<'store>) -> Self {
        self.filter_annotations(&textselections.annotations().into())
    }

    /// Returns annotations along with an iterator to go over related text (the operator determines the type of the relation).
    /// Note that results are in chronological annotation order, not textual order.
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

    /// Maps annotations to resources, consuming the iterator.
    pub fn resources(self) -> ResourcesIter<'store> {
        let store = self.store;
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources().map(|x| x.handle()))
            .flatten()
            .collect();
        ResourcesIter::new(IntersectionIter::new_with_set(collection), store)
    }

    /// Maps annotations to resources, consuming the iterator. This only covers resources targeted via a ResourceSelector (i.e. annotations as metadata)
    pub fn resources_as_metadata(self) -> ResourcesIter<'store> {
        let store = self.store;
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources_as_metadata().map(|x| x.handle()))
            .flatten()
            .collect();
        ResourcesIter::new(IntersectionIter::new_with_set(collection), store)
    }

    /// Maps annotations to resources, consuming the iterator. This only covers resources targeted via a TextSelect (i.e. annotations on the text)
    pub fn resources_on_text(self) -> ResourcesIter<'store> {
        let store = self.store;
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources_on_text().map(|x| x.handle()))
            .flatten()
            .collect();
        ResourcesIter::new(IntersectionIter::new_with_set(collection), store)
    }
}
*/

/// An iterator over annotations along with matching data as requested
/// via [`AnnotationsIter::filter_data()`], [`AnnotationsIter::filter_find_data()`] or [`AnnotationsIter::filter_annotationdata()`]).
/// Implicit filters on data via e.g. `filter_annotations(data.annotations())` will **NOT** be included.
/*
pub struct AnnotationsWithDataIter<'store>(AnnotationsIter<'store>);

impl<'store> Iterator for AnnotationsWithDataIter<'store> {
    type Item = (ResultItem<'store, Annotation>, Data<'store>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.0.iter.as_mut() {
                if let Some(item) = iter.next() {
                    if let Some(annotation) = self.0.store.annotation(item) {
                        if !self.0.test_filters(&annotation) {
                            continue;
                        }
                        let mut dataiter = annotation.data();
                        for filter in self.0.filters.iter() {
                            match filter {
                                Filter::AnnotationData(set, data) => {
                                    dataiter = dataiter.filter_handle(*set, *data);
                                }
                                Filter::Data(data, _) => {
                                    dataiter = dataiter.filter_data(data.iter());
                                }
                                Filter::BorrowedData(data, _) => {
                                    dataiter = dataiter.filter_data(data.iter());
                                }
                                _ => {}
                            }
                        }
                        let data = dataiter.to_collection();
                        if !data.is_empty() {
                            return Some((annotation, data));
                        }
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        None
    }
}
*/

pub trait AnnotationIterator<'store>: Iterator<Item = ResultItem<'store, Annotation>>
where
    Self: Sized,
{
    /// Iterates over all the annotations that reference any annotations (i.e. via a [`Selector::AnnotationSelector`]) in this iterator.
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    ///
    /// If you want annotations unsorted and with possible duplicates, then just do:  `.map(|a| a.annotations()).flatten()`
    fn annotations(
        self,
    ) -> MaybeIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|annotation| annotation.annotations())
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        MaybeIter::new_sorted(annotations.into_iter())
    }

    /// Iterates over all the annotations targeted by the annotation in this iterator (i.e. via a [`Selector::AnnotationSelector`])
    /// Use [`Self::annotations()`] if you want to find the annotations that reference these ones (the reverse).
    /// Annotations will be returned sorted chronologically, without duplicates
    fn annotations_in_targets(
        self,
        recursive: bool,
    ) -> MaybeIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|annotation| annotation.annotations_in_targets(recursive))
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        MaybeIter::new_sorted(annotations.into_iter())
    }

    /// Maps annotations to data, consuming the iterator. Returns a new iterator over the AnnotationData in
    /// all the annotations. This returns data sorted chronologically and
    /// without duplicates. It does not include the annotations, use [`Self::iter_with_data()`] instead if you want to know which annotations
    /// have which data.
    fn data(
        self,
    ) -> MaybeIter<<Vec<ResultItem<'store, AnnotationData>> as IntoIterator>::IntoIter> {
        let mut data: Vec<_> = self.map(|annotation| annotation.data()).flatten().collect();
        data.sort_unstable();
        data.dedup();
        MaybeIter::new_sorted(data.into_iter())
    }

    /// Maps annotations to resources, consuming the iterator. Will return in chronological order without duplicates.
    fn resources(
        self,
    ) -> MaybeIter<<BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter> {
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources())
            .flatten()
            .collect();
        MaybeIter::new_sorted(collection.into_iter())
    }

    /// Maps annotations to resources, consuming the iterator. This only covers resources targeted via a ResourceSelector (i.e. annotations as metadata)
    /// Will return in chronological order without duplicates.
    fn resources_as_metadata(
        self,
    ) -> MaybeIter<<BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter> {
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources_as_metadata())
            .flatten()
            .collect();
        MaybeIter::new_sorted(collection.into_iter())
    }

    /// Maps annotations to resources, consuming the iterator. This only covers resources targeted via a TextSelector (i.e. annotations on the text)
    /// Will return in chronological order without duplicates.
    fn resources_on_text(
        self,
    ) -> MaybeIter<<BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter> {
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources_on_text())
            .flatten()
            .collect();
        MaybeIter::new_sorted(collection.into_iter())
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
    fn filter_annotations(self, annotations: Annotations) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotations(annotations),
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

    fn filter_data(mut self, data: Data) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Data(data, FilterMode::Any),
        }
    }

    fn filter_data_all(mut self, data: Data) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Data(data, FilterMode::All),
        }
    }

    fn filter_annotationdata(
        mut self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::AnnotationData(data.set().handle(), data.handle()),
        }
    }

    fn filter_key_value(
        mut self,
        key: &ResultItem<'store, DataKey>,
        value: DataOperator<'store>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKeyAndOperator(key.set().handle(), key.handle(), value),
        }
    }

    fn filter_key(
        mut self,
        key: &ResultItem<'store, DataKey>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle()),
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
            Filter::Data(data, FilterMode::Any) => {
                annotation.data().filter_data(data.clone()).next().is_some()
            }
            Filter::Data(data, FilterMode::All) => {
                annotation.data().filter_data(data.clone()).count() >= data.len()
            }
            Filter::DataKey(set, key) => annotation
                .data()
                .filter_key_handle(set, key)
                .next()
                .is_some(),
            Filter::DataKeyAndOperator(set, key, value) => annotation
                .data()
                .filter_key_handle_value(set, key, value)
                .next()
                .is_some(),
            _ => unimplemented!(
                "Filter {:?} not implemented for FilteredAnnotations",
                self.filter
            ),
        }
    }
}

/*

        let mut datafilter: Option<DataIter> = None;
        for filter in self.filters.iter() {
            match filter {
                Filter::Annotation(handle) => {
                    if annotation.handle() != *handle {
                        return false;
                    }
                }
                Filter::OnData(filters) => {}
                Filter::AnnotationData(set, data) => {
                    if datafilter.is_none() {
                        datafilter = Some(annotation.data());
                    }
                    datafilter = datafilter.map(|dataiter| dataiter.filter_handle(*set, *data));
                }
                Filter::Data(data, FilterMode::Any) => {
                    datafilter = datafilter.filter_data(data); //this has immediate effect
                }
                Filter::Data(data, FilterMode::All) => {
                    if datafilter.is_none() {
                        datafilter = Some(annotation.data());
                    }
                    let expected_count = data.len();
                    datafilter = datafilter.filter_data(data); //this has immediate effect
                    if datafilter.len() != Some(expected_count) {
                        return false;
                    }
                    datafilter = None;
                }
                Filter::BorrowedData(data, FilterMode::All) => {
                    if datafilter.is_none() {
                        datafilter = Some(annotation.data());
                    }
                    let expected_count = data.len();
                    if datafilter.unwrap().filter_data(data.iter()).count() != expected_count {
                        return false;
                    }
                    datafilter = None;
                }
                Filter::TextSelectionOperator(operator) => {
                    if !annotation.related_text(*operator).test() {
                        return false;
                    }
                }
                Filter::TextResource(resource_handle) => {
                    if !annotation
                        .resources()
                        .any(|resource| resource.handle() == *resource_handle)
                    {
                        return false;
                    }
                }
                Filter::Text(reftext, textmode, delimiter) => {
                    if let Some(text) = annotation.text_simple() {
                        match textmode {
                            TextMode::Exact => {
                                if text != reftext.as_str() {
                                    return false;
                                }
                            }
                            TextMode::Lowercase => {
                                if text.to_lowercase() != reftext.as_str() {
                                    return false;
                                }
                            }
                        }
                    } else {
                        let mut text = annotation.text_join(delimiter);
                        if *textmode == TextMode::Lowercase {
                            text = text.to_lowercase();
                        }
                        if text != reftext.as_str() {
                            return false;
                        }
                    }
                }
                Filter::BorrowedText(reftext, textmode, delimiter) => {
                    if let Some(text) = annotation.text_simple() {
                        match textmode {
                            TextMode::Exact => {
                                if text != *reftext {
                                    return false;
                                }
                            }
                            TextMode::Lowercase => {
                                if text.to_lowercase() != *reftext {
                                    return false;
                                }
                            }
                        }
                    } else {
                        let mut text = annotation.text_join(delimiter);
                        if *textmode == TextMode::Lowercase {
                            text = text.to_lowercase();
                        }
                        if text != *reftext {
                            return false;
                        }
                    }
                }
                _ => unimplemented!("Filter {:?} not implemented for AnnotationsIter", filter),
            }
        }
        if let Some(datafilter) = datafilter {
            if !datafilter.test() {
                return false;
            }
        }
        true
    }
}
*/

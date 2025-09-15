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
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use crate::annotation::{Annotation, AnnotationHandle, TargetIter};
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::api::*;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::{TextResource, TextResourceHandle};
use crate::selector::{Selector, SelectorKind};
use crate::substore::AnnotationSubStore;
use crate::textselection::{
    ResultTextSelection, ResultTextSelectionSet, TextSelectionOperator, TextSelectionSet,
};
use crate::{Filter, FilterMode, TextMode};

impl<'store> FullHandle<Annotation> for ResultItem<'store, Annotation> {
    fn fullhandle(&self) -> <Annotation as Storable>::FullHandleType {
        self.handle()
    }
}

/// This is the implementation of the high-level API for [`Annotation`].
impl<'store> ResultItem<'store, Annotation> {
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
    pub fn resources(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, TextResource>>> {
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
    /// Results will be in textual order unless `depth` is set to [`AnnotationDepth::Max`] or a [`Selector::DirectionalSelector`] is involved, then they are in the exact order as they were selected.
    pub fn annotations_in_targets(
        &self,
        depth: AnnotationDepth,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let selector = self.as_ref().target();
        let iter: TargetIter<Annotation> =
            TargetIter::new(selector.iter(self.store(), depth == AnnotationDepth::Max));
        let sorted =
            depth == AnnotationDepth::One && selector.kind() != SelectorKind::DirectionalSelector;
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

    /// Returns the number of textselections under the annotation
    pub fn textselections_count(&self) -> usize {
        self.as_ref().target().len()
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

    /// Returns all underlying text for this annotation concatenated
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
    /// This will be a bit quicker than using `.data().filter_one()`.
    pub fn has_data(&self, data: &ResultItem<AnnotationData>) -> bool {
        self.as_ref().has_data(data.set().handle(), data.handle())
    }

    /// Returns an iterator over the AnnotationData that this annotation targets directly.
    /// This returns only resources that are targeted via a [`Selector::AnnotationDataSelector`] and
    /// returns no duplicates even if data is referenced multiple times.
    pub fn data_as_metadata(
        &self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, AnnotationData>>> {
        let collection: BTreeSet<(AnnotationDataSetHandle, AnnotationDataHandle)> = self
            .as_ref()
            .target()
            .iter(self.store(), true)
            .filter_map(|selector| {
                if let Selector::AnnotationDataSelector(set_handle, data_handle) = selector.as_ref()
                {
                    Some((*set_handle, *data_handle))
                } else {
                    None
                }
            })
            .collect();
        ResultIter::new_sorted(FromHandles::new(collection.into_iter(), self.store()))
    }

    /// Get an iterator over all keys ([`DataKey`]) used by data of this annotation. Shortcut for `.data().keys()`.
    pub fn keys(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, DataKey>>> {
        self.data().keys()
    }

    /// Returns an iterator over the DataKeys that this annotation targets directly.
    /// This returns only resources that are targeted via a [`Selector::DataKeySelector`] and
    /// returns no duplicates even if a key is referenced multiple times.
    pub fn keys_as_metadata(
        &self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, DataKey>>> {
        let collection: BTreeSet<(AnnotationDataSetHandle, DataKeyHandle)> = self
            .as_ref()
            .target()
            .iter(self.store(), true)
            .filter_map(|selector| {
                if let Selector::DataKeySelector(set_handle, key_handle) = selector.as_ref() {
                    Some((*set_handle, *key_handle))
                } else {
                    None
                }
            })
            .collect();
        ResultIter::new_sorted(FromHandles::new(collection.into_iter(), self.store()))
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

    /// Returns the text this resources references as a single text selection set.
    /// This only works if the resource references text *AND* all text pertains to a single resource.
    /// If it can reference multiple resources, use `textselectionsets()` instead.
    pub fn textselectionset(&self) -> Option<ResultTextSelectionSet<'store>> {
        self.try_into().ok()
    }

    /// Returns the text this resources references as a single text selection set.
    /// The text must pertain to the specified resource.
    pub fn textselectionset_in(
        &self,
        resource: impl Request<TextResource>,
    ) -> Option<ResultTextSelectionSet<'store>> {
        let mut textselections: Vec<ResultTextSelection<'store>> = Vec::new();
        let handle = resource
            .to_handle(self.rootstore())
            .expect("resource must have handle");
        for tsel in self.textselections() {
            if tsel.resource().handle() == handle {
                textselections.push(tsel);
            }
        }
        //important check because into_iter().collect() [from_iter()] panics if passed an empty iter!
        if textselections.is_empty() {
            None
        } else {
            Some(textselections.into_iter().collect())
        }
    }

    /// Groups text selections targeting the same resource together in a TextSelectionSet.
    pub fn textselectionsets(&self) -> impl Iterator<Item = ResultTextSelectionSet<'store>> {
        let mut map: BTreeMap<TextResourceHandle, Vec<ResultTextSelection<'store>>> =
            BTreeMap::new();
        for tsel in self.textselections() {
            map.entry(tsel.resource().handle())
                .or_insert_with(move || Vec::new())
                .push(tsel);
        }
        map.into_iter().map(|(_, v)| v.into_iter().collect())
    }

    /// Compares whether a particular spatial relation holds between two annotations
    pub fn test(
        &self,
        operator: &TextSelectionOperator,
        other: &ResultItem<'store, Annotation>,
    ) -> bool {
        for tset in self.textselectionsets() {
            for tset2 in other.textselectionsets() {
                if tset.resource() == tset2.resource() && tset.test_set(operator, &tset2) {
                    return true;
                }
            }
        }
        false
    }

    /// Compares whether a particular spatial relation holds between this annotation and a textselection set.
    pub fn test_textselectionset(
        &self,
        operator: &TextSelectionOperator,
        other: &ResultTextSelectionSet,
    ) -> bool {
        for tset in self.textselectionsets() {
            if tset.resource() == other.resource() && tset.test_set(operator, other) {
                return true;
            }
        }
        false
    }

    /// Compares whether a particular spatial relation holds between this annotation and a textselection
    pub fn test_textselection(
        &self,
        operator: &TextSelectionOperator,
        other: &ResultTextSelection,
    ) -> bool {
        for tset in self.textselectionsets() {
            if tset.resource() == other.resource() && tset.test(operator, other) {
                return true;
            }
        }
        false
    }

    /// Returns the substore this annotation is a part of (if any)
    pub fn substore(&self) -> Option<ResultItem<'store, AnnotationSubStore>> {
        let store = self.store();
        store
            .annotation_substore_map
            .get(self.handle())
            .map(|substore_handle| {
                store
                    .substore(substore_handle)
                    .expect("substore must exist")
            })
    }
}

/// Holds a collection of [`Annotation`] (by reference to an [`AnnotationStore`] and handles). This structure is produced by calling
/// [`ToHandles::to_handles()`], which is available on all iterators over annotations ([`ResultItem<Annotation>`]).
pub type Annotations<'store> = Handles<'store, Annotation>;

impl<'store, I> FullHandleToResultItem<'store, Annotation> for FromHandles<'store, Annotation, I>
where
    I: Iterator<Item = AnnotationHandle>,
{
    fn get_item(&self, handle: AnnotationHandle) -> Option<ResultItem<'store, Annotation>> {
        self.store.annotation(handle)
    }
}

impl<'store, I> FullHandleToResultItem<'store, Annotation> for FilterAllIter<'store, Annotation, I>
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    fn get_item(&self, handle: AnnotationHandle) -> Option<ResultItem<'store, Annotation>> {
        self.store.annotation(handle)
    }
}

/// Trait for iteration over annotations ([`ResultItem<Annotation>`]; encapsulation over
/// [`Annotation`]). Implements numerous filter methods to further constrain the iterator, as well
/// as methods to map from annotations to other items.
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
        depth: AnnotationDepth,
    ) -> ResultIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|annotation| annotation.annotations_in_targets(depth))
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

    /// Get an iterator over all data ([`AnnotationData`]) that are targeted by this annotation via a [`Selector::AnnotationDataSelector`].
    fn data_as_metadata(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, AnnotationData>> as IntoIterator>::IntoIter> {
        let mut data: Vec<_> = self
            .map(|annotation| annotation.data_as_metadata())
            .flatten()
            .collect();
        data.sort_unstable();
        data.dedup();
        ResultIter::new_sorted(data.into_iter())
    }

    /// Get an iterator over all keys ([`DataKey`]) used by data of this annotation. Shortcut for `.data().keys()`.
    fn keys(self) -> ResultIter<<Vec<ResultItem<'store, DataKey>> as IntoIterator>::IntoIter> {
        let mut keys: Vec<_> = self.map(|annotation| annotation.keys()).flatten().collect();
        keys.sort_unstable();
        keys.dedup();
        ResultIter::new_sorted(keys.into_iter())
    }

    /// Get an iterator over all keys ([`DataKey`]) that are targeted by this annotation via a [`Selector::DataKeySelector`].
    fn keys_as_metadata(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, DataKey>> as IntoIterator>::IntoIter> {
        let mut keys: Vec<_> = self
            .map(|annotation| annotation.keys_as_metadata())
            .flatten()
            .collect();
        keys.sort_unstable();
        keys.dedup();
        ResultIter::new_sorted(keys.into_iter())
    }

    /// Shortcut for `.textselections().text()`
    fn text(
        self,
    ) -> TextIter<'store, <Vec<ResultTextSelection<'store>> as IntoIterator>::IntoIter> {
        self.textselections().text()
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
    fn resources(
        self,
    ) -> ResultIter<<BTreeSet<ResultItem<'store, TextResource>> as IntoIterator>::IntoIter> {
        let collection: BTreeSet<_> = self
            .map(|annotation| annotation.resources())
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
    /// This method can only be used once! Use [`Self::filter_any()`] to filter on multiple annotations (disjunction).
    fn filter_one(self, annotation: &ResultItem<Annotation>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotation(
                annotation.handle(),
                SelectionQualifier::Normal,
                AnnotationDepth::Zero,
            ),
        }
    }

    /// Constrain this iterator to filter on *any one* of the mentioned annotations
    fn filter_any(self, annotations: Annotations<'store>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotations(
                annotations,
                FilterMode::Any,
                SelectionQualifier::Normal,
                AnnotationDepth::Zero,
            ),
        }
    }

    /// Constrain this iterator to filter on *all* of the mentioned annotations, that is.
    /// If not all of the items in the parameter exist in the iterator, the iterator returns nothing.
    fn filter_all(
        self,
        annotations: Annotations<'store>,
        store: &'store AnnotationStore,
    ) -> FilterAllIter<'store, Annotation, Self> {
        FilterAllIter::new(self, annotations, store)
    }

    /// Constrain this iterator to filter on *any one* of the mentioned annotations
    fn filter_any_byref(
        self,
        annotations: &'store Annotations<'store>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::BorrowedAnnotations(
                annotations,
                FilterMode::Any,
                SelectionQualifier::Normal,
                AnnotationDepth::Zero,
            ),
        }
    }

    /// Constrain this iterator to filter only a single annotation (by handle) amongst its own results. This is a lower-level method, use [`Self::filter_annotation()`] instead.
    /// This method can only be used once! Use [`Self::filter_annotations()`] to filter on multiple annotations (disjunction).
    fn filter_handle(self, handle: AnnotationHandle) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotation(handle, SelectionQualifier::Normal, AnnotationDepth::Zero),
        }
    }

    /// Constrain this iterator to filter on annotations that are annotated by this annotation
    ///
    /// If you are looking to directly constrain the annotations in this iterator, then use [`Self.filter_one()`] instead.
    fn filter_annotation(
        self,
        annotation: &ResultItem<Annotation>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotation(
                annotation.handle(),
                SelectionQualifier::Normal,
                AnnotationDepth::One, //MAYBE TODO: expose this as a parameter
            ),
        }
    }

    /// Constrain this iterator to filter on annotations that are annotated by by *one of* the mentioned annotations
    ///
    /// The mode parameter determines whether to constrain on *any* match ([`FilterMode::Any`]) or whether to require that the iterator contains *all* of the items in the collection ([`FilterMode::All`]). If in doubt, pick the former.
    ///
    /// If you are looking to directly constrain the annotations in this iterator, then use [`Self.filter_any()`] instead.
    fn filter_annotations(
        self,
        annotations: Annotations<'store>,
        mode: FilterMode,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotations(
                annotations,
                mode,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ),
        }
    }

    /// Constrain this iterator to filter on annotations that are annotated by *one of* the mentioned annotations
    ///
    /// The mode parameter determines whether to constrain on *any* match ([`FilterMode::Any`]) or whether to require that the iterator contains *all* of the items in the collection ([`FilterMode::All`]). If in doubt, pick the former.
    ///
    /// If you are looking to directly constrain the annotations in this iterator, then use [`Self.filter_any_byref()`] instead.
    fn filter_annotations_byref(
        self,
        annotations: &'store Annotations<'store>,
        mode: FilterMode,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::BorrowedAnnotations(
                annotations,
                mode,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ),
        }
    }

    /// Constrain this iterator to filter on annotations that annotate the annotation in the parameter.
    ///
    /// If you are looking to directly constrain the annotations in this iterator, then use [`Self.filter_one()`] instead.
    fn filter_annotation_in_targets(
        self,
        annotation: &ResultItem<Annotation>,
        depth: AnnotationDepth,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotation(annotation.handle(), SelectionQualifier::Metadata, depth),
        }
    }

    /// Constrain this iterator to filter on annotations that annotate *any one of* the annotations in the parameter.
    ///
    /// The mode parameter determines whether to constrain on *any* match ([`FilterMode::Any`]) or whether to require that the iterator contains *all* of the items in the collection ([`FilterMode::All`]). If in doubt, pick the former.
    ///
    /// If you are looking to directly constrain the annotations in this iterator, then use [`Self.filter_any()`] instead.
    fn filter_annotations_in_targets(
        self,
        annotations: Annotations<'store>,
        depth: AnnotationDepth,
        mode: FilterMode,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Annotations(annotations, mode, SelectionQualifier::Metadata, depth),
        }
    }

    /// Constrain this iterator to filter on annotations that annotate *any one of* the annotations in the parameter.
    ///
    /// The mode parameter determines whether to constrain on *any* match ([`FilterMode::Any`]) or whether to require that the iterator contains *all* of the items in the collection ([`FilterMode::All`]). If in doubt, pick the former.
    ///
    /// If you are looking to directly constrain the annotations in this iterator, then use [`Self.filter_multiple_byref()`] instead.
    fn filter_annotations_in_targets_byref(
        self,
        annotations: &'store Annotations<'store>,
        depth: AnnotationDepth,
        mode: FilterMode,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::BorrowedAnnotations(
                annotations,
                mode,
                SelectionQualifier::Metadata,
                depth,
            ),
        }
    }

    /// Constrain the iterator to only return annotations that have data that corresponds with the items in the passed data.
    ///
    /// The mode parameter determines whether to constrain on *any* match ([`FilterMode::Any`]) or whether to require that the iterator contains *all* of the items in the collection ([`FilterMode::All`]). If in doubt, pick the former.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a borrowed reference, use [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    fn filter_data(
        self,
        data: Data<'store>,
        mode: FilterMode,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Data(data, mode, SelectionQualifier::Normal),
        }
    }

    /// Constrain the iterator to only return annotations that have data that corresponds with the items in the passed data.
    ///
    /// The mode parameter determines whether to constrain on *any* match ([`FilterMode::Any`]) or whether to require that the iterator contains *all* of the items in the collection ([`FilterMode::All`]). If in doubt, pick the former.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you do not have a borrowed reference, use [`Self::filter_data()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    fn filter_data_byref(
        self,
        data: &'store Data<'store>,
        mode: FilterMode,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::BorrowedData(data, mode, SelectionQualifier::Normal),
        }
    }

    /// Filter annotations that target text in the specified resource (i.e. via a TextSelector)
    fn filter_resource(
        self,
        resource: &ResultItem<'store, TextResource>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::TextResource(resource.handle(), SelectionQualifier::Normal),
        }
    }

    /// Filter annotations that target the specified resource as whole as metadata (i.e. via a ResourceSelector)
    fn filter_resource_as_metadata(
        self,
        resource: &ResultItem<'store, TextResource>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::TextResource(resource.handle(), SelectionQualifier::Metadata),
        }
    }

    /// Constrain the iterator to return only the annotations that have this exact data item
    /// To filter by multiple data instances (union/disjunction), use [`Self::filter_data()`] or (intersection/conjunction) [`Self::filter_data()`] with [`FilterMode::All`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    fn filter_annotationdata(
        self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::AnnotationData(
                data.set().handle(),
                data.handle(),
                SelectionQualifier::Normal,
            ),
        }
    }

    fn filter_key_value(
        self,
        key: &ResultItem<'store, DataKey>,
        value: DataOperator<'store>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKeyAndOperator(
                key.set().handle(),
                key.handle(),
                value,
                SelectionQualifier::Normal,
            ),
        }
    }

    fn filter_key(self, key: &ResultItem<'store, DataKey>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_key_handle(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataKey(set, key, SelectionQualifier::Normal),
        }
    }

    fn filter_value(self, value: DataOperator<'store>) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::DataOperator(value, SelectionQualifier::Normal),
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
            filter: Filter::DataKeyAndOperator(set, key, value, SelectionQualifier::Normal),
        }
    }

    fn filter_set(
        self,
        set: &ResultItem<'store, AnnotationDataSet>,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_set_handle(self, set: AnnotationDataSetHandle) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::AnnotationDataSet(set, SelectionQualifier::Normal),
        }
    }

    fn filter_substore(
        self,
        substore: Option<ResultItem<'store, AnnotationSubStore>>,
    ) -> FilteredAnnotations<'store, Self> {
        if let Some(substore) = substore {
            FilteredAnnotations {
                inner: self,
                filter: Filter::AnnotationSubStore(Some(substore.handle())),
            }
        } else {
            FilteredAnnotations {
                inner: self,
                filter: Filter::AnnotationSubStore(None),
            }
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
                filter: Filter::Text(text.to_lowercase(), TextMode::CaseInsensitive, delimiter),
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
        FilteredAnnotations {
            inner: self,
            filter: Filter::BorrowedText(
                text,
                if case_sensitive {
                    TextMode::Exact
                } else {
                    TextMode::CaseInsensitive
                },
                delimiter,
            ),
        }
    }

    /// Constrain the iterator to only return text selections that have text matching the specified regular expression.
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    ///
    /// The `delimiter` parameter determines how multiple possible non-contiguous text selections are joined prior to comparison, you most likely want to set it to either a space or an empty string.
    fn filter_text_regex(
        self,
        regex: Regex,
        delimiter: &'store str,
    ) -> FilteredAnnotations<'store, Self> {
        FilteredAnnotations {
            inner: self,
            filter: Filter::Regex(regex, delimiter),
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
            filter: Filter::TextSelectionOperator(operator, SelectionQualifier::Normal),
        }
    }
}

impl<'store, I> AnnotationIterator<'store> for I
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    //blanket implementation
}

/// An iterator that applies a filter to constrain annotations.
/// This iterator implements [`AnnotationIterator`]
/// and is itself produced by the various `filter_*()` methods on that trait.
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
    #[allow(suspicious_double_ref_op)]
    fn test_filter(&self, annotation: &ResultItem<'store, Annotation>) -> bool {
        match &self.filter {
            Filter::Annotation(handle, SelectionQualifier::Normal, AnnotationDepth::Zero) => {
                annotation.handle() == *handle
            }
            Filter::Annotations(
                handles,
                FilterMode::Any,
                SelectionQualifier::Normal,
                AnnotationDepth::Zero,
            ) => handles.contains(&annotation.fullhandle()),
            Filter::BorrowedAnnotations(
                handles,
                FilterMode::Any,
                SelectionQualifier::Normal,
                AnnotationDepth::Zero,
            ) => handles.contains(&annotation.fullhandle()),
            Filter::Annotation(handle, SelectionQualifier::Normal, AnnotationDepth::One) => {
                annotation.annotations().filter_handle(*handle).test()
            }
            Filter::Annotations(
                handles,
                FilterMode::Any,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ) => annotation.annotations().filter_any_byref(handles).test(),
            Filter::Annotations(
                handles,
                FilterMode::All,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ) => annotation
                .annotations()
                .filter_all(handles.clone(), annotation.store())
                .test(),
            Filter::BorrowedAnnotations(
                handles,
                FilterMode::Any,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ) => annotation.annotations().filter_any_byref(handles).test(),
            Filter::BorrowedAnnotations(
                handles,
                FilterMode::All,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ) => annotation
                .annotations()
                .filter_all(handles.deref().clone(), annotation.store())
                .test(),
            Filter::Annotation(handle, SelectionQualifier::Metadata, depth) => annotation
                .annotations_in_targets(*depth)
                .filter_handle(*handle)
                .test(),
            Filter::Data(data, FilterMode::Any, _) => {
                annotation.data().filter_any_byref(&data).test()
            }
            Filter::Data(data, FilterMode::All, _) => annotation
                .data()
                .filter_all(data.clone(), annotation.store())
                .test(),
            Filter::BorrowedData(data, FilterMode::Any, _) => {
                annotation.data().filter_any_byref(data).test()
            }
            Filter::BorrowedData(data, FilterMode::All, _) => annotation
                .data()
                .filter_all(data.deref().clone(), annotation.store())
                .test(),
            Filter::DataKey(set, key, _) => annotation.data().filter_key_handle(*set, *key).test(),
            Filter::DataKeyAndOperator(set, key, value, _) => annotation
                .data()
                .filter_key_handle_value(*set, *key, value.clone())
                .test(),
            Filter::DataOperator(value, _) => annotation.data().filter_value(value.clone()).test(),
            Filter::AnnotationDataSet(set, _) => annotation.data().filter_set_handle(*set).test(),
            Filter::AnnotationData(set, data, _) => {
                annotation.data().filter_handle(*set, *data).test()
            }
            Filter::TextResource(res_handle, SelectionQualifier::Normal) => annotation
                .resources()
                .any(|res| res.handle() == *res_handle),
            Filter::TextResource(res_handle, SelectionQualifier::Metadata) => annotation
                .resources_as_metadata()
                .any(|res| res.handle() == *res_handle),
            Filter::Text(reftext, textmode, delimiter) => {
                if let Some(text) = annotation.text_simple() {
                    match textmode {
                        TextMode::Exact => text == reftext.as_str(),
                        TextMode::CaseInsensitive => text.to_lowercase() == reftext.as_str(),
                    }
                } else {
                    let mut text = annotation.text_join(delimiter);
                    if *textmode == TextMode::CaseInsensitive {
                        text = text.to_lowercase();
                    }
                    text == reftext.as_str()
                }
            }
            Filter::BorrowedText(reftext, textmode, delimiter) => {
                if let Some(text) = annotation.text_simple() {
                    match textmode {
                        TextMode::Exact => text == *reftext,
                        TextMode::CaseInsensitive => text.to_lowercase() == *reftext,
                    }
                } else {
                    let mut text = annotation.text_join(delimiter);
                    if *textmode == TextMode::CaseInsensitive {
                        text = text.to_lowercase();
                    }
                    text == *reftext
                }
            }
            Filter::Regex(regex, delimiter) => {
                if let Some(text) = annotation.text_simple() {
                    regex.is_match(text)
                } else {
                    let text = annotation.text_join(delimiter);
                    regex.is_match(text.as_str())
                }
            }
            Filter::AnnotationSubStore(substore) => {
                annotation.substore().map(|x| x.handle()) == *substore
            }
            Filter::TextSelectionOperator(operator, _) => annotation.related_text(*operator).test(),
            Filter::Annotations(_handles, FilterMode::All, _, AnnotationDepth::Zero) => {
                unreachable!("not handled by this iterator but by FilterAllIter")
            }
            Filter::BorrowedAnnotations(_handles, FilterMode::All, _, AnnotationDepth::Zero) => {
                unreachable!("not handled by this iterator but by FilterAllIter")
            }
            _ => unreachable!(
                "Filter {:?} not implemented for FilteredAnnotations",
                self.filter
            ),
        }
    }
}

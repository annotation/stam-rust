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
use crate::api::annotationdata::{Data, DataIter};
use crate::api::textselection::TextSelectionsIter;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::TextResource;
use crate::selector::{Selector, SelectorKind};
use crate::store::*;
use crate::textselection::{
    ResultTextSelection, ResultTextSelectionSet, TextSelectionOperator, TextSelectionSet,
};
use crate::{Filter, IntersectionIter};

use crate::api::textselection::SortTextualOrder;

/// This is the implementation of the high-level API for [`Annotation`].
impl<'store> ResultItem<'store, Annotation> {
    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns no duplicates even if a resource is referenced multiple times.
    /// If you want to distinguish between resources references as metadata and on text, use [`Self::resources_as_metadata()`] or [`Self::resources_on_text()` ] instead.
    pub fn resources(&self) -> impl Iterator<Item = ResultItem<'store, TextResource>> + 'store {
        let selector = self.as_ref().target();
        let iter: TargetIter<TextResource> = TargetIter::new(selector.iter(self.store(), true));
        //                                                                               ^--- recurse
        let store = self.store();
        iter.map(|handle| store.resource(handle).unwrap())
    }

    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns only resources that are targeted via a [`Selector::ResourceSelector`] and
    /// returns no duplicates even if a resource is referenced multiple times.
    pub fn resources_as_metadata(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.as_ref()
            .target()
            .iter(self.store(), true)
            .filter_map(|selector| {
                if let Selector::ResourceSelector(res_handle) = selector.as_ref() {
                    let store = self.store();
                    store.resource(*res_handle)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns an iterator over the resources that this annotation (by its target selector) references.
    /// This returns only resources that are targeted via a [`Selector::TextSelector`] and
    /// returns no duplicates even if a resource is referenced multiple times.
    pub fn resources_on_text(&self) -> BTreeSet<ResultItem<'store, TextResource>> {
        self.as_ref()
            .target()
            .iter(self.store(), true)
            .filter_map(|selector| {
                if let Selector::TextSelector(res_handle, ..) = selector.as_ref() {
                    let store = self.store();
                    store.resource(*res_handle)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns an iterator over the datasets that this annotation (by its target selector) references via a [`Selector::DataSetSelector`].
    /// This returns no duplicates even if a dataset is referenced multiple times.
    pub fn datasets(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store {
        let selector = self.as_ref().target();
        let iter: TargetIter<AnnotationDataSet> =
            TargetIter::new(selector.iter(self.store(), false));
        let store = self.store();
        iter.map(|handle| store.dataset(handle).unwrap())
    }

    /// Iterates over all the annotations this annotation targets (i.e. via a [`Selector::AnnotationSelector`])
    /// Use [`Self::annotations()`] if you want to find the annotations that reference this one (the reverse operation).
    /// Results will be in textual order unless `recursive` is set or a [`Selector::DirectionalSelector`] is involved, then they are in the exact order as they were selected.
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
    /// Results will be in chronological order and without duplicates, if you want results in textual order, add `.textual_order()`
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        let annotations = self.store().annotations_by_annotation(self.handle());
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

    /// Iterate over all text selections this annotation references (i.e. via [`Selector::TextSelector`])
    /// They are returned in textual order, except in case a [`Selector::DirectionalSelector`] is involved, then they are in the exact order as they were selected.
    pub fn textselections(&self) -> TextSelectionsIter<'store> {
        let textselections = self
            .store()
            .textselections_by_selector(self.as_ref().target());
        TextSelectionsIter::new_lowlevel(textselections, self.store())
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
    pub fn data(&self) -> DataIter<'store> {
        DataIter::new(
            IntersectionIter::new(Cow::Borrowed(self.as_ref().raw_data()), false),
            self.store(),
        )
    }

    /// Find data ([`AnnotationData`]) amongst the data for this annotation. Returns an iterator over the data.
    /// If you have a particular annotation data instance and want to test if the annotation uses it, then use [`Self::has_data()`] instead.
    pub fn find_data<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> DataIter<'store>
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
    pub fn related_text(&self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        //first we gather all textselections for this annotation in a set, as the chosen operator may apply to them jointly
        let tset: TextSelectionSet = self.textselections().collect();
        tset.as_resultset(self.store()).related_text(operator)
    }
}

/// Holds a collection of annotations.
/// This structure is produced by calling [`AnnotationsIter::to_collection()`].
/// Use [`Annotations::iter()`] to iterate over the collection.
pub struct Annotations<'store> {
    array: Cow<'store, [AnnotationHandle]>,
    sorted: bool,
    store: &'store AnnotationStore,
}

impl<'store> Debug for Annotations<'store> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Annotations")
            .field("array", &self.array)
            .field("sorted", &self.sorted)
            .finish()
    }
}

impl<'a> Annotations<'a> {
    /// Returns an iterator over the annotations, the iterator exposes further high-level API methods.
    /// The iterator returns annotations as [`ResultItem<Annotation>`].
    pub fn iter(&self) -> AnnotationsIter<'a> {
        AnnotationsIter::new(
            IntersectionIter::new(self.array.clone(), self.sorted),
            self.store,
        )
    }

    /// Returns the number of annotations in this collection.
    pub fn len(&self) -> usize {
        self.array.len()
    }

    /// Returns a boolean indicating whether the collection is empty or not.
    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    /// Low-level method to instantiate annotations from an existing vector of handles (either owned or borrowed).
    /// Warning: Use of this function is dangerous and discouraged in most cases as there is no validity check on the handles you pass!
    pub fn from_handles(
        array: Cow<'a, [AnnotationHandle]>,
        sorted: bool,
        store: &'a AnnotationStore,
    ) -> Self {
        Self {
            array,
            sorted,
            store,
        }
    }

    /// Low-level method to take the underlying vector of handles
    pub fn take(mut self) -> Vec<AnnotationHandle> {
        self.array.to_mut().to_vec()
    }

    /// Tests if the collection contains a specific element
    pub fn contains(&self, handle: &AnnotationHandle) -> bool {
        if self.sorted {
            match self.array.binary_search(&handle) {
                Ok(_) => true,
                Err(_) => false,
            }
        } else {
            self.array.contains(&handle)
        }
    }
}

/// The AnnotationsIter iterates over annotations, it returns [`ResultItem<Annotation>`] instances.
/// The iterator offers a various high-level API methods that operate on a collection of annotations, and
/// allow to further filter or map annotations.
///
/// The iterator is produced by calling the `annotations()` method that is implemented for several objects, such
/// as [`ResultItem<Annotation>::annotations()`], or on other iterators like [`DataIter::annotations()`] and [`TextSelectionsIter::annotations()`].
pub struct AnnotationsIter<'store> {
    iter: Option<IntersectionIter<'store, AnnotationHandle>>,
    store: &'store AnnotationStore,

    filters: SmallVec<[Filter<'store>; 1]>,
}

impl<'store> AnnotationsIter<'store> {
    pub(crate) fn new(
        iter: IntersectionIter<'store, AnnotationHandle>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            iter: Some(iter),
            filters: SmallVec::new(),
            store,
        }
    }

    pub(crate) fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            iter: None,
            filters: SmallVec::new(),
            store,
        }
    }

    /// Builds a new annotation iterator from any other iterator of annotations.
    /// Eagerly consumes the iterator first.
    pub fn from_iter(
        iter: impl Iterator<Item = ResultItem<'store, Annotation>>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self {
        let data: Vec<_> = iter.map(|a| a.handle()).collect();
        Self {
            iter: Some(IntersectionIter::new(Cow::Owned(data), sorted)),
            filters: SmallVec::new(),
            store,
        }
    }

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
        AnnotationsIter::new(IntersectionIter::new(Cow::Owned(annotations), true), store)
    }

    /// Iterates over all the annotations targeted by the annotation in this iterator (i.e. via a [`Selector::AnnotationSelector`])
    /// Use [`Self::annotations()`] if you want to find the annotations that reference these ones (the reverse).
    /// Unlike [`Self::annotations_in_targets()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant).
    pub fn annotations_in_targets_unchecked(self, recursive: bool) -> AnnotationsIter<'store> {
        let store = self.store;
        AnnotationsIter::new(
            IntersectionIter::new_with_iterator(
                Box::new(
                    self.map(move |annotation| {
                        annotation
                            .annotations_in_targets(recursive)
                            .map(|a| a.handle())
                    })
                    .flatten(),
                ),
                false,
            ),
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
        AnnotationsIter::new(IntersectionIter::new(Cow::Owned(annotations), true), store)
    }

    /// Iterates over all the annotations that reference any annotations in this iterator (i.e. via a [`Selector::AnnotationSelector`])
    /// Unlike [`Self::annotations()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant).
    pub fn annotations_unchecked(self) -> AnnotationsIter<'store> {
        let store = self.store;
        AnnotationsIter::new(
            IntersectionIter::new_with_iterator(
                Box::new(
                    self.map(|annotation| annotation.annotations().map(|a| a.handle()))
                        .flatten(),
                ),
                false,
            ),
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
    /// This method can only be used once, to filter by multiple data instances, use [`Self::filter_data()`] instead.
    pub fn filter_annotationdata(mut self, data: &ResultItem<'store, AnnotationData>) -> Self {
        self.filters
            .push(Filter::AnnotationData(data.set().handle(), data.handle()));
        self
    }

    /// Constrain the iterator to return only the annotations that have this exact data item. This is a lower-level method that takes handles, use [`Self::filter_annotationdata()`] instead.
    /// This method can only be used once, to filter by multiple data instances, use [`Self::filter_data()`] instead.
    pub fn filter_annotationdata_handle(
        mut self,
        set_handle: AnnotationDataSetHandle,
        data_handle: AnnotationDataHandle,
    ) -> Self {
        self.filters
            .push(Filter::AnnotationData(set_handle, data_handle));
        self
    }

    /// Constrain the iterator to only return annotations that have data that corresponds with the passed data.
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a borrowed reference, use [`Self::filter_data_byref()`] instead.
    pub fn filter_data(mut self, data: Data<'store>) -> Self {
        self.filters.push(Filter::Data(data));
        self
    }

    /// Constrain the iterator to only return annotations that have data that corresponds with the passed data.
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a owned data, use [`Self::filter_data()`] instead.
    pub fn filter_data_byref(mut self, data: &'store Data<'store>) -> Self {
        self.filters.push(Filter::BorrowedData(data));
        self
    }

    /// Constrain the iterator to only return annotations that have data matching the search parameters.
    /// This is a just shortcut method for `self.filter_data( store.find_data(..).to_collection() )`
    ///
    /// Note: Do not call this method in a loop, it will be very inefficient! Compute it once before and cache it (`let data = store.find_data(..).to_collection()`), then
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
    pub fn filter_annotations(mut self, annotations: AnnotationsIter<'store>) -> Self {
        if self.iter.is_some() {
            if annotations.iter.is_some() {
                self.iter = Some(self.iter.unwrap().merge(annotations.iter.unwrap()));
            } else {
                //invalidate the iterator, there will be no results
                self.abort();
            }
        }
        self
    }

    /// Constrain this iterator to only a single annotation
    /// This method can only be used once! Use [`Self::filter_annotations()`] to filter on multiple annotations (disjunction).
    pub fn filter_annotation(self, annotation: &ResultItem<Annotation>) -> Self {
        if self.iter.is_some() {
            self.filter_handle(annotation.handle())
        } else {
            self
        }
    }

    /// Constrain this iterator to filter only a single annotation (by handle). This is a lower-level method, use [`Self::filter_annotation()`] instead.
    /// This method can only be used once! Use [`Self::filter_annotations()`] to filter on multiple annotations (disjunction).
    pub fn filter_handle(mut self, handle: AnnotationHandle) -> Self {
        if self.iter.is_some() {
            self.iter = Some(self.iter.unwrap().with_singleton(handle));
        }
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
        self.filter_annotations(textselection.annotations())
    }

    /// Constrain the iterator to only return annotations that reference any of the specified text selections
    /// This is a just shortcut method for `.filter_annotations( textselections.annotations(..) )`
    pub fn filter_textselections(self, textselections: TextSelectionsIter<'store>) -> Self {
        self.filter_annotations(textselections.annotations())
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

    /// Produces the union between two annotation iterators
    /// Any constraints on either iterator remain valid!
    pub fn extend(mut self, other: AnnotationsIter<'store>) -> AnnotationsIter<'store> {
        if self.iter.is_some() && other.iter.is_some() {
            self.iter = Some(self.iter.unwrap().extend(other.iter.unwrap()));
        } else if self.iter.is_none() {
            return other;
        }
        self
    }

    /// Produces the intersection between two annotation iterators
    /// Any constraints on either iterator remain valid!
    pub fn merge(mut self, other: AnnotationsIter<'store>) -> AnnotationsIter<'store> {
        if self.iter.is_some() && other.iter.is_some() {
            self.iter = Some(self.iter.unwrap().extend(other.iter.unwrap()));
        } else if self.iter.is_none() {
            return other;
        }
        self
    }

    /// Does this iterator return items in sorted order?
    pub fn returns_sorted(&self) -> bool {
        if let Some(iter) = self.iter.as_ref() {
            iter.returns_sorted()
        } else {
            true //empty iterators can be considered sorted
        }
    }

    /// Constrain this iterator by a vector of handles (intersection).
    /// You can use [`Self::to_collection()`] on an AnnotationsIter and then later reload it with this method.
    pub fn filter_from(self, annotations: &Annotations<'store>) -> Self {
        self.filter_annotations(annotations.iter())
    }

    /// Exports the iterator to a low-level vector that can be reused at will by invoking `.iter()`.
    /// This consumes the iterator.
    /// Note: This is different than running `collect()`, which produces high-level objects.
    pub fn to_collection(self) -> Annotations<'store> {
        let store = self.store;
        let sorted = self.returns_sorted();

        //handle special case where we may be able to just clone the reference (Cow::Borrowed) from the iterator
        if self.iter.is_some()
            && self.iter.as_ref().unwrap().sources.len() == 1
            && self.iter.as_ref().unwrap().sources[0].array.is_some()
            && self.filters.is_empty()
        {
            Annotations {
                array: self.iter.unwrap().sources[0].array.clone().unwrap(),
                store,
                sorted,
            }
        } else {
            Annotations {
                array: Cow::Owned(self.map(|x| x.handle()).collect()),
                store,
                sorted,
            }
        }
    }

    /// Exports the iterator to a low-level vector that can be reused at will by invoking `.iter()`.
    /// This consumes the iterator but takes only the first n elements up to the specified limit.
    pub fn to_collection_limit(self, limit: usize) -> Annotations<'store> {
        let store = self.store;
        let sorted = self.returns_sorted();
        Annotations {
            array: Cow::Owned(self.take(limit).map(|x| x.handle()).collect()),
            store,
            sorted,
        }
    }

    /// Set the iterator to abort, no further results will be returned
    pub fn abort(&mut self) {
        self.iter.as_mut().map(|iter| iter.abort = true);
    }

    /// Returns true if the iterator has items, false otherwise
    pub fn test(mut self) -> bool {
        self.next().is_some()
    }

    /// See if the filters match for the annotation
    /// This does not include any filters directly on annotations, as those is handled already by the underlying IntersectionsIter
    fn test_filters(&self, annotation: &ResultItem<'store, Annotation>) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        let mut datafilter: Option<DataIter> = None;
        for filter in self.filters.iter() {
            match filter {
                Filter::AnnotationData(set, data) => {
                    if datafilter.is_none() {
                        datafilter = Some(annotation.data());
                    }
                    datafilter = datafilter.map(|dataiter| dataiter.filter_handle(*set, *data));
                }
                Filter::Data(data) => {
                    if datafilter.is_none() {
                        datafilter = Some(annotation.data());
                    }
                    datafilter = datafilter.map(|dataiter| dataiter.filter_data(data.iter()));
                }
                Filter::BorrowedData(data) => {
                    if datafilter.is_none() {
                        datafilter = Some(annotation.data());
                    }
                    datafilter = datafilter.map(|dataiter| dataiter.filter_data(data.iter()));
                }
                Filter::TextSelectionOperator(operator) => {
                    if !annotation.related_text(*operator).test() {
                        return false;
                    }
                }
                _ => unimplemented!("Filter {:?} not implemented for AnnotatationsIter", filter),
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

impl<'store> Iterator for AnnotationsIter<'store> {
    type Item = ResultItem<'store, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iter.as_mut() {
                if let Some(item) = iter.next() {
                    if let Some(annotation) = self.store.annotation(item) {
                        if !self.test_filters(&annotation) {
                            continue;
                        }
                        return Some(annotation);
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

/// An iterator over annotations along with matching data as requested
/// via [`AnnotationsIter::filter_data()`], [`AnnotationsIter::filter_find_data()`] or [`AnnotationsIter::filter_annotationdata()`]).
/// Implicit filters on data via e.g. `filter_annotations(data.annotations())` will **NOT** be included.
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
                                Filter::Data(data) => {
                                    dataiter = dataiter.filter_data(data.iter());
                                }
                                Filter::BorrowedData(data) => {
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

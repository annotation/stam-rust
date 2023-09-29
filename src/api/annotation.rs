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
    /// If you want to distinguish between resources references as metadata and on text, use [`Self.resources_as_metadata()`] or [`Self.resources_on_text()` ] instead.
    pub fn resources(&self) -> impl Iterator<Item = ResultItem<'store, TextResource>> + 'store {
        let selector = self.as_ref().target();
        let iter: TargetIter<TextResource> = TargetIter::new(selector.iter(self.store(), true));
        //                                                                               ^--- recurse
        let store = self.store();
        iter.map(|handle| store.resource(handle).unwrap())
    }

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
    /// They are returned in textual order, except in case if a DirectionSelector is involved, then they are in the exact order as they were selected.
    pub fn textselections(&self) -> TextSelectionsIter<'store> {
        let textselections = self
            .store()
            .textselections_by_selector(self.as_ref().target());
        TextSelectionsIter::new_lowlevel(textselections, self.store())
    }

    /// Iterates over all text slices this annotation refers to
    /// They are returned in textual order, or in case if a DirectionSelector is involved, in the exact order as they were selected.
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

    /// Returns the (single!) resource the annotation points to. Only works for TextSelector,
    /// ResourceSelector and AnnotationSelector, and not for complex selectors.
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

    /// Get an iterator over all data for this annotation
    pub fn data<'q>(&self) -> DataIter<'store, 'q> {
        DataIter::new(
            IntersectionIter::new(Cow::Borrowed(self.as_ref().raw_data()), false),
            self.store(),
        )
    }

    /// Tests if the annotation has certain data, returns a boolean.
    /// This will be a bit quicker than using `.data().filter_annotationdata()`.
    pub fn has_data(&self, data: &ResultItem<AnnotationData>) -> bool {
        self.as_ref().has_data(data.set().handle(), data.handle())
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the text relations pertaining to the annotations. Returns an iterator over the [`TextSelection`] instances, in textual order.
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
    pub fn data(self) -> DataIter<'store, 'store> {
        let store = self.store;
        let mut data: Vec<_> = self
            .map(|annotation| annotation.as_ref().data().copied())
            .flatten()
            .collect();
        data.sort_unstable();
        data.dedup();
        DataIter::new(IntersectionIter::new(Cow::Owned(data), true), store)
    }

    /// Constrain the iterator to return only the annotations that have this exact data item
    /// To filter by multiple data instances, use [`Self.filter_data()`] instead.
    pub fn filter_annotationdata(mut self, data: &ResultItem<'store, AnnotationData>) -> Self {
        let data_annotations = self
            .store
            .annotations_by_data_indexlookup(data.set().handle(), data.handle());
        if self.iter.is_some() {
            if let Some(data_annotations) = data_annotations {
                self.iter = Some(
                    self.iter
                        .unwrap()
                        .with(Cow::Borrowed(data_annotations), true),
                );
            } else {
                self.abort(); //data is not used, invalidate the iterator
            }
        }
        self
    }

    /// Constrain the iterator to only return annotations that have data that occurs in the passed data iterator.
    /// If you have a single AnnotationData instance, use [`Self.filter_annotationdata()`] instead.
    pub fn filter_data(self, data: DataIter<'store, '_>) -> Self {
        self.filter_annotations(data.annotations())
    }

    /// Constrain the iterator to only return annotations that have data matching the search parameters.
    /// This is a just shortcut method for `self.filter_data( store.find_data(..) )`
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
        self.filter_data(store.find_data(set, key, value))
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

    /// Returns annotations along with matching data (matching the search parameters), either may occur multiple times!
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

    /// Find all text selections that are related to any text selections in this iterator, the operator
    /// determines the type of the relation. Shortcut method for `.textselections().related_text(operator)`.
    pub fn related_text(self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        self.textselections().related_text(operator)
    }

    /// Maps annotations to textselections, consuming the itetor. Results will be returned in textual order.
    pub fn textselections(self) -> TextSelectionsIter<'store> {
        let store = self.store;
        TextSelectionsIter::new(
            self.map(|annotation| annotation.textselections())
                .flatten()
                .textual_order(),
            store,
        )
    }

    /// Returns annotations along with matching text selections, either may occur multiple times!
    /// If an annotation references multiple text selections, they are returned as a set.
    /// Note that results are in chronological annotation order, not textual order.
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

    /// Constrain the iterator to only return annotations that reference the specified text selection
    /// This is a just shortcut method for `.filter_annotations( textselection.annotations(..) )`
    pub fn filter_textselection(mut self, textselection: &ResultTextSelection<'store>) -> Self {
        self.filter_annotations(textselection.annotations())
    }

    /// Constrain the iterator to only return annotations that reference any of the specified text selections
    /// This is a just shortcut method for `.filter_annotations( textselections.annotations(..) )`
    pub fn filter_textselections(mut self, textselections: TextSelectionsIter<'store>) -> Self {
        self.filter_annotations(textselections.annotations())
    }

    /// Returns annotations along with an iterator to over related text (the operator determines the type of the relation).
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

    /// Extract a low-level vector of handles from this iterator.
    /// This is different than running `collect()`, which produces high-level objects.
    ///
    /// An extracted vector can be easily turned back into a DataIter again with [`Self.from_vec()`]
    pub fn to_vec(mut self) -> Vec<AnnotationHandle> {
        let mut results: Vec<AnnotationHandle> = Vec::new();
        loop {
            if let Some(iter) = self.iter.as_mut() {
                if let Some(handle) = iter.next() {
                    results.push(handle);
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        results
    }

    /// Turns a low-level vector into an AnnotationsIter. Borrows the underlying vec, use [`Self::from_vec_owned()`] if you need an owned variant.
    /// A low-level vector can be produced from any AnnotationsIter with [`Self.to_vec()`]
    /// The `sorted` parameter expressed whether the underlying vector is sorted (you need to set it, it does not do anything itself)
    pub fn from_vec<'a>(
        vec: &'a Vec<AnnotationHandle>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self
    where
        'a: 'store,
    {
        Self::new(IntersectionIter::new(Cow::Borrowed(vec), sorted), store)
    }

    /// Turns a low-level vector into an AnnotationsIter. Owns the underlying vec, use [`Self::from_vec()`] if you want a borrowed variant.
    /// A low-level vector can be produced from any AnnotationsIter with [`Self.to_vec()`]
    /// The `sorted` parameter expressed whether the underlying vector is sorted (you need to set it, it does not do anything itself)
    pub fn from_vec_owned(
        vec: Vec<AnnotationHandle>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self {
        Self::new(IntersectionIter::new(Cow::Owned(vec), sorted), store)
    }

    /// Set the iterator to abort, no further results will be returned
    pub fn abort(&mut self) {
        self.iter.as_mut().map(|iter| iter.abort = true);
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

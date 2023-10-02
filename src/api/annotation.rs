use rayon::prelude::*;
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
    pub fn data(&self) -> DataIter<'store> {
        DataIter::new(
            IntersectionIter::new(Cow::Borrowed(self.as_ref().raw_data()), false),
            self.store(),
        )
    }

    /// Find data amongst the data for this annotation. Returns an iterator over the data.
    /// If you have a particular annotation data instance, then use [`Self.has_data()`] instead.
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

pub struct Annotations<'store> {
    array: Cow<'store, [AnnotationHandle]>,
    store: &'store AnnotationStore,
    sorted: bool,
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
    pub fn iter(&self) -> AnnotationsIter<'a> {
        AnnotationsIter::new(
            IntersectionIter::new(self.array.clone(), self.sorted),
            self.store,
        )
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }
}

pub struct AnnotationsIter<'store> {
    iter: Option<IntersectionIter<'store, AnnotationHandle>>,
    cursor: usize,
    store: &'store AnnotationStore,

    data_filter: Option<Data<'store>>,
    single_data_filter: Option<(AnnotationDataSetHandle, AnnotationDataHandle)>,
}

impl<'store> AnnotationsIter<'store> {
    pub(crate) fn new(
        iter: IntersectionIter<'store, AnnotationHandle>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            cursor: 0,
            iter: Some(iter),
            data_filter: None,
            single_data_filter: None,
            store,
        }
    }

    pub(crate) fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            cursor: 0,
            iter: None,
            data_filter: None,
            single_data_filter: None,
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
            cursor: 0,
            data_filter: None,
            single_data_filter: None,
            store,
        }
    }

    /// Produce a parallel iterator, iterator methods like `filter` and `map` *after* this will run in parallel.
    /// It does not parallelize the operation of AnnotationsIter itself.
    /// This first consumes the sequential iterator into a newly allocated buffer.
    pub fn parallel(self) -> impl ParallelIterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.collect::<Vec<_>>().into_par_iter()
    }

    /// Iterates over all the annotations targeted by the annotation in this iterator (i.e. via a [`Selector::AnnotationSelector'])
    /// Use [`Self.annotations()'] if you want to find the annotations that reference these ones (the reverse).
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

    /// Iterates over all the annotations that reference any annotations in this iterator (i.e. via a [`Selector::AnnotationSelector'])
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

    /// Maps annotations to data, consuming the iterator. Returns a new iterator over the data in
    /// all the annotations. This returns data without annotations (sorted chronologically and
    /// without duplicates), use [`Self.with_data()`] instead if you want to know which annotations
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

    /// Find data for the annotations in this iterator. Returns an iterator over the data (losing the information about annotations).
    /// If you want specifically know what annotation has what data, use [`Self.zip_find_data()`] instead.
    /// If you want to constrain annotations by a data search, use [`Self.filter_find_data()`] instead.
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
    /// To filter by multiple data instances, use [`Self.filter_data()`] instead.
    pub fn filter_annotationdata(mut self, data: &ResultItem<'store, AnnotationData>) -> Self {
        self.single_data_filter = Some((data.set().handle(), data.handle()));
        self
    }

    /// Constrain the iterator to only return annotations that have data that corresponds with the passed data.
    /// If you have a single AnnotationData instance, use [`Self.filter_annotationdata()`] instead.
    pub fn filter_data<'a>(mut self, data: Data<'store>) -> Self
    where
        'a: 'store,
    {
        self.data_filter = Some(data);
        self
    }

    /// Constrain the iterator to only return annotations that have data matching the search parameters.
    /// This is a just shortcut method for `self.filter_data( store.find_data(..).to_cache() )`
    ///
    /// Note: Do not call this method in a loop, it will be very inefficient! Compute it once before and cache it (`let data = store.find_data(..).to_cache()`), then
    ///       pass the result to [`Self.filter_data(data.clone())`], the clone will be cheap.
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
        self.filter_data(store.find_data(set, key, value).to_cache())
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
    /// This method can be called multiple times
    ///
    /// You can cast various tuples or vectors of ResultItem<Annotation> to AnnotationIter via `.into_iter()`.
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
    /// This method can only be used once! Use [`Self.filter_annotations()`] to filter on multiple annotations (disjunction).
    pub fn filter_annotation(mut self, annotation: &ResultItem<Annotation>) -> Self {
        if self.iter.is_some() {
            self.iter = Some(self.iter.unwrap().with_singleton(annotation.handle()));
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

    /// Does this iterator return items in sorted order?
    pub fn returns_sorted(&self) -> bool {
        if let Some(iter) = self.iter.as_ref() {
            iter.returns_sorted()
        } else {
            true //empty iterators can be considered sorted
        }
    }

    /// Constrain this iterator by a vector of handles (intersection).
    /// You can use [`Self.to_cache()`] on an AnnotationIter and then later reload it with this method.
    pub fn filter_from(self, annotations: &Annotations<'store>) -> Self {
        self.filter_annotations(annotations.iter())
    }

    /// Exports the iterator to a low-level vector that can be reused at will by invoking `.iter()`.
    /// This consumes the iterator.
    /// Note: This is different than running `collect()`, which produces high-level objects.
    pub fn to_cache(self) -> Annotations<'store> {
        let store = self.store;
        let sorted = self.returns_sorted();

        //handle special case where we may be able to just clone the reference (Cow::Borrowed) from the iterator
        if self.iter.is_some()
            && self.iter.as_ref().unwrap().sources.len() == 1
            && self.iter.as_ref().unwrap().sources[0].array.is_some()
            && self.data_filter.is_none()
            && self.single_data_filter.is_none()
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

    /// Set the iterator to abort, no further results will be returned
    pub fn abort(&mut self) {
        self.iter.as_mut().map(|iter| iter.abort = true);
    }

    /// Returns true if the iterator has items, false otherwise
    pub fn test(mut self) -> bool {
        self.next().is_some()
    }
}

impl<'store> Iterator for AnnotationsIter<'store> {
    type Item = ResultItem<'store, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iter.as_mut() {
                if let Some(item) = iter.next() {
                    if let Some(annotation) = self.store.annotation(item) {
                        if let Some((set_handle, data_handle)) = self.single_data_filter {
                            if !annotation
                                .data()
                                .filter_handle(set_handle, data_handle)
                                .test()
                            {
                                continue;
                            }
                        }
                        if let Some(data_filter) = &self.data_filter {
                            if !annotation.data().filter_data(data_filter.iter()).test() {
                                continue;
                            }
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

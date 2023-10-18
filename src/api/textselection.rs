/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`TextSelection`]. This API is implemented on
//! [`ResultTextSelection`], which encapsulates a [`TextSelection`] instance.

use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::AnnotationDataSetHandle;
use crate::annotationstore::AnnotationStore;
use crate::api::annotation::{Annotations, AnnotationsIter};
use crate::api::annotationdata::Data;
use crate::error::*;
use crate::resources::{TextResource, TextResourceHandle, TextSelectionIter};
use crate::selector::{Offset, OffsetMode};
use crate::store::*;
use crate::text::Text;
use crate::textselection::{
    FindTextSelectionsIter, ResultTextSelection, ResultTextSelectionSet, TextSelection,
    TextSelectionHandle, TextSelectionOperator, TextSelectionSet,
};
use crate::types::*;
use crate::IntersectionIter;
use sealed::sealed;

use rayon::prelude::*;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cmp::Ordering;

/// This is the implementation of the high-level API for [`TextSelection`], though most of it is more commonly used via [`ResultTextSelection`].
impl<'store> ResultItem<'store, TextSelection> {
    /// Return the begin position (unicode points)
    pub fn begin(&self) -> usize {
        self.as_ref().begin()
    }

    /// Return the end position (unicode points)
    pub fn end(&self) -> usize {
        self.as_ref().end()
    }

    /// Return the resource ([`ResultItem<TextResource>`]) this text selection references.
    pub fn resource(&self) -> ResultItem<'store, TextResource> {
        let rootstore = self.rootstore();
        self.store().as_resultitem(rootstore, rootstore)
    }

    /// Iterates over all annotations that reference this TextSelection
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        if let Some(annotations) = self
            .rootstore()
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
        {
            AnnotationsIter::new(
                IntersectionIter::new(Cow::Borrowed(annotations), true),
                self.rootstore(),
            )
        } else {
            //dummy iterator that yields nothing
            AnnotationsIter::new_empty(self.rootstore())
        }
    }

    /// Returns the number of annotations that reference this text selection
    /// This is more performant than doing `self.annotations().count()`.
    pub fn annotations_len(&self) -> usize {
        if let Some(vec) = self
            .rootstore()
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
        {
            vec.len()
        } else {
            0
        }
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultTextSelection`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let tset: TextSelectionSet = self.clone().into();
        self.resource().related_text(operator, tset)
    }
}

impl<'store> ResultTextSelection<'store> {
    /// Return a reference to the inner textselection.
    /// This works in all cases but will have a limited lifetime.
    /// Use [`Self.as_ref()`] instead if you have bound item.
    pub fn inner(&self) -> &TextSelection {
        match self {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, _, item) => item,
        }
    }

    /// Return a reference to the textselection in the store.
    /// Only works on bound items.
    /// Use [`Self.inner()`] instead if
    pub fn as_ref(&self) -> Option<&'store TextSelection> {
        match self {
            Self::Bound(item) => Some(item.as_ref()),
            Self::Unbound(..) => None,
        }
    }

    /// Return a reference to the textselection in the store.
    /// Only works on bound items.
    /// Use [`Self.inner()`] instead if
    pub fn as_resultitem(&self) -> Option<&ResultItem<'store, TextSelection>> {
        match self {
            Self::Bound(item) => Some(item),
            Self::Unbound(..) => None,
        }
    }

    /// Return the begin position (unicode points)
    pub fn begin(&self) -> usize {
        match self {
            Self::Bound(item) => item.as_ref().begin(),
            Self::Unbound(_, _, item) => item.begin(),
        }
    }

    /// Return the end position (non-inclusive) in unicode points
    pub fn end(&self) -> usize {
        match self {
            Self::Bound(item) => item.as_ref().end(),
            Self::Unbound(_, _, item) => item.end(),
        }
    }

    /// Returns the begin cursor of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_begin(&self, container: &ResultTextSelection<'store>) -> Option<usize> {
        if self.store() != container.store() {
            None
        } else {
            let container = match container {
                Self::Bound(item) => item.as_ref(),
                Self::Unbound(_, _, item) => &item,
            };
            match self {
                Self::Bound(item) => item.as_ref().relative_begin(container),
                Self::Unbound(_, _, item) => item.relative_begin(container),
            }
        }
    }

    /// Returns the end cursor (begin-aligned) of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_end(&self, container: &ResultTextSelection<'store>) -> Option<usize> {
        let container = match container {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, _, item) => &item,
        };
        match self {
            Self::Bound(item) => item.as_ref().relative_end(container),
            Self::Unbound(_, _, item) => item.relative_end(container),
        }
    }

    /// Returns the offset of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_offset(
        &self,
        container: &ResultTextSelection<'store>,
        offsetmode: OffsetMode,
    ) -> Option<Offset> {
        let container = match container {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, _, item) => &item,
        };
        match self {
            Self::Bound(item) => item.as_ref().relative_offset(container, offsetmode),
            Self::Unbound(_, _, item) => item.relative_offset(container, offsetmode),
        }
    }

    pub(crate) fn store(&self) -> &'store TextResource {
        match self {
            Self::Bound(item) => item.store(),
            Self::Unbound(_, store, ..) => store,
        }
    }

    /// Return the [`AnnotationStore`] this text selection references.
    pub fn rootstore(&self) -> &'store AnnotationStore {
        match self {
            Self::Bound(item) => item.rootstore(),
            Self::Unbound(rootstore, ..) => rootstore,
        }
    }

    /// Return the resource ([`ResultItem<TextResource>`]) this text selection references.
    pub fn resource(&self) -> ResultItem<'store, TextResource> {
        let rootstore = self.rootstore();
        self.store().as_resultitem(rootstore, rootstore)
    }

    /// Returns the internal handle used by the TextSelection.
    /// If the TextSelection is unbound, this will return `None`.
    pub fn handle(&self) -> Option<TextSelectionHandle> {
        match self {
            Self::Bound(item) => Some(item.handle()),
            Self::Unbound(..) => None,
        }
    }

    pub fn take(self) -> Result<TextSelection, StamError> {
        match self {
            Self::Bound(_) => Err(StamError::AlreadyBound(
                "Item is bound, can't be taken out!",
            )),
            Self::Unbound(_, _, item) => Ok(item),
        }
    }

    /// Iterates over all annotations that are referenced by this TextSelection, if any.
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        match self {
            Self::Bound(item) => item.annotations(),
            Self::Unbound(..) => AnnotationsIter::new_empty(self.rootstore()),
        }
    }

    /// Returns the number of annotations that reference this text selection
    pub fn annotations_len(&self) -> usize {
        match self {
            Self::Bound(item) => item.annotations_len(),
            Self::Unbound(..) => 0,
        }
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.annotations_by_related_text()`] instead.
    pub fn related_text(&self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        let mut tset: TextSelectionSet =
            TextSelectionSet::new(self.store().handle().expect("resource must have handle"));
        tset.add(match self {
            Self::Bound(item) => item.as_ref().clone().into(),
            Self::Unbound(_, _, textselection) => textselection.clone(),
        });
        self.resource().related_text(operator, tset)
    }
}

impl<'store> ResultTextSelectionSet<'store> {
    pub fn rootstore(&self) -> &'store AnnotationStore {
        self.rootstore
    }

    pub fn resource(&self) -> ResultItem<'store, TextResource> {
        self.rootstore()
            .resource(self.tset.resource())
            .expect("resource must exist")
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current text selection set. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    pub fn related_text(self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        let resource = self.resource();
        let store = self.rootstore();
        TextSelectionsIter::new_with_findtextiterator(
            resource
                .as_ref()
                .textselections_by_operator(operator, self.tset),
            store,
        )
    }
}

/// This trait allows sorting a collection in textual order, meaning
/// that items are returned in the same order as they appear in the original text.
pub trait SortTextualOrder<T>
where
    T: PartialOrd,
{
    /// Sorts items in the iterator in textual order,
    /// meaningthat items are returned in the same order as they appear in the original text.
    /// items that do not relate to text at all will be put at the end with arbitrary sorting
    /// This method allocates and returns a buffer to do the sorting.
    ///
    /// It does deduplication only for iterators over [`TextSelection`], [`ResultTextSelection`].
    fn textual_order(&mut self) -> Vec<T>;
}

// Comparison function for two annotations, returns their ordering in textual order
pub fn compare_annotation_textual_order<'store>(
    a: &ResultItem<'store, Annotation>,
    b: &ResultItem<'store, Annotation>,
) -> Ordering {
    let tset_a: TextSelectionSet = a.textselections().collect();
    let tset_b: TextSelectionSet = b.textselections().collect();
    if tset_a.is_empty() && tset_b.is_empty() {
        //compare by handle
        a.handle().cmp(&b.handle())
    } else if tset_a.is_empty() {
        Ordering::Greater
    } else if tset_b.is_empty() {
        Ordering::Less
    } else {
        tset_a
            .partial_cmp(&tset_b)
            .expect("textual_order() can only be applied if annotations reference text!")
        //should never occur because I tested for this already
    }
}

impl<'store, I> SortTextualOrder<ResultItem<'store, Annotation>> for I
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    fn textual_order(&mut self) -> Vec<ResultItem<'store, Annotation>> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(compare_annotation_textual_order);
        v
    }
}

impl<'store, I> SortTextualOrder<ResultTextSelection<'store>> for I
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    fn textual_order(&mut self) -> Vec<ResultTextSelection<'store>> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| {
            a.partial_cmp(b)
                .expect("PartialOrd must work for ResultTextSelection")
        });
        v.dedup();
        v
    }
}

impl<'store, I> SortTextualOrder<ResultTextSelectionSet<'store>> for I
where
    I: Iterator<Item = ResultTextSelectionSet<'store>>,
{
    fn textual_order(&mut self) -> Vec<ResultTextSelectionSet<'store>> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| {
            if a.tset.is_empty() && b.tset.is_empty() {
                Ordering::Equal
            } else if a.tset.is_empty() {
                Ordering::Greater
            } else if b.tset.is_empty() {
                Ordering::Less
            } else {
                a.partial_cmp(b)
                    .expect("PartialOrd must work for ResultTextSelectionSet")
            }
        });
        v
    }
}

impl<'store, I> SortTextualOrder<TextSelectionSet> for I
where
    I: Iterator<Item = TextSelectionSet>,
{
    fn textual_order(&mut self) -> Vec<TextSelectionSet> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| {
            if a.is_empty() && b.is_empty() {
                Ordering::Equal
            } else if a.is_empty() {
                Ordering::Greater
            } else if b.is_empty() {
                Ordering::Less
            } else {
                a.partial_cmp(b)
                    .expect("PartialOrd must work for TextSelectionSet")
            }
        });
        v
    }
}

impl<'store, I> SortTextualOrder<TextSelection> for I
where
    I: Iterator<Item = TextSelection>,
{
    fn textual_order(&mut self) -> Vec<TextSelection> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| a.cmp(b));
        v.dedup();
        v
    }
}

/// Source for TextSelectionsIter
pub(crate) enum TextSelectionsSource<'store> {
    HighVec(Vec<ResultTextSelection<'store>>),
    LowVec(SmallVec<[(TextResourceHandle, TextSelectionHandle); 2]>), //used with AnnotationStore.textselections_by_selector
    FindIter(FindTextSelectionsIter<'store>), //used with textselections_by_operator()
    TSIter(TextSelectionIter<'store>),        //used by resource.textselections(), double-ended
    HighIter(Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>),
}

/// Iterator over TextSelections (yields [`ResultTextSelection`] instances)
/// offering high-level API methods.
pub struct TextSelectionsIter<'store> {
    source: TextSelectionsSource<'store>,
    cursor: isize,
    store: &'store AnnotationStore,
    /// Direction of iteration
    forward: Option<bool>,
    annotations_filter: Option<Annotations<'store>>,
    single_annotation_filter: Option<AnnotationHandle>,
    single_data_filter: Option<(AnnotationDataSetHandle, AnnotationDataHandle)>,
    data_filter: Option<Data<'store>>,
}

impl<'store> Iterator for TextSelectionsIter<'store> {
    type Item = ResultTextSelection<'store>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.forward != Some(true) {
            self.forward = Some(true);
            self.cursor = 0;
        }
        loop {
            let result = match &mut self.source {
                TextSelectionsSource::HighVec(textselections) => {
                    if let Some(textselection) = textselections.get(self.cursor as usize) {
                        self.cursor += 1;
                        Some(textselection.clone())
                    } else {
                        None
                    }
                }
                TextSelectionsSource::HighIter(iter) => iter.next(),
                TextSelectionsSource::LowVec(handles) => {
                    if let Some((res_handle, tsel_handle)) = handles.get(self.cursor as usize) {
                        let resource = self
                            .store
                            .resource(*res_handle)
                            .expect("resource must exist");
                        let tsel: &TextSelection = resource
                            .as_ref()
                            .get(*tsel_handle)
                            .expect("text selection must exist");
                        self.cursor += 1;
                        Some(ResultTextSelection::Bound(
                            tsel.as_resultitem(resource.as_ref(), self.store),
                        ))
                    } else {
                        None
                    }
                }
                TextSelectionsSource::FindIter(iter) => {
                    if let Some(tsel_handle) = iter.next() {
                        let resource = iter.resource();
                        let tsel: &TextSelection = resource
                            .get(tsel_handle)
                            .expect("text selection must exist");
                        self.cursor += 1; //not really used in this context
                        Some(ResultTextSelection::Bound(
                            tsel.as_resultitem(resource, self.store),
                        ))
                    } else {
                        None
                    }
                }
                TextSelectionsSource::TSIter(iter) => {
                    if let Some(tsel) = iter.next() {
                        let resource = iter.resource();
                        self.cursor += 1; //not really used in this context
                        Some(ResultTextSelection::Bound(
                            tsel.as_resultitem(resource, self.store),
                        ))
                    } else {
                        None
                    }
                }
            };
            if result.is_some() && self.annotations_filter.is_some()
                || self.single_annotation_filter.is_some()
            {
                if !self.pass_filter(result.as_ref().unwrap()) {
                    continue;
                }
            }
            return result;
        }
    }
}

impl<'store> DoubleEndedIterator for TextSelectionsIter<'store> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.forward != Some(false) {
            self.forward = Some(false);
            self.cursor = match &self.source {
                TextSelectionsSource::HighVec(textselections) => textselections.len() as isize - 1,
                TextSelectionsSource::LowVec(handles) => handles.len() as isize - 1,
                TextSelectionsSource::HighIter(_) => {
                    //this is a bit dangerous, no proper error propagation here
                    unimplemented!("No backward iteration on TextSelectionsSource::HighIter")
                }
                TextSelectionsSource::FindIter(_) => {
                    unimplemented!("No backward iteration on TextSelectionsSource::FindIter")
                }
                TextSelectionsSource::TSIter(iter) => iter.size_hint().0 as isize,
            };
        }
        loop {
            if self.cursor < 0 {
                return None;
            }
            let result = match &mut self.source {
                TextSelectionsSource::HighVec(textselections) => {
                    if let Some(item) = textselections.get(self.cursor as usize) {
                        self.cursor -= 1;
                        Some(item.clone())
                    } else {
                        None
                    }
                }
                TextSelectionsSource::LowVec(handles) => {
                    if let Some((res_handle, tsel_handle)) = handles.get(self.cursor as usize) {
                        let resource = self
                            .store
                            .resource(*res_handle)
                            .expect("resource must exist");
                        let tsel: &TextSelection = resource
                            .as_ref()
                            .get(*tsel_handle)
                            .expect("text selection must exist");
                        self.cursor -= 1;
                        Some(ResultTextSelection::Bound(
                            tsel.as_resultitem(resource.as_ref(), self.store),
                        ))
                    } else {
                        None
                    }
                }
                TextSelectionsSource::HighIter(_) => {
                    unimplemented!("No backward iteration on TextSelectionsSource::HighIter")
                }
                TextSelectionsSource::FindIter(_) => {
                    unimplemented!("No backward iteration on TextSelectionsSource::FindIter")
                }
                TextSelectionsSource::TSIter(iter) => {
                    if let Some(tsel) = iter.next_back() {
                        let resource = iter.resource();
                        self.cursor -= 1; //not really used in this context
                        Some(ResultTextSelection::Bound(
                            tsel.as_resultitem(resource, self.store),
                        ))
                    } else {
                        None
                    }
                }
            };
            if result.is_some() && !self.pass_filter(result.as_ref().unwrap()) {
                continue;
            }
            return result;
        }
    }
}

impl<'store> TextSelectionsIter<'store> {
    pub(crate) fn new(
        data: Vec<ResultTextSelection<'store>>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::HighVec(data),
            store,
            cursor: 0,
            forward: None,
            single_annotation_filter: None,
            annotations_filter: None,
            single_data_filter: None,
            data_filter: None,
        }
    }

    pub(crate) fn new_lowlevel(
        data: SmallVec<[(TextResourceHandle, TextSelectionHandle); 2]>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::LowVec(data),
            store,
            cursor: 0,
            forward: None,
            single_annotation_filter: None,
            annotations_filter: None,
            single_data_filter: None,
            data_filter: None,
        }
    }

    pub fn from_handles(
        data: Vec<(TextResourceHandle, TextSelectionHandle)>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::LowVec(data.into_iter().collect()),
            store,
            cursor: 0,
            forward: None,
            single_annotation_filter: None,
            annotations_filter: None,
            single_data_filter: None,
            data_filter: None,
        }
    }

    pub(crate) fn new_with_findtextiterator(
        iter: FindTextSelectionsIter<'store>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::FindIter(iter),
            store,
            cursor: 0,
            forward: Some(true),
            single_annotation_filter: None,
            annotations_filter: None,
            single_data_filter: None,
            data_filter: None,
        }
    }

    pub(crate) fn new_with_iterator(
        iter: Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::HighIter(iter),
            store,
            cursor: 0,
            forward: Some(true),
            single_annotation_filter: None,
            annotations_filter: None,
            single_data_filter: None,
            data_filter: None,
        }
    }

    pub(crate) fn new_with_resiterator(
        iter: TextSelectionIter<'store>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::TSIter(iter),
            store,
            cursor: 0,
            forward: None,
            single_annotation_filter: None,
            annotations_filter: None,
            single_data_filter: None,
            data_filter: None,
        }
    }

    /// Checks whether the textsections passes the filters (if any)
    fn pass_filter(&self, textselection: &ResultTextSelection<'store>) -> bool {
        if let Some(annotation) = self.single_annotation_filter {
            if !textselection.annotations().filter_handle(annotation).test() {
                return false;
            }
        }
        if let Some(annotations) = &self.annotations_filter {
            if !textselection
                .annotations()
                .filter_annotations(annotations.iter())
                .test()
            {
                return false;
            }
        }
        if let Some((set_handle, data_handle)) = self.single_data_filter {
            if !textselection
                .annotations()
                .filter_annotationdata_handle(set_handle, data_handle)
                .test()
            {
                return false;
            }
        }
        if let Some(data) = &self.data_filter {
            if !textselection.annotations().filter_data(data.clone()).test() {
                //MAYBE TODO: this clone may be a bit too expensive?
                return false;
            }
        }
        true
    }

    pub fn to_handles(self) -> Vec<(TextResourceHandle, TextSelectionHandle)> {
        match self.source {
            TextSelectionsSource::LowVec(v) => v.into_iter().collect(),
            _ => self
                .filter_map(|textselection| {
                    if let Some(handle) = textselection.handle() {
                        Some((textselection.resource().handle(), handle))
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }

    pub fn to_handles_limit(self, limit: usize) -> Vec<(TextResourceHandle, TextSelectionHandle)> {
        match self.source {
            TextSelectionsSource::LowVec(v) => v.into_iter().take(limit).collect(),
            _ => self
                .take(limit)
                .filter_map(|textselection| {
                    if let Some(handle) = textselection.handle() {
                        Some((textselection.resource().handle(), handle))
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }

    /// Produce a parallel iterator, iterator methods like `filter` and `map` *after* this will run in parallel.
    /// It does not parallelize the operation of TextSelectionsIter itself.
    /// This first consumes the sequential iterator into a newly allocated buffer.
    pub fn parallel(self) -> impl ParallelIterator<Item = ResultTextSelection<'store>> + 'store {
        self.collect::<Vec<_>>().into_par_iter()
    }

    /// Iterate over the annotations that make use of text selections in this iterator. No duplicates are returned and results are in chronological order.
    pub fn annotations(self) -> AnnotationsIter<'store> {
        let mut annotations: Vec<AnnotationHandle> = Vec::new();
        let store = self.store;
        for textselection in self {
            if let Some(moreannotations) = store.annotations_by_textselection(
                textselection.resource().handle(),
                textselection.inner(),
            ) {
                annotations.extend(moreannotations);
            }
        }
        annotations.sort_unstable();
        annotations.dedup();
        AnnotationsIter::new(IntersectionIter::new(Cow::Owned(annotations), true), store)
    }

    /// Iterates over all the annotations make use of text selections in this iterator.
    /// Unlike [`self.annotations()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant)
    pub fn annotations_unchecked(self) -> AnnotationsIter<'store> {
        let store = self.store;
        AnnotationsIter::new(
            IntersectionIter::new_with_iterator(
                Box::new(
                    self.filter_map(|textselection| {
                        if let Some(annotations) = store.annotations_by_textselection(
                            textselection.resource().handle(),
                            textselection.inner(),
                        ) {
                            Some(annotations.into_iter().copied())
                        } else {
                            None
                        }
                    })
                    .flatten(),
                ),
                false,
            ),
            store,
        )
    }

    /// Filter by a single annotation. Only text selections will be returned that are a part of the specified annotation.
    pub fn filter_annotation(mut self, annotation: &ResultItem<'store, Annotation>) -> Self {
        self.single_annotation_filter = Some(annotation.handle());
        self
    }

    /// Filter by a single annotation. Only text selections will be returned that are a part of the specified annotation.
    /// This is a lower-level method, use [`Self.filter_annotation`] instead.
    pub fn filter_annotation_handle(mut self, annotation: AnnotationHandle) -> Self {
        self.single_annotation_filter = Some(annotation);
        self
    }

    /// Filter by annotations. Only text selections will be returned that are a part of any of the specified annotations.
    pub fn filter_annotations(mut self, annotations: Annotations<'store>) -> Self {
        self.annotations_filter = Some(annotations);
        self
    }

    /// Constrain the iterator to return only textselections targeted by annotations that have this exact data item
    /// This method can only be used once, to filter by multiple data instances, use [`Self.filter_data()`] instead.
    pub fn filter_annotationdata(mut self, data: &ResultItem<'store, AnnotationData>) -> Self {
        self.single_data_filter = Some((data.set().handle(), data.handle()));
        self
    }

    /// Constrain the iterator to return only textselections targeted by annotations that have this exact data item. This is a lower-level method that takes handles, use [`Self.filter_annotationdata()`] instead.
    /// This method can only be used once, to filter by multiple data instances, use [`Self.filter_data()`] instead.
    pub fn filter_annotationdata_handle(
        mut self,
        set_handle: AnnotationDataSetHandle,
        data_handle: AnnotationDataHandle,
    ) -> Self {
        self.single_data_filter = Some((set_handle, data_handle));
        self
    }

    /// Constrain the iterator to only return textselections targeted by annotations that have data that corresponds with the passed data.
    /// If you have a single AnnotationData instance, use [`Self.filter_annotationdata()`] instead.
    pub fn filter_data(mut self, data: Data<'store>) -> Self {
        self.data_filter = Some(data);
        self
    }

    /// Find all text selections that are related to any text selections in this iterator, the operator
    /// determines the type of the relation.
    pub fn related_text(self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        let mut textselections: Vec<ResultTextSelection<'store>> = Vec::new();
        let store = self.store;
        for textselection in self {
            textselections.extend(textselection.related_text(operator))
        }
        textselections.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        textselections.dedup();
        TextSelectionsIter::new(textselections, store)
    }

    /// Iterates over all text slices in this iterator
    pub fn text(self) -> impl Iterator<Item = &'store str> {
        self.map(|textselection| textselection.text())
    }

    /// Returns true if the iterator has items, false otherwise
    pub fn test(mut self) -> bool {
        self.next().is_some()
    }

    /// Returns all underlying text concatenated into a single String
    pub fn text_join(self, delimiter: &str) -> String {
        let mut s = String::new();
        for textselection in self {
            let text = textselection.text();
            if !s.is_empty() {
                s += delimiter;
            }
            s += text;
        }
        s
    }

    /// If this collection refers to a single simple text slice,
    /// this returns it. If it contains no text or multiple text references, it returns None.
    pub fn text_simple(self) -> Option<&'store str> {
        let mut iter = self.text();
        let text = iter.next();
        if let None = iter.next() {
            return text;
        } else {
            None
        }
    }
}

#[sealed]
impl TypeInfo for Option<ResultTextSelection<'_>> {
    fn typeinfo() -> Type {
        Type::TextSelection
    }
}

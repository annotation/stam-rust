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
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::*;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
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
use crate::{Filter, FilterMode, IntersectionIter, TextMode};
use sealed::sealed;

use rayon::prelude::*;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Debug;

impl<'store> FullHandle<TextSelection> for ResultItem<'store, TextSelection> {
    fn fullhandle(&self) -> <TextSelection as Storable>::FullHandleType {
        (self.resource().handle(), self.handle())
    }
}

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
    pub fn annotations(&self) -> MaybeIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        if let Some(annotations) = self
            .rootstore()
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
        {
            MaybeIter::new(
                FromHandles::new(annotations.iter().copied(), self.rootstore()),
                true,
            )
        } else {
            //dummy iterator that yields nothing
            MaybeIter::new_empty()
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
    /// If you are interested in the annotations associated with the found text selections, append `annotations()`.
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
    /// Use [`Self::as_ref()`] instead if you have bound item.
    pub fn inner(&self) -> &TextSelection {
        match self {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, _, item) => item,
        }
    }

    /// Return a reference to the textselection in the store, with the appropriate lifetime.
    /// Only works on bound items.
    /// Use [`Self::inner()`] instead if you want something that always works.
    pub fn as_ref(&self) -> Option<&'store TextSelection> {
        match self {
            Self::Bound(item) => Some(item.as_ref()),
            Self::Unbound(..) => None,
        }
    }

    /// Return a reference to the textselection in the store.
    /// Only works on bound items.
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
    /// If the TextSelection is unbound, this will return `None` by definition.
    pub fn handle(&self) -> Option<TextSelectionHandle> {
        match self {
            Self::Bound(item) => Some(item.handle()),
            Self::Unbound(..) => None,
        }
    }

    /// Takes ownership of the TextSelection, only works on unbound items, returns an error otherwise.
    pub fn take(self) -> Result<TextSelection, StamError> {
        match self {
            Self::Bound(_) => Err(StamError::AlreadyBound(
                "Item is bound, can't be taken out!",
            )),
            Self::Unbound(_, _, item) => Ok(item),
        }
    }

    /// Iterates over all annotations that are referenced by this TextSelection, if any.
    /// For unbound selections, this returns an empty iterator by definition.
    pub fn annotations(&self) -> MaybeIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        match self {
            Self::Bound(item) => item.annotations(),
            Self::Unbound(..) => MaybeIter::new_empty(),
        }
    }

    /// Returns the number of annotations that reference this text selection
    /// For unbound selections, this is always 0.
    pub fn annotations_len(&self) -> usize {
        match self {
            Self::Bound(item) => item.annotations_len(),
            Self::Unbound(..) => 0,
        }
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// If you are interested in the annotations associated with the found text selections, then append `.annotations()`.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
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
    /// If you are interested in the annotations associated with the found text selections, then use [`Self::find_annotations()`] instead.
    pub fn related_text(
        self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let resource = self.resource().as_ref();
        let rootstore = self.rootstore();
        ResultTextSelections::new(
            resource
                .textselections_by_operator(operator, self.tset)
                .filter_map(|handle| {
                    resource
                        .get(handle)
                        .ok()
                        .map(|x| x.as_resultitem(resource, rootstore))
                }),
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

impl<'store, I> FullHandleToResultItem<'store, TextSelection>
    for FromHandles<'store, TextSelection, I>
where
    I: Iterator<Item = (TextResourceHandle, TextSelectionHandle)>,
{
    fn get_item(
        &self,
        handle: (TextResourceHandle, TextSelectionHandle),
    ) -> Option<ResultItem<'store, TextSelection>> {
        if let Some(resource) = self.store.resource(handle.0) {
            let ts: &TextSelection = resource.as_ref().get(handle.1).unwrap();
            Some(ts.as_resultitem(resource.as_ref(), self.store))
        } else {
            None
        }
    }
}

/*
/// Source for TextSelectionsIter
pub(crate) enum TextSelectionsSource<'store> {
    HighVec(Vec<ResultTextSelection<'store>>),
    SmallHandlesVec(SmallVec<[(TextResourceHandle, TextSelectionHandle); 2]>), //used with AnnotationStore.textselections_by_selector
    Handles(Cow<'store, [(TextResourceHandle, TextSelectionHandle)]>), //used with TextSelections
    FindTextSelectionsIter(FindTextSelectionsIter<'store>), //used with textselections_by_operator()
    TextSelectionIter(TextSelectionIter<'store>), //used by resource.textselections(), double-ended
    DynIterator(Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>),
}

/// Iterator over TextSelections (yields [`ResultTextSelection`] instances)
/// offering high-level API methods.
pub struct TextSelectionsIter<'store> {
    source: TextSelectionsSource<'store>,
    cursor: isize,
    store: &'store AnnotationStore,
    /// Direction of iteration
    forward: Option<bool>,

    filters: SmallVec<[Filter<'store>; 1]>,
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
                TextSelectionsSource::DynIterator(iter) => iter.next(),
                TextSelectionsSource::SmallHandlesVec(handles) => {
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
                TextSelectionsSource::Handles(handles) => {
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
                TextSelectionsSource::FindTextSelectionsIter(iter) => {
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
                TextSelectionsSource::TextSelectionIter(iter) => {
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
            if result.is_some() && !self.test_filters(result.as_ref().unwrap()) {
                continue;
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
                TextSelectionsSource::Handles(handles) => handles.len() as isize - 1,
                TextSelectionsSource::SmallHandlesVec(handles) => handles.len() as isize - 1,
                TextSelectionsSource::DynIterator(_) => {
                    //this is a bit dangerous, no proper error propagation here
                    unimplemented!("No backward iteration on TextSelectionsSource::HighIter")
                }
                TextSelectionsSource::FindTextSelectionsIter(_) => {
                    unimplemented!("No backward iteration on TextSelectionsSource::FindIter")
                }
                TextSelectionsSource::TextSelectionIter(iter) => iter.size_hint().0 as isize,
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
                TextSelectionsSource::Handles(handles) => {
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
                TextSelectionsSource::SmallHandlesVec(handles) => {
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
                TextSelectionsSource::DynIterator(_) => {
                    unimplemented!("No backward iteration on TextSelectionsSource::HighIter")
                }
                TextSelectionsSource::FindTextSelectionsIter(_) => {
                    unimplemented!("No backward iteration on TextSelectionsSource::FindIter")
                }
                TextSelectionsSource::TextSelectionIter(iter) => {
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
            if result.is_some() && !self.test_filters(result.as_ref().unwrap()) {
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
            filters: SmallVec::new(),
        }
    }

    pub(crate) fn new_lowlevel(
        data: SmallVec<[(TextResourceHandle, TextSelectionHandle); 2]>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::SmallHandlesVec(data),
            store,
            cursor: 0,
            forward: None,
            filters: SmallVec::new(),
        }
    }

    pub fn from_handles(data: TextSelections<'store>, store: &'store AnnotationStore) -> Self {
        Self {
            source: TextSelectionsSource::Handles(data.take()),
            store,
            cursor: 0,
            forward: None,
            filters: SmallVec::new(),
        }
    }

    pub(crate) fn new_with_findtextiterator(
        iter: FindTextSelectionsIter<'store>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::FindTextSelectionsIter(iter),
            store,
            cursor: 0,
            forward: Some(true),
            filters: SmallVec::new(),
        }
    }

    pub(crate) fn new_with_iterator(
        iter: Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::DynIterator(iter),
            store,
            cursor: 0,
            forward: Some(true),
            filters: SmallVec::new(),
        }
    }

    pub(crate) fn new_with_resiterator(
        iter: TextSelectionIter<'store>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::TextSelectionIter(iter),
            store,
            cursor: 0,
            forward: None,
            filters: SmallVec::new(),
        }
    }

    /// Checks whether the textselection passes the filters (if any)
    fn test_filters(&self, textselection: &ResultTextSelection<'store>) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        let mut annotationfilter: Option<AnnotationsIter> = None;
        for filter in self.filters.iter() {
            match filter {
                Filter::Annotation(handle) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(textselection.annotations());
                    }
                    annotationfilter = annotationfilter.map(|iter| iter.filter_handle(*handle));
                }
                Filter::Annotations(annotations) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(textselection.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_annotations(annotations.iter()));
                }
                Filter::BorrowedAnnotations(annotations) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(textselection.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_annotations(annotations.iter()));
                }
                Filter::AnnotationData(set, data) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(textselection.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_annotationdata_handle(*set, *data));
                }
                Filter::Data(data, FilterMode::Any) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(textselection.annotations());
                    }
                    annotationfilter = annotationfilter.map(|iter| iter.filter_data_byref(data));
                }
                Filter::BorrowedData(data, FilterMode::Any) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(textselection.annotations());
                    }
                    annotationfilter = annotationfilter.map(|iter| iter.filter_data_byref(data));
                }
                Filter::Data(data, FilterMode::All) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(textselection.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_data_byref_multi(data));
                }
                Filter::BorrowedData(data, FilterMode::All) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(textselection.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_data_byref_multi(data));
                }
                Filter::TextSelectionOperator(operator) => {
                    if !textselection.related_text(*operator).test() {
                        return false;
                    }
                }
                Filter::TextResource(resource) => {
                    if textselection.resource().handle() != *resource {
                        return false;
                    }
                }
                Filter::Text(reftext, textmode, _delimiter) => {
                    let text = textselection.text();
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
                }
                Filter::BorrowedText(reftext, textmode, _delimiter) => {
                    let text = textselection.text();
                    match *textmode {
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
                }
                _ => unimplemented!("Filter {:?} not implemented for TextSelectionsIter", filter),
            }
        }
        if let Some(annotationfilter) = annotationfilter {
            if !annotationfilter.test() {
                return false;
            }
        }
        true
    }

    pub fn to_collection(self) -> TextSelections<'store> {
        let store = self.store;
        match self.source {
            TextSelectionsSource::SmallHandlesVec(v) => TextSelections {
                array: v.into_iter().collect(),
                sorted: false, //sorted by handle? no. we're more typically sorted in textual order (but not always)
                store,
            },
            _ => TextSelections {
                array: self
                    .filter_map(|textselection| {
                        if let Some(handle) = textselection.handle() {
                            Some((textselection.resource().handle(), handle))
                        } else {
                            None
                        }
                    })
                    .collect(),
                sorted: false,
                store,
            },
        }
    }

    pub fn to_collection_limit(self, limit: usize) -> TextSelections<'store> {
        let store = self.store;
        match self.source {
            TextSelectionsSource::SmallHandlesVec(v) => TextSelections {
                array: v.into_iter().take(limit).collect(),
                sorted: false,
                store,
            },
            _ => TextSelections {
                array: self
                    .take(limit)
                    .filter_map(|textselection| {
                        if let Some(handle) = textselection.handle() {
                            Some((textselection.resource().handle(), handle))
                        } else {
                            None
                        }
                    })
                    .collect(),
                sorted: false,
                store,
            },
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
    /// Unlike [`Self::annotations()`], this does no sorting or deduplication whatsoever and the returned iterator is lazy (which makes it more performant)
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

    /// Iterate over the resources used by the text selections in this iterator. No duplicates are returned and results are in chronological order.
    pub fn resources(self) -> ResourcesIter<'store> {
        let mut resources: Vec<TextResourceHandle> = Vec::new();
        let store = self.store;
        for textselection in self {
            resources.push(textselection.resource().handle())
        }
        resources.sort_unstable();
        resources.dedup();
        ResourcesIter::new(IntersectionIter::new(Cow::Owned(resources), true), store)
    }

    /// Filter by a single annotation. Only text selections will be returned that are a part of the specified annotation.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    pub fn filter_annotation(mut self, annotation: &ResultItem<'store, Annotation>) -> Self {
        self.filters.push(Filter::Annotation(annotation.handle()));
        self
    }

    /// Filter by a single annotation. Only text selections will be returned that are a part of the specified annotation.
    /// This is a lower-level method, use [`Self::filter_annotation`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    pub fn filter_annotation_handle(mut self, annotation: AnnotationHandle) -> Self {
        self.filters.push(Filter::Annotation(annotation));
        self
    }

    /// Filter by annotations. Only text selections will be returned that are a part of any of the specified annotations.
    /// If you have a borrowed reference, use [`Self::filter_annotations_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    pub fn filter_annotations(mut self, annotations: Annotations<'store>) -> Self {
        self.filters.push(Filter::Annotations(annotations));
        self
    }

    /// Filter by annotations. Only text selections will be returned that are a part of any of the specified annotations.
    /// If you have owned annotations, use [`Self::filter_annotations()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    pub fn filter_annotations_byref(mut self, annotations: &'store Annotations<'store>) -> Self {
        self.filters.push(Filter::BorrowedAnnotations(annotations));
        self
    }

    /// Constrain the iterator to return only textselections targeted by annotations that have this exact data item
    /// This method can only be used once, to filter by multiple data instances, use [`Self::filter_data()`] or [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    pub fn filter_annotationdata(mut self, data: &ResultItem<'store, AnnotationData>) -> Self {
        self.filters
            .push(Filter::AnnotationData(data.set().handle(), data.handle()));
        self
    }

    /// Constrain the iterator to return only textselections targeted by annotations that have this exact data item. This is a lower-level method that takes handles, use [`Self::filter_annotationdata()`] instead.
    /// This method can only be used once, to filter by multiple data instances, use [`Self::filter_data()`] or [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    pub fn filter_annotationdata_handle(
        mut self,
        set_handle: AnnotationDataSetHandle,
        data_handle: AnnotationDataHandle,
    ) -> Self {
        self.filters
            .push(Filter::AnnotationData(set_handle, data_handle));
        self
    }

    /// Constrain the iterator to only return textselections targeted by annotations that have data that corresponds with the passed data.
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a borrowed reference, use [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    pub fn filter_data(mut self, data: Data<'store>) -> Self {
        self.filters.push(Filter::Data(data, FilterMode::Any));
        self
    }

    /// Constrain the iterator to only return text selections targeted by annotations that have data matching the search parameters.
    /// This is a just shortcut method for `self.filter_data( store.find_data(..).to_collection() )`
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

    /// Constrain the iterator to only return textselections targeted by annotations that have data that corresponds with the passed data.
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have owned data, use [`Self::filter_data()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    pub fn filter_data_byref(mut self, data: &'store Data<'store>) -> Self {
        self.filters
            .push(Filter::BorrowedData(data, FilterMode::Any));
        self
    }

    /// Constrain the iterator to only return textselections targeted by annotations that, in a single annotation, have data that corresponds with *ALL* of the items in the passed data.
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

    /// Constrain the iterator to only return textselections targeted by annotations that, in a single annotation, has data that corresponds with *ALL* of the items in the passed data.
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

    /// Constrain the iterator to only return annotations that have text matching the specified text
    ///
    /// If you have a borrowed reference, use [`Self::filter_text_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    ///
    /// The `delimiter` parameter determines how multiple possible non-contiguous text selections are joined prior to comparison, you most likely want to set it to either a space or an empty string.
    pub fn filter_text(mut self, text: String, case_sensitive: bool) -> Self {
        if case_sensitive {
            self.filters.push(Filter::Text(text, TextMode::Exact, " ")); //delimiter (last parameter) is irrelevant in this context
        } else {
            self.filters
                .push(Filter::Text(text.to_lowercase(), TextMode::Lowercase, " "));
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
    pub fn filter_text_byref(mut self, text: &'store str, case_sensitive: bool) -> Self {
        self.filters.push(Filter::BorrowedText(
            text,
            if case_sensitive {
                TextMode::Lowercase
            } else {
                TextMode::Exact
            },
            " ", //delimiter is irrelevant in this context
        ));
        self
    }

    /// Constrain this iterator to only return text selections that reference a particular resource
    pub fn filter_resource(self, resource: &ResultItem<TextResource>) -> Self {
        self.filter_resource_handle(resource.handle())
    }

    /// Constrain this iterator to only return text selections that reference a particular resource
    pub fn filter_resource_handle(mut self, handle: TextResourceHandle) -> Self {
        self.filters.push(Filter::TextResource(handle));
        self
    }

    /// Constrain this iterator to only return text selections that are also in the other collection
    pub fn filter_textselections(self, textselections: TextSelectionsIter) -> Self {
        let mut textselections: Vec<_> = textselections.to_collection().take().to_vec();
        textselections.sort_unstable();
        let store = self.store;
        TextSelectionsIter::new_with_iterator(
            Box::new(self.filter(move |tsel| {
                let handle = (tsel.resource().handle(), tsel.handle().unwrap());
                match textselections.binary_search(&handle) {
                    Ok(_) => true,
                    Err(_) => false,
                }
            })),
            store,
        )
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

impl TestableIterator for TextSelectionsIter<'_> {}
*/

#[sealed]
impl TypeInfo for Option<ResultTextSelection<'_>> {
    fn typeinfo() -> Type {
        Type::TextSelection
    }
}

/// Holds a collection of text selections pertaining to resources.
/// This structure is produced by calling [`ResourcesIter::to_collection()`].
/// Use [`Resources::iter()`] to iterate over the collection.
pub type TextSelections<'store> = Handles<'store, TextSelection>;

/// Iterator that turns iterators over [`ResultItem<TextSelection>`] into [`ResultTextSelection`].
pub struct ResultTextSelections<'store, I>
where
    I: Iterator<Item = ResultItem<'store, TextSelection>>,
{
    inner: I,
}

impl<'store, I> ResultTextSelections<'store, I>
where
    I: Iterator<Item = ResultItem<'store, TextSelection>>,
{
    pub fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<'store, I> Iterator for ResultTextSelections<'store, I>
where
    I: Iterator<Item = ResultItem<'store, TextSelection>>,
{
    type Item = ResultTextSelection<'store>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|textselection| ResultTextSelection::Bound(textselection))
    }
}

impl<'store, I> DoubleEndedIterator for ResultTextSelections<'store, I>
where
    I: DoubleEndedIterator<Item = ResultItem<'store, TextSelection>>,
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|textselection| ResultTextSelection::Bound(textselection))
    }
}

pub trait TextSelectionIterator<'store>: Iterator<Item = ResultTextSelection<'store>>
where
    Self: Sized,
{
    /// Iterates over all the annotations for all text selections in this iterator.
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    ///
    /// If you want annotations unsorted and with possible duplicates, then just do:  `.map(|ts| ts.annotations()).flatten()` instead
    fn annotations(
        self,
    ) -> MaybeIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|resource| resource.annotations())
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        MaybeIter::new_sorted(annotations.into_iter())
    }

    /// Iterates over all text slices in this iterator
    fn text(self) -> TextIter<'store, Self> {
        TextIter { inner: self }
    }

    /// Returns all underlying text concatenated into a single String
    fn text_join(self, delimiter: &str) -> String {
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
    fn text_simple(self) -> Option<&'store str> {
        let mut iter = self.text();
        let text = iter.next();
        if let None = iter.next() {
            return text;
        } else {
            None
        }
    }
}

impl<'store, I> TextSelectionIterator<'store> for I
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    //blanket implementation
}

struct TextIter<'store, I>
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    inner: I,
}

impl<'store, I> Iterator for TextIter<'store, I>
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    type Item = &'store str;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|ts| ts.text())
    }
}

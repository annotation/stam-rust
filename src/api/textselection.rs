/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`TextSelection`]. This API is implemented on
//! [`ResultTextSelection`], which encapsulates a [`TextSelection`] instance.

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::*;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::error::*;
use crate::resources::{PositionMode, TextResource, TextResourceHandle};
use crate::selector::{Offset, OffsetMode};
use crate::store::*;
use crate::text::Text;
use crate::textselection::{
    ResultTextSelection, ResultTextSelectionSet, TextSelection, TextSelectionHandle,
    TextSelectionOperator, TextSelectionSet,
};
use crate::types::*;
use crate::{Filter, FilterMode, TextMode};
use sealed::sealed;

use rayon::prelude::*;
use std::cmp::Ordering;

impl<'store> FullHandle<TextSelection> for ResultItem<'store, TextSelection> {
    fn fullhandle(&self) -> <TextSelection as Storable>::FullHandleType {
        (self.resource().handle(), self.handle())
    }
}

/// This is the implementation of the high-level API for [`TextSelection`], though most of it is more commonly used via [`ResultTextSelection`].
impl<'store> ResultItem<'store, TextSelection> {
    /// Returns the higher level structure
    pub fn as_resulttextselection(self) -> ResultTextSelection<'store> {
        self.into()
    }

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
    pub fn annotations(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        if let Some(annotations) = self
            .rootstore()
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
        {
            ResultIter::new(
                FromHandles::new(annotations.iter().copied(), self.rootstore()),
                true,
            )
        } else {
            //dummy iterator that yields nothing
            ResultIter::new_empty()
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
    pub fn annotations(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        match self {
            Self::Bound(item) => item.annotations(),
            Self::Unbound(..) => ResultIter::new_empty(),
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

    /// Returns a sorted iterator over all absolute positions (begin aligned cursors) that overlap with this textselection
    /// and are in use in the underlying resource.
    /// By passing a [`PositionMode`] parameter you can specify whether you want only positions where a textselection begins, ends or both.
    /// This is a low-level function. Consider using `.related_text(TextSelectionOperator::overlaps())` instead.
    pub fn positions<'a>(&'a self, mode: PositionMode) -> Box<dyn Iterator<Item = &'a usize> + 'a> {
        self.resource()
            .as_ref()
            .positions_in_range(mode, self.begin(), self.end())
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

#[sealed]
impl TypeInfo for Option<ResultTextSelection<'_>> {
    fn typeinfo() -> Type {
        Type::TextSelection
    }
}

/// Holds a collection of [`TextSelection`] (by reference to an [`AnnotationStore`] and handles). This structure is produced by calling
/// [`ToHandles::to_handles()`], which is available on all iterators over texts selections.
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

/// Trait for iteration over text selections ([`ResultTextSelection`]; encapsulation over
/// [`TextSelection`]). Implements numerous filter methods to further constrain the iterator, as well
/// as methods to map from text selections to other items.
pub trait TextSelectionIterator<'store>: Iterator<Item = ResultTextSelection<'store>>
where
    Self: Sized,
{
    fn parallel(self) -> rayon::vec::IntoIter<ResultTextSelection<'store>> {
        let annotations: Vec<_> = self.collect();
        annotations.into_par_iter()
    }

    /// Iterates over all the annotations for all text selections in this iterator.
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    ///
    /// If you want annotations unsorted and with possible duplicates, then just do:  `.map(|ts| ts.annotations()).flatten()` instead
    fn annotations(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|resource| resource.annotations())
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultIter::new_sorted(annotations.into_iter())
    }

    /// Find all text selections that are related to any text selections in this iterator, the operator
    /// determines the type of the relation.
    fn related_text(
        self,
        operator: TextSelectionOperator,
    ) -> <Vec<ResultTextSelection<'store>> as IntoIterator>::IntoIter {
        let mut textselections: Vec<ResultTextSelection<'store>> = Vec::new();
        for textselection in self {
            textselections.extend(textselection.related_text(operator))
        }
        textselections.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        textselections.dedup();
        textselections.into_iter()
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

    /// Constrain the iterator to only return text selections with annotations that match the ones passed
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection
    fn filter_annotations(
        self,
        annotations: Annotations<'store>,
        mode: FilterMode,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::Annotations(
                annotations,
                mode,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ),
        }
    }

    /// Constrain the iterator to only return text selections with annotations that match the ones passed
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection
    fn filter_annotations_byref(
        self,
        annotations: &'store Annotations<'store>,
        mode: FilterMode,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::BorrowedAnnotations(
                annotations,
                mode,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ),
        }
    }

    /// Constrain this iterator to text selections with this specific annotation
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    fn filter_annotation(
        self,
        annotation: &ResultItem<Annotation>,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::Annotation(
                annotation.handle(),
                SelectionQualifier::Normal,
                AnnotationDepth::One,
            ),
        }
    }

    /// Constrain the iterator to only return text selections that have text matching the specified text
    ///
    /// If you have a borrowed reference, use [`Self::filter_text_byref()`] instead.
    /// If you want to filter based on a regular expression, use [`Self::filter_text_regex()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    fn filter_text(
        self,
        text: String,
        case_sensitive: bool,
    ) -> FilteredTextSelections<'store, Self> {
        if case_sensitive {
            FilteredTextSelections {
                inner: self,
                filter: Filter::Text(text, TextMode::Exact, ""), //delimiter is irrelevant in this context
            }
        } else {
            FilteredTextSelections {
                inner: self,
                filter: Filter::Text(text.to_lowercase(), TextMode::CaseInsensitive, ""),
            }
        }
    }

    /// Constrain the iterator to only return text selections that have text matching the specified text
    ///
    /// If you set `case_sensitive` to false, then `text` *MUST* be a lower-cased &str!
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    fn filter_text_byref(
        self,
        text: &'store str,
        case_sensitive: bool,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::BorrowedText(
                text,
                if case_sensitive {
                    TextMode::Exact
                } else {
                    TextMode::CaseInsensitive
                },
                "",
            ),
        }
    }

    /// Constrain the iterator to only return text selections that have text matching the specified regular expression.
    ///
    /// This filter is evaluated lazily, it will obtain and check the text for each annotation.
    fn filter_text_regex(self, regex: Regex) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::Regex(regex, ""),
        }
    }

    /// Constrain the iterator to only return text selections with annotations that have data that corresponds with any of the items in the passed data.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    fn filter_data(
        self,
        data: Data<'store>,
        mode: FilterMode,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::Data(data, mode, SelectionQualifier::Normal),
        }
    }

    /// Constrain the iterator to only return text selections with annotations that have data that corresponds with any of the items in the passed data.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    fn filter_data_byref(
        self,
        data: &'store Data<'store>,
        mode: FilterMode,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::BorrowedData(data, mode, SelectionQualifier::Normal),
        }
    }

    fn filter_data_all_byref(
        self,
        data: &'store Data<'store>,
        mode: FilterMode,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::BorrowedData(data, mode, SelectionQualifier::Normal),
        }
    }

    /// Constrain the iterator to return only the text selections that have this exact data item
    /// To filter by multiple data instances (union/disjunction), use [`Self::filter_data()`] or (intersection/conjunction) [`Self::filter_data_all_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    fn filter_annotationdata(
        self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
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
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::DataKeyAndOperator(
                key.set().handle(),
                key.handle(),
                value,
                SelectionQualifier::Normal,
            ),
        }
    }

    fn filter_key(self, key: &ResultItem<'store, DataKey>) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_key_handle(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::DataKey(set, key, SelectionQualifier::Normal),
        }
    }

    fn filter_value(self, value: DataOperator<'store>) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::DataOperator(value, SelectionQualifier::Normal),
        }
    }

    fn filter_key_handle_value(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
        value: DataOperator<'store>,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::DataKeyAndOperator(set, key, value, SelectionQualifier::Normal),
        }
    }

    fn filter_set(
        self,
        set: &ResultItem<'store, AnnotationDataSet>,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_set_handle(
        self,
        set: AnnotationDataSetHandle,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::AnnotationDataSet(set, SelectionQualifier::Normal),
        }
    }

    fn filter_resource(
        self,
        resource: &ResultItem<'store, TextResource>,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::TextResource(resource.handle(), SelectionQualifier::Normal),
        }
    }

    fn filter_one(
        self,
        textselection: &ResultItem<'store, TextSelection>, //no ResultTextSelection because we can only work with bound items anyway
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::TextSelection(
                textselection.resource().handle(),
                textselection.handle(),
            ),
        }
    }

    fn filter_any(
        self,
        textselections: Handles<'store, TextSelection>,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::TextSelections(textselections, FilterMode::Any),
        }
    }

    fn filter_handle(
        self,
        resource: TextResourceHandle,
        textselection: TextSelectionHandle,
    ) -> FilteredTextSelections<'store, Self> {
        FilteredTextSelections {
            inner: self,
            filter: Filter::TextSelection(resource, textselection),
        }
    }

    fn to_handles(&mut self, store: &'store AnnotationStore) -> Handles<'store, TextSelection> {
        Handles::from_iter(
            self.filter_map(|item| {
                if let Some(handle) = item.handle() {
                    Some((item.resource().handle(), handle))
                } else {
                    None
                }
            }),
            store,
        )
    }
}

impl<'store, I> TextSelectionIterator<'store> for I
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    //blanket implementation
}

pub struct FilteredTextSelections<'store, I>
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    inner: I,
    filter: Filter<'store>,
}

impl<'store, I> Iterator for FilteredTextSelections<'store, I>
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    type Item = ResultTextSelection<'store>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(textselection) = self.inner.next() {
                if self.test_filter(&textselection) {
                    return Some(textselection);
                }
            } else {
                return None;
            }
        }
    }
}

impl<'store, I> FilteredTextSelections<'store, I>
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    fn test_filter(&self, textselection: &ResultTextSelection<'store>) -> bool {
        match &self.filter {
            Filter::TextSelection(res_handle, ts_handle) => {
                textselection.resource().handle() == *res_handle
                    && textselection.handle() == Some(*ts_handle)
            }
            Filter::TextSelections(handles, FilterMode::Any) => {
                if let Some(textselection) = textselection.as_resultitem() {
                    handles.contains(&textselection.fullhandle())
                } else {
                    false
                }
            }
            Filter::TextSelections(_handles, FilterMode::All) => {
                //TODO: implement (not here but in dedicated copy of FilterAllIter)
                unreachable!("not implemented here")
            }
            Filter::Resources(handles, FilterMode::Any, _) => {
                handles.contains(&textselection.resource().handle())
            }
            Filter::Annotations(annotations, mode, _, AnnotationDepth::One) => textselection
                .annotations()
                .filter_annotations_byref(annotations, *mode)
                .test(),
            Filter::BorrowedAnnotations(annotations, mode, _, AnnotationDepth::One) => {
                textselection
                    .annotations()
                    .filter_annotations_byref(annotations, *mode)
                    .test()
            }
            Filter::Annotation(annotation, SelectionQualifier::Normal, AnnotationDepth::One) => {
                textselection
                    .annotations()
                    .filter_handle(*annotation)
                    .test()
            }
            Filter::TextResource(res_handle, _) => textselection.resource().handle() == *res_handle,
            Filter::Text(reftext, textmode, _) => {
                let text = textselection.text();
                if *textmode == TextMode::CaseInsensitive {
                    text.to_lowercase().as_str() == reftext
                } else {
                    text == reftext.as_str()
                }
            }
            Filter::BorrowedText(reftext, textmode, _) => {
                let text = textselection.text();
                if *textmode == TextMode::CaseInsensitive {
                    text.to_lowercase().as_str() == *reftext
                } else {
                    text == *reftext
                }
            }
            Filter::Regex(regex, _) => regex.is_match(textselection.text()),
            Filter::TextSelectionOperator(operator, _) => {
                textselection.related_text(*operator).test()
            }
            Filter::Data(data, mode, _) => textselection
                .annotations()
                .filter_data_byref(data, *mode)
                .test(),
            Filter::BorrowedData(data, mode, _) => textselection
                .annotations()
                .filter_data_byref(data, *mode)
                .test(),
            Filter::DataKey(set, key, _) => textselection
                .annotations()
                .data()
                .filter_key_handle(*set, *key)
                .test(),
            Filter::DataKeyAndOperator(set, key, value, _) => textselection
                .annotations()
                .data()
                .filter_key_handle_value(*set, *key, value.clone())
                .test(),
            Filter::DataOperator(value, _) => textselection
                .annotations()
                .data()
                .filter_value(value.clone())
                .test(),
            Filter::AnnotationDataSet(set, _) => textselection
                .annotations()
                .data()
                .filter_set_handle(*set)
                .test(),
            Filter::AnnotationData(set, data, _) => textselection
                .annotations()
                .data()
                .filter_handle(*set, *data)
                .test(),
            _ => unreachable!(
                "Filter {:?} not implemented for FilteredTextSelections",
                self.filter
            ),
        }
    }
}

pub struct TextIter<'store, I>
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

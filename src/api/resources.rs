use regex::{Regex, RegexSet};

use crate::annotation::Annotation;
use crate::error::*;
use crate::resources::TextResource;
use crate::selector::Offset;
use crate::store::*;
use crate::text::{FindNoCaseTextIter, FindRegexIter, FindTextIter, SplitTextIter, Text};
use crate::textselection::{
    ResultTextSelection, TextSelection, TextSelectionOperator, TextSelectionSet,
};

impl<'store> ResultItem<'store, TextResource> {
    /// Returns an iterator over all annotations about this resource as a whole, i.e. Annotations with a ResourceSelector.
    /// Such annotations can be considered metadata.
    pub fn annotations_as_metadata(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_resource_metadata(self.handle())
            .into_iter()
            .map(|v| v.iter())
            .flatten()
            .filter_map(|a_handle| store.annotation(*a_handle))
    }

    /// Returns an iterator over all annotations about any text in this resource i.e. Annotations with a TextSelector.
    pub fn annotations_on_text(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_resource(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| store.annotation(a_handle))
    }

    /// Returns an iterator over all annotations that reference this resource, both annotations that can be considered metadata as well
    /// annotations that reference a portion of the text. The former are always returned before the latter.
    /// Use `annotations_as_metadata()` or `annotations_on_text()` instead if you want to differentiate the two.
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.annotations_as_metadata()
            .chain(self.annotations_on_text())
    }

    /// Returns an iterator over all text selections that are marked in this resource (i.e. there are one or more annotations on it).
    /// They are returned in textual order, but this does not come with any significant performance overhead. If you want an unsorted version use [`self.as_ref().textselections_unsorted()`] instead.
    /// This is a double-ended iterator that can be traversed in both directions.
    pub fn textselections(&self) -> impl DoubleEndedIterator<Item = ResultTextSelection<'store>> {
        let resource = self.as_ref();
        resource
            .iter()
            .map(|item| item.as_resultitem(resource).into())
    }

    /// Returns a sorted double-ended iterator over a range of all textselections and returns all
    /// textselections that either start or end in this range (depending on the direction you're
    /// iterating in)
    pub fn textselections_in_range(
        &self,
        begin: usize,
        end: usize,
    ) -> impl DoubleEndedIterator<Item = ResultTextSelection<'store>> {
        let resource = self.as_ref();
        resource
            .range(begin, end)
            .map(|item| item.as_resultitem(resource).into())
    }

    /// Returns the number of textselections that are marked in this resource (i.e. there are one or more annotations on it).
    pub fn textselections_len(&self) -> usize {
        self.as_ref().textselections_len()
    }

    /// Find textselections by applying a text selection operator ([`TextSelectionOperator`]) to a
    /// one or more querying textselections. Returns an iterator over all matching
    /// text selections in the resource.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
        refset: impl Into<TextSelectionSet>,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let resource = self.as_ref();
        resource
            .textselections_by_operator(operator, refset.into())
            .map(|ts_handle| {
                let textselection: &'store TextSelection = resource
                    .get(ts_handle)
                    .expect("textselection handle must be valid");
                textselection.as_resultitem(resource).into()
            })
    }

    /*
    /// Find textselections by applying a text selection operator ([`TextSelectionOperator`]) to a
    /// one or more querying textselections (in an [`TextSelectionSet']). Returns an iterator over all matching
    /// text selections in the resource, as [`ResultItem<TextSelection>`].
    pub fn find_textselections_ref<'q>(
        &self,
        operator: TextSelectionOperator,
        refset: &'q TextSelectionSet,
    ) -> impl Iterator<Item = ResultItem<'store, TextSelection>> + 'q
    where
        'store: 'q, //store lives at least as long as 'q
    {
        self.as_ref()
            .textselections_by_operator_ref(operator, reset)
            .map(|ts_handle| {
                let textselection: &'store TextSelection = self
                    .as_ref()
                    .get(ts_handle)
                    .expect("textselection handle must be valid");
                textselection.as_resultitem(self.as_ref())
            })
    }
    */
}

/// this implementation mostly defers directly to the wrapped item, documentation is found on the trait and not repeated here
impl<'store> Text<'store, 'store> for ResultItem<'store, TextResource> {
    fn textlen(&self) -> usize {
        self.as_ref().textlen()
    }

    fn text(&'store self) -> &'store str {
        self.as_ref().text()
    }

    fn text_by_offset(&'store self, offset: &Offset) -> Result<&'store str, StamError> {
        self.as_ref().text_by_offset(offset)
    }

    fn absolute_cursor(&self, cursor: usize) -> usize {
        cursor
    }

    fn utf8byte(&self, abscursor: usize) -> Result<usize, StamError> {
        self.as_ref().utf8byte(abscursor)
    }

    fn utf8byte_to_charpos(&self, bytecursor: usize) -> Result<usize, StamError> {
        self.as_ref().utf8byte_to_charpos(bytecursor)
    }

    fn textselection(
        &'store self,
        offset: &Offset,
    ) -> Result<ResultTextSelection<'store>, StamError> {
        self.as_ref().textselection(offset)
    }

    fn find_text_regex<'regex>(
        &'store self,
        expressions: &'regex [Regex],
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<FindRegexIter<'store, 'regex>, StamError> {
        self.as_ref()
            .find_text_regex(expressions, precompiledset, allow_overlap)
    }

    fn find_text<'fragment>(
        &'store self,
        fragment: &'fragment str,
    ) -> FindTextIter<'store, 'fragment> {
        self.as_ref().find_text(fragment)
    }

    fn find_text_nocase(&'store self, fragment: &str) -> FindNoCaseTextIter<'store> {
        self.as_ref().find_text_nocase(fragment)
    }

    fn split_text<'b>(&'store self, delimiter: &'b str) -> SplitTextIter<'store, 'b> {
        self.as_ref().split_text(delimiter)
    }

    fn subslice_utf8_offset(&self, subslice: &str) -> Option<usize> {
        self.as_ref().subslice_utf8_offset(subslice)
    }
}

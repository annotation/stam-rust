use regex::{Regex, RegexSet};
use std::marker::PhantomData;

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::error::*;
use crate::resources::TextResource;
use crate::selector::Offset;
use crate::store::*;
use crate::text::{
    FindNoCaseTextIter, FindRegexIter, FindRegexMatch, FindTextIter, SplitTextIter, Text,
};
use crate::textselection::{ResultTextSelection, TextSelection};
use crate::types::*;

impl AnnotationStore {
    /// Requests a specific [`TextResource`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a &str/String or handle.
    ///
    /// The item is returned as a fat pointer [`ResultItem<TextResource>']) in an Option.
    /// Returns `None` if it does not exist.
    pub fn resource(
        &self,
        request: impl Request<TextResource>,
    ) -> Option<ResultItem<TextResource>> {
        self.get(request).map(|x| x.as_resultitem(self)).ok()
    }

    /// Requests a specific [`AnnotationDataSet`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a &str,String or handle.
    ///
    /// The item is returned as a fat pointer [`ResultItem<AnnotationDataSet>']) in an Option.
    /// Returns `None` if it does not exist.
    pub fn dataset(
        &self,
        request: impl Request<AnnotationDataSet>,
    ) -> Option<ResultItem<AnnotationDataSet>> {
        self.get(request).map(|x| x.as_resultitem(self)).ok()
    }

    /// Requests a specific [`Annotation`] from the store to be returned by reference.
    /// The `request` parameter encapsulates some kind of identifier, it can be a &str,String or handle.
    ///
    /// The item is returned as a fat pointer [`ResultItem<Annotation>']) in an Option.
    /// Returns `None` if it does not exist.
    pub fn annotation(&self, request: impl Request<Annotation>) -> Option<ResultItem<Annotation>> {
        self.get(request).map(|x| x.as_resultitem(self)).ok()
    }

    /// Returns an iterator over all text resources ([`TextResource`] instances) in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>']) .
    pub fn resources(&self) -> impl Iterator<Item = ResultItem<TextResource>> {
        self.iter()
            .map(|item: &TextResource| item.as_resultitem(self))
    }

    /// Returns an iterator over all [`AnnotationDataSet`] instances in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>']) .
    pub fn datasets<'a>(&'a self) -> impl Iterator<Item = ResultItem<AnnotationDataSet>> {
        self.iter()
            .map(|item: &AnnotationDataSet| item.as_resultitem(self))
    }

    /// Returns an iterator over all annotations ([`Annotation`] instances) in the store.
    /// Items are returned as a fat pointer [`ResultItem<AnnotationDataSet>']) .
    pub fn annotations<'a>(&'a self) -> impl Iterator<Item = ResultItem<Annotation>> {
        self.iter()
            .map(|item: &Annotation| item.as_resultitem(self))
    }

    /// Searches for text in all resources using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// See [`TextResource.find_text_regex()`].
    /// Note that this method, unlike its counterpart [`TextResource.find_text_regex()`], silently ignores any deeper errors that might occur.
    pub fn find_text_regex<'store, 'r>(
        &'store self,
        expressions: &'r [Regex],
        precompiledset: &'r Option<RegexSet>,
        allow_overlap: bool,
    ) -> impl Iterator<Item = FindRegexMatch<'store, 'r>> {
        self.resources()
            .filter_map(move |resource: ResultItem<'store, TextResource>| {
                //      ^-- the move is only needed to move the bool in, otherwise we had to make it &'r bool and that'd be weird
                resource
                    .as_ref()
                    .find_text_regex(expressions, precompiledset.as_ref(), allow_overlap)
                    .ok() //ignore errors!
            })
            .flatten()
    }

    /// Finds the [`AnnotationData'] in a specific set. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use `test_data` instead.
    ///
    /// Provide `key` as an Option, if set to `None`, all keys in the specified set will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    pub fn find_data<'store>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: Option<impl Request<DataKey>>,
        value: DataOperator<'store>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>>> {
        self.dataset(set)
            .map(|set| set.find_data(key, value))
            .flatten()
    }

    /// Returns an iterator over all data in all sets
    pub fn data(&self) -> impl Iterator<Item = ResultItem<AnnotationData>> {
        self.datasets()
            .map(|set| {
                let set = set.as_ref();
                set.data().map(|item| item.as_resultitem(set))
            })
            .flatten()
    }

    /// Tests if for annotation data in a specific set, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `key` as Option, if set to `None`, all keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    pub fn test_data<'store>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: Option<impl Request<DataKey>>,
        value: DataOperator<'store>,
    ) -> bool {
        match self.find_data(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }
}

impl<'store> ResultItem<'store, AnnotationDataSet> {
    /// Returns an iterator over all data in this set
    pub fn data(&self) -> impl Iterator<Item = ResultItem<AnnotationData>> {
        self.as_ref()
            .data()
            .map(|item| item.as_resultitem(self.as_ref()))
    }

    /// Returns an iterator over all keys in this set
    pub fn keys(&self) -> impl Iterator<Item = ResultItem<DataKey>> {
        self.as_ref()
            .keys()
            .map(|item| item.as_resultitem(self.as_ref()))
    }

    /// Returns an iterator over annotations that directly point at the resource, i.e. are metadata for it.
    /// If you want to iterator over all annotations that reference data from this set, use [`annotations()`] instead.
    pub fn annotations_about(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_dataset_metadata(self.handle())
            .into_iter()
            .map(|v| v.iter())
            .flatten()
            .filter_map(|a_handle| store.annotation(*a_handle))
    }

    /// Returns a single [`AnnotationData'] in the annotation dataset that matches they key and value.
    /// Returns a single match, use `Self::find_data()` for a more extensive search.
    pub fn data_by_value(
        &self,
        key: impl Request<DataKey>,
        value: &DataValue,
    ) -> Option<ResultItem<'store, AnnotationData>> {
        self.as_ref()
            .data_by_value(key, value)
            .map(|annotationdata| annotationdata.as_resultitem(self.as_ref()))
    }

    /// Finds the [`AnnotationData'] in the annotation dataset. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use `test_data` instead.
    ///
    /// Provide `key`  as an Options, if set to `None`, all keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    pub fn find_data<'a>(
        &self,
        key: Option<impl Request<DataKey>>,
        value: DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'a: 'store,
    {
        let mut key_handle: Option<DataKeyHandle> = None; //this means 'any' in this context
        if let Some(key) = key {
            key_handle = key.to_handle(self.as_ref());
            if key_handle.is_none() {
                //requested key doesn't exist, bail out early, we won't find anything at all
                return None;
            }
        };
        let store = self.as_ref();
        Some(store.data().filter_map(move |annotationdata| {
            if (key_handle.is_none() || key_handle.unwrap() == annotationdata.key())
                && annotationdata.value().test(&value)
            {
                Some(annotationdata.as_resultitem(store))
            } else {
                None
            }
        }))
    }

    /// Tests if the dataset has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored.
    pub fn test_data<'a>(
        &self,
        key: Option<impl Request<DataKey>>,
        value: DataOperator<'a>,
    ) -> bool {
        match self.find_data(key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Returns all annotations that use this dataset.
    /// The returned annotations are not part of the dataset, they merely reference data from it.
    /// Use [`Self.annotations_about()`] instead if you are looking for annotations that reference
    /// the dataset as a whole via a DataSetSelector.
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_dataset(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| store.annotation(a_handle))
    }
}

impl<'store> ResultItem<'store, TextResource> {
    /// Returns an iterator over all annotations about this resource as a whole, i.e. Annotations with a ResourceSelector.
    /// Such annotations can be considered metadata.
    pub fn annotations_about_metadata(
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
    pub fn annotations_about_text(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        store
            .annotations_by_resource(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|a_handle| store.annotation(a_handle))
    }

    /// Returns an iterator over all annotations about this resource, both annotations that can be considered metadata as well
    /// annotations that reference a portion of the text. The former are always returned before the latter.
    /// Use `annotations_about_metadata()` or `annotations_about_text()` instead if you want to differentiate the two.
    pub fn annotations_about(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.annotations_about_metadata()
            .chain(self.annotations_about_text())
    }

    /// Returns an iterator over all text selections that are marked in this resource (i.e. there are one or more annotations on it).
    /// They are returned in textual order, but this does not come with any significant performance overhead. If you want an unsorted version use [`self.as_ref().textselections_unsorted()`] instead.
    /// This is a double-ended iterator that can be traversed in both directions.
    pub fn textselections(
        &self,
    ) -> impl DoubleEndedIterator<Item = ResultItem<'store, TextSelection>> {
        let resource = self.as_ref();
        resource.iter().map(|item| item.as_resultitem(resource))
    }

    /// Returns a sorted double-ended iterator over a range of all textselections and returns all
    /// textselections that either start or end in this range (depending on the direction you're
    /// iterating in)
    pub fn textselections_in_range(
        &self,
        begin: usize,
        end: usize,
    ) -> impl DoubleEndedIterator<Item = ResultItem<'store, TextSelection>> {
        let resource = self.as_ref();
        resource
            .range(begin, end)
            .map(|item| item.as_resultitem(resource))
    }

    /// Returns the number of textselections that are marked in this resource (i.e. there are one or more annotations on it).
    pub fn textselections_len(&self) -> usize {
        self.as_ref().textselections_len()
    }
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

/*
pub struct AnnotationsIter<T, I> {
    iter: I,
    _phantomdata: PhantomData<T>,
}

impl<'store, I> Iterator for AnnotationsIter<TextResource, I>
where
    I: Iterator<Item = ResultItem<'store, TextResource>>,
{
    type Item = ResultItem<'store, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {
    }
}

impl<'store, I> Iterator for AnnotationsIter<Annotation, I>
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    type Item = ResultItem<'store, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {}
}
*/

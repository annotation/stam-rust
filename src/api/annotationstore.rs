use regex::{Regex, RegexSet};

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::TextResource;
use crate::store::*;
use crate::text::{FindRegexMatch, Text};
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

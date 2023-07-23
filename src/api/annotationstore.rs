use regex::{Regex, RegexSet};

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::datakey::{DataKey, DataKeyHandle};
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

    /// internal helper method
    pub(crate) fn find_data_request_resolver<'store>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
    ) -> Option<(Option<AnnotationDataSetHandle>, Option<DataKeyHandle>)> {
        let mut test_set_handle: Option<AnnotationDataSetHandle> = None; //None means 'any' in this context
        let mut test_key_handle: Option<DataKeyHandle> = None; //idem

        if !set.any() {
            if let Ok(set) = self.get(set) {
                test_set_handle = Some(set.handle().expect("set must have handle"));
                if !key.any() {
                    test_key_handle = key.to_handle(set);
                    if test_key_handle.is_none() {
                        //requested key doesn't exist, bail out early, we won't find anything at all
                        return None;
                    }
                }
            } else {
                //requested set doesn't exist, bail out early, we won't find anything at all
                return None;
            }
        } else if !key.any() {
            // Not the most elegant solution but it'll have to do, I don't want to wrap this in Result<>, and I don't
            // want to be entirely silent about this error either:
            eprintln!("STAM warning: Providing a key without a set in data searches is invalid! Key will be ignored!");
        }

        Some((test_set_handle, test_key_handle))
    }

    /// Finds [`AnnotationData'] using data search criteria.
    /// This returns an iterator over all matches.
    ///
    /// If you are not interested in returning the results but merely testing the presence of particular data,
    /// then use `test_data` instead..
    ///
    /// You can pass a boolean (true/false, doesn't matter) or empty string literal for set or key to represent any set/key.
    /// To search for any value, `value` can must be explicitly set to `DataOperator::Any` to return all values.
    ///
    /// Value is a DataOperator that can apply a data test to the value. Use `DataOperator::Equals` to search
    /// for an exact value. As a shortcut, you can pass `"value".into()`  to the automatically conver into an equality
    /// DataOperator.
    ///
    /// Example call to retrieve all data indiscriminately: `annotation.data(false,false, DataOperator::Any)`
    ///  .. or just use the alias function `data_all()`.
    ///
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored!! You can not
    ///       search for keys if you don't know their set.
    /// Note: If you already know the set and you have lots of sets in your data, then it may be
    ///       slightly more performant to call [`AnnotationDataSet.find_data()`] directly.
    pub fn find_data<'store, 'a>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>>>
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) = self.find_data_request_resolver(set, key)
        {
            Some(
                self.datasets() //we do have to go over all and test because otherwise we have distinct return types
                    .filter_map(move |dataset| {
                        if test_set_handle.is_none() || dataset.handle() == test_set_handle.unwrap()
                        {
                            Some(
                                dataset
                                    .find_data(test_key_handle, value)
                                    .into_iter()
                                    .flatten(),
                            )
                        } else {
                            None
                        }
                    })
                    .into_iter()
                    .flatten(),
            )
        } else {
            None
        }
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

    /// Tests if certain annotation data exists, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `key` as Option, if set to `None`, all keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    ///
    /// Note: This gives no guarantee that data, although it exists, is actually used by annotations
    pub fn test_data<'store, 'a>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> bool
    where
        'a: 'store,
    {
        match self.find_data(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Searches for annotations by data.
    /// Returns an iterator returning both the annotation, as well the matching data item
    ///
    /// This may return the same annotation multiple times if different matching data references it!
    /// This iterator does not guarantee any ordering of the returned annotations.
    ///
    /// If you already have a `ResultItem<AnnotationData>` instance, just use `ResultItem<AnnotationData>.annotations()` instead, it'll be much more efficient.
    ///
    /// See `find_data()` for further parameter explanation.
    pub fn annotations_by_data<'store, 'a>(
        &'store self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> impl Iterator<
        Item = (
            ResultItem<'store, Annotation>,
            ResultItem<'store, AnnotationData>,
        ),
    >
    where
        'a: 'store,
    {
        self.find_data(set, key, value)
            .into_iter()
            .flatten()
            .map(|data| {
                data.annotations(self)
                    .map(move |annotation| (annotation, data.clone()))
            })
            .into_iter()
            .flatten()
    }
}

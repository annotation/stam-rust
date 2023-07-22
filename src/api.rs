use regex::{Regex, RegexSet};
use std::io::Write;
use std::marker::PhantomData;

use crate::annotation::{Annotation, TargetIter, TargetIterItem};
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::error::*;
use crate::resources::TextResource;
use crate::selector::{Offset, Selector, SelectorIter, SelectorIterItem};
use crate::store::*;
use crate::text::{
    FindNoCaseTextIter, FindRegexIter, FindRegexMatch, FindTextIter, SplitTextIter, Text,
};
use crate::textselection::{
    ResultTextSelection, TextSelection, TextSelectionHandle, TextSelectionOperator,
    TextSelectionSet,
};
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

    /// Retrieve a key in this set
    pub fn key(&self, key: impl Request<DataKey>) -> Option<ResultItem<DataKey>> {
        self.as_ref()
            .get(key)
            .map(|x| x.as_resultitem(self.as_ref()))
            .ok()
    }

    /// Retrieve a single [`AnnotationData`] in this set
    ///
    /// Returns a reference to [`AnnotationData`] that is wrapped in a fat pointer
    /// ([`WrappedItem<AnnotationData>`]) that also contains reference to the store and which is
    /// immediately implements various methods for working with the type. If you need a more
    /// performant low-level method, use `StoreFor<T>::get()` instead.
    pub fn annotationdata<'a>(
        &'a self,
        annotationdata: impl Request<AnnotationData>,
    ) -> Option<ResultItem<'a, AnnotationData>> {
        self.as_ref()
            .get(annotationdata)
            .map(|x| x.as_resultitem(self.as_ref()))
            .ok()
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

    /// Tests whether two AnnotationDataSets are the same
    pub fn test(&self, other: impl Request<AnnotationDataSet>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }
}

impl<'store> ResultItem<'store, Annotation> {
    /// Returns an iterator over over the data for this annotation
    pub fn data(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store {
        let store = self.store();
        self.as_ref().data().map(|(dataset_handle, data_handle)| {
            store
                .get(*dataset_handle)
                .map(|set| {
                    set.annotationdata(*data_handle)
                        .map(|data| data.as_resultitem(set))
                        .expect("data must exist")
                })
                .expect("set must exist")
        })
    }

    /// Returns an iterator over the resources that this annotation (by its target selector) references
    pub fn resources(&self) -> TargetIter<'store, TextResource> {
        let selector_iter: SelectorIter<'store> =
            self.as_ref().target().iter(self.store(), true, true);
        //                                                  ^ -- we track ancestors because it is needed to resolve relative offsets
        TargetIter {
            store: self.store(),
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterates over all the annotations this annotation points to directly (i.e. via a [`Selector::AnnotationSelector'])
    /// Use [`Self.annotations_reverse()'] if you want to find the annotations this resource is pointed by.
    pub fn annotations(
        &self,
        recursive: bool,
        track_ancestors: bool,
    ) -> TargetIter<'store, Annotation> {
        let selector_iter: SelectorIter<'store> =
            self.as_ref()
                .target()
                .iter(self.store(), recursive, track_ancestors);
        TargetIter {
            store: self.store(),
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterates over all the annotations that reference this annotation, if any
    pub fn annotations_about(
        &self,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        self.store()
            .annotations_by_annotation_reverse(self.handle())
            .into_iter()
            .flatten()
            .map(|a_handle| {
                store
                    .annotation(*a_handle)
                    .expect("annotation handle must be valid")
            })
    }

    /// Iterates over the annotation data sets this annotation references using a DataSetSelector, i.e. as metadata
    pub fn annotationsets(&self) -> TargetIter<'store, AnnotationDataSet> {
        let selector_iter: SelectorIter<'store> =
            self.as_ref().target().iter(self.store(), true, false);
        TargetIter {
            store: self.store(),
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterate over all text selections this annotation references (i.e. via [`Selector::TextSelector`])
    /// They are returned in the exact order as they were selected.
    pub fn textselections(&self) -> impl Iterator<Item = ResultTextSelection<'store>> + 'store {
        let store = self.store();
        self.resources().filter_map(|targetitem| {
            //process offset relative offset
            store
                .textselection_by_selector(
                    targetitem.selector(),
                    Some(targetitem.ancestors().iter().map(|x| x.as_ref())),
                )
                .ok() //ignores errors!
        })
    }

    /// Iterates over all text slices this annotation refers to
    pub fn text(&self) -> impl Iterator<Item = &'store str> {
        self.textselections()
            .map(|textselection| textselection.text())
    }

    /// Returns the (single!) resource the annotation points to. Only works for TextSelector,
    /// ResourceSelector and AnnotationSelector, and not for complex selectors.
    pub fn resource(&self) -> Option<ResultItem<'store, TextResource>> {
        match self.as_ref().target() {
            Selector::TextSelector(res_id, _) | Selector::ResourceSelector(res_id) => {
                self.store().resource(*res_id)
            }
            Selector::AnnotationSelector(a_id, _) => {
                if let Some(annotation) = self.store().annotation(*a_id) {
                    annotation.resource()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Finds the [`AnnotationData'] in the annotation. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use `test_data` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored.
    pub fn find_data<'a>(
        &self,
        set: Option<impl Request<AnnotationDataSet>>,
        key: Option<impl Request<DataKey>>,
        value: DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'a: 'store,
    {
        let mut set_handle: Option<AnnotationDataSetHandle> = None; //None means 'any' in this context
        let mut key_handle: Option<DataKeyHandle> = None; //idem

        if let Some(set) = set {
            if let Ok(set) = self.store().get(set) {
                set_handle = Some(set.handle().expect("set must have handle"));
                if let Some(key) = key {
                    key_handle = key.to_handle(set);
                    if key_handle.is_none() {
                        //requested key doesn't exist, bail out early, we won't find anything at all
                        return None;
                    }
                }
            } else {
                //requested set doesn't exist, bail out early, we won't find anything at all
                return None;
            }
        }

        Some(self.data().filter_map(move |annotationdata| {
            if (set_handle.is_none() || set_handle == annotationdata.store().handle())
                && (key_handle.is_none() || key_handle.unwrap() == annotationdata.key().handle())
                && annotationdata.as_ref().value().test(&value)
            {
                Some(annotationdata)
            } else {
                None
            }
        }))
    }

    /// Tests if the annotation has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored.
    pub fn test_data<'a>(
        &self,
        set: Option<BuildItem<AnnotationDataSet>>,
        key: Option<BuildItem<DataKey>>,
        value: DataOperator<'a>,
    ) -> bool {
        match self.find_data(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the text relations pertaining to the annotations. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`WrappedItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    pub fn find_textselections(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultItem<'store, TextSelection>> {
        //first we gather all textselections for this annotation in a set, as the chosen operator may apply to them jointly
        let tset: TextSelectionSet = self.textselections().collect();
        tset.find_textselections(operator, self.store())
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the text selections of the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`WrappedItem<TextSelection>`]).
    /// If you are interested in the text selections only, use [`Self.find_textselections()`] instead.
    pub fn find_annotations(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let store = self.store();
        self.find_textselections(operator)
            .map(|tsel| tsel.annotations(store))
            .flatten()
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

    /// Find textselections by applying a text selection operator ([`TextSelectionOperator`]) to a
    /// one or more querying textselections (in an [`TextSelectionSet']). Returns an iterator over all matching
    /// text selections in the resource, as [`WrappedItem<TextSelection>`].
    pub fn find_textselections(
        &self,
        operator: TextSelectionOperator,
        refset: TextSelectionSet,
    ) -> impl Iterator<Item = ResultItem<'store, TextSelection>> {
        let resource = self.as_ref();
        resource
            .textselections_by_operator(operator, refset)
            .map(|ts_handle| {
                let textselection: &'store TextSelection = resource
                    .get(ts_handle)
                    .expect("textselection handle must be valid");
                textselection.as_resultitem(resource)
            })
    }

    /*
    /// Find textselections by applying a text selection operator ([`TextSelectionOperator`]) to a
    /// one or more querying textselections (in an [`TextSelectionSet']). Returns an iterator over all matching
    /// text selections in the resource, as [`WrappedItem<TextSelection>`].
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

//TODO: implement reference to rootstore so we don't need to pass AnnotationStore to various methods
impl<'store> ResultItem<'store, DataKey> {
    /// Method to return a reference to the dataset that holds this key
    pub fn set(&self, store: &'store AnnotationStore) -> ResultItem<'store, AnnotationDataSet> {
        self.store().as_resultitem(store)
    }

    /// Returns the public identifier that identifies the key
    pub fn as_str(&self) -> &'store str {
        self.as_ref().as_str()
    }

    /// Returns an iterator over all data ([`AnnotationData`]) that makes use of this key.
    pub fn data(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store {
        let store = self.store();
        store
            .data_by_key(self.handle())
            .into_iter()
            .flatten()
            .filter_map(|data_handle| {
                store
                    .annotationdata(*data_handle)
                    .map(|d| d.as_resultitem(store))
            })
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that make use of this key.
    /// Especially useful in combination with a call to  [`AnnotationDataSet.key()`] first.
    pub fn annotations(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + 'store> {
        if let Some(iter) = annotationstore.annotations_by_key(
            self.store().handle().expect("set must have handle"),
            self.handle(),
        ) {
            Some(iter.filter_map(|a_handle| annotationstore.annotation(a_handle)))
        } else {
            None
        }
    }

    /// Returns the number of annotations that make use of this key.
    ///  Note: this method has suffix `_count` instead of `_len` because it is not O(1) but does actual counting (O(n) at worst).
    pub fn annotations_count(&self, annotationstore: &'store AnnotationStore) -> usize {
        if let Some(iter) = annotationstore.annotations_by_key(
            self.store().handle().expect("set must have handle"),
            self.handle(),
        ) {
            iter.count()
        } else {
            0
        }
    }

    /// Tests whether two DataKeys are the same
    pub fn test(&self, other: &BuildItem<DataKey>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }
}

impl<'store> ResultItem<'store, AnnotationData> {
    /// Method to return a reference to the dataset that holds this data
    pub fn set(&self, store: &'store AnnotationStore) -> ResultItem<'store, AnnotationDataSet> {
        self.store().as_resultitem(store)
    }

    /// Return a reference to data value
    pub fn value(&self) -> &'store DataValue {
        self.as_ref().value()
    }

    pub fn key(&self) -> ResultItem<'store, DataKey> {
        self.store()
            .key(self.as_ref().key())
            .expect("AnnotationData must always have a key at this point")
            .as_resultitem(self.store())
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that makes use of this data.
    /// The iterator returns the annoations as [`WrappedItem<Annotation>`].
    /// Especially useful in combination with a call to  [`WrappedItem<AnnotationDataSet>.find_data()`] or [`AnnotationDataSet.annotationdata()`] first.
    pub fn annotations(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + 'store> {
        if let Some(vec) = annotationstore.annotations_by_data(
            self.store().handle().expect("set must have handle"),
            self.handle(),
        ) {
            Some(
                vec.iter()
                    .filter_map(|a_handle| annotationstore.annotation(*a_handle)),
            )
        } else {
            None
        }
    }

    /// Returns the number of annotations ([`Annotation`]) that make use of this data.
    pub fn annotations_len(&self, annotationstore: &'store AnnotationStore) -> usize {
        if let Some(vec) = annotationstore.annotations_by_data(
            self.store().handle().expect("set must have handle"),
            self.handle(),
        ) {
            vec.len()
        } else {
            0
        }
    }

    pub fn test(&self, key: Option<&BuildItem<DataKey>>, operator: &DataOperator) -> bool {
        if key.is_none() || self.key().test(key.unwrap()) {
            self.as_ref().value().test(operator)
        } else {
            false
        }
    }
}

impl<'store> ResultItem<'store, TextSelection> {
    pub fn wrap(self) -> ResultTextSelection<'store> {
        ResultTextSelection::Bound(self)
    }
    pub fn begin(&self) -> usize {
        self.as_ref().begin()
    }

    pub fn end(&self) -> usize {
        self.as_ref().end()
    }

    pub fn resource(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> ResultItem<'store, TextResource> {
        self.store().as_resultitem(annotationstore)
    }

    /// Iterates over all annotations that reference this TextSelection, if any.
    /// Note that you need to explicitly specify the `AnnotationStore` for this method.
    pub fn annotations(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> {
        annotationstore
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
            .map(|v| {
                v.into_iter()
                    .map(|a_handle| annotationstore.annotation(*a_handle).unwrap())
            })
            .into_iter()
            .flatten()
    }

    /// Returns the number of annotations that reference this text selection
    pub fn annotations_len(&self, annotationstore: &'store AnnotationStore) -> usize {
        if let Some(vec) = annotationstore
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
        {
            vec.len()
        } else {
            0
        }
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`WrappedItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    pub fn find_textselections(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, TextSelection>> {
        let tset: TextSelectionSet = self.clone().into();
        self.resource(annotationstore)
            .find_textselections(operator, tset)
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`WrappedItem<TextSelection>`]).
    /// If you are interested in the text selections only, use [`Self.find_textselections()`] instead.
    pub fn find_annotations(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> {
        let tset: TextSelectionSet = self.clone().into();
        self.resource(annotationstore)
            .find_textselections(operator, tset)
            .map(|tsel| tsel.annotations(annotationstore))
            .flatten()
    }
}

impl<'store> ResultTextSelection<'store> {
    /// Return a reference to the inner textselection.
    /// This works in all cases but will have a limited lifetime.
    /// Use [`Self.as_ref()`] instead if you have bound item.
    pub fn inner(&self) -> &TextSelection {
        match self {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, item) => item,
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
            Self::Unbound(_, item) => item.begin(),
        }
    }

    /// Return the end position (non-inclusive) in unicode points
    pub fn end(&self) -> usize {
        match self {
            Self::Bound(item) => item.as_ref().end(),
            Self::Unbound(_, item) => item.end(),
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
                Self::Unbound(_, item) => &item,
            };
            match self {
                Self::Bound(item) => item.as_ref().relative_begin(container),
                Self::Unbound(_, item) => item.relative_begin(container),
            }
        }
    }

    /// Returns the end cursor (begin-aligned) of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_end(&self, container: &ResultTextSelection<'store>) -> Option<usize> {
        let container = match container {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, item) => &item,
        };
        match self {
            Self::Bound(item) => item.as_ref().relative_end(container),
            Self::Unbound(_, item) => item.relative_end(container),
        }
    }

    /// Returns the offset of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_offset(&self, container: &ResultTextSelection<'store>) -> Option<Offset> {
        let container = match container {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, item) => &item,
        };
        match self {
            Self::Bound(item) => item.as_ref().relative_offset(container),
            Self::Unbound(_, item) => item.relative_offset(container),
        }
    }

    pub fn store(&self) -> &'store TextResource {
        match self {
            Self::Bound(item) => item.store(),
            Self::Unbound(store, ..) => store,
        }
    }

    pub fn resource(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> ResultItem<'store, TextResource> {
        self.store().as_resultitem(annotationstore)
    }

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
            Self::Unbound(_store, item) => Ok(item),
        }
    }

    /// Iterates over all annotations that are referenced by this TextSelection, if any.
    /// Note that you need to explicitly specify the `AnnotationStore` for this method.
    pub fn annotations(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        match self {
            Self::Bound(item) => Some(item.annotations(annotationstore)),
            Self::Unbound(..) => None,
        }
    }

    /// Returns the number of annotations that reference this text selection
    pub fn annotations_len(&self, annotationstore: &'store AnnotationStore) -> usize {
        match self {
            Self::Bound(item) => item.annotations_len(annotationstore),
            Self::Unbound(..) => 0,
        }
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`WrappedItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    pub fn find_textselections(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, TextSelection>> {
        let mut tset: TextSelectionSet =
            TextSelectionSet::new(self.store().handle().expect("resource must have handle"));
        tset.add(match self {
            Self::Bound(item) => item.as_ref().clone().into(),
            Self::Unbound(_, textselection) => textselection.clone(),
        });
        self.resource(annotationstore)
            .find_textselections(operator, tset)
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`WrappedItem<TextSelection>`]).
    /// If you are interested in the text selections only, use [`Self.find_textselections()`] instead.
    pub fn find_annotations(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> {
        let mut tset: TextSelectionSet =
            TextSelectionSet::new(self.store().handle().expect("resource must have handle"));
        tset.add(match self {
            Self::Bound(item) => item.as_ref().clone().into(),
            Self::Unbound(_, textselection) => textselection.clone(),
        });
        self.resource(annotationstore)
            .find_textselections(operator, tset)
            .map(|tsel| tsel.annotations(annotationstore))
            .flatten()
    }
}

impl<'store> TextSelectionSet {
    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current text selection set. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`WrappedItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    /// This variant consumes the TextSelectionSet, use `find_textselections_ref()` for a borrowed version.
    pub fn find_textselections(
        self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, TextSelection>> {
        annotationstore
            .resource(self.resource())
            .map(|resource| {
                resource
                    .as_ref()
                    .textselections_by_operator(operator, self)
                    .map(move |ts_handle| {
                        let textselection: &'store TextSelection = resource
                            .as_ref()
                            .get(ts_handle)
                            .expect("textselection handle must be valid");
                        textselection.as_resultitem(resource.as_ref())
                    })
            })
            .into_iter()
            .flatten()
    }

    /*
    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current text selection set. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`WrappedItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    /// This variant borrows the TextSelectionSet, use `find_textselections()` for an owned version that consumes the set.
    pub fn find_textselections_ref(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = ResultItem<'store, TextSelection>> + 'store> {
        if let Some(resource) = annotationstore.resource(self.resource()) {
            Some(
                resource
                    .as_ref()
                    .textselections_by_operator_ref(operator, self)
                    .map(move |ts_handle| {
                        let textselection: &'store TextSelection = resource
                            .as_ref()
                            .get(ts_handle)
                            .expect("textselection handle must be valid");
                        textselection.as_resultitem(resource.as_ref())
                    }),
            )
        } else {
            None
        }
    }
    */
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

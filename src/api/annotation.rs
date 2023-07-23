use std::io::Write;
use std::marker::PhantomData;

use crate::annotation::{Annotation, TargetIter};
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::DataOperator;
use crate::error::*;
use crate::resources::TextResource;
use crate::selector::{Selector, SelectorIter};
use crate::store::*;
use crate::text::Text;
use crate::textselection::{
    ResultTextSelection, TextSelection, TextSelectionOperator, TextSelectionSet,
};
use crate::types::*;

//impl Annotation
impl<'store> ResultItem<'store, Annotation> {
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

    /// Iterates over all the annotations this annotation targets (i.e. via a [`Selector::AnnotationSelector'])
    /// Use [`Self.annotations()'] if you want to find the annotations that reference this one (the reverse).
    pub fn annotations_in_targets(
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
    pub fn annotations(&self) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
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

    /// Finds [`AnnotationData'] pertaining directly to this annotation, using data search criteria.
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
    pub fn find_data<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'a: 'store,
    {
        let store = self.store();
        if let Some((test_set_handle, test_key_handle)) = store.find_data_request_resolver(set, key)
        {
            Some(
                self.as_ref()
                    .data()
                    .filter_map(move |(dataset_handle, data_handle)| {
                        if test_set_handle.is_none() || test_set_handle.unwrap() == *dataset_handle
                        {
                            Some(
                                store
                                    .get(*dataset_handle)
                                    .map(|set| {
                                        set.annotationdata(*data_handle)
                                            .map(|data| data.as_resultitem(set))
                                            .expect("data must exist")
                                    })
                                    .expect("set must exist"),
                            )
                        } else {
                            None
                        }
                    })
                    .filter_map(move |annotationdata| {
                        if (test_key_handle.is_none()
                            || test_key_handle.unwrap() == annotationdata.key().handle())
                            && annotationdata.as_ref().value().test(&value)
                        {
                            Some(annotationdata)
                        } else {
                            None
                        }
                    }),
            )
        } else {
            None
        }
    }

    /// Shortcut method to get all data
    pub fn data(&self) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store {
        self.find_data(false, false, &DataOperator::Any)
            .expect("must return an iterator")
    }

    /// Tests if the annotation has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `data()` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    ///
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored!! You can not
    ///       search for keys if you don't know their set.
    pub fn test_data<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
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

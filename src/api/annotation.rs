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

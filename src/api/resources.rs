/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`TextResource`]. This API is implemented on
//! [`ResultItem<TextResource>`].

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::api::*;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::error::*;
use crate::resources::{PositionMode, TextResource, TextResourceHandle};
use crate::selector::Offset;
use crate::store::*;
use crate::substore::AnnotationSubStore;
use crate::text::Text;
use crate::textselection::{ResultTextSelection, TextSelectionOperator, TextSelectionSet};
use crate::{Filter, FilterMode};

use rayon::prelude::*;

impl<'store> FullHandle<TextResource> for ResultItem<'store, TextResource> {
    fn fullhandle(&self) -> <TextResource as Storable>::FullHandleType {
        self.handle()
    }
}

/// This is the implementation of the high-level API for [`TextResource`].
impl<'store> ResultItem<'store, TextResource> {
    /// Returns an iterator over all annotations about this resource as a whole, i.e. Annotations with a ResourceSelector.
    /// Such annotations can be considered metadata.
    pub fn annotations_as_metadata(
        &self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        if let Some(annotations) = self.store().annotations_by_resource_metadata(self.handle()) {
            ResultIter::new_sorted(FromHandles::new(annotations.iter().copied(), self.store()))
        } else {
            ResultIter::new_empty()
        }
    }

    /// Returns an iterator over all annotations about any text in this resource i.e. Annotations with a TextSelector.
    pub fn annotations(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        if let Some(iter) = self.store().annotations_by_resource(self.handle()) {
            let mut data: Vec<_> = iter.collect();
            data.sort_unstable();
            data.dedup();
            ResultIter::new_sorted(FromHandles::new(data.into_iter(), self.store()))
        } else {
            ResultIter::new_empty()
        }
    }

    /// Returns an iterator over all text selections that are marked in this resource (i.e. there are one or more annotations on it).
    /// They are returned in textual order, but this does not come with any significant performance overhead. If you want an unsorted version use [`self.as_ref().textselections_unsorted()`] instead.
    /// Note: This is a double-ended iterator that can be traversed in both directions.
    pub fn textselections(&self) -> impl DoubleEndedIterator<Item = ResultTextSelection<'store>> {
        let resource = self.as_ref();
        let rootstore = self.rootstore();
        ResultTextSelections::new(
            resource
                .iter()
                .map(|x| x.as_resultitem(resource, rootstore)),
        )
    }

    pub fn textselection_by_handle(
        &self,
        handle: TextSelectionHandle,
    ) -> Result<ResultTextSelection<'store>, StamError> {
        let textselection: &TextSelection = self.as_ref().get(handle)?;
        Ok(ResultTextSelection::Bound(
            textselection.as_resultitem(self.as_ref(), self.store()),
        ))
    }

    /// Get a text selection pointing to the whole resource, you can also call this implicitly via the `From` trait (`resource.into()`).
    pub fn to_textselection(self) -> ResultTextSelection<'store> {
        self.textselection(&Offset::whole())
            .expect("to_textselection() should never fail")
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
        let rootstore = self.rootstore();
        ResultTextSelections::new(
            resource
                .range(begin, end)
                .map(|x| x.as_resultitem(resource, rootstore)),
        )
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
        let rootstore = self.rootstore();
        ResultTextSelections::new(
            resource
                .textselections_by_operator(operator, refset.into())
                .filter_map(|handle| {
                    resource
                        .get(handle)
                        .ok()
                        .map(|x| x.as_resultitem(resource, rootstore))
                }),
        )
    }

    /// Search for annotations *about* this resource, satisfying certain exact data that is already known.
    /// For a higher-level variant, see `find_data_about`, this method is more efficient.
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn annotations_by_metadata_about(
        &self,
        data: ResultItem<'store, AnnotationData>,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.annotations_as_metadata()
            .filter(move |annotation| annotation.has_data(&data))
    }

    /// Tests if the resource has certain data in annotations that reference this textselection, returns a boolean.
    /// If you don't have a data instance yet, use `test_data_about()` instead.
    /// This method is much more efficient than `test_data_about()`.
    pub fn has_metadata_about(&self, data: ResultItem<'store, AnnotationData>) -> bool {
        self.annotations_by_metadata_about(data).test()
    }

    /// Iterator covering the full text of the resource as a sequence of minimum-length non-overlapping TextSelections, in textual order
    pub fn segmentation(&self) -> SegmentationIter<'store> {
        SegmentationIter {
            positions: self.as_ref().positions(PositionMode::Both),
            resource: self.clone(),
            cursor: 0,
            end: self.as_ref().textlen(),
        }
    }

    /// Iterator covering a range of text of the resource as a sequence of minimum-length non-overlapping TextSelections, in textual order
    pub fn segmentation_in_range(&self, begin: usize, end: usize) -> SegmentationIter<'store> {
        SegmentationIter {
            positions: self
                .as_ref()
                .positions_in_range(PositionMode::Both, begin, end),
            resource: self.clone(),
            cursor: begin,
            end,
        }
    }

    /// Returns an iterator over all substores ([`AnnotationSubStore`] instances) this resource is a part of.
    pub fn substores<'a>(
        &'a self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'a, AnnotationSubStore>>> {
        let handle = self.handle();
        let store = self.store();
        ResultIter::new_sorted(
            store
                .resource_substore_map
                .get(handle)
                .into_iter()
                .flatten()
                .map(|substorehandle| store.substore(*substorehandle).expect("handle must exist")),
        )
    }
}

/// Holds a collection of [`TextResource`] (by reference to an [`AnnotationStore`] and handles). This structure is produced by calling
/// [`ToHandles::to_handles()`], which is available on all iterators over resources ([`ResultItem<TextResource>`]).
pub type Resources<'store> = Handles<'store, TextResource>;

impl<'store, I> FullHandleToResultItem<'store, TextResource>
    for FromHandles<'store, TextResource, I>
where
    I: Iterator<Item = TextResourceHandle>,
{
    fn get_item(&self, handle: TextResourceHandle) -> Option<ResultItem<'store, TextResource>> {
        self.store.resource(handle)
    }
}

impl<'store, I> FullHandleToResultItem<'store, TextResource>
    for FilterAllIter<'store, TextResource, I>
where
    I: Iterator<Item = ResultItem<'store, TextResource>>,
{
    fn get_item(&self, handle: TextResourceHandle) -> Option<ResultItem<'store, TextResource>> {
        self.store.resource(handle)
    }
}

/// Trait for iteration over resources ([`ResultItem<TextResource>`]; encapsulation over
/// [`TextResource`]). Implements numerous filter methods to further constrain the iterator, as well
/// as methods to map from resources to other items.
pub trait ResourcesIterator<'store>: Iterator<Item = ResultItem<'store, TextResource>>
where
    Self: Sized,
{
    fn parallel(self) -> rayon::vec::IntoIter<ResultItem<'store, TextResource>> {
        let annotations: Vec<_> = self.collect();
        annotations.into_par_iter()
    }

    /// Iterates over all the annotations for all resources in this iterator.
    /// This only returns annotations that target the resource via a ResourceSelector.
    ///
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    ///
    /// If you want annotations unsorted and with possible duplicates, then just do:  `.map(|res| res.annotations()).flatten()` instead
    fn annotations_as_metadata(
        self,
    ) -> ResultIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|resource| resource.annotations_as_metadata())
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        ResultIter::new_sorted(annotations.into_iter())
    }

    /// Iterates over all the annotations for all resources in this iterator.
    /// This only returns annotations that target the resource via a TextSelector.
    ///
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
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

    /// Iterates over all textselections for all resources in this iterator, in resource order and textual order.
    fn textselections(
        self,
    ) -> ResultIter<<Vec<ResultTextSelection<'store>> as IntoIterator>::IntoIter> {
        let textselections: Vec<_> = self
            .map(|resource| resource.textselections())
            .flatten()
            .collect();
        ResultIter::new_unsorted(textselections.into_iter()) //not chronologically sorted
    }

    /// Constrain this iterator to filter only a single resource (by handle). This is a lower-level method, use [`Self::filter_one()`] instead.
    /// This method can only be used once! Use [`Self::filter_any()`], [`Self::filter_any_byref()`] to filter on multiple annotations (disjunction), or [`Self::filter_all()`] (conjunction).
    fn filter_handle(self, handle: TextResourceHandle) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::TextResource(handle, SelectionQualifier::Normal),
        }
    }

    /// Constrain this iterator to only a single resource
    /// This method can only be used once! Use [`Self::filter_any()`], [`Self::filter_any_byref()`] to filter on multiple annotations (disjunction), or [`Self::filter_all()`] (conjunction).
    fn filter_one(self, resource: &ResultItem<TextResource>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::TextResource(resource.handle(), SelectionQualifier::Normal),
        }
    }

    /// Constrain this iterator to filter on one of the mentioned resources
    fn filter_any(self, resources: Resources<'store>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Resources(resources, FilterMode::Any, SelectionQualifier::Normal),
        }
    }

    /// Constrain this iterator to filter on one of the mentioned resources
    fn filter_any_byref(
        self,
        resources: &'store Resources<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::BorrowedResources(
                resources,
                FilterMode::Any,
                SelectionQualifier::Normal,
            ),
        }
    }

    /// Constrain this iterator to filter on *all* of the mentioned resources.
    /// If not all of the items in the parameter exist in the iterator, the iterator returns nothing.
    fn filter_all(
        self,
        resources: Resources<'store>,
        store: &'store AnnotationStore,
    ) -> FilterAllIter<'store, TextResource, Self> {
        FilterAllIter::new(self, resources, store)
    }

    /// Constrain the iterator to only return resources with metadata annotations (i.e. via a ResourceSelector) that have data that corresponds with any of the items in the passed data.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each resource.
    fn filter_metadata(
        self,
        data: Data<'store>,
        mode: FilterMode,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Data(data, mode, SelectionQualifier::Metadata),
        }
    }

    /// Constrain the iterator to only return resources with annotations via a TextSelector that have data that corresponds with any of the items in the passed data.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each resource.
    fn filter_data_on_text(
        self,
        data: Data<'store>,
        mode: FilterMode,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Data(data, mode, SelectionQualifier::Normal),
        }
    }

    /// Constrain the iterator to only return resources with annotations that match the ones passed
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource
    fn filter_annotations_on_text(
        self,
        annotations: Annotations<'store>,
        mode: FilterMode,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Annotations(
                annotations,
                mode,
                SelectionQualifier::Normal,
                AnnotationDepth::default(),
            ),
        }
    }

    /// Constrain the iterator to only return resources with annotations that match the ones passed
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource
    fn filter_annotations_byref(
        self,
        annotations: &'store Annotations<'store>,
        mode: FilterMode,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::BorrowedAnnotations(
                annotations,
                mode,
                SelectionQualifier::Normal,
                AnnotationDepth::default(),
            ),
        }
    }

    /// Constrain this iterator to resources with this specific annotation
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource.
    fn filter_annotation_on_text(
        self,
        annotation: &ResultItem<Annotation>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Annotation(
                annotation.handle(),
                SelectionQualifier::Normal,
                AnnotationDepth::default(),
            ),
        }
    }

    /// Constrain the iterator to only return resources with annotations (as metadata, i.e. via a ResourceSelector) that match the ones passed
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource
    fn filter_annotations_as_metadata(
        self,
        annotations: Annotations<'store>,
        mode: FilterMode,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Annotations(
                annotations,
                mode,
                SelectionQualifier::Metadata,
                AnnotationDepth::default(),
            ),
        }
    }

    /// Constrain this iterator to resources with this specific annotation (as metadata, i.e. via a ResourceSelector)
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource.
    fn filter_annotation_as_metadata(
        self,
        annotation: &ResultItem<Annotation>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Annotation(
                annotation.handle(),
                SelectionQualifier::Metadata,
                AnnotationDepth::default(),
            ),
        }
    }

    fn filter_annotationdata_on_text(
        self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationData(
                data.set().handle(),
                data.handle(),
                SelectionQualifier::Normal,
            ),
        }
    }

    /// Constrain the iterator to return only the resources that have this exact data item.
    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_annotationdata_in_metadata(
        self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationData(
                data.set().handle(),
                data.handle(),
                SelectionQualifier::Metadata,
            ),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key_value_in_metadata(
        self,
        key: &ResultItem<'store, DataKey>,
        value: DataOperator<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKeyAndOperator(
                key.set().handle(),
                key.handle(),
                value,
                SelectionQualifier::Metadata,
            ),
        }
    }

    fn filter_key_value_on_text(
        self,
        key: &ResultItem<'store, DataKey>,
        value: DataOperator<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKeyAndOperator(
                key.set().handle(),
                key.handle(),
                value,
                SelectionQualifier::Normal,
            ),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key_on_text(
        self,
        key: &ResultItem<'store, DataKey>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle(), SelectionQualifier::Normal),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key_in_metadata(
        self,
        key: &ResultItem<'store, DataKey>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKey(
                key.set().handle(),
                key.handle(),
                SelectionQualifier::Metadata,
            ),
        }
    }

    /// This filter considers only annotations on the text (i.e. via a TextSelector)
    fn filter_key_handle_on_text(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKey(set, key, SelectionQualifier::Normal),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key_handle_in_metadata(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKey(set, key, SelectionQualifier::Metadata),
        }
    }

    /// This filter considers only annotations on the text (i.e. via a TextSelector)
    fn filter_value_on_text(self, value: DataOperator<'store>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataOperator(value, SelectionQualifier::Normal),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_value_in_metadata(
        self,
        value: DataOperator<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataOperator(value, SelectionQualifier::Metadata),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key_handle_value_in_metadata(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
        value: DataOperator<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKeyAndOperator(set, key, value, SelectionQualifier::Metadata),
        }
    }

    /// This filter considers only annotations on the text (i.e. via a TextSelector)
    fn filter_key_handle_value_on_text(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
        value: DataOperator<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKeyAndOperator(set, key, value, SelectionQualifier::Normal),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_set_in_metadata(
        self,
        set: &ResultItem<'store, AnnotationDataSet>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle(), SelectionQualifier::Metadata),
        }
    }

    /// This filter considers only annotations on the text (i.e. via a TextSelector)
    fn filter_set_on_text(
        self,
        set: &ResultItem<'store, AnnotationDataSet>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle(), SelectionQualifier::Normal),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_set_handle_in_metadata(
        self,
        set: AnnotationDataSetHandle,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationDataSet(set, SelectionQualifier::Metadata),
        }
    }

    /// This filter considers only annotations on the text (i.e. via a TextSelector)
    fn filter_set_handle(self, set: AnnotationDataSetHandle) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationDataSet(set, SelectionQualifier::Normal),
        }
    }

    fn filter_substore(
        self,
        substore: Option<ResultItem<'store, AnnotationSubStore>>,
    ) -> FilteredResources<'store, Self> {
        if let Some(substore) = substore {
            FilteredResources {
                inner: self,
                filter: Filter::AnnotationSubStore(Some(substore.handle())),
            }
        } else {
            FilteredResources {
                inner: self,
                filter: Filter::AnnotationSubStore(None),
            }
        }
    }
}

/*
//not needed yet
pub(crate) trait ResourcesPrivIterator<'store>:
    Iterator<Item = ResultItem<'store, TextResource>>
where
    Self: Sized,
{
    fn filter_custom(self, filter: Filter) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter,
        }
    }
}
*/

impl<'store, I> ResourcesIterator<'store> for I
where
    I: Iterator<Item = ResultItem<'store, TextResource>>,
{
    //blanket implementation
}

/// An iterator that applies a filter to constrain resources.
/// This iterator implements [`ResourcesIterator`]
/// and is itself produced by the various `filter*()` methods on that trait.
pub struct FilteredResources<'store, I>
where
    I: Iterator<Item = ResultItem<'store, TextResource>>,
{
    inner: I,
    filter: Filter<'store>,
}

impl<'store, I> Iterator for FilteredResources<'store, I>
where
    I: Iterator<Item = ResultItem<'store, TextResource>>,
{
    type Item = ResultItem<'store, TextResource>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(resource) = self.inner.next() {
                if self.test_filter(&resource) {
                    return Some(resource);
                }
            } else {
                return None;
            }
        }
    }
}

impl<'store, I> FilteredResources<'store, I>
where
    I: Iterator<Item = ResultItem<'store, TextResource>>,
{
    fn test_filter(&self, resource: &ResultItem<'store, TextResource>) -> bool {
        match &self.filter {
            Filter::TextResource(handle, _) => resource.handle() == *handle,
            Filter::Resources(handles, FilterMode::Any, _) => {
                handles.contains(&resource.fullhandle())
            }
            Filter::BorrowedResources(handles, FilterMode::Any, _) => {
                handles.contains(&resource.fullhandle())
            }
            Filter::Data(data, mode, SelectionQualifier::Metadata) => resource
                .annotations_as_metadata()
                .filter_data_byref(data, *mode)
                .test(),
            Filter::Data(data, mode, SelectionQualifier::Normal) => {
                resource.annotations().filter_data_byref(data, *mode).test()
            }
            Filter::BorrowedData(data, mode, SelectionQualifier::Normal) => {
                resource.annotations().filter_data_byref(data, *mode).test()
            }
            Filter::BorrowedData(data, mode, SelectionQualifier::Metadata) => {
                resource.annotations().filter_data_byref(data, *mode).test()
            }
            Filter::Annotations(annotations, mode, SelectionQualifier::Normal, _) => resource
                .annotations()
                .filter_annotations_byref(annotations, *mode)
                .test(),
            Filter::Annotations(annotations, mode, SelectionQualifier::Metadata, _) => resource
                .annotations_as_metadata()
                .filter_annotations_byref(annotations, *mode)
                .test(),
            Filter::BorrowedAnnotations(annotations, mode, SelectionQualifier::Normal, _) => {
                resource
                    .annotations()
                    .filter_annotations_byref(annotations, *mode)
                    .test()
            }
            Filter::Annotation(annotation, SelectionQualifier::Normal, _) => {
                resource.annotations().filter_handle(*annotation).test()
            }
            Filter::Annotation(annotation, SelectionQualifier::Metadata, _) => resource
                .annotations_as_metadata()
                .filter_handle(*annotation)
                .test(),
            Filter::DataKey(set, key, SelectionQualifier::Normal) => resource
                .annotations()
                .data()
                .filter_key_handle(*set, *key)
                .test(),
            Filter::DataKey(set, key, SelectionQualifier::Metadata) => resource
                .annotations_as_metadata()
                .data()
                .filter_key_handle(*set, *key)
                .test(),
            Filter::DataKeyAndOperator(set, key, value, SelectionQualifier::Normal) => resource
                .annotations()
                .data()
                .filter_key_handle_value(*set, *key, value.clone())
                .test(),
            Filter::DataKeyAndOperator(set, key, value, SelectionQualifier::Metadata) => resource
                .annotations_as_metadata()
                .data()
                .filter_key_handle_value(*set, *key, value.clone())
                .test(),
            Filter::DataOperator(value, SelectionQualifier::Normal) => resource
                .annotations()
                .data()
                .filter_value(value.clone())
                .test(),
            Filter::DataOperator(value, SelectionQualifier::Metadata) => resource
                .annotations_as_metadata()
                .data()
                .filter_value(value.clone())
                .test(),
            Filter::AnnotationDataSet(set, SelectionQualifier::Normal) => {
                resource.annotations().data().filter_set_handle(*set).test()
            }
            Filter::AnnotationDataSet(set, SelectionQualifier::Metadata) => resource
                .annotations_as_metadata()
                .data()
                .filter_set_handle(*set)
                .test(),
            Filter::AnnotationData(set, data, SelectionQualifier::Normal) => resource
                .annotations()
                .data()
                .filter_handle(*set, *data)
                .test(),
            Filter::AnnotationData(set, data, SelectionQualifier::Metadata) => resource
                .annotations_as_metadata()
                .data()
                .filter_handle(*set, *data)
                .test(),
            Filter::AnnotationSubStore(substore) => {
                if let Some(substore) = substore {
                    resource.substores().any(|x| x.handle() == *substore)
                } else {
                    resource.substores().count() == 0
                }
            }
            _ => unreachable!(
                "Filter {:?} not implemented for FilteredResources",
                self.filter
            ),
        }
    }
}

pub struct SegmentationIter<'a> {
    positions: Box<dyn Iterator<Item = &'a usize> + 'a>,
    resource: ResultItem<'a, TextResource>,
    cursor: usize,
    end: usize,
}

impl<'a> Iterator for SegmentationIter<'a> {
    type Item = ResultTextSelection<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cursor >= self.end {
                return None;
            }

            if let Some(pos) = self.positions.next() {
                // get the positionitem to filter out textselections that have no annotations associated (e.g. the milestones)
                let positionitem = self
                    .resource
                    .as_ref()
                    .position(*pos)
                    .expect("positionitem must exist");
                if *pos > self.cursor
                    && (positionitem.len_begin2end() > 0 || positionitem.len_end2begin() > 0)
                {
                    if *pos > self.end {
                        //clipped segment
                        let textselection = self
                            .resource
                            .textselection(&Offset::simple(self.cursor, self.end))
                            .expect("textselection must succeed");
                        self.cursor = self.end;
                        return Some(textselection);
                    } else {
                        //normal segment
                        let textselection = self
                            .resource
                            .textselection(&Offset::simple(self.cursor, *pos))
                            .expect("textselection must succeed");
                        self.cursor = *pos;
                        return Some(textselection);
                    }
                } else {
                    continue;
                }
            } else {
                let textselection = self
                    .resource
                    .textselection(&Offset::simple(self.cursor, self.end))
                    .expect("textselection must succeed");
                self.cursor = self.end;
                return Some(textselection);
            }
        }
    }
}

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
use crate::resources::{TextResource, TextResourceHandle};
use crate::store::*;
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
    ) -> MaybeIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        if let Some(annotations) = self.store().annotations_by_resource_metadata(self.handle()) {
            MaybeIter::new_sorted(FromHandles::new(annotations.iter().copied(), self.store()))
        } else {
            MaybeIter::new_empty()
        }
    }

    /// Returns an iterator over all annotations about any text in this resource i.e. Annotations with a TextSelector.
    pub fn annotations_on_text(
        &self,
    ) -> MaybeIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        if let Some(iter) = self.store().annotations_by_resource(self.handle()) {
            let mut data: Vec<_> = iter.collect();
            data.sort_unstable();
            data.dedup();
            MaybeIter::new_sorted(FromHandles::new(data.into_iter(), self.store()))
        } else {
            MaybeIter::new_empty()
        }
    }

    /// Returns an iterator over all annotations that reference this resource, both annotations that can be considered metadata as well
    /// annotations that reference a portion of the text.
    /// Use `annotations_as_metadata()` or `annotations_on_text()` instead if you want to differentiate the two.
    pub fn annotations(&self) -> MaybeIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let mut data: Vec<_> = self
            .store()
            .annotations_by_resource_metadata(self.handle())
            .into_iter()
            .flatten()
            .copied()
            .collect();
        data.extend(
            self.store()
                .annotations_by_resource(self.handle())
                .into_iter()
                .flatten(),
        );
        data.sort_unstable();
        data.dedup();
        MaybeIter::new_sorted(FromHandles::new(data.into_iter(), self.store()))
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
        self.annotations_by_metadata_about(data).next().is_some()
    }
}

/// Holds a collection of resources.
/// This structure is produced by calling [`ResourcesIter::to_collection()`].
/// Use [`Resources::iter()`] to iterate over the collection.
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

pub trait ResourcesIterator<'store>: Iterator<Item = ResultItem<'store, TextResource>>
where
    Self: Sized,
{
    fn parallel(self) -> rayon::vec::IntoIter<ResultItem<'store, TextResource>> {
        let annotations: Vec<_> = self.collect();
        annotations.into_par_iter()
    }

    /// Iterates over all the annotations for all resources in this iterator.
    /// This returns both annotations that target the resource via a ResourceSelector or via a TextSelector.
    ///
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    ///
    /// If you want annotations unsorted and with possible duplicates, then just do:  `.map(|res| res.annotations()).flatten()` instead
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

    /// Iterates over all the annotations for all resources in this iterator.
    /// This only returns annotations that target the resource via a ResourceSelector. See [`Self.annotations()`] for all.
    ///
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    ///
    /// If you want annotations unsorted and with possible duplicates, then just do:  `.map(|res| res.annotations()).flatten()` instead
    fn annotations_as_metadata(
        self,
    ) -> MaybeIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|resource| resource.annotations_as_metadata())
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        MaybeIter::new_sorted(annotations.into_iter())
    }

    /// Iterates over all the annotations for all resources in this iterator.
    /// This only returns annotations that target the resource via a TextSelector. See [`Self.annotations()`] for all.
    ///
    /// The iterator will be consumed and an extra buffer is allocated.
    /// Annotations will be returned sorted chronologically and returned without duplicates
    ///
    /// If you want annotations unsorted and with possible duplicates, then just do:  `.map(|res| res.annotations()).flatten()` instead
    fn annotations_on_text(
        self,
    ) -> MaybeIter<<Vec<ResultItem<'store, Annotation>> as IntoIterator>::IntoIter> {
        let mut annotations: Vec<_> = self
            .map(|resource| resource.annotations_on_text())
            .flatten()
            .collect();
        annotations.sort_unstable();
        annotations.dedup();
        MaybeIter::new_sorted(annotations.into_iter())
    }

    /// Iterates over all textselections for all resources in this iterator, in resource order and textual order.
    fn textselections(
        self,
    ) -> MaybeIter<<Vec<ResultTextSelection<'store>> as IntoIterator>::IntoIter> {
        let textselections: Vec<_> = self
            .map(|resource| resource.textselections())
            .flatten()
            .collect();
        MaybeIter::new_unsorted(textselections.into_iter()) //not chronologically sorted
    }

    /// Constrain this iterator to filter only a single resource (by handle). This is a lower-level method, use [`Self::filter_resource()`] instead.
    /// This method can only be used once! Use [`Self::filter_resources()`] to filter on multiple resources (disjunction).
    fn filter_handle(self, handle: TextResourceHandle) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::TextResource(handle),
        }
    }

    /// Constrain this iterator to only a single resource
    /// This method can only be used once! Use [`Self::filter_resources()`] to filter on multiple annotations (disjunction).
    fn filter_resource(
        self,
        resource: &ResultItem<TextResource>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::TextResource(resource.handle()),
        }
    }

    /// Constrain this iterator to filter on one of the mentioned annotations
    fn filter_resources(self, resources: Resources<'store>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Resources(resources),
        }
    }

    /// Constrain the iterator to only return resources with metadata annotations (i.e. via a ResourceSelector) that have data that corresponds with any of the items in the passed data.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each resource.
    fn filter_metadata(self, data: Data<'store>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::MetaData(data, FilterMode::Any),
        }
    }

    /// Constrain the iterator to only return resources with annotations on its text (i.e. via a TextSelector) that have data that corresponds with any of the items in the passed data.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each resource.
    fn filter_data_on_text(self, data: Data<'store>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataOnText(data, FilterMode::Any),
        }
    }

    /// Constrain the iterator to only return resources with annotations that have data that corresponds with any of the items in the passed data.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each resource.
    fn filter_data(self, data: Data<'store>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Data(data, FilterMode::Any),
        }
    }

    /// Constrain the iterator to only return resources with annotations that match the ones passed
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource
    fn filter_annotations(
        self,
        annotations: Annotations<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Annotations(annotations),
        }
    }

    /// Constrain this iterator to resources with this specific annotation
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource.
    fn filter_annotation(
        self,
        annotation: &ResultItem<Annotation>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::Annotation(annotation.handle()),
        }
    }

    /// Constrain the iterator to only return resources with annotations (as metadata, i.e. via a ResourceSelector) that match the ones passed
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource
    fn filter_annotations_as_metadata(
        self,
        annotations: Annotations<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationsAsMetadata(annotations),
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
            filter: Filter::AnnotationAsMetadata(annotation.handle()),
        }
    }

    /// Constrain the iterator to only return resources with annotations (on text, i.e. via a TextSelector) that match the ones passed
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource
    fn filter_annotations_on_text(
        self,
        annotations: Annotations<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationsOnText(annotations),
        }
    }

    /// Constrain this iterator to resources with this specific annotation (on text, i.e. via a TextSelector)
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each resource.
    fn filter_annotation_on_text(
        self,
        annotation: &ResultItem<Annotation>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationOnText(annotation.handle()),
        }
    }

    /// Constrain the iterator to return only the resources that have this exact data item.
    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_annotationdata(
        self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationData(data.set().handle(), data.handle()),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key_value(
        self,
        key: &ResultItem<'store, DataKey>,
        value: DataOperator<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKeyAndOperator(key.set().handle(), key.handle(), value),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key(self, key: &ResultItem<'store, DataKey>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKey(key.set().handle(), key.handle()),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key_handle(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKey(set, key),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_value(self, value: DataOperator<'store>) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataOperator(value),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_key_handle_value(
        self,
        set: AnnotationDataSetHandle,
        key: DataKeyHandle,
        value: DataOperator<'store>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::DataKeyAndOperator(set, key, value),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_set(
        self,
        set: &ResultItem<'store, AnnotationDataSet>,
    ) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationDataSet(set.handle()),
        }
    }

    /// This filter considers only annotations as metadata (i.e. via a ResourceSelector)
    fn filter_set_handle(self, set: AnnotationDataSetHandle) -> FilteredResources<'store, Self> {
        FilteredResources {
            inner: self,
            filter: Filter::AnnotationDataSet(set),
        }
    }
}

impl<'store, I> ResourcesIterator<'store> for I
where
    I: Iterator<Item = ResultItem<'store, TextResource>>,
{
    //blanket implementation
}

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
            Filter::TextResource(handle) => resource.handle() == *handle,
            Filter::Resources(handles) => handles.contains(&resource.fullhandle()),
            Filter::MetaData(data, FilterMode::Any) => resource
                .annotations_as_metadata()
                .filter_data(data.clone())
                .next()
                .is_some(),
            Filter::DataOnText(data, FilterMode::Any) => resource
                .annotations_as_metadata()
                .filter_data(data.clone())
                .next()
                .is_some(),
            Filter::Data(data, FilterMode::Any) => resource
                .annotations()
                .filter_data(data.clone())
                .next()
                .is_some(),
            Filter::Annotations(annotations) => resource
                .annotations()
                .filter_annotations(annotations.clone())
                .next()
                .is_some(),
            Filter::Annotation(annotation) => resource
                .annotations()
                .filter_handle(*annotation)
                .next()
                .is_some(),
            Filter::AnnotationsAsMetadata(annotations) => resource
                .annotations_as_metadata()
                .filter_annotations(annotations.clone())
                .next()
                .is_some(),
            Filter::AnnotationAsMetadata(annotation) => resource
                .annotations_as_metadata()
                .filter_handle(*annotation)
                .next()
                .is_some(),
            Filter::AnnotationsOnText(annotations) => resource
                .annotations_on_text()
                .filter_annotations(annotations.clone())
                .next()
                .is_some(),
            Filter::AnnotationOnText(annotation) => resource
                .annotations_on_text()
                .filter_handle(*annotation)
                .next()
                .is_some(),

            // these data filters act on ANNOTATIONS AS METADATA only:
            Filter::DataKey(set, key) => resource
                .annotations_as_metadata()
                .data()
                .filter_key_handle(*set, *key)
                .next()
                .is_some(),
            Filter::DataKeyAndOperator(set, key, value) => resource
                .annotations_as_metadata()
                .data()
                .filter_key_handle_value(*set, *key, value.clone())
                .next()
                .is_some(),
            Filter::DataOperator(value) => resource
                .annotations_as_metadata()
                .data()
                .filter_value(value.clone())
                .next()
                .is_some(),
            Filter::AnnotationDataSet(set) => resource
                .annotations_as_metadata()
                .data()
                .filter_set_handle(*set)
                .next()
                .is_some(),
            Filter::AnnotationData(set, data) => resource
                .annotations_as_metadata()
                .data()
                .filter_handle(*set, *data)
                .next()
                .is_some(),
            _ => unreachable!(
                "Filter {:?} not implemented for FilteredResources",
                self.filter
            ),
        }
    }
}

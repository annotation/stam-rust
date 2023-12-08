/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`TextResource`]. This API is implemented on
//! [`ResultItem<TextResource>`].

use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::*;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::{TextResource, TextResourceHandle};
use crate::store::*;
use crate::textselection::{ResultTextSelection, TextSelectionOperator, TextSelectionSet};
use crate::{Filter, FilterMode, IntersectionIter};

use rayon::prelude::*;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::fmt::Debug;

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

/*
/// The ResourcesIter iterates over text resources, it returns [`ResultItem<Annotation>`] instances.
/// The iterator offers a various high-level API methods that operate on a collection of annotations, and
/// allow to further filter or map annotations.
///
/// The iterator is produced by calling the `resources()` method that is implemented for several objects, such
/// as [`ResultItem<Annotation>::resources()`], or on other iterators like [`TextSelectionsIter::resources()`].
pub struct ResourcesIter<'store> {
    iter: Option<IntersectionIter<'store, TextResourceHandle>>,
    store: &'store AnnotationStore,

    filters: SmallVec<[Filter<'store>; 1]>,
}

impl<'store> ResourcesIter<'store> {
    pub(crate) fn new(
        iter: IntersectionIter<'store, TextResourceHandle>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            iter: Some(iter),
            filters: SmallVec::new(),
            store,
        }
    }

    /// Builds a new annotation iterator from any other iterator of annotations.
    /// Eagerly consumes the iterator first.
    pub fn from_iter(
        iter: impl Iterator<Item = ResultItem<'store, TextResource>>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self {
        let data: Vec<_> = iter.map(|a| a.handle()).collect();
        Self {
            iter: Some(IntersectionIter::new(Cow::Owned(data), sorted)),
            filters: SmallVec::new(),
            store,
        }
    }

    /// Transform the iterator into a parallel iterator; subsequent iterator methods like `filter` and `map` will run in parallel.
    /// This first consumes the sequential iterator into a newly allocated buffer.
    ///
    /// Note: It does not parallelize the operation of AnnotationsIter itself.
    pub fn parallel(
        self,
    ) -> impl ParallelIterator<Item = ResultItem<'store, TextResource>> + 'store {
        self.collect::<Vec<_>>().into_par_iter()
    }

    /// Set the iterator to abort, no further results will be returned
    pub fn abort(&mut self) {
        self.iter.as_mut().map(|iter| iter.abort = true);
    }

    /// Returns true if the iterator has items, false otherwise
    pub fn test(mut self) -> bool {
        self.next().is_some()
    }

    /// Filter by a single annotation. Only text resources will be returned that are a part of the specified annotation.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    pub fn filter_annotation(mut self, annotation: &ResultItem<'store, Annotation>) -> Self {
        self.filters.push(Filter::Annotation(annotation.handle()));
        self
    }

    /// Filter by a single annotation. Only text resources will be returned that are a part of the specified annotation.
    /// This is a lower-level method, use [`Self::filter_annotation`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    pub fn filter_annotation_handle(mut self, annotation: AnnotationHandle) -> Self {
        self.filters.push(Filter::Annotation(annotation));
        self
    }

    /// Filter by annotations. Only text resources will be returned that are a part of any of the specified annotations.
    /// If you have a borrowed reference, use [`Self::filter_annotations_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    pub fn filter_annotations(mut self, annotations: Annotations<'store>) -> Self {
        self.filters.push(Filter::Annotations(annotations));
        self
    }

    /// Filter by annotations. Only text resources will be returned that are a part of any of the specified annotations.
    /// If you have owned annotations, use [`Self::filter_annotations()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations for each text selection.
    pub fn filter_annotations_byref(mut self, annotations: &'store Annotations<'store>) -> Self {
        self.filters.push(Filter::BorrowedAnnotations(annotations));
        self
    }

    /// Constrain the iterator to return only resources targeted by annotations that have this exact data item
    /// This method can only be used once, to filter by multiple data instances, use [`Self::filter_data()`] or [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    pub fn filter_annotationdata(mut self, data: &ResultItem<'store, AnnotationData>) -> Self {
        self.filters
            .push(Filter::AnnotationData(data.set().handle(), data.handle()));
        self
    }

    /// Constrain the iterator to return only resources targeted by annotations that have this exact data item. This is a lower-level method that takes handles, use [`Self::filter_annotationdata()`] instead.
    /// This method can only be used once, to filter by multiple data instances, use [`Self::filter_data()`] or [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    pub fn filter_annotationdata_handle(
        mut self,
        set_handle: AnnotationDataSetHandle,
        data_handle: AnnotationDataHandle,
    ) -> Self {
        self.filters
            .push(Filter::AnnotationData(set_handle, data_handle));
        self
    }

    /// Constrain the iterator to only return resources targeted by annotations that have data that corresponds with the passed data.
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a borrowed reference, use [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    pub fn filter_data(mut self, data: Data<'store>) -> Self {
        self.filters.push(Filter::Data(data, FilterMode::Any));
        self
    }

    /// Constrain the iterator to only return text resources targeted by annotations that have data matching the search parameters.
    /// This is a just shortcut method for `self.filter_data( store.find_data(..).to_collection() )`
    ///
    /// Note: This filter is evaluated lazily, it will obtain and check the data for each annotation.
    ////      Do not call this method in a loop, it will be very inefficient! Compute it once before and cache it (`let data = store.find_data(..).to_collection()`), then
    ///       pass the result to [`Self::filter_data(data.clone())`], the clone will be cheap.
    pub fn filter_find_data<'a>(
        self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: DataOperator<'a>,
    ) -> Self
    where
        'a: 'store,
    {
        let store = self.store;
        self.filter_data(store.find_data(set, key, value).to_collection())
    }

    /// Constrain the iterator to only return resources targeted by annotations that have data that corresponds with the passed data.
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have owned data, use [`Self::filter_data()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the annotations and data for each text selection.
    pub fn filter_data_byref(mut self, data: &'store Data<'store>) -> Self {
        self.filters
            .push(Filter::BorrowedData(data, FilterMode::Any));
        self
    }

    /// Constrain the iterator to only return resources targeted by annotations that, in a single annotation, have data that corresponds with *ALL* of the items in the passed data.
    /// All items have to be found or none will be returned.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have a borrowed reference, use [`Self::filter_data_byref_multi()`] instead.
    /// If you want to check for *ANY* match rather than requiring multiple matches in a single annotation, then use [`Self::filter_data()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    pub fn filter_data_multi(mut self, data: Data<'store>) -> Self {
        self.filters.push(Filter::Data(data, FilterMode::All));
        self
    }

    /// Constrain the iterator to only return resources targeted by annotations that, in a single annotation, has data that corresponds with *ALL* of the items in the passed data.
    /// All items have to be found or none will be returned.
    ///
    /// If you have a single AnnotationData instance, use [`Self::filter_annotationdata()`] instead.
    /// If you have owned data, use [`Self::filter_data_multi()`] instead.
    /// If you want to check for *ANY* match rather than requiring multiple matches in a single annotation, then use [`Self::filter_data_byref()`] instead.
    ///
    /// This filter is evaluated lazily, it will obtain and check the data for each annotation.
    pub fn filter_data_byref_multi(mut self, data: &'store Data<'store>) -> Self {
        self.filters
            .push(Filter::BorrowedData(data, FilterMode::All));
        self
    }

    /// Produces the union between two resource iterators
    /// Any filters on either iterator remain valid!
    pub fn union(mut self, other: ResourcesIter<'store>) -> ResourcesIter<'store> {
        if self.iter.is_some() && other.iter.is_some() {
            self.filters.extend(other.filters.into_iter());
            self.iter = Some(self.iter.unwrap().union(other.iter.unwrap()));
        } else if self.iter.is_none() {
            return other;
        }
        self
    }

    /// Produces the intersection between two resource iterators
    /// Any filters on either iterator remain valid!
    pub fn intersection(mut self, other: ResourcesIter<'store>) -> ResourcesIter<'store> {
        if self.iter.is_some() && other.iter.is_some() {
            self.filters.extend(other.filters.into_iter());
            self.iter = Some(self.iter.unwrap().intersection(other.iter.unwrap()));
        } else if self.iter.is_none() {
            return other;
        }
        self
    }

    /// Does this iterator return items in sorted order?
    pub fn returns_sorted(&self) -> bool {
        if let Some(iter) = self.iter.as_ref() {
            iter.returns_sorted()
        } else {
            true //empty iterators can be considered sorted
        }
    }

    /// See if the filters match for the resource
    /// This does not include any filters directly on resources, as those are handled already by the underlying IntersectionsIter
    fn test_filters(&self, resource: &ResultItem<'store, TextResource>) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        let mut annotationfilter: Option<AnnotationsIter> = None;
        for filter in self.filters.iter() {
            match filter {
                Filter::Annotation(handle) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(resource.annotations());
                    }
                    annotationfilter = annotationfilter.map(|iter| iter.filter_handle(*handle));
                }
                Filter::Annotations(annotations) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(resource.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_annotations(annotations.iter()));
                }
                Filter::BorrowedAnnotations(annotations) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(resource.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_annotations(annotations.iter()));
                }
                Filter::AnnotationData(set, data) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(resource.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_annotationdata_handle(*set, *data));
                }
                Filter::Data(data, FilterMode::Any) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(resource.annotations());
                    }
                    annotationfilter = annotationfilter.map(|iter| iter.filter_data_byref(data));
                }
                Filter::BorrowedData(data, FilterMode::Any) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(resource.annotations());
                    }
                    annotationfilter = annotationfilter.map(|iter| iter.filter_data_byref(data));
                }
                Filter::Data(data, FilterMode::All) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(resource.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_data_byref_multi(data));
                }
                Filter::BorrowedData(data, FilterMode::All) => {
                    if annotationfilter.is_none() {
                        annotationfilter = Some(resource.annotations());
                    }
                    annotationfilter =
                        annotationfilter.map(|iter| iter.filter_data_byref_multi(data));
                }
                _ => unimplemented!("Filter {:?} not implemented for ResourcesIter", filter),
            }
        }
        if let Some(annotationfilter) = annotationfilter {
            if !annotationfilter.test() {
                return false;
            }
        }
        true
    }
}

impl<'store> Iterator for ResourcesIter<'store> {
    type Item = ResultItem<'store, TextResource>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iter.as_mut() {
                if let Some(item) = iter.next() {
                    if let Some(resource) = self.store.resource(item) {
                        if !self.test_filters(&resource) {
                            continue;
                        }
                        return Some(resource);
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        None
    }
}
*/

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
            _ => unreachable!(
                "Filter {:?} not implemented for FilteredResources",
                self.filter
            ),
        }
    }
}

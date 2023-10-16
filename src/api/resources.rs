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
use crate::api::annotation::AnnotationsIter;
use crate::api::textselection::TextSelectionsIter;
use crate::resources::TextResource;
use crate::store::*;
use crate::textselection::{TextSelectionOperator, TextSelectionSet};
use crate::IntersectionIter;

use std::borrow::Cow;

impl<'store> ResultItem<'store, TextResource> {
    /// Returns an iterator over all annotations about this resource as a whole, i.e. Annotations with a ResourceSelector.
    /// Such annotations can be considered metadata.
    pub fn annotations_as_metadata(&self) -> AnnotationsIter<'store> {
        let store = self.store();
        if let Some(annotations) = store.annotations_by_resource_metadata(self.handle()) {
            AnnotationsIter::new(
                IntersectionIter::new(Cow::Borrowed(annotations), true),
                self.store(),
            )
        } else {
            AnnotationsIter::new_empty(self.store())
        }
    }

    /// Returns an iterator over all annotations about any text in this resource i.e. Annotations with a TextSelector.
    pub fn annotations_on_text(&self) -> AnnotationsIter<'store> {
        let store = self.store();
        if let Some(iter) = store.annotations_by_resource(self.handle()) {
            let mut data: Vec<_> = iter.collect();
            data.sort_unstable();
            data.dedup();
            AnnotationsIter::new(IntersectionIter::new(Cow::Owned(data), true), self.store())
        } else {
            AnnotationsIter::new_empty(self.store())
        }
    }

    /// Returns an iterator over all annotations that reference this resource, both annotations that can be considered metadata as well
    /// annotations that reference a portion of the text.
    /// Use `annotations_as_metadata()` or `annotations_on_text()` instead if you want to differentiate the two.
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        self.annotations_as_metadata()
            .extend(self.annotations_on_text())
    }

    /// Returns an iterator over all text selections that are marked in this resource (i.e. there are one or more annotations on it).
    /// They are returned in textual order, but this does not come with any significant performance overhead. If you want an unsorted version use [`self.as_ref().textselections_unsorted()`] instead.
    /// Note: This is a double-ended iterator that can be traversed in both directions.
    pub fn textselections(&self) -> TextSelectionsIter<'store> {
        let resource = self.as_ref();
        TextSelectionsIter::new_with_resiterator(resource.iter(), self.rootstore())
    }

    /// Returns a sorted double-ended iterator over a range of all textselections and returns all
    /// textselections that either start or end in this range (depending on the direction you're
    /// iterating in)
    pub fn textselections_in_range(&self, begin: usize, end: usize) -> TextSelectionsIter<'store> {
        let resource = self.as_ref();
        TextSelectionsIter::new_with_resiterator(resource.range(begin, end), self.rootstore())
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
    ) -> TextSelectionsIter<'store> {
        let resource = self.as_ref();
        TextSelectionsIter::new_with_findtextiterator(
            resource.textselections_by_operator(operator, refset.into()),
            self.rootstore(),
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

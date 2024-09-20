/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`AnnotationSubStore`]. This API is implemented on
//! [`ResultItem<AnnotationSubStore>`].

use crate::api::*;
use crate::resources::TextResource;
use crate::substore::{AnnotationSubStore, AnnotationSubStoreHandle};

impl<'store> FullHandle<AnnotationSubStore> for ResultItem<'store, AnnotationSubStore> {
    fn fullhandle(&self) -> <AnnotationSubStore as Storable>::FullHandleType {
        self.handle()
    }
}

impl<'store, I> FullHandleToResultItem<'store, AnnotationSubStore>
    for FromHandles<'store, AnnotationSubStore, I>
where
    I: Iterator<Item = AnnotationSubStoreHandle>,
{
    fn get_item(
        &self,
        handle: AnnotationSubStoreHandle,
    ) -> Option<ResultItem<'store, AnnotationSubStore>> {
        self.store.substore(handle)
    }
}

impl<'store, I> FullHandleToResultItem<'store, AnnotationSubStore>
    for FilterAllIter<'store, AnnotationSubStore, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationSubStore>>,
{
    fn get_item(
        &self,
        handle: AnnotationSubStoreHandle,
    ) -> Option<ResultItem<'store, AnnotationSubStore>> {
        self.store.substore(handle)
    }
}

/// This is the implementation of the high-level API for [`AnnotationSubStore`].
impl<'store> ResultItem<'store, AnnotationSubStore> {
    /// Returns an iterator over all annotations in this substore.
    /// Results will be in chronological order
    pub fn annotations(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        ResultIter::new_sorted(FromHandles::new(
            self.as_ref().annotations.iter().copied(),
            self.store(),
        ))
    }

    /// Returns an iterator over all resources in this substore.
    /// Results will be in chronological order
    pub fn resources(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, TextResource>>> {
        ResultIter::new_sorted(FromHandles::new(
            self.as_ref().resources.iter().copied(),
            self.store(),
        ))
    }

    /// Returns an iterator over all datasets in this substore.
    /// Results will be in chronological order
    pub fn datasets(
        &self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'store, AnnotationDataSet>>> {
        ResultIter::new_sorted(FromHandles::new(
            self.as_ref().annotationsets.iter().copied(),
            self.store(),
        ))
    }

    /// Returns an iterator over all substores ([`AnnotationSubStore`] instances) in under this substore.
    /// The resulting iterator yields items as a fat pointer [`ResultItem<AnnotationSubStore>`]),
    /// which exposes the high-level API. Note that each substore may itself consist of further substores!
    pub fn substores<'a>(
        &'a self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'a, AnnotationSubStore>>> {
        let handle = self.handle();
        let store = self.store();
        ResultIter::new_sorted(store.iter().filter_map(move |a: &'a AnnotationSubStore| {
            if a.parents.contains(&Some(handle)) {
                Some(a.as_resultitem(store, store))
            } else {
                None
            }
        }))
    }
}

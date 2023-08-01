mod annotation;
mod annotationdata;
mod annotationdataset;
mod annotationstore;
mod datakey;
mod resources;
mod text;
mod textselection;

pub use annotation::*;
pub use annotationdata::*;
pub use annotationdataset::*;
pub use annotationstore::*;
pub use datakey::*;
pub use resources::*;
pub use text::*;
pub use textselection::*;

use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Deref;

use crate::annotation::Annotation;
use crate::annotationdataset::AnnotationDataSet;
use crate::resources::TextResource;
use crate::selector::{AncestorVec, Selector, SelectorIter, SelectorIterItem};
use crate::store::*;
use crate::textselection::TextSelection;

// This root module contains some common structures used by multiple parts of the higher-level API.
// See api/* for the high-level API implementations for each STAM object.

pub struct TargetIter<'a, T>
where
    T: Storable,
{
    pub(crate) store: &'a T::StoreType,
    pub(crate) iter: SelectorIter<'a>,
    pub(crate) _phantomdata: PhantomData<T>,
}

impl<'a, T> TargetIter<'a, T>
where
    T: Storable,
{
    pub fn new(store: &'a T::StoreType, iter: SelectorIter<'a>) -> Self {
        Self {
            store,
            iter,
            _phantomdata: PhantomData,
        }
    }
}

pub struct TargetIterItem<'store, T>
where
    T: Storable,
{
    pub(crate) item: ResultItem<'store, T>,
    pub(crate) selectoriteritem: SelectorIterItem<'store>,
}

impl<'a, T> Deref for TargetIterItem<'a, T>
where
    T: Storable,
{
    type Target = ResultItem<'a, T>;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl<'store, T> TargetIterItem<'store, T>
where
    T: Storable,
{
    pub fn depth(&self) -> usize {
        self.selectoriteritem.depth()
    }
    pub fn selector<'b>(&'b self) -> &'b Cow<'store, Selector> {
        self.selectoriteritem.selector()
    }
    pub fn ancestors<'b>(&'b self) -> &'b AncestorVec<'store> {
        self.selectoriteritem.ancestors()
    }
    pub fn is_leaf(&self) -> bool {
        self.selectoriteritem.is_leaf()
    }

    // some copied methods from ResultItem:
    pub fn as_ref(&self) -> &'store T {
        self.item.as_ref()
    }
    pub fn handle(&self) -> T::HandleType {
        self.item.handle()
    }
    pub fn id(&self) -> Option<&'store str> {
        self.item.id()
    }
}

impl<'a> Iterator for TargetIter<'a, TextResource> {
    type Item = TargetIterItem<'a, TextResource>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.selector().as_ref() {
                    Selector::TextSelector(res_id, _, _)
                    | Selector::ResourceSelector(res_id)
                    | Selector::AnnotationSelector(_, Some((res_id, _, _))) => {
                        let resource: &TextResource =
                            self.iter.store.get(*res_id).expect("Resource must exist");
                        return Some(TargetIterItem {
                            item: resource.as_resultitem(self.store, self.iter.store),
                            selectoriteritem: selectoritem,
                        });
                    }
                    _ => continue,
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a> Iterator for TargetIter<'a, AnnotationDataSet> {
    type Item = TargetIterItem<'a, AnnotationDataSet>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.selector().as_ref() {
                    Selector::DataSetSelector(set_id) => {
                        let annotationset: &AnnotationDataSet =
                            self.iter.store.get(*set_id).expect("Dataset must exist");
                        return Some(TargetIterItem {
                            item: annotationset.as_resultitem(self.store, self.iter.store),
                            selectoriteritem: selectoritem,
                        });
                    }
                    _ => continue,
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a> Iterator for TargetIter<'a, Annotation> {
    type Item = TargetIterItem<'a, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.selector().as_ref() {
                    Selector::AnnotationSelector(a_id, _) => {
                        let annotation: &Annotation =
                            self.iter.store.get(*a_id).expect("Annotation must exist");
                        return Some(TargetIterItem {
                            item: annotation.as_resultitem(self.store, self.iter.store),
                            selectoriteritem: selectoritem,
                        });
                    }
                    _ => continue,
                }
            } else {
                return None;
            }
        }
    }
}

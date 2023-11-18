/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This is the root module for the high-level API. See the different submodules for further
//! documentation regarding each STAM object.

mod annotation;
mod annotationdata;
mod annotationdataset;
mod annotationstore;
mod datakey;
mod query;
mod resources;
mod text;
mod textselection;

pub use annotation::*;
pub use annotationdata::*;
pub use annotationdataset::*;
pub use annotationstore::*;
pub use datakey::*;
pub use query::*;
pub use resources::*;
pub use text::*;
pub use textselection::*;

use crate::annotationstore::AnnotationStore;

use std::borrow::Cow;
use std::fmt::Debug;

pub trait HandleCollection<'store>: Debug
where
    Self: 'store + Sized,
{
    type Handle: Copy + Ord;
    type Item;
    type Iter: Iterator<Item = Self::Item>;

    fn array(&self) -> &Cow<'store, [Self::Handle]>;
    fn returns_sorted(&self) -> bool;
    fn store(&self) -> &'store AnnotationStore;

    fn iter(&self) -> Self::Iter;

    /// Low-level method to instantiate annotations from an existing vector of handles (either owned or borrowed).
    /// Warning: Use of this function is dangerous and discouraged in most cases as there is no validity check on the handles you pass!
    fn from_handles(
        array: Cow<'store, [Self::Handle]>,
        sorted: bool,
        store: &'store AnnotationStore,
    ) -> Self;

    /// Low-level method to take the underlying vector of handles
    fn take(self) -> Vec<Self::Handle>;

    /// Returns the number of items in this collection.
    fn len(&self) -> usize {
        self.array().len()
    }

    /// Returns a boolean indicating whether the collection is empty or not.
    fn is_empty(&self) -> bool {
        self.array().is_empty()
    }

    /// Tests if the collection contains a specific element
    fn contains(&self, handle: &Self::Handle) -> bool {
        if self.returns_sorted() {
            match self.array().binary_search(&handle) {
                Ok(_) => true,
                Err(_) => false,
            }
        } else {
            self.array().contains(&handle)
        }
    }
}

/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module implements the low-level concept of a [`Store`], which is essentially a vector over certain
//! items that are [`Storable`]. The items can be retrieved by a [`Handle`], which simply points
//! at an index in the store vector. The [`StoreFor<T>`] trait is implemented on data types that act
//! as a store for a particular storable item. An iterator to iterate over all items in a store is available as well: [`StoreIter`]
//! Do not confuse this more abstract notion of [`Store`] with [`AnnotationStore`].
//!
//! This module also implements a structure [`IdMap`] to map public identifiers (strings) to these internal handles.
//! Moreover, it implements relations maps ([`RelationMap`],[`TripleRelationMap`]) that are used to build the various
//! reverse indices. These map one type of handle to another and effectively define the edges of the graph model.

use sealed::sealed;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;
use std::slice::{Iter, IterMut};

use datasize::{data_size, DataSize};
use minicbor::{Decode, Encode};
use nanoid::nanoid;

use crate::annotationstore::AnnotationStore;
use crate::config::Configurable;
use crate::error::StamError;
use crate::types::*;

/// Type for Store elements. The struct that owns a field of this type should implement the trait [`StoreFor<T>`]
pub type Store<T> = Vec<Option<T>>;

/// A map mapping public IDs to internal ids, implemented as a HashMap.
/// Used to resolve public IDs to internal ones.
#[derive(Debug, Clone, DataSize, Decode, Encode)]
pub struct IdMap<HandleType> {
    /// The actual map
    #[n(0)] //these macros are field index numbers for cbor binary (de)serialisation
    data: HashMap<String, HandleType>,

    /// A prefix that automatically generated IDs will get when added to this map
    #[n(1)]
    autoprefix: String,

    /// Resolve temp IDs
    #[n(2)]
    resolve_temp_ids: bool,
}

impl<HandleType> Default for IdMap<HandleType>
where
    HandleType: Handle,
{
    fn default() -> Self {
        Self {
            data: HashMap::new(),
            autoprefix: "_".to_string(),
            resolve_temp_ids: true,
        }
    }
}

impl<HandleType> IdMap<HandleType>
where
    HandleType: Handle,
{
    pub(crate) fn new(autoprefix: String) -> Self {
        Self {
            autoprefix,
            ..Self::default()
        }
    }

    pub(crate) fn with_resolve_temp_ids(mut self, value: bool) -> Self {
        self.set_resolve_temp_ids(value);
        self
    }

    pub(crate) fn set_resolve_temp_ids(&mut self, value: bool) {
        self.resolve_temp_ids = value;
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn meminfo(&self) -> usize {
        data_size(self)
    }

    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    pub(crate) fn reindex(&mut self, gaps: &[(HandleType, isize)]) {
        for handle in self.data.values_mut() {
            *handle = handle.reindex(gaps);
        }
    }
}

/// This models relations or 'edges' in graph terminology, between handles. It acts as a reverse index is used for various purposes.
#[derive(Debug, Clone, DataSize, Decode, Encode)]
pub(crate) struct RelationMap<A, B> {
    /// The actual map
    #[n(0)]
    pub(crate) data: Vec<Vec<B>>,
    //                   ^-- a Vec is sufficient, we don't need a BTreeSet; the way these maps are used as reverse indices, items are always inserted in sorted order
    #[n(1)]
    _marker: PhantomData<A>, //zero-size, only needed to bind generic A
}

impl<A, B> Default for RelationMap<A, B>
where
    A: Handle,
    B: Handle,
{
    fn default() -> Self {
        Self {
            data: Vec::new(),
            _marker: PhantomData,
        }
    }
}

impl<A, B> RelationMap<A, B>
where
    A: Handle,
    B: Handle,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a relation into the map
    pub fn insert(&mut self, x: A, y: B) {
        if x.as_usize() >= self.data.len() {
            //expand the map
            self.data.resize_with(x.as_usize() + 1, Default::default);
        }
        self.data[x.as_usize()].push(y);
    }

    /// Remove a relation from the map
    pub fn remove(&mut self, x: A, y: B) {
        if let Some(values) = self.data.get_mut(x.as_usize()) {
            if let Some(pos) = values.iter().position(|z| *z == y) {
                values.remove(pos); //note: this shifts the array and may take O(n)
            }
        }
    }

    pub fn get(&self, x: A) -> Option<&Vec<B>> {
        self.data.get(x.as_usize())
    }

    pub fn totalcount(&self) -> usize {
        let mut total = 0;
        for v in self.data.iter() {
            total += v.len();
        }
        total
    }

    /// Like countinfo(), but returns an extra value at the end of the tuple with the lower-bound estimated memory consumption in bytes.
    pub fn meminfo(&self) -> usize {
        data_size(self)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn shrink_to_fit(&mut self, recursive: bool) {
        if recursive {
            for element in self.data.iter_mut() {
                element.shrink_to_fit();
            }
        }
        self.data.shrink_to_fit();
    }

    /// Returns a new reindexed map, copies all contents, not the most efficient
    pub(crate) fn reindex(&self, gaps_a: &[(A, isize)], gaps_b: &[(B, isize)]) -> Self {
        let mut newmap = Self::new();
        for (handle_a, item) in self.data.iter().enumerate() {
            let handle_a = A::new(handle_a).reindex(gaps_a);
            for handle_b in item {
                let handle_b = handle_b.reindex(gaps_b);
                newmap.insert(handle_a, handle_b);
            }
        }
        newmap
    }
}

impl<A, B> Extend<(A, B)> for RelationMap<A, B>
where
    A: Handle,
    B: Handle,
{
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (A, B)>,
    {
        for (x, y) in iter {
            self.insert(x, y);
        }
    }
}

/// This models relations or 'edges' in graph terminology, between handles. It acts as a reverse index is used for various purposes.
#[derive(Debug, Clone, DataSize, Decode, Encode)]
pub(crate) struct RelationBTreeMap<A, B>
where
    A: Handle,
    B: Handle,
{
    /// The actual map
    #[n(0)]
    pub(crate) data: BTreeMap<A, Vec<B>>,
    //                           ^-- a Vec is sufficient, we don't need a BTreeSet; the way these maps are used as reverse indices, items are always inserted in sorted order
}

impl<A, B> Default for RelationBTreeMap<A, B>
where
    A: Handle,
    B: Handle,
{
    fn default() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl<A, B> RelationBTreeMap<A, B>
where
    A: Handle,
    B: Handle,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a relation into the map
    pub fn insert(&mut self, x: A, y: B) {
        if self.data.contains_key(&x) {
            self.data.get_mut(&x).unwrap().push(y);
        } else {
            self.data.insert(x, vec![y]);
        }
    }

    /// Remove a relation from the map
    pub fn remove(&mut self, x: A, y: B) {
        if let Some(values) = self.data.get_mut(&x) {
            if let Some(pos) = values.iter().position(|z| *z == y) {
                values.remove(pos); //note: this shifts the array and may take O(n)
            }
        }
    }

    pub fn get(&self, x: A) -> Option<&Vec<B>> {
        self.data.get(&x)
    }

    pub fn totalcount(&self) -> usize {
        let mut total = 0;
        for v in self.data.values() {
            total += v.len();
        }
        total
    }

    /// Like countinfo(), but returns an extra value at the end of the tuple with the lower-bound estimated memory consumption in bytes.
    pub fn meminfo(&self) -> usize {
        data_size(self)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn shrink_to_fit(&mut self, recursive: bool) {
        if recursive {
            for element in self.data.values_mut() {
                element.shrink_to_fit();
            }
        }
    }

    /// Returns a new reindexed map, copies all contents, not the most efficient
    pub(crate) fn reindex(&self, gaps_a: &[(A, isize)], gaps_b: &[(B, isize)]) -> Self {
        let mut newmap = Self::new();
        for (handle_a, item) in self.data.iter() {
            let handle_a = handle_a.reindex(gaps_a);
            for handle_b in item {
                let handle_b = handle_b.reindex(gaps_b);
                newmap.insert(handle_a, handle_b);
            }
        }
        newmap
    }
}

impl<A, B> Extend<(A, B)> for RelationBTreeMap<A, B>
where
    A: Handle,
    B: Handle,
{
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (A, B)>,
    {
        for (x, y) in iter {
            self.insert(x, y);
        }
    }
}

#[derive(Debug, Clone, DataSize, Decode, Encode)]
pub(crate) struct TripleRelationMap<A, B, C> {
    /// The actual map
    #[n(0)]
    pub(crate) data: Vec<RelationMap<B, C>>,
    #[n(1)]
    _marker: PhantomData<A>,
}

impl<A, B, C> Default for TripleRelationMap<A, B, C> {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            _marker: PhantomData,
        }
    }
}

impl<A, B, C> TripleRelationMap<A, B, C>
where
    A: Handle,
    B: Handle,
    C: Handle,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, x: A, y: B, z: C) {
        if x.as_usize() >= self.data.len() {
            //expand the map
            self.data.resize_with(x.as_usize() + 1, Default::default);
        }
        self.data[x.as_usize()].insert(y, z);
    }

    pub fn get(&self, x: A, y: B) -> Option<&Vec<C>> {
        if let Some(v) = self.data.get(x.as_usize()) {
            v.get(y)
        } else {
            None
        }
    }

    pub fn totalcount(&self) -> usize {
        let mut total = 0;
        for v in self.data.iter() {
            total += v.totalcount();
        }
        total
    }

    /// Returns partcial count, does not count the deepest layer
    pub fn partialcount(&self) -> usize {
        let mut total = 0;
        for v in self.data.iter() {
            total += v.len();
        }
        total
    }

    /// Like countinfo(), but returns an extra value at the end of the tuple with the lower-estimate  memory consumption in bytes.
    pub fn meminfo(&self) -> usize {
        data_size(self)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn shrink_to_fit(&mut self, recursive: bool) {
        if recursive {
            for element in self.data.iter_mut() {
                element.shrink_to_fit(recursive);
            }
        }
        self.data.shrink_to_fit();
    }

    /// Returns a new reindexed map, copies all contents, not the most efficient
    pub(crate) fn reindex(
        &self,
        gaps_a: &[(A, isize)],
        gaps_b: &[(B, isize)],
        gaps_c: &[(C, isize)],
    ) -> Self {
        let mut newmap = Self::new();
        for (handle_a, inner) in self.data.iter().enumerate() {
            let handle_a = A::new(handle_a).reindex(gaps_a);
            for (handle_b, item) in inner.data.iter().enumerate() {
                let handle_b = B::new(handle_b).reindex(gaps_b);
                for handle_c in item {
                    let handle_c = handle_c.reindex(gaps_c);
                    newmap.insert(handle_a, handle_b, handle_c);
                }
            }
        }
        newmap
    }
}

impl<A, B, C> Extend<(A, B, C)> for TripleRelationMap<A, B, C>
where
    A: Handle,
    B: Handle,
    C: Handle,
{
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (A, B, C)>,
    {
        for (x, y, z) in iter {
            self.insert(x, y, z);
        }
    }
}

#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait Storable: PartialEq + TypeInfo + Debug + Sized {
    type HandleType: Handle;
    type StoreType: StoreFor<Self>;

    /// Retrieve the internal (numeric) id. For any type T uses in `StoreFor<T>`, this may be None only in the initial
    /// stage when it is still unbounded to a store.
    fn handle(&self) -> Option<Self::HandleType> {
        None
    }

    /// Like [`Self::handle()`] but returns a [`StamError::Unbound`] error if there is no internal id.
    fn handle_or_err(&self) -> Result<Self::HandleType, StamError> {
        self.handle().ok_or(StamError::Unbound(""))
    }

    /// Get the public ID
    fn id(&self) -> Option<&str> {
        None
    }

    /// Generate a temporary public ID based on the internal handle.
    fn temp_id(&self) -> Result<String, StamError> {
        Ok(format!(
            "{}{}",
            Self::temp_id_prefix(),
            self.handle_or_err()?.as_usize()
        ))
    }

    /// Does this type support an ID?
    fn carries_id() -> bool;

    /// Returns the item as a ResultItem, i.e. a wrapped reference that includes a reference to
    /// both this item as well as the store that owns it. All high-level API functions are implemented
    /// on such Result types. You should not need to invoke this yourself.
    fn as_resultitem<'store>(
        &'store self,
        store: &'store Self::StoreType,
        rootstore: &'store AnnotationStore,
    ) -> ResultItem<'store, Self>
    where
        Self: Sized,
    {
        ResultItem::new(self, store, rootstore)
    }

    /// Set the internal ID for an item. May only be called once just after instantiation.
    /// This is a low-level API method that can not be used publicly due to ownership restrictions.
    fn with_handle(self, _handle: <Self as Storable>::HandleType) -> Self {
        //no-op in default implementation
        self
    }

    /// Generate a random ID in a given idmap (adds it to the map and assigns it to the item)
    fn generate_id(self, idmap: Option<&mut IdMap<Self::HandleType>>) -> Self
    where
        Self: Sized,
    {
        if let Some(intid) = self.handle() {
            if let Some(idmap) = idmap {
                loop {
                    let id = format!("{}{}", idmap.autoprefix, nanoid!());
                    let id_copy = id.clone();
                    if idmap.data.insert(id, intid).is_none() {
                        //checks for collisions (extremely unlikely)
                        //returns none if the key did not exist yet
                        return self.with_id(id_copy);
                    }
                }
            }
        }
        // if the item is not bound or has no IDmap, we can't check collisions, but that's okay
        self.with_id(format!("X{}", nanoid!()))
    }

    /// Builder pattern to set the public ID
    #[allow(unused_variables)]
    fn with_id(self, id: impl Into<String>) -> Self
    where
        Self: Sized,
    {
        if Self::carries_id() {
            unimplemented!("with_id() not implemented");
        }
        //no-op
        self
    }
}

/// This trait is implemented on types that provide storage for a certain other generic type (T)
/// It is a sealed trait, not implementable outside this crate.
#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait StoreFor<T: Storable>: Configurable + private::StoreCallbacks<T> {
    /// Get a reference to the entire store for the associated type
    /// This is a low-level API method.
    fn store(&self) -> &Store<T>;

    /// Get a mutable reference to the entire store for the associated type
    /// This is a low-level API method.
    fn store_mut(&mut self) -> &mut Store<T>;

    /// Get a reference to the id map for the associated type, mapping global ids to internal ids
    /// This is a low-level API method.
    fn idmap(&self) -> Option<&IdMap<T::HandleType>> {
        None
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    /// This is a low-level API method.
    fn idmap_mut(&mut self) -> Option<&mut IdMap<T::HandleType>> {
        None
    }

    fn store_typeinfo() -> &'static str;

    /// Adds an item to the store. Returns a handle to it upon success.
    fn insert(&mut self, mut item: T) -> Result<T::HandleType, StamError> {
        debug(self.config(), || {
            format!("StoreFor<{}>.insert: new item", Self::store_typeinfo())
        });
        let handle = if let Some(intid) = item.handle() {
            intid
        } else {
            // item has no internal id yet, i.e. it is unbound
            // we generate an id and bind it now
            let intid = self.next_handle();

            // Bind an item to the store *PRIOR* to it being actually added:

            //we already pass the internal id this item will get upon the next insert()
            //so it knows its internal id immediate after construction
            if item.handle().is_some() {
                return Err(StamError::AlreadyBound("bind()"));
            } else {
                item = item.with_handle(self.next_handle());
            }
            intid
        };

        if T::carries_id() {
            //insert a mapping from the public ID to the internal numeric ID in the idmap
            if let Some(id) = item.id() {
                //check if public ID does not already exist
                if self.has(id) {
                    //ok. the already ID exists, now is the existing item exactly the same as the item we're about to insert?
                    //in that case we can discard this error and just return the existing handle without actually inserting a new one
                    let existing_item = self.get(id).unwrap();
                    if *existing_item == item {
                        return Ok(existing_item.handle().unwrap());
                    }
                    //in all other cases, we return an error
                    return Err(StamError::DuplicateIdError(
                        id.to_string(),
                        Self::store_typeinfo(),
                    ));
                }

                self.idmap_mut().map(|idmap| {
                    //                 v-- MAYBE TODO: optimise the id copy away
                    idmap.data.insert(id.to_string(), item.handle().unwrap())
                });

                debug(self.config(), || {
                    format!(
                        "StoreFor<{}>.insert: ^--- id={:?}",
                        Self::store_typeinfo(),
                        id
                    )
                });
            } else if self.config().generate_ids {
                item = item.generate_id(self.idmap_mut());
                debug(self.config(), || {
                    format!(
                        "StoreFor<{}>.insert: ^--- autogenerated id {}",
                        Self::store_typeinfo(),
                        item.id().unwrap(),
                    )
                });
            }
        }

        self.preinsert(&mut item)?;

        //add the resource
        self.store_mut().push(Some(item));

        self.inserted(handle)?;

        debug(self.config(), || {
            format!(
                "StoreFor<{}>.insert: ^--- {:?} (insertion complete now)",
                Self::store_typeinfo(),
                handle
            )
        });

        assert_eq!(handle, T::HandleType::new(self.store().len() - 1), "sanity check to ensure no item can determine its own internal id that does not correspond with what's allocated
");

        Ok(handle)
    }

    /// Inserts items into the store using a builder pattern
    fn add(mut self, item: T) -> Result<Self, StamError>
    where
        Self: Sized,
    {
        self.insert(item)?;
        Ok(self)
    }

    /// Returns true if the store has the item
    fn has(&self, item: impl Request<T>) -> bool {
        if let Some(handle) = item.to_handle(self) {
            self.store().get(handle.as_usize()).is_some()
        } else {
            false
        }
    }

    /// Get a reference to an item from the store, by handle, without checking validity.
    ///
    /// ## Safety
    /// Calling this method with an out-of-bounds index is [undefined behavior](https://doc.rust-lang.org/reference/behavior-considered-undefined.html)  │       
    /// even if the resulting reference is not used.                                                                                                     │       
    unsafe fn get_unchecked(&self, handle: T::HandleType) -> Option<&T> {
        self.store().get_unchecked(handle.as_usize()).as_ref()
    }

    /// Get a reference to an item from the store
    /// This is a low-level API method, you usually want to use dedicated high-level methods like `annotation()`, `resource()` instead.
    fn get(&self, item: impl Request<T>) -> Result<&T, StamError> {
        if let Some(handle) = item.to_handle(self) {
            if let Some(Some(item)) = self.store().get(handle.as_usize()) {
                return Ok(item);
            }
        }
        Err(StamError::HandleError(Self::store_typeinfo()))
    }

    /// Get a mutable reference to an item from the store by internal ID
    /// This is a low-level API method
    fn get_mut(&mut self, item: impl Request<T>) -> Result<&mut T, StamError> {
        if let Some(handle) = item.to_handle(self) {
            if let Some(Some(item)) = self.store_mut().get_mut(handle.as_usize()) {
                return Ok(item);
            }
        }
        Err(StamError::HandleError(Self::store_typeinfo()))
    }

    /// Removes an item by handle, returns an error if the item has dependencies and can't be removed
    fn remove(&mut self, handle: T::HandleType) -> Result<(), StamError> {
        //callback to remove the item from relation maps, may return an error and refuse to remove an item
        self.preremove(handle)?;

        //remove item from idmap
        if let Some(Some(item)) = self.store().get(handle.as_usize()) {
            let id: Option<String> = item.id().map(|x| x.to_string());
            if let Some(id) = id {
                if let Some(idmap) = self.idmap_mut() {
                    idmap.data.remove(id.as_str());
                }
            }
        } else {
            return Err(StamError::HandleError(
                "Unable to remove non-existing handle",
            ));
        }

        //now remove the actual item, removing means just setting its previously occupied index to None
        //(and the actual item is owned so will be deallocated)
        let item = self.store_mut().get_mut(handle.as_usize()).unwrap();
        *item = None;
        Ok(())
    }

    /// Resolves an ID to a handle.
    /// Also works for temporary IDs if enabled.
    /// This is a low-level API method. You usually don't want to call this directly.
    fn resolve_id(&self, id: &str) -> Result<T::HandleType, StamError> {
        if let Some(idmap) = self.idmap() {
            if idmap.resolve_temp_ids {
                if let Some(handle) = resolve_temp_id(id) {
                    return Ok(T::HandleType::new(handle));
                }
            }
            if let Some(handle) = idmap.data.get(id) {
                Ok(*handle)
            } else {
                Err(StamError::IdNotFoundError(
                    id.to_string(),
                    Self::store_typeinfo(),
                ))
            }
        } else {
            Err(StamError::NoIdError(Self::store_typeinfo()))
        }
    }

    /// Iterate over all items in the store
    /// This is a low-level API method, use dedicated high-level iterators like `annotations()`, `resources()` instead.  
    fn iter(&self) -> StoreIter<T>
    where
        T: Storable<StoreType = Self>,
    {
        StoreIter {
            iter: self.store().iter(),
            count: 0,
            len: self.store().len(),
        }
    }

    /// Iterate over the store, mutably
    /// This is a low-level API method.
    fn iter_mut(&mut self) -> StoreIterMut<T> {
        let len = self.store().len();
        StoreIterMut {
            iter: self.store_mut().iter_mut(),
            count: 0,
            len,
        }
    }

    /// Return the internal id that will be assigned for the next item to the store
    /// This is a low-level API method.
    fn next_handle(&self) -> T::HandleType {
        T::HandleType::new(self.store().len()) //this is one of the very few places in the code where we create a handle from scratch
    }

    /// Return the internal id that was assigned to last inserted item
    /// This is a low-level API method.
    fn last_handle(&self) -> T::HandleType {
        T::HandleType::new(self.store().len() - 1)
    }
}

pub(crate) mod private {
    //we need a public trait in a private mod as a trick to have a sealed traits (private supertraits to publicly exposed traits)
    //None of these traits and methods within are exposed publicly

    pub trait StoreCallbacks<T: crate::store::Storable> {
        /// Called prior to inserting an item into to the store
        /// If it returns an error, the insert will be cancelled.
        /// Allows for bookkeeping such as inheriting configuration
        /// parameters from parent to the item
        #[allow(unused_variables)]
        #[doc(hidden)]
        fn preinsert(&self, item: &mut T) -> Result<(), crate::error::StamError> {
            //default implementation does nothing
            Ok(())
        }

        /// Called after an item was inserted to the store
        /// Allows the store to do further bookkeeping
        /// like updating relation maps
        #[allow(unused_variables)]
        #[doc(hidden)]
        fn inserted(&mut self, handle: T::HandleType) -> Result<(), crate::error::StamError> {
            //default implementation does nothing
            Ok(())
        }

        /// Called before an item is removed from the store
        /// Allows the store to do further bookkeeping
        /// like updating relation maps
        #[allow(unused_variables)]
        #[doc(hidden)]
        fn preremove(&mut self, handle: T::HandleType) -> Result<(), crate::error::StamError> {
            //default implementation does nothing
            Ok(())
        }
    }
}

pub(crate) trait WrappableStore<T: Storable>: StoreFor<T> {
    /// Wraps the entire store along with a reference to self
    /// Low-level method that you won't need
    fn wrap_store(&self) -> WrappedStore<T, Self>
    where
        Self: Sized,
    {
        WrappedStore {
            store: self.store(),
            parent: self,
        }
    }
}

pub(crate) trait ReindexStore<T>
where
    T: Storable,
{
    fn gaps(&self) -> Vec<(T::HandleType, isize)>;
    fn reindex(self, gaps: &[(T::HandleType, isize)]) -> Self;
}

impl<T> ReindexStore<T> for Vec<Option<T>>
where
    T: Storable,
{
    /// Low-level method that returns gaps in the store
    /// The gaps can be resolved by calling `reindex()`.
    fn gaps(&self) -> Vec<(T::HandleType, isize)> {
        let mut gaps = Vec::new();
        let mut gapsize: isize = 0;
        for item in self.iter() {
            if item.is_none() {
                gapsize -= 1;
            } else if gapsize != 0 {
                let handle = item.as_ref().unwrap().handle().expect("must have handle");
                gaps.push((handle, gapsize));
                gapsize = 0;
            }
        }
        gaps
    }

    fn reindex(self, gaps: &[(T::HandleType, isize)]) -> Self {
        if !gaps.is_empty() {
            let totaldelta: isize = gaps.iter().map(|x| x.1).sum();
            let newsize: usize = (self.len() as isize + totaldelta) as usize;
            if newsize == 0 {
                return Vec::new();
            }
            let mut newstore: Vec<Option<T>> = Vec::with_capacity(newsize);
            for item in self {
                if let Some(mut item) = item {
                    let handle = item.handle().expect("handle must exist");
                    let newhandle = handle.reindex(gaps); //this does iterate over all gaps every time, not very efficient if there are many
                    item = item.with_handle(newhandle);
                    newstore.push(Some(item));
                }
            }
            return newstore;
        }
        self
    }
}

//  generic iterator implementations, these take care of skipping over deleted items (None)

/// This is the iterator to iterate over a Store,  it is created by the iter() method from the [`StoreFor<T>`] trait
/// It produces a references to the item wrapped in a fat pointer ([`ResultItem<T>`]) that also contains reference to the store
/// and which is immediately implements various methods for working with the type.
pub struct StoreIter<'store, T>
where
    T: Storable,
{
    iter: Iter<'store, Option<T>>,
    count: usize,
    len: usize,
}

impl<'store, T> Iterator for StoreIter<'store, T>
where
    T: Storable,
{
    type Item = &'store T;

    fn next(&mut self) -> Option<Self::Item> {
        self.count += 1;
        loop {
            match self.iter.next() {
                Some(Some(item)) => return Some(item),
                Some(None) => continue,
                None => return None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let l = self.len - self.count;
        //the lower-bound may be an overestimate (if there are deleted items)
        (l, Some(l))
    }
}

/// Mutable variant of [`StoreIter<T>`], but unlike that one this does not wrap results in a fat pointer but returns them directly, ready for mutation.
pub struct StoreIterMut<'a, T> {
    iter: IterMut<'a, Option<T>>,
    count: usize,
    len: usize,
}

impl<'a, T> Iterator for StoreIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.count += 1;
        loop {
            match self.iter.next() {
                Some(Some(item)) => return Some(item),
                Some(None) => continue,
                None => return None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let l = self.len - self.count;
        //the lower-bound may be an overestimate (if there are deleted items)
        (l, Some(l))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This is a smart pointer that encapsulates both the item and the store that owns it.
/// It allows the item to have some more introspection as it knows who its immediate parent is.
/// It is heavily used as a return type all throughout the higher-level API. Most API traits
/// are implemented for a particular variant of this type.
pub struct ResultItem<'store, T>
where
    T: Storable,
{
    // a reference to the item
    item: &'store T,

    // a reference the store that holds the item
    store: &'store T::StoreType,

    // a reference to the root AnnotationStore, can lead to some duplication if it's the same as store
    rootstore: Option<&'store AnnotationStore>,
}

#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
impl<'store, T> TypeInfo for ResultItem<'store, T>
where
    T: Storable + TypeInfo,
{
    fn typeinfo() -> Type {
        T::typeinfo()
    }
}

impl<'store, T> Debug for ResultItem<'store, T>
where
    T: Storable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultItem")
            .field("item", &self.item)
            .finish()
    }
}

impl<'store, T> Clone for ResultItem<'store, T>
where
    T: Storable + Clone,
{
    fn clone(&self) -> Self {
        Self {
            item: self.item,
            store: self.store,
            rootstore: self.rootstore,
        }
    }
}

impl<'store, T> PartialEq for ResultItem<'store, T>
where
    T: Storable,
{
    fn eq(&self, other: &Self) -> bool {
        self.handle() == other.handle()
    }
}
impl<'store, T> Eq for ResultItem<'store, T> where T: Storable {}
impl<'store, T> Hash for ResultItem<'store, T>
where
    T: Storable,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handle().hash(state)
    }
}
impl<'store, T> PartialOrd for ResultItem<'store, T>
where
    T: Storable,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.handle().cmp(&other.handle()))
    }
}
impl<'store, T> Ord for ResultItem<'store, T>
where
    T: Storable,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.handle().cmp(&other.handle())
    }
}

impl<'store, T> ResultItem<'store, T>
where
    T: Storable,
{
    /// Create a new result item. Not public, called by [`StoreFor<T>::as_resultitem()`].
    /// will panic if called on an unbound item!
    pub(crate) fn new(
        item: &'store T,
        store: &'store T::StoreType,
        rootstore: &'store AnnotationStore,
    ) -> Self {
        if item.handle().is_none() {
            panic!("can't wrap unbound items");
        }
        Self {
            item,
            store,
            rootstore: Some(rootstore),
        }
    }

    /// Create a new result item. Not public.
    /// Partial result items are dangerous as they are not bound to a rootstore, and
    /// this may cause run-time panics elsewhere.
    /// Do not return them in the public API!
    pub(crate) fn new_partial(item: &'store T, store: &'store T::StoreType) -> Self {
        if item.handle().is_none() {
            panic!("can't wrap unbound items");
        }
        Self {
            item,
            store,
            rootstore: None,
        }
    }

    /// Get the store this item is a direct member of
    pub fn store(&self) -> &'store T::StoreType {
        self.store
    }

    /// Get the underlying AnnotationStore
    pub fn rootstore(&self) -> &'store AnnotationStore {
        // This will panic for partial result items!
        self.rootstore
            .expect("Got a partial ResultItem, unable to get root annotationstore! This should not happen in the public API.")
    }

    /// Returns the contained reference with the original lifetime
    pub fn as_ref(&self) -> &'store T {
        self.item
    }

    /// Get the handle (internal identifier) for the contained item
    pub fn handle(&self) -> T::HandleType {
        self.item
            .handle()
            .expect("handle was already guaranteed for ResultItem, this should always work")
    }

    /// Get the public identifier for the contained item
    pub fn id(&self) -> Option<&'store str> {
        self.item.id()
    }
}

pub trait StamResult<T>
where
    T: TypeInfo,
{
    fn or_fail(self) -> Result<T, StamError>;
}

impl<'store, T> StamResult<T> for Option<T>
where
    T: TypeInfo,
{
    fn or_fail(self) -> Result<T, StamError> {
        match self {
            Some(item) => Ok(item),
            None => Err(StamError::NotFoundError(T::typeinfo(), "")),
        }
    }
}

#[sealed]
impl<T> TypeInfo for Option<ResultItem<'_, T>>
where
    T: Storable,
{
    fn typeinfo() -> Type {
        T::typeinfo()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// the following structure may be a bit obscure but it required internally to
// make serialization via serde work on our stores
// (ideally it needn't be public)

/// Helper structure that contains a store and a reference to self. Mostly for internal use.
pub(crate) struct WrappedStore<'a, T, S: StoreFor<T>>
where
    T: Storable,
    S: Sized,
{
    pub(crate) store: &'a Store<T>,
    pub(crate) parent: &'a S,
}

impl<'a, T, S> Deref for WrappedStore<'a, T, S>
where
    T: Storable,
    S: StoreFor<T>,
{
    type Target = Store<T>;

    fn deref(&self) -> &Self::Target {
        self.store
    }
}

///////////////////////////////// Any

#[derive(Debug, Deserialize)]
#[serde(untagged)]
/// `BuildItem` offers various ways of referring to a data structure of type `T` in the core STAM model
/// It abstracts over public IDs (both owned an and borrowed), handles, and references.
pub enum BuildItem<'a, T>
where
    T: Storable,
{
    Id(String), //for deserialisation only this variant is available

    #[serde(skip)]
    IdRef(&'a str),

    #[serde(skip)]
    Ref(&'a T),

    #[serde(skip)]
    Handle(T::HandleType),

    #[serde(skip)]
    None,
}

impl<'a, T> Clone for BuildItem<'a, T>
where
    T: Storable,
{
    fn clone(&self) -> Self {
        match self {
            Self::Id(s) => Self::Id(s.clone()),
            Self::IdRef(s) => Self::Id(s.to_string()),
            Self::Ref(r) => Self::Ref(*r),
            Self::Handle(h) => Self::Handle(*h),
            Self::None => Self::None,
        }
    }
}

impl<'a, T> Default for BuildItem<'a, T>
where
    T: Storable,
{
    fn default() -> Self {
        Self::None
    }
}

impl<'a, T> PartialEq for BuildItem<'a, T>
where
    T: Storable,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Handle(x), Self::Handle(y)) => x == y,
            (Self::Id(x), Self::Id(y)) => x == y,
            (Self::IdRef(x), Self::IdRef(y)) => x == y,
            (Self::Id(x), Self::IdRef(y)) => x.as_str() == *y,
            (Self::IdRef(x), Self::Id(y)) => *x == y.as_str(),
            (Self::Ref(x), Self::Ref(y)) => x == y,
            (Self::Ref(x), Self::Id(y)) => x.id() == Some(y.as_str()),
            (Self::Ref(x), Self::IdRef(y)) => x.id() == Some(y),
            (Self::Id(x), Self::Ref(y)) => Some(x.as_str()) == y.id(),
            (Self::IdRef(x), Self::Ref(y)) => Some(*x) == y.id(),
            _ => false,
        }
    }
}

impl<'a, T> BuildItem<'a, T>
where
    T: Storable,
{
    pub fn is_handle(&self) -> bool {
        matches!(self, Self::Handle(_))
    }

    pub fn is_id(&self) -> bool {
        match self {
            Self::Id(_) => true,
            Self::IdRef(_) => true,
            _ => false,
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    pub fn is_some(&self) -> bool {
        !matches!(self, Self::None)
    }

    // raises an ID error
    pub(crate) fn error(&self, contextmsg: &'static str) -> StamError {
        match self {
            Self::Handle(_) => StamError::HandleError(contextmsg),
            Self::Id(id) => StamError::IdNotFoundError(id.to_string(), contextmsg),
            Self::IdRef(id) => StamError::IdNotFoundError(id.to_string(), contextmsg),
            Self::Ref(instance) => StamError::IdNotFoundError(
                instance.id().unwrap_or("(no id)").to_string(),
                contextmsg,
            ),
            Self::None => StamError::Unbound("Supplied AnyId is not bound to anything!"),
        }
    }

    /// Returns the ID as a new string, returns None if only handle is contained
    pub fn to_string(self) -> Option<String> {
        if let Self::Id(s) = self {
            Some(s)
        } else if let Self::IdRef(s) = self {
            Some(s.to_string())
        } else {
            None
        }
    }

    /// Returns the ID as str, returns None if only handle is contained
    pub fn as_str<'slf>(&'slf self) -> Option<&'slf str> {
        if let Self::Id(s) = self {
            Some(s.as_str())
        } else if let Self::IdRef(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /*
    pub fn to_id<'slf, 'store, S>(&'slf self, store: &'store S) -> Option<&'slf str>
    where
        S: StoreFor<T>,
        'store: 'slf,
    {
        match self {
            BuildItem::Id(id) => Some(id.as_str()),
            BuildItem::IdRef(id) => Some(id),
            BuildItem::Handle(_) => {
                if let Some(instance) = self.to_ref(store) {
                    instance.id()
                } else {
                    None
                }
            }
            BuildItem::Ref(instance) => instance.id(),
            BuildItem::None => None,
        }
    }
    */
}

pub trait Request<T>
where
    T: Storable,
    Self: Sized,
{
    /// Returns the handle for this item, looking it up in the store
    fn to_handle<'store, S>(&self, store: &'store S) -> Option<T::HandleType>
    where
        S: StoreFor<T>;

    /// If this type encapsulated an Id, this returns it (borrowed)
    fn requested_id(&self) -> Option<&str> {
        None
    }
    /// If this type encapsulated an Id, this returns it (oened)
    fn requested_id_owned(self) -> Option<String> {
        None
    }
    /// If this type encapsulated a handle, this returns it
    fn requested_handle(&self) -> Option<T::HandleType> {
        None
    }

    /// Represents a request for any value in certain contexts
    fn any(&self) -> bool {
        false
    }

    /*
    /// Resolves a requested item from a store, producing a ResultItem.
    fn resolve<'store, S>(self, store: &'store S) -> Option<ResultItem<'store, T>>
    where
        S: StoreFor<T>,
    {
        if let Ok(item) = store.get(self) {
            Some(ResultItem { store, item })
        } else {
            None
        }
    }
    */
}

impl<'a, T> Request<T> for &'a str
where
    T: Storable,
{
    fn to_handle<'store, S>(&self, store: &'store S) -> Option<T::HandleType>
    where
        S: StoreFor<T>,
    {
        store.resolve_id(self).ok()
    }
    fn requested_id(&self) -> Option<&'a str> {
        Some(self)
    }
    fn requested_id_owned(self) -> Option<String> {
        Some(self.to_string())
    }
    fn any(&self) -> bool {
        self.is_empty()
    }
}

impl<'a, T> Request<T> for bool
where
    T: Storable,
{
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<T::HandleType>
    where
        S: StoreFor<T>,
    {
        None
    }
    fn any(&self) -> bool {
        true
    }
}

impl<'a, T> Request<T> for String
where
    T: Storable,
{
    fn to_handle<'store, S>(&self, store: &'store S) -> Option<T::HandleType>
    where
        S: StoreFor<T>,
    {
        store.resolve_id(self.as_str()).ok()
    }
    fn requested_id(&self) -> Option<&str> {
        Some(self.as_str())
    }
    fn requested_id_owned(self) -> Option<String> {
        Some(self)
    }
    fn any(&self) -> bool {
        self.is_empty()
    }
}

impl<'a, T> Request<T> for ResultItem<'a, T>
where
    T: Storable,
{
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<T::HandleType>
    where
        S: StoreFor<T>,
    {
        Some(self.handle())
    }
}

impl<'a, T> Request<T> for &ResultItem<'a, T>
where
    T: Storable,
{
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<T::HandleType>
    where
        S: StoreFor<T>,
    {
        Some(self.handle())
    }
}

impl<'a, T> Request<T> for BuildItem<'a, T>
where
    T: Storable,
{
    fn to_handle<'store, S>(&self, store: &'store S) -> Option<T::HandleType>
    where
        S: StoreFor<T>,
    {
        match self {
            BuildItem::Id(id) => store.resolve_id(id.as_str()).ok(),
            BuildItem::IdRef(id) => store.resolve_id(id).ok(),
            BuildItem::Handle(handle) => Some(*handle),
            BuildItem::Ref(instance) => instance.handle(),
            BuildItem::None => None,
        }
    }
}

impl<'a, T> Request<T> for &BuildItem<'a, T>
where
    T: Storable,
{
    fn to_handle<'store, S>(&self, store: &'store S) -> Option<T::HandleType>
    where
        S: StoreFor<T>,
    {
        match self {
            BuildItem::Id(id) => store.resolve_id(id.as_str()).ok(),
            BuildItem::IdRef(id) => store.resolve_id(id).ok(),
            BuildItem::Handle(handle) => Some(*handle),
            BuildItem::Ref(instance) => instance.handle(),
            BuildItem::None => None,
        }
    }
}

impl<'a, T> From<&'a str> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(id: &'a str) -> Self {
        if id.is_empty() {
            Self::None
        } else {
            Self::IdRef(id)
        }
    }
}
impl<'a, T> From<Option<&'a str>> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(id: Option<&'a str>) -> Self {
        if let Some(id) = id {
            if id.is_empty() {
                Self::None
            } else {
                Self::IdRef(id)
            }
        } else {
            Self::None
        }
    }
}

impl<'a, T> From<String> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(id: String) -> Self {
        if id.is_empty() {
            Self::None
        } else {
            Self::Id(id)
        }
    }
}

impl<'a, T> From<&'a String> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(id: &'a String) -> Self {
        if id.is_empty() {
            Self::None
        } else {
            Self::IdRef(id.as_str())
        }
    }
}

impl<'a, T> From<Option<String>> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(id: Option<String>) -> Self {
        if let Some(id) = id {
            if id.is_empty() {
                Self::None
            } else {
                Self::Id(id)
            }
        } else {
            Self::None
        }
    }
}

impl<'a, T> From<&'a T> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(instance: &'a T) -> Self {
        Self::Ref(instance)
    }
}

impl<'a, T> From<usize> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(handle: usize) -> Self {
        Self::Handle(T::HandleType::new(handle))
    }
}

impl<'a, T> From<Option<usize>> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(handle: Option<usize>) -> Self {
        if let Some(handle) = handle {
            Self::Handle(T::HandleType::new(handle))
        } else {
            Self::None
        }
    }
}

impl<'a, T> From<&ResultItem<'a, T>> for BuildItem<'a, T>
where
    T: Storable,
{
    fn from(result: &ResultItem<'a, T>) -> Self {
        Self::Ref(result.as_ref())
    }
}

impl<'a, T> PartialEq<&str> for BuildItem<'a, T>
where
    T: Storable,
{
    fn eq(&self, other: &&str) -> bool {
        match self {
            Self::Id(v) => v.as_str() == *other,
            _ => false,
        }
    }
}

impl<'a, T> PartialEq<str> for BuildItem<'a, T>
where
    T: Storable,
{
    fn eq(&self, other: &str) -> bool {
        match self {
            Self::Id(v) => v.as_str() == other,
            Self::IdRef(v) => *v == other,
            Self::Ref(r) => r.id() == Some(other),
            _ => false,
        }
    }
}

impl<'a, T> PartialEq<String> for BuildItem<'a, T>
where
    T: Storable,
{
    fn eq(&self, other: &String) -> bool {
        match self {
            Self::Id(v) => v == other,
            Self::IdRef(v) => *v == other.as_str(),
            Self::Ref(r) => r.id() == Some(other),
            _ => false,
        }
    }
}

/// Test if this is a temporary public identifier,
/// they have a form like `!A0` . They start with an exclamation mark,
/// a capital letter indicates the type (A for Annotation), and a number
/// corresponds to whatever was the internal handle.
pub(crate) fn resolve_temp_id(id: &str) -> Option<usize> {
    let mut iter = id.chars();
    if let Some('!') = iter.next() {
        if let Some(x) = iter.next() {
            if !x.is_uppercase() {
                return None;
            }
            return Some(id[2..].parse().ok()?);
        }
    }
    None
}

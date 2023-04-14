use sealed::sealed;
use serde::Deserialize;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::slice::{Iter, IterMut};

use nanoid::nanoid;

use crate::config::Configurable;
use crate::error::StamError;
use crate::types::*;

/// Type for Store elements. The struct that owns a field of this type should implement the trait [`StoreFor<T>`]
pub type Store<T> = Vec<Option<T>>;

/// A map mapping public IDs to internal ids, implemented as a HashMap.
/// Used to resolve public IDs to internal ones.
#[derive(Debug, Clone)]
pub struct IdMap<HandleType> {
    /// The actual map
    data: HashMap<String, HandleType>,

    /// A prefix that automatically generated IDs will get when added to this map
    autoprefix: String,
}

impl<HandleType> Default for IdMap<HandleType>
where
    HandleType: Handle,
{
    fn default() -> Self {
        Self {
            data: HashMap::new(),
            autoprefix: "_".to_string(),
        }
    }
}

impl<HandleType> IdMap<HandleType>
where
    HandleType: Handle,
{
    pub fn new(autoprefix: String) -> Self {
        Self {
            autoprefix,
            ..Self::default()
        }
    }

    /// Sets a prefix that automatically generated IDs will get when added to this map
    pub fn set_autoprefix(&mut self, autoprefix: String) {
        self.autoprefix = autoprefix;
    }
}

/// This models relations or 'edges' in graph terminology, between handles. It acts as a reverse index is used for various purposes.
#[derive(Debug, Clone)]
pub struct RelationMap<A, B> {
    /// The actual map
    pub(crate) data: Vec<Vec<B>>,
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
        if x.unwrap() >= self.data.len() {
            //expand the map
            self.data.resize_with(x.unwrap() + 1, Default::default);
        }
        self.data[x.unwrap()].push(y);
    }

    /// Remove a relation from the map
    pub fn remove(&mut self, x: A, y: B) {
        if let Some(values) = self.data.get_mut(x.unwrap()) {
            if let Some(pos) = values.iter().position(|z| *z == y) {
                values.remove(pos); //note: this shifts the array and may take O(n)
            }
        }
    }

    pub fn get(&self, x: A) -> Option<&Vec<B>> {
        self.data.get(x.unwrap())
    }

    pub fn totalcount(&self) -> usize {
        let mut total = 0;
        for v in self.data.iter() {
            total += v.len();
        }
        total
    }

    pub fn count(&self, x: A) -> usize {
        self.data.get(x.unwrap()).map(|v| v.len()).unwrap_or(0)
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

#[derive(Debug, Clone)]
pub struct TripleRelationMap<A, B, C> {
    /// The actual map
    pub(crate) data: Vec<RelationMap<B, C>>,
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
        if x.unwrap() >= self.data.len() {
            //expand the map
            self.data.resize_with(x.unwrap() + 1, Default::default);
        }
        self.data[x.unwrap()].insert(y, z);
    }

    pub fn get(&self, x: A, y: B) -> Option<&Vec<C>> {
        if let Some(v) = self.data.get(x.unwrap()) {
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

    pub fn count(&self, x: A, y: B) -> usize {
        if let Some(v) = self.data.get(x.unwrap()) {
            v.get(y).map(|v| v.len()).unwrap_or(0)
        } else {
            0
        }
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
pub trait Storable: PartialEq + TypeInfo
where
    Self: Sized,
{
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
    /// Like [`Self::id()`] but returns a [`StamError::NoIdError`] error if there is no internal id.
    fn id_or_err(&self) -> Result<&str, StamError> {
        self.id().ok_or(StamError::NoIdError(""))
    }

    /// Builder pattern to set the public Id
    #[allow(unused_variables)]
    fn with_id(self, id: String) -> Self
    where
        Self: Sized,
    {
        //no-op
        self
    }

    /// Does this type support an ID?
    fn carries_id() -> bool;

    /// Returns a wrapped reference to this item and the store that owns it. This allows for some
    /// more introspection on the part of the item.
    /// reverse of [`StoreFor<T>::wrap()`]
    fn wrap_in<'store>(
        &'store self,
        store: &'store Self::StoreType,
    ) -> Result<WrappedItem<'store, Self>, StamError>
    where
        Self: Sized,
    {
        store.wrap(self)
    }

    /// Returns a wrapped reference to this item and the store that owns it. This allows for some
    /// more introspection on the part of the item.
    /// reverse of [`StoreFor<T>::wrap_owned()`]
    fn wrap_owned_in<'store>(
        self,
        store: &'store Self::StoreType,
    ) -> Result<WrappedItem<'store, Self>, StamError>
    where
        Self: Sized,
    {
        store.wrap_owned(self)
    }

    /// Set the internal ID. May only be called once (though currently not enforced).
    #[allow(unused_variables)]
    fn set_handle(&mut self, handle: <Self as Storable>::HandleType) {
        //no-op in default implementation
    }

    /// Callback function that is called after an item is bound to a store
    fn bound(&mut self) {
        //no-op by default
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
}

/// This trait is implemented on types that provide storage for a certain other generic type (T)
/// It is a sealed trait, not implementable outside this crate.
#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait StoreFor<T: Storable>: Configurable {
    /// Get a reference to the entire store for the associated type
    fn store(&self) -> &Store<T>;
    /// Get a mutable reference to the entire store for the associated type
    fn store_mut(&mut self) -> &mut Store<T>;
    /// Get a reference to the id map for the associated type, mapping global ids to internal ids
    fn idmap(&self) -> Option<&IdMap<T::HandleType>> {
        None
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
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
            item = self.bind(item)?;
            intid
        };

        if T::carries_id() {
            //insert a mapping from the public ID to the internal numeric ID in the idmap
            if let Some(id) = item.id() {
                //check if public ID does not already exist
                if self.has(&id.into()) {
                    //ok. the already ID exists, now is the existing item exactly the same as the item we're about to insert?
                    //in that case we can discard this error and just return the existing handle without actually inserting a new one
                    let existing_item = self.get(&id.into()).unwrap();
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

    /// Called prior to inserting an item into to the store
    /// If it returns an error, the insert will be cancelled.
    /// Allows for bookkeeping such as inheriting configuration
    /// parameters from parent to the item
    #[allow(unused_variables)]
    fn preinsert(&self, item: &mut T) -> Result<(), StamError> {
        //default implementation does nothing
        Ok(())
    }

    /// Called after an item was inserted to the store
    /// Allows the store to do further bookkeeping
    /// like updating relation maps
    #[allow(unused_variables)]
    fn inserted(&mut self, handle: T::HandleType) -> Result<(), StamError> {
        //default implementation does nothing
        Ok(())
    }

    fn add(mut self, item: T) -> Result<Self, StamError>
    where
        Self: Sized,
    {
        self.insert(item)?;
        Ok(self)
    }

    /// Returns true if the store has the item
    fn has<'a, 'b>(&'a self, item: &Item<'b, T>) -> bool {
        match item {
            Item::Handle(handle) => self.store().get(handle.unwrap()).is_some(),
            Item::Id(id) => {
                if let Some(idmap) = self.idmap() {
                    idmap.data.contains_key(id)
                } else {
                    false
                }
            }
            Item::IdRef(id) => {
                if let Some(idmap) = self.idmap() {
                    idmap.data.contains_key(*id)
                } else {
                    false
                }
            }
            Item::Ref(instance) => {
                if let (Some(idmap), Some(id)) = (self.idmap(), instance.id()) {
                    idmap.data.contains_key(id)
                } else {
                    false
                }
            }
            Item::None => false,
        }
    }

    /// Get a reference to an item from the store, by handle, without checking validity.
    ///
    /// ## Safety
    /// Calling this method with an out-of-bounds index is [undefined behavior](https://doc.rust-lang.org/reference/behavior-considered-undefined.html)  │       
    /// even if the resulting reference is not used.                                                                                                     │       
    unsafe fn get_unchecked(&self, handle: T::HandleType) -> Option<&T> {
        self.store().get_unchecked(handle.unwrap()).as_ref()
    }

    /// Get a reference to an item from the store
    fn get<'a, 'b>(&'a self, item: &Item<'b, T>) -> Result<&'a T, StamError> {
        if let Some(handle) = item.to_handle(self) {
            if let Some(Some(item)) = self.store().get(handle.unwrap()) {
                return Ok(item);
            }
        }
        Err(StamError::HandleError(Self::store_typeinfo()))
    }

    /// Get a mutable reference to an item from the store by internal ID
    fn get_mut<'a, 'b>(&mut self, item: &Item<'b, T>) -> Result<&mut T, StamError> {
        if let Some(handle) = item.to_handle(self) {
            if let Some(Some(item)) = self.store_mut().get_mut(handle.unwrap()) {
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
        if let Some(Some(item)) = self.store().get(handle.unwrap()) {
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
        let item = self.store_mut().get_mut(handle.unwrap()).unwrap();
        *item = None;
        Ok(())
    }

    /// Called before an item is removed from the store
    /// Allows the store to do further bookkeeping
    /// like updating relation maps
    #[allow(unused_variables)]
    fn preremove(&mut self, handle: T::HandleType) -> Result<(), StamError> {
        //default implementation does nothing
        Ok(())
    }

    /// Resolves an ID to a handle
    /// You usually don't want to call this directly
    fn resolve_id(&self, id: &str) -> Result<T::HandleType, StamError> {
        if let Some(idmap) = self.idmap() {
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

    /// Tests if the item is owner by the store, returns None if ownership is unknown
    #[allow(unused_variables)]
    fn owns(&self, item: &T) -> Option<bool> {
        None
    }

    /// Iterate over the store
    fn iter<'a>(&'a self) -> StoreIter<'a, T>
    where
        T: Storable<StoreType = Self>,
    {
        StoreIter {
            store: self,
            iter: self.store().iter(),
            count: 0,
            len: self.store().len(),
        }
    }

    /// Iterate over the store, mutably
    fn iter_mut<'a>(&'a mut self) -> StoreIterMut<'a, T> {
        let len = self.store().len();
        StoreIterMut {
            iter: self.store_mut().iter_mut(),
            count: 0,
            len,
        }
    }

    /// Return the internal id that will be assigned for the next item to the store
    fn next_handle(&self) -> T::HandleType {
        T::HandleType::new(self.store().len()) //this is one of the very few places in the code where we create a handle from scratch
    }

    /// Return the internal id that was assigned to last inserted item
    fn last_handle(&self) -> T::HandleType {
        T::HandleType::new(self.store().len() - 1)
    }

    /// This binds an item to the store *PRIOR* to it being actually added
    /// You should never need to call this directly (it can only be called once per item anyway).
    fn bind(&mut self, mut item: T) -> Result<T, StamError> {
        //we already pass the internal id this item will get upon the next insert()
        //so it knows its internal id immediate after construction
        if item.handle().is_some() {
            Err(StamError::AlreadyBound("bind()"))
        } else {
            item.set_handle(self.next_handle());
            item.bound();
            Ok(item)
        }
    }

    /// Wraps the item in a smart pointer that also holds a reference to this store
    /// This method performs some extra checks to verify if the item is indeed owned by the store
    /// and returns an error if not.
    fn wrap<'a>(&'a self, item: &'a T) -> Result<WrappedItem<T>, StamError>
    where
        T: Storable<StoreType = Self>,
    {
        WrappedItem::borrow(item, self)
    }

    /// Wraps the item in a smart pointer that also holds a reference to this store
    /// Ownership is retained with this method, i.e. the store does *NOT* own the item.
    fn wrap_owned<'a>(&'a self, item: T) -> Result<WrappedItem<T>, StamError>
    where
        T: Storable<StoreType = Self>,
    {
        WrappedItem::own(item, self)
    }

    /// Wraps the entire store along with a reference to self
    /// Low-level method that you won't need
    // TODO: shouldn't be public
    fn wrap_store<'a>(&'a self) -> WrappedStore<T, Self>
    where
        Self: Sized,
    {
        WrappedStore {
            store: self.store(),
            parent: self,
        }
    }
}

//  generic iterator implementations, these take care of skipping over deleted items (None)

/// This is the iterator to iterate over a Store,  it is created by the iter() method from the [`StoreFor<T>`] trait
/// It produces a references to the item wrapped in a fat pointer ([`WrappedItem<T>`]) that also contains reference to the store
/// and which is immediately implements various methods for working with the type.
pub struct StoreIter<'store, T>
where
    T: Storable,
{
    store: &'store T::StoreType,
    iter: Iter<'store, Option<T>>,
    count: usize,
    len: usize,
}

impl<'store, T> Iterator for StoreIter<'store, T>
where
    T: Storable,
{
    type Item = WrappedItem<'store, T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.count += 1;
        match self.iter.next() {
            Some(Some(item)) => Some(self.store.wrap(item).expect("wrap must succeed")),
            Some(None) => self.next(),
            None => None,
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
        match self.iter.next() {
            Some(Some(item)) => Some(item),
            Some(None) => self.next(),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let l = self.len - self.count;
        //the lower-bound may be an overestimate (if there are deleted items)
        (l, Some(l))
    }
}

/// This is a smart pointer that encapsulates both the item and the store that owns it.
/// It allows the item to have some more introspection as it knows who its immediate parent is.
/// It is used for example in serialization.
#[derive(Debug)]
pub enum WrappedItem<'store, T>
where
    T: Storable,
{
    Borrowed {
        item: &'store T,
        store: &'store T::StoreType,
    },
    Owned {
        item: T,
        store: &'store T::StoreType,
    },
}
impl<'store, T> Clone for WrappedItem<'store, T>
where
    T: Storable + Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Borrowed { item, store } => Self::Borrowed { item, store },
            Self::Owned { item, store } => Self::Owned {
                item: item.clone(),
                store,
            },
        }
    }
}

impl<'store, T> Deref for WrappedItem<'store, T>
where
    T: Storable,
{
    type Target = T;

    /// This may be a cause for lifetime problems, as it returns a reference with the lifetime of WrappedItem
    /// rather than the contained item. Use [`Self::unwrap()`] instead in those cases.
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed { item, .. } => item,
            Self::Owned { item, .. } => &item,
        }
    }
}

impl<'store, T> WrappedItem<'store, T>
where
    T: Storable,
{
    //Create a new wrapped item. Not public, called by [`StoreFor<T>::wrap()`] instead.
    pub(crate) fn borrow(item: &'store T, store: &'store T::StoreType) -> Result<Self, StamError> {
        if item.handle().is_none() {
            return Err(StamError::Unbound("can't wrap unbound items"));
        } else if store.owns(item) == Some(false) {
            return Err(StamError::Unbound(
                "Can't wrap an item in a store that doesn't own it!",
            ));
        }
        Ok(WrappedItem::Borrowed { item, store })
    }

    pub(crate) fn own(item: T, store: &'store T::StoreType) -> Result<Self, StamError> {
        Ok(WrappedItem::Owned { item, store })
    }

    pub fn store(&self) -> &'store T::StoreType {
        match self {
            Self::Borrowed { store, .. } | Self::Owned { store, .. } => store,
        }
    }

    /// Returns the contained reference with the original lifetime, unlike [`Self.deref()`]!
    /// This only works on Borrowed types though! Will panic on owned types!
    /// Using this is usually safe except when dealing with [`TextSelection`].
    ///
    /// If strict lifetime preservation is not an issue, you can get away with just using ['Self::deref()`],
    /// which is implicit (deref coercion).
    pub fn unwrap<'slf>(&'slf self) -> &'store T {
        match self {
            Self::Borrowed { item, .. } => *item,
            Self::Owned { .. } => panic!("Can't use WrappedItem::unwrap() on an owned type"),
        }
    }

    pub fn unwrap_owned<'slf>(self) -> T {
        match self {
            Self::Borrowed { item, .. } => {
                panic!("Can't use WrappedItem::unwrap_owned() on a borrowed type")
            }
            Self::Owned { item, .. } => item,
        }
    }

    /// Returns the contained reference with the original lifetime, unlike [`Self.deref()`]!
    /// This only works on Borrowed types though! Will panic on owned types!
    pub fn expect<'slf>(&'slf self, msg: &str) -> &'store T {
        match self {
            Self::Borrowed { item, .. } => *item,
            Self::Owned { .. } => panic!("{}", msg),
        }
    }

    pub fn is_ref(&self) -> bool {
        match self {
            Self::Borrowed { .. } => true,
            _ => false,
        }
    }
}

// the following structure may be a bit obscure but it required internally to
// make serialization via serde work on our stores
// (ideally it needn't be public)

/// Helper structure that contains a store and a reference to self. Mostly for internal use.
pub struct WrappedStore<'a, T, S: StoreFor<T>>
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

impl<'a, T, S> WrappedStore<'a, T, S>
where
    T: Storable,
    S: Sized,
    S: StoreFor<T>,
{
    pub fn parent(&self) -> &S {
        self.parent
    }
}

///////////////////////////////// Any

#[derive(Debug, Deserialize)]
#[serde(untagged)]
/// `Any` offers various ways of referring to a data structure of type `T` in the core STAM model
/// It abstracts over public IDs, handles, and references.
pub enum Item<'a, T>
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

impl<'a, T> Clone for Item<'a, T>
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

impl<'a, T> Default for Item<'a, T>
where
    T: Storable,
{
    fn default() -> Self {
        Self::None
    }
}

impl<'a, T> PartialEq for Item<'a, T>
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

impl<'a, T> Item<'a, T>
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
    pub fn error(&self, contextmsg: &'static str) -> StamError {
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
    pub fn as_str(&'a self) -> Option<&'a str> {
        if let Self::Id(s) = self {
            Some(s.as_str())
        } else if let Self::IdRef(s) = self {
            Some(s)
        } else {
            None
        }
    }
}

impl<'a, T> From<&'a str> for Item<'a, T>
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
impl<'a, T> From<Option<&'a str>> for Item<'a, T>
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

impl<'a, T> From<String> for Item<'a, T>
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

impl<'a, T> From<&'a String> for Item<'a, T>
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

impl<'a, T> From<Option<String>> for Item<'a, T>
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

impl<'a, T> From<&'a T> for Item<'a, T>
where
    T: Storable,
{
    fn from(instance: &'a T) -> Self {
        Self::Ref(instance)
    }
}

/* //this doesn't work:
        conflicting implementation in crate `core`:
                    - impl<T> From<T> for T;

impl<'a, T> From<<T as Storable>::HandleType> for Item<'a, T>
where
    T: Storable,
{
    fn from(handle: <T as Storable>::HandleType) -> Self {
        Self::Handle(handle)
    }
}

//this works but not if made generic over the handle
impl<'a, T> From<&AnnotationHandle> for Item<'a, T>
where
    T: Storable<HandleType = AnnotationHandle>,
{
    fn from(handle: &H) -> Self {
        Self::Handle(handle)
    }
}
*/

impl<'a, T> From<usize> for Item<'a, T>
where
    T: Storable,
{
    fn from(handle: usize) -> Self {
        Self::Handle(T::HandleType::new(handle))
    }
}

impl<'a, T> From<Option<usize>> for Item<'a, T>
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

impl<'a, T> Item<'a, T>
where
    T: Storable,
{
    pub fn to_handle<S>(&self, store: &S) -> Option<T::HandleType>
    where
        S: StoreFor<T>,
    {
        match self {
            Item::Id(id) => store.resolve_id(id.as_str()).ok(),
            Item::IdRef(id) => store.resolve_id(id).ok(),
            Item::Handle(handle) => Some(*handle),
            Item::Ref(instance) => instance.handle(),
            Item::None => None,
        }
    }

    pub fn to_id<S>(&'a self, store: &'a S) -> Option<&'a str>
    where
        S: StoreFor<T>,
    {
        match self {
            Item::Id(id) => Some(id.as_str()),
            Item::IdRef(id) => Some(id),
            Item::Handle(_) => {
                if let Some(instance) = self.to_ref(store) {
                    instance.id()
                } else {
                    None
                }
            }
            Item::Ref(instance) => instance.id(),
            Item::None => None,
        }
    }

    pub fn to_ref<'s, S>(&'a self, store: &'s S) -> Option<&'s T>
    where
        S: StoreFor<T>,
    {
        store.get(&self).ok()
    }
}

/*
impl<'a, T> From<Option<T::HandleType>> for Any<'a, T>
where
    T: Storable,
{
    fn from(handle: Option<HandleType>) -> Self {
        if let Some(handle) = handle {
            Self::Handle(handle)
        } else {
            Self::None
        }
    }
}

impl<HandleType> From<Option<&str>> for Any<HandleType>
where
    HandleType: Handle,
{
    fn from(id: Option<&str>) -> Self {
        if let Some(id) = id {
            if id.is_empty() {
                Self::None
            } else {
                Self::Id(id.to_string())
            }
        } else {
            Self::None
        }
    }
}

impl<HandleType> From<Option<String>> for Any<HandleType>
where
    HandleType: Handle,
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
*/

/// This allows us to pass a reference to any stored item and get back the best AnyId for it
/// Will panic on totally unbounded that also don't have a public ID

/*impl<HandleType> From<&dyn Storable<HandleType = HandleType>> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn from(item: &dyn Storable<HandleType = HandleType>) -> Self {
        if let Some(handle) = item.handle() {
            Self::Handle(handle)
        } else if let Some(id) = item.id() {
            Self::Id(id.into())
        } else {
            panic!("Passed a reference to an unbound item without a public ID! Unable to convert to AnyId");
        }
    }
}*/

impl<'a, T> PartialEq<&str> for Item<'a, T>
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

impl<'a, T> PartialEq<str> for Item<'a, T>
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

impl<'a, T> PartialEq<String> for Item<'a, T>
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

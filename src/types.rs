use sealed::sealed;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::slice::{Iter, IterMut};
use std::sync::{Arc, RwLock};

use crate::error::StamError;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

/// Type for Store elements. The struct that owns a field of this type should implement the trait StoreFor<T>.
pub type Store<T> = Vec<Option<T>>;
//                       ^------- may be None when an element gets deleted
/// A cursor points to a specific point in a text. I
/// Used to select offsets. Units are unicode codepoints (not bytes!)
/// and are 0-indexed.
///
/// The cursor can be either begin-aligned or end-aligned. Where BeginAlignedCursor(0)
/// is the first unicode codepoint in a referenced text, and EndAlignedCursor(0) the last one.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(tag = "@type", content = "value")]
pub enum Cursor {
    /// Cursor relative to the start of a text. Has a value of 0 or higher
    #[serde(rename = "BeginAlignedCursor")]
    BeginAligned(usize),
    /// Cursor relative to the end of a text. Has a value of 0 or lower. The last character of a text begins at EndAlignedCursor(-1) and ends at EndAlignedCursor(0)
    #[serde(rename = "EndAlignedCursor")]
    EndAligned(isize),
}

impl From<usize> for Cursor {
    fn from(cursor: usize) -> Self {
        Self::BeginAligned(cursor)
    }
}

impl TryFrom<isize> for Cursor {
    type Error = &'static str;
    fn try_from(cursor: isize) -> Result<Self, Self::Error> {
        if cursor > 0 {
            Err("Cursor is a signed integer and converts to EndAlignedCursor, expected a value <= 0. Convert from an unsigned integer for a normal BeginAlignedCursor")
        } else {
            Ok(Self::EndAligned(cursor))
        }
    }
}

/// The handle trait is implemented on various handle types. They have in common that refer to the internal id
/// a [`Storable`] item in a [`Store`] by index. Types implementing this are lightweigt and do not borrow anything, they can be passed and copied freely.
// To get an actual reference to the item from a handle type, call the `get()`` method on the store that holds it.
/// This is a sealed trait, not implementable outside this crate.
#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait Handle:
    Clone + Copy + core::fmt::Debug + PartialEq + Eq + PartialOrd + Ord + Hash
{
    /// Create a new handle for an internal ID. You shouldn't need to use this as handles will always be generated for you by higher-level functions.
    fn new(intid: usize) -> Self;
    /// Returns the internal index for this handle
    fn unwrap(&self) -> usize;
}

/// A map mapping public IDs to internal ids, implemented as a HashMap.
/// Used to resolve public IDs to internal ones.
#[derive(Debug)]
pub struct IdMap<HandleType> {
    /// The actual map
    data: HashMap<String, HandleType>,

    /// A prefix that automatically generated IDs will get when added to this map
    autoprefix: String,

    ///Sequence number used for ID generation
    seqnr: usize,
}

impl<HandleType> Default for IdMap<HandleType>
where
    HandleType: Handle,
{
    fn default() -> Self {
        Self {
            data: HashMap::new(),
            autoprefix: "_".to_string(),
            seqnr: 0,
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SerializeMode {
    /// Allow serialisation of stand-off files (which means we allow @include)
    AllowInclude,

    ///We are in standoff mode to serialized stand-off files (which means we don't output @include again)
    NoInclude,
}

impl Default for SerializeMode {
    fn default() -> Self {
        Self::AllowInclude
    }
}

//Configuration pertaining to a store
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StoreConfig {
    ///generate ids when missing
    pub generate_ids: bool,

    /// The working directory
    pub workdir: Option<PathBuf>,

    /// This flag can be flagged on or off (using internal mutability) to indicate whether we are serializing for the standoff include mechanism
    #[serde(skip)]
    serialize_mode: Arc<RwLock<SerializeMode>>,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            generate_ids: true,
            workdir: None,
            serialize_mode: Arc::new(RwLock::new(SerializeMode::AllowInclude)),
        }
    }
}

impl StoreConfig {
    /// This sets a parameter we use in serialisation
    pub fn set_serialize_mode(&self, mode: SerializeMode) {
        if let Ok(mut serialize_mode) = self.serialize_mode.write() {
            *serialize_mode = mode;
        }
    }

    /// This tests a parameter we use in serialisation
    pub fn serialize_mode(&self) -> SerializeMode {
        if let Ok(mut serialize_mode) = self.serialize_mode.read() {
            *serialize_mode
        } else {
            panic!("Unable to get lock for serialize mode");
        }
    }

    ///  Return the working directory
    pub fn workdir(&self) -> Option<&Path> {
        self.workdir.as_ref().map(|x| x.as_path())
    }
}

// ************** The following are high-level abstractions so we only have to implement a certain logic once ***********************

#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait Storable: PartialEq {
    type HandleType: Handle;
    /// Retrieve the internal (numeric) id. For any type T uses in StoreFor<T>, this may be None only in the initial
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

    /// Returns a wrapped reference to this item and the store that owns it. This allows for some
    /// more introspection on the part of the item.
    /// reverse of [`StoreFor<T>::wrap()`]
    fn wrap_in<'a, S: StoreFor<Self>>(
        &'a self,
        store: &'a S,
    ) -> Result<WrappedStorable<Self, S>, StamError>
    where
        Self: Sized,
    {
        store.wrap(self)
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

    /// Generate a random ID in a given idmap (adds it to the map), Item must be bound
    fn generate_id(self, idmap: Option<&mut IdMap<Self::HandleType>>) -> Self
    where
        Self: Sized,
    {
        if let Some(intid) = self.handle() {
            if let Some(idmap) = idmap {
                loop {
                    let id = format!("{}{}", idmap.autoprefix, idmap.seqnr);
                    if idmap.data.insert(id, intid).is_none() {
                        //returns none if the key did not exist yet
                        break;
                    }
                    idmap.seqnr += 1
                }
            }
        }
        self
    }
}

/// This trait is implemented on types that provide storage for a certain other generic type (T)
/// It requires the types to also implemnet GetStore<T> and HasIdMap<T>
/// It is a sealed trait, not implementable outside this crate.
#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait StoreFor<T: Storable> {
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

    fn introspect_type(&self) -> &'static str;

    /// Adds an item to the store. Returns a handle to it upon success.
    fn insert(&mut self, mut item: T) -> Result<T::HandleType, StamError> {
        let handle = if let Some(intid) = item.handle() {
            intid
        } else {
            // item has no internal id yet, i.e. it is unbound
            // we generate an id and bind it now
            let intid = self.next_handle();
            item = self.bind(item)?;
            intid
        };

        //insert a mapping from the public ID to the numeric ID in the idmap
        if let Some(id) = item.id() {
            //check if public ID does not already exist
            if self.has_id(id) {
                //ok. the already ID exists, now is the existing item exactly the same as the item we're about to insert?
                //in that case we can discard this error and just return the existing handle without actually inserting a new one
                let existing_item = self.get_by_id(id).unwrap();
                if *existing_item == item {
                    return Ok(existing_item.handle().unwrap());
                }
                //in all other cases, we return an error
                return Err(StamError::DuplicateIdError(
                    id.to_string(),
                    self.introspect_type(),
                ));
            }

            self.idmap_mut().map(|idmap| {
                //                 v-- MAYBE TODO: optimise the id copy away
                idmap.data.insert(id.to_string(), item.handle().unwrap())
            });
        } else if self.config().generate_ids {
            item = item.generate_id(self.idmap_mut());
        }

        self.preinsert(&mut item)?;

        //add the resource
        self.store_mut().push(Some(item));

        self.inserted(handle)?;

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

    /// Returns true if the store contains the item
    fn contains(&self, item: &T) -> bool {
        if let (Some(intid), Some(true)) = (item.handle(), self.owns(item)) {
            self.has(intid)
        } else if let Some(id) = item.id() {
            self.has_id(id)
        } else {
            false
        }
    }

    /// Retrieves the internal id for the item as it occurs in the store. The passed item and reference item may be distinct instances.
    fn find(&self, item: &T) -> Option<T::HandleType> {
        if let (Some(intid), Some(true)) = (item.handle(), self.owns(item)) {
            Some(intid)
        } else if let Some(id) = item.id() {
            if let Some(idmap) = self.idmap() {
                idmap.data.get(id).copied() //note, only the handle is copied (=cheap)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Returns true if the store has the item with the specified internal id
    fn has(&self, handle: T::HandleType) -> bool {
        self.store().len() > handle.unwrap()
    }

    /// Returns true if the store has the item with the specified global id
    fn has_id(&self, id: &str) -> bool {
        if let Some(idmap) = self.idmap() {
            idmap.data.contains_key(id)
        } else {
            false
        }
    }

    /// Get a reference to an item from the store by its global ID
    fn get_by_id<'a>(&'a self, id: &str) -> Result<&'a T, StamError> {
        let handle = self.resolve_id(id)?;
        self.get(handle)
    }

    /// Get a mutable reference to an item from the store by its global ID
    fn get_mut_by_id<'a>(&'a mut self, id: &str) -> Result<&'a mut T, StamError> {
        let handle = self.resolve_id(id)?;
        self.get_mut(handle)
    }

    /// Get a reference to an item from the store by internal ID
    fn get(&self, handle: T::HandleType) -> Result<&T, StamError> {
        if let Some(Some(item)) = self.store().get(handle.unwrap()) {
            Ok(item)
        } else {
            Err(StamError::HandleError(self.introspect_type()))
        }
    }

    /// Get a mutable reference to an item from the store by internal ID
    fn get_mut(&mut self, handle: T::HandleType) -> Result<&mut T, StamError> {
        if let Some(Some(item)) = self.store_mut().get_mut(handle.unwrap()) {
            Ok(item)
        } else {
            Err(StamError::HandleError("StoreFor::get_mut")) //MAYBE TODO: self.introspect_type didn't work here (cannot borrow `*self` as immutable because it is also borrowed as mutable)
        }
    }

    /// Removes an item by handle, returns an error if the item has dependencies and can't be removed
    fn remove(&mut self, handle: T::HandleType) -> Result<(), StamError> {
        //callback to remove the item from relation maps, may return an error and refuse to remove an item
        self.preremove(handle)?;

        //remove item from idmap
        let item: &T = self.get(handle)?;
        let id: Option<String> = item.id().map(|x| x.to_string());
        if let Some(id) = id {
            if let Some(idmap) = self.idmap_mut() {
                idmap.data.remove(id.as_str());
            }
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
                Err(StamError::IdError(id.to_string(), self.introspect_type()))
            }
        } else {
            Err(StamError::NoIdError(self.introspect_type()))
        }
    }

    /// Tests if the item is owner by the store, returns None if ownership is unknown
    #[allow(unused_variables)]
    fn owns(&self, item: &T) -> Option<bool> {
        None
    }

    /// Iterate over the store
    fn iter<'a>(&'a self) -> StoreIter<'a, T> {
        StoreIter {
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

    /// Get the item from the store if it already exists, if not, add it
    fn get_or_add(&mut self, item: T) -> Result<&T, StamError> {
        if let Some(intid) = self.find(&item) {
            self.get(intid)
        } else {
            match self.insert(item) {
                Ok(intid) => self.get(intid),
                Err(err) => Err(err),
            }
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

    /// Get a reference to an item from the store by any ID
    /// The advantage of AnyId is that you can coerce strings, references and handles to it using `into()`.
    /// If the item does not exist, None will be returned
    fn get_by_anyid(&self, anyid: &AnyId<T::HandleType>) -> Option<&T> {
        match anyid {
            AnyId::None => None,
            AnyId::Handle(handle) => self.get(*handle).ok(),
            AnyId::Id(id) => self.get_by_id(id).ok(),
        }
    }

    /// Get a reference to an item from the store by any ID. Returns an error on failure.
    /// The advantage of AnyId is that you can coerce strings, references and handles to it using `into()`.
    /// If the item does not exist, None will be returned
    fn get_by_anyid_or_err(&self, anyid: &AnyId<T::HandleType>) -> Result<&T, StamError> {
        match anyid {
            AnyId::None => Err(anyid.error("")),
            AnyId::Handle(handle) => self.get(*handle),
            AnyId::Id(id) => self.get_by_id(id),
        }
    }

    /// Get a reference to an item from the store by any ID
    /// If the item does not exist, None will be returned
    fn get_mut_by_anyid(&mut self, anyid: &AnyId<T::HandleType>) -> Option<&mut T> {
        match anyid {
            AnyId::None => None,
            AnyId::Handle(handle) => self.get_mut(*handle).ok(),
            AnyId::Id(id) => self.get_mut_by_id(id).ok(),
        }
    }

    /// Wraps the reference with a reference to the store
    fn wrap<'a>(&'a self, item: &'a T) -> Result<WrappedStorable<T, Self>, StamError>
    where
        Self: Sized,
    {
        WrappedStorable::new(item, self)
    }

    /// Wraps the entire store along with a reference to self
    fn wrappedstore<'a>(&'a self) -> WrappedStore<T, Self>
    where
        Self: Sized,
    {
        WrappedStore {
            store: self.store(),
            parent: self,
        }
    }

    /// Return associated configuration
    fn config(&self) -> &StoreConfig;
}

//  generic iterator implementations, these take care of skipping over deleted items (None)

/// This is the iterator to iterate over a Store,  it is created by the iter() method from the [`StoreFor<T>`] trait
pub struct StoreIter<'a, T> {
    iter: Iter<'a, Option<T>>,
    count: usize,
    len: usize,
}

impl<'a, T> Iterator for StoreIter<'a, T> {
    type Item = &'a T;

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

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(untagged)]
/// This is either an public ID or a Handle
pub enum AnyId<HandleType>
where
    HandleType: Handle,
{
    Id(String), //for deserialisation only this variant is avaiable

    #[serde(skip)]
    Handle(HandleType),

    #[serde(skip)]
    None,
}

impl<HandleType> Default for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn default() -> Self {
        Self::None
    }
}

impl<HandleType> AnyId<HandleType>
where
    HandleType: Handle,
{
    pub fn is_handle(&self) -> bool {
        matches!(self, Self::Handle(_))
    }

    pub fn is_id(&self) -> bool {
        matches!(self, Self::Id(_))
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
            Self::Id(id) => StamError::IdError(id.to_string(), contextmsg),
            Self::None => StamError::Unbound("Supplied AnyId is not bound to anything!"),
        }
    }

    pub fn to_string(self) -> Option<String> {
        if let Self::Id(s) = self {
            Some(s)
        } else {
            None
        }
    }
}

impl<HandleType> From<&str> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn from(id: &str) -> Self {
        if id.is_empty() {
            Self::None
        } else {
            Self::Id(id.to_string())
        }
    }
}
impl<HandleType> From<String> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn from(id: String) -> Self {
        if id.is_empty() {
            Self::None
        } else {
            Self::Id(id)
        }
    }
}
impl<HandleType> From<HandleType> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn from(handle: HandleType) -> Self {
        Self::Handle(handle)
    }
}
impl<HandleType> From<&HandleType> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn from(handle: &HandleType) -> Self {
        Self::Handle(*handle)
    }
}

impl<HandleType> From<Option<HandleType>> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn from(handle: Option<HandleType>) -> Self {
        if let Some(handle) = handle {
            Self::Handle(handle)
        } else {
            Self::None
        }
    }
}

impl<HandleType> From<Option<&str>> for AnyId<HandleType>
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

impl<HandleType> From<Option<String>> for AnyId<HandleType>
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

impl<HandleType> PartialEq<&str> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn eq(&self, other: &&str) -> bool {
        match self {
            Self::Id(v) => v.as_str() == *other,
            _ => false,
        }
    }
}

impl<HandleType> PartialEq<str> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn eq(&self, other: &str) -> bool {
        match self {
            Self::Id(v) => v.as_str() == other,
            _ => false,
        }
    }
}

impl<HandleType> PartialEq<String> for AnyId<HandleType>
where
    HandleType: Handle,
{
    fn eq(&self, other: &String) -> bool {
        match self {
            Self::Id(v) => v == other,
            _ => false,
        }
    }
}

/// This is a smart pointer that encapsulates both the item and the store that owns it.
/// It allows the item to have some more introspection as it knows who its immediate parent is.
/// It is used for example in serialization.
#[derive(Clone, Copy, Debug)]
pub struct WrappedStorable<'a, T, S: StoreFor<T>>
where
    T: Storable,
{
    item: &'a T,
    store: &'a S,
}

impl<'a, T, S> Deref for WrappedStorable<'a, T, S>
where
    T: Storable,
    S: StoreFor<T>,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item
    }
}

impl<'a, T, S> WrappedStorable<'a, T, S>
where
    T: Storable,
    S: StoreFor<T>,
{
    //Create a new wrapped item
    pub(crate) fn new(item: &'a T, store: &'a S) -> Result<Self, StamError> {
        if item.handle().is_none() {
            return Err(StamError::Unbound("can't wrap unbound items"));
        } else if store.owns(item) == Some(false) {
            return Err(StamError::Unbound(
                "Can't wrap an item in a store that doesn't own it!",
            ));
        }
        Ok(WrappedStorable { item, store })
    }

    pub fn store(&self) -> &S {
        self.store
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

/// Get a file for reading or writing, this resolves relative files more intelligently
pub(crate) fn get_filepath(filename: &str, workdir: Option<&Path>) -> Result<PathBuf, StamError> {
    if filename.starts_with("https://") || filename.starts_with("http://") {
        //TODO: implement downloading of remote URLs and storing them locally
        return Err(StamError::OtherError("Loading URLs is not supported yet"));
    }
    let path = if filename.starts_with("file://") {
        //strip the file:// prefix
        PathBuf::from(&filename[7..])
    } else {
        PathBuf::from(filename)
    };
    if path.is_absolute() {
        Ok(path)
    } else {
        //check whether we can find one in our workdir first
        if let Some(workdir) = workdir {
            let path = workdir.join(&path);
            if path.is_file() {
                //should also work with symlinks
                return Ok(path);
            }
        }

        //final fallback is simply relative to the current working directly
        // we don't test for existance here
        Ok(path)
    }
}

/// Auxiliary function to help open files
pub(crate) fn open_file(filename: &str, workdir: Option<&Path>) -> Result<File, StamError> {
    let path = get_filepath(filename, workdir)?;
    File::open(filename).map_err(|e| StamError::IOError(e, "Opening file for reading failed"))
}

/// Auxiliary function to help open files
pub(crate) fn open_file_reader(
    filename: &str,
    workdir: Option<&Path>,
) -> Result<BufReader<File>, StamError> {
    Ok(BufReader::new(open_file(filename, workdir)?))
}

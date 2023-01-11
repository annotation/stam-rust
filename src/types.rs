use std::collections::HashMap;
use std::slice::{Iter,IterMut};
use std::borrow::Cow;
use crate::error::StamError;
use serde::Deserialize;
use std::hash::Hash;

/// Type for Store elements. The struct that owns a field of this type should implement the trait StoreFor<T>.
pub type Store<T> = Vec<Option<Box<T>>>;
//                             ^----- actual T is allocated on heap (i.e. the Vec itself will contain pointers)
//                       ^------- may be None when an element gets deleted
/// A cursor points to a specific point in a text. I
/// Used to select offsets. Units are unicode codepoints (not bytes!)
/// and are 0-indexed.
///
/// The cursor can be either begin-aligned or end-aligned. Where BeginAlignedCursor(0)
/// is the first unicode codepoint in a referenced text, and EndAlignedCursor(0) the last one.
#[derive(Debug,Clone,Copy,Deserialize,PartialEq)]
#[serde(tag="@type",content = "value")]
pub enum Cursor {
    /// Cursor relative to the start of a text. Has a value of 0 or higher
    #[serde(rename="BeginAlignedCursor")]
    BeginAligned(usize),
    /// Cursor relative to the end of a text. Has a value of 0 or lower. The last character of a text begins at EndAlignedCursor(-1) and ends at EndAlignedCursor(0)
    #[serde(rename="EndAlignedCursor")]
    EndAligned(isize)
}


impl From<usize> for Cursor {
    fn from(cursor: usize) -> Self {
        Self::BeginAligned(cursor)
    }
}

impl TryFrom<isize> for Cursor {
    type Error = &'static str;
    fn try_from(cursor: isize) -> Result<Self,Self::Error> {
        if cursor > 0 {
            Err("Cursor is a signed integer and converts to EndAlignedCursor, expected a value <= 0. Conver from an unsigned integer for a normal BeginAlignedCursor")
        } else {
            Ok(Self::EndAligned(cursor))
        }
    }
}

/// The pointer trait is implemented on various pointer types. They have in common that refer to the internal id 
/// a [`Storable`] item in a [`Store`]. Types implementing this are lightweigt and do ott borrow anything, they can be passed and copied freely.
// To get an actual reference to the item from a pointer type, call the `get()`` method on the store that holds it.
pub trait Pointer: Clone + Copy + core::fmt::Debug + PartialEq + Eq + PartialOrd + Hash {
    fn new(intid: usize) -> Self;
    fn unwrap(&self) -> usize;
}


/// A map mapping public IDs to internal ids, implemented as a HashMap.
/// Used to resolve public IDs to internal ones.
#[derive(Debug)]
pub struct IdMap<PointerType> {
    /// The actual map
    data: HashMap<String,PointerType>,

    /// A prefix that automatically generated IDs will get when added to this map
    autoprefix: String,

    ///Sequence number used for ID generation
    seqnr: usize,
}

impl<PointerType> Default for IdMap<PointerType> where PointerType: Pointer {
    fn default() -> Self {
        Self {
            data: HashMap::new(),
            autoprefix: "_".to_string(),
            seqnr: 0,
        }
    }
}

impl<PointerType> IdMap<PointerType> where PointerType: Pointer {
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


pub struct RelationMap<A,B> {
    /// The actual map
    pub(crate) data: HashMap<A,Vec<B>>
}

impl<A,B> Default for RelationMap<A,B> where A: Pointer, B: Pointer {
    fn default() -> Self {
        Self {
            data: HashMap::new()
        }
    }
}

impl<A,B> RelationMap<A,B> where A: Pointer, B: Pointer {
    pub fn new() -> Self { Self::default() }

    pub fn insert(&mut self, x: A, y: B) {
        self.data.entry(x).or_default().push(y);
    }
}

impl<A,B> Extend<(A,B)> for RelationMap<A,B> where A: Pointer, B: Pointer {
    fn extend<T>(&mut self, iter: T)  where T: IntoIterator<Item=(A,B)> {
        for (x,y) in iter {
            self.insert(x,y);
        }
    }
}

pub struct TripleRelationMap<A,B,C> {
    /// The actual map
    pub(crate) data: HashMap<A,RelationMap<B,C>>
}

impl<A,B,C> Default for TripleRelationMap<A,B,C> {
    fn default() -> Self {
        Self {
            data: HashMap::new()
        }
    }
}

impl<A,B,C> TripleRelationMap<A,B,C> where A: Pointer, B: Pointer, C: Pointer {
    pub fn new() -> Self { Self::default() }

    pub fn insert(&mut self, x: A, y: B, z: C) {
        self.data.entry(x).or_default().insert(y,z);
    }
}

impl<A,B,C> Extend<(A,B,C)> for TripleRelationMap<A,B,C> where A: Pointer, B: Pointer, C: Pointer  {
    fn extend<T>(&mut self, iter: T)  where T: IntoIterator<Item=(A,B,C)> {
        for (x,y,z) in iter {
            self.insert(x,y,z);
        }
    }
}


// ************** The following are high-level abstractions so we only have to implement a certain logic once ***********************


pub trait Storable {
    type PointerType: Pointer;
    /// Retrieve the internal (numeric) id. For any type T uses in StoreFor<T>, this may be None only in the initial
    /// stage when it is still unbounded to a store.
    fn get_pointer(&self) -> Option<Self::PointerType> {
        None
    }

    /// Like [`Self::get_intid()`] but returns a [`StamError:Unbound`] error if there is no internal id.
    fn get_pointer_or_err(&self) -> Result<Self::PointerType,StamError> {
        self.get_pointer().ok_or(StamError::Unbound(""))
    }

    /// Get the public ID
    fn get_id(&self) -> Option<&str> {
        None
    }
    fn get_id_or_err(&self) -> Result<&str,StamError> {
        self.get_id().ok_or(StamError::NoIdError(""))
    }

    /// Builder pattern to set the public Id
    #[allow(unused_variables)]
    fn with_id(self, id: String) -> Self where Self: Sized {
        //no-op
        self
    }

}

//v -- this trait separate from the above because we don't want to expose it publicly.
//     internal ID setting is an internal business.

pub(crate) trait MutableStorable: Storable {
    /// Set the internal ID. May only be called once (though currently not enforced).
    #[allow(unused_variables)]
    fn set_pointer(&mut self, pointer: <Self as Storable>::PointerType) {
        //no-op in default implementation
    }

    /// Callback function that is called after an item is bound to a store
    fn bound(&mut self) {
        //no-op by default
    }
    /// Generate a random ID in a given idmap (adds it to the map), Item must be bound
    fn generate_id(self, idmap: Option<&mut IdMap<Self::PointerType>>) -> Self where Self: Sized {
        if let Some(intid) = self.get_pointer() {
            if let Some(idmap) = idmap {
                loop {
                    let id = format!("{}{}", idmap.autoprefix, idmap.seqnr);
                    if idmap.data.insert(id, intid).is_none() { //returns none if the key did not exist yet
                        break
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
pub(crate) trait StoreFor<T: MutableStorable + Storable> {
    /// Get a reference to the entire store for the associated type
    fn get_store(&self) -> &Store<T>;
    /// Get a mutable reference to the entire store for the associated type
    fn get_mut_store(&mut self) -> &mut Store<T>;
    /// Get a reference to the id map for the associated type, mapping global ids to internal ids
    fn get_idmap(&self) -> Option<&IdMap<T::PointerType>> {
        None
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap<T::PointerType>> {
        None
    }

    fn introspect_type(&self) -> &'static str;

    /// Adds an item to the store. Returns its internal id upon success
    /// This is a fairly low level method. You will likely want to use [`add`] instead.
    fn insert(&mut self, mut item: T) -> Result<T::PointerType,StamError> {
        let pointer = if let Some(intid) = item.get_pointer() {
            intid
        } else {
            // item has no internal id yet, i.e. it is unbound
            // we generate an id and bind it now
            let intid = self.next_pointer();
            item = self.bind(item)?;
            intid
        };

        //insert a mapping from the public ID to the numeric ID in the idmap
        if let Some(id) = item.get_id() {
            //check if public ID does not already exist
            if self.get_by_id(id).is_ok() {
                return Err(StamError::DuplicateIdError(id.to_string(), self.introspect_type()));
            }

            self.get_mut_idmap().map(|idmap| {
                //                 v-- MAYBE TODO: optimise the id copy away
                idmap.data.insert(id.to_string(), item.get_pointer().unwrap())
            });
        } else {
            item = item.generate_id(self.get_mut_idmap());
        }

        //add the resource
        self.get_mut_store().push(Some(Box::new(item)));

        self.inserted(pointer);

        //sanity check to ensure no item can determine its own internal id that does not correspond with what's allocated
        assert_eq!(pointer, T::PointerType::new(self.get_store().len() - 1));

        Ok(pointer)
    }

    /// Called after an item was inserted to the store
    /// Allows the store to do further bookkeeping
    /// like updating relation maps
    #[allow(unused_variables)]
    fn inserted(&mut self, pointer: T::PointerType) {
        //default implementation does nothing
    }

    fn add(mut self, item: T) -> Result<Self, StamError> where Self: Sized {
        self.insert(item)?;
        Ok(self)
    }

    /// Returns true if the store contains the item
    fn contains(&self, item: &T) -> bool {
        if let (Some(intid), Some(true)) = (item.get_pointer(), self.owns(item)) {
            self.has(intid)
        } else if let Some(id) = item.get_id() {
            self.has_by_id(id)
        } else {
            false
        }
    }

    /// Retrieves the internal id for the item as it occurs in the store. The passed item and reference item may be distinct instances.
    fn find(&self, item: &T) -> Option<T::PointerType> {
        if let (Some(intid), Some(true)) = (item.get_pointer(), self.owns(item)) {
            Some(intid)
        } else if let Some(id) = item.get_id() {
            if let Some(idmap) = self.get_idmap() {
                idmap.data.get(id).map(|x| *x)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Returns true if the store has the item with the specified internal id
    fn has(&self, pointer: T::PointerType) -> bool {
        self.get_store().len() > pointer.unwrap()
    }

    /// Returns true if the store has the item with the specified global id
    fn has_by_id(&self, id: &str) -> bool {
        if let Some(idmap) = self.get_idmap() {
            idmap.data.contains_key(id)
        } else {
            false
        }
    }

    /// Get a reference to an item from the store by its global ID
    fn get_by_id<'a>(&'a self, id: &str) -> Result<&'a T,StamError> {
        let pointer = self.resolve_id(id)?;
        self.get(pointer)
    }

    /// Get a mutable reference to an item from the store by its global ID
    fn get_mut_by_id<'a>(&'a mut self, id: &str) -> Result<&'a mut T,StamError> {
        let pointer = self.resolve_id(id)?;
        self.get_mut(pointer)
    }

    /// Get a reference to an item from the store by internal ID
    fn get(&self, pointer: T::PointerType) -> Result<&T,StamError> {
        if let Some(Some(item)) = self.get_store().get(pointer.unwrap()) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(self.introspect_type()))
        }
    }

    /// Get a mutable reference to an item from the store by internal ID
    fn get_mut(&mut self, pointer: T::PointerType) -> Result<&mut T,StamError> {
        if let Some(Some(item)) = self.get_mut_store().get_mut(pointer.unwrap()) {
            Ok(item)
        } else {
            Err(StamError::IntIdError("Store::get_mut")) //MAYBE TODO: self.introspect_type didn't work here (cannot borrow `*self` as immutable because it is also borrowed as mutable)
        }
    }

    /// Resolves an ID to a pointer
    /// You usually don't want to call this directly
    fn resolve_id(&self, id: &str) -> Result<T::PointerType, StamError> {
        if let Some(idmap) = self.get_idmap() {
            if let Some(pointer) = idmap.data.get(id) {
                Ok(*pointer)
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
        StoreIter(self.get_store().iter())
    }

    /// Iterate over the store, mutably
    fn iter_mut<'a>(&'a mut self) -> StoreIterMut<'a,T>  {
        StoreIterMut(self.get_mut_store().iter_mut())
    }

    /// Get the item from the store if it already exists, if not, add it
    fn get_or_add(&mut self, item: T) -> Result<&T,StamError>  {
        if let Some(intid) = self.find(&item) {
            self.get(intid)
        } else {
            match self.insert(item) {
                Ok(intid) => self.get(intid),
                Err(err) => Err(err)
            }
        }
    }

    /// Return the internal id that will be assigned for the next item to the store
    fn next_pointer(&self) -> T::PointerType {
        T::PointerType::new(self.get_store().len()) //this is one of the very few places in the code where we create a pointer from scratch
    }

    /// Return the internal id that was assigned to last inserted item
    fn last_pointer(&self) -> T::PointerType {
        T::PointerType::new(self.get_store().len() - 1)
    }

    /// This binds an item to the store *PRIOR* to it being actually added
    /// You should never need to call this directly (it can only be called once per item anyway).
    fn bind(&mut self, mut item: T) -> Result<T,StamError> {
        //we already pass the internal id this item will get upon the next insert()
        //so it knows its internal id immediate after construction
        if item.get_pointer().is_some() {
            Err(StamError::AlreadyBound("bind()") )
        } else {
            item.set_pointer(self.next_pointer());
            item.bound();
            Ok(item)
        }
    }

    /// Get a reference to an item from the store by any ID
    /// If the item does not exist, None will be returned
    fn get_by_anyid(&self, anyid: &AnyId<T::PointerType>) -> Option<&T> {
        match anyid {
            AnyId::None => None,
            AnyId::Pointer(pointer) => self.get(*pointer).ok(),
            AnyId::Id(id) => self.get_by_id(id).ok()
        }
    }

    fn get_by_anyid_or_err(&self, anyid: &AnyId<T::PointerType>) -> Result<&T, StamError> {
        match anyid {
            AnyId::None => Err(anyid.get_error("")),
            AnyId::Pointer(pointer) => self.get(*pointer),
            AnyId::Id(id) => self.get_by_id(id)
        }
    }

    /// Get a reference to an item from the store by any ID
    /// If the item does not exist, None will be returned
    fn get_mut_by_anyid(&mut self, anyid: &AnyId<T::PointerType>) -> Option<&mut T> {
        match anyid {
            AnyId::None => None,
            AnyId::Pointer(pointer) => self.get_mut(*pointer).ok(),
            AnyId::Id(id) => self.get_mut_by_id(id).ok()
        }
    }
}

//  generic iterator implementations, these take care of skipping over deleted items (None) and providing a cleaner output reference (no Boxes)

/// This is the iterator to iterate over a Store,  it is created by the iter() method from the [`StoreFor<T>`] trait
pub struct StoreIter<'a, T>(Iter<'a, Option<Box<T>>>);


impl<'a, T> Iterator for StoreIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Some(item)) => Some(item),
            Some(None) => self.next(),
            None => None
        }
    }
}

pub struct StoreIterMut<'a, T>(IterMut<'a, Option<Box<T>>>);

impl<'a, T> Iterator for StoreIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Some(item)) => Some(item),
            Some(None) => self.next(),
            None => None
        }
    }
}





#[derive(Debug,Clone,Deserialize,PartialEq)]
#[serde(untagged)]
/// This is either an public ID or a Pointer
pub enum AnyId<PointerType> where PointerType: Pointer {
    Id(String), //for deserialisation only this variant is avaiable

    #[serde(skip)]
    Pointer(PointerType),

    #[serde(skip)]
    None,
}

impl<PointerType> Default for AnyId<PointerType> where PointerType: Pointer {
    fn default() -> Self {
        Self::None
    }
}


impl<PointerType> AnyId<PointerType> where PointerType: Pointer {
    pub fn is_pointer(&self) -> bool {
        match self {
            Self::Pointer(_) => true,
            _ => false
        }
    }

    pub fn is_id(&self) -> bool {
        match self {
            Self::Id(_) => true,
            _ => false
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            Self::None => true,
            _ => false
        }
    }

    pub fn is_some(&self) -> bool {
        match self {
            Self::None => false,
            _ => true
        }
    }

    // raises an ID error
    pub fn get_error(&self, contextmsg: &'static str) -> StamError {
        match self {
            Self::Pointer(pointer) => StamError::IntIdError(contextmsg),
            Self::Id(id) => StamError::IdError(id.to_string(), contextmsg),
            Self::None => StamError::Unbound("Supplied AnyId is not bound to anything!")
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


impl<PointerType> From<&str> for AnyId<PointerType> where PointerType: Pointer {
    fn from(id: &str) -> Self {
        if id.is_empty()  {
            Self::None
        } else {
            Self::Id(id.to_string())
        }
    }
}
impl<PointerType> From<String> for AnyId<PointerType> where PointerType: Pointer {
    fn from(id: String) -> Self {
        if id.is_empty()  {
            Self::None
        } else {
            Self::Id(id)
        }
    }
}
impl<PointerType> From<PointerType> for AnyId<PointerType> where PointerType: Pointer {
    fn from(pointer: PointerType) -> Self {
        Self::Pointer(pointer)
    }
}
impl<PointerType> From<&PointerType> for AnyId<PointerType> where PointerType: Pointer   {
    fn from(pointer: &PointerType) -> Self {
        Self::Pointer(*pointer)
    }
}

impl<PointerType> From<Option<PointerType>> for AnyId<PointerType> where PointerType: Pointer {
    fn from(pointer: Option<PointerType>) -> Self {
        if let Some(pointer) = pointer {
            Self::Pointer(pointer)
        } else {
            Self::None
        }
    }
}

impl<PointerType> From<Option<&str>> for AnyId<PointerType> where PointerType: Pointer {
    fn from(id: Option<&str>) -> Self {
        if let Some(id) = id {
            if id.is_empty()  {
                Self::None
            } else {
                Self::Id(id.to_string())
            }
        } else {
            Self::None
        }
    }
}

impl<PointerType> From<Option<String>> for AnyId<PointerType> where PointerType: Pointer {
    fn from(id: Option<String>) -> Self {
        if let Some(id) = id {
            if id.is_empty()  {
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
impl<PointerType> From<&dyn Storable<PointerType=PointerType>> for AnyId<PointerType> where PointerType: Pointer {
    fn from(item: &dyn Storable<PointerType=PointerType>) -> Self {
        if let Some(pointer) = item.get_pointer() {
            Self::Pointer(pointer)
        } else if let Some(id) = item.get_id() {
            Self::Id(id.into())
        } else {
            panic!("Passed a reference to an unbound item without a public ID! Unable to convert to IdOrPointer");
        }
    }
}

impl<PointerType> PartialEq<&str> for AnyId<PointerType> where PointerType: Pointer {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Self::Id(v) => v.as_str() == *other,
            _ => false
        }
    }
}

impl<PointerType> PartialEq<str> for AnyId<PointerType> where PointerType: Pointer {
    fn eq(&self, other: &str) -> bool {
        match self {
            Self::Id(v) => v.as_str() == other,
            _ => false
        }
    }
}

impl<PointerType> PartialEq<String> for AnyId<PointerType> where PointerType: Pointer {
    fn eq(&self, other: &String) -> bool {
        match self {
            Self::Id(v) => v == other,
            _ => false
        }
    }
}

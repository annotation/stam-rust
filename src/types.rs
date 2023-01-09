use std::collections::HashMap;
use std::slice::{Iter,IterMut};
use std::borrow::Cow;
use crate::error::StamError;

/// Type for internal numeric IDs. There are nothing more than indices to a vector and this determines the size of the address space
pub type IntId = u32;

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
#[derive(Debug,Clone,Copy)]
pub enum Cursor {
    /// Cursor relative to the start of a text. Has a value of 0 or higher
    BeginAligned(usize),
    /// Cursor relative to the end of a text. Has a value of 0 or lower. The last character of a text begins at EndAlignedCursor(-1) and ends at EndAlignedCursor(0)
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


/// A map mapping public IDs to internal ids, implemented as a HashMap.
/// Used to resolve public IDs to internal ones.
#[derive(Debug)]
pub struct IdMap {
    /// The actual map
    data: HashMap<String,IntId>,

    /// A prefix that automatically generated IDs will get when added to this map
    autoprefix: String,

    ///Sequence number used for ID generation
    seqnr: usize,
}

impl Default for IdMap {
    fn default() -> Self {
        Self {
            data: HashMap::new(),
            autoprefix: "_".to_string(),
            seqnr: 0,
        }
    }
}

impl IdMap {
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


pub struct RelationMap {
    /// The actual map
    pub(crate) data: HashMap<IntId,Vec<IntId>>
}

impl Default for RelationMap {
    fn default() -> Self {
        Self {
            data: HashMap::new()
        }
    }
}

impl RelationMap {
    pub fn new() -> Self { Self::default() }

    pub fn insert(&mut self, x: IntId, y: IntId) {
        self.data.entry(x).or_default().push(y);
    }
}

impl Extend<(IntId,IntId)> for RelationMap {
    fn extend<T>(&mut self, iter: T)  where T: IntoIterator<Item=(IntId,IntId)> {
        for (x,y) in iter {
            self.insert(x,y);
        }
    }
}

pub struct TripleRelationMap {
    /// The actual map
    pub(crate) data: HashMap<IntId,RelationMap>
}

impl Default for TripleRelationMap {
    fn default() -> Self {
        Self {
            data: HashMap::new()
        }
    }
}

impl TripleRelationMap {
    pub fn new() -> Self { Self::default() }

    pub fn insert(&mut self, x: IntId, y: IntId, z: IntId) {
        self.data.entry(x).or_default().insert(y,z);
    }
}

impl Extend<(IntId,IntId,IntId)> for TripleRelationMap {
    fn extend<T>(&mut self, iter: T)  where T: IntoIterator<Item=(IntId,IntId,IntId)> {
        for (x,y,z) in iter {
            self.insert(x,y,z);
        }
    }
}

// ************** The following are high-level abstractions so we only have to implement a certain logic once ***********************

pub trait Storable {
    /// Retrieve the internal (numeric) id. For any type T uses in StoreFor<T>, this may be None only in the initial
    /// stage when it is still unbounded to a store.
    fn get_intid(&self) -> Option<IntId> {
        None
    }

    /// Like [`Self::get_intid()`] but returns a [`StamError:Unbound`] error if there is no internal id.
    fn get_intid_or_err(&self) -> Result<IntId,StamError> {
        self.get_intid().ok_or(StamError::Unbound(""))
    }

    /// Get the global ID
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
    fn set_intid(&mut self, intid: IntId) {
        //no-op in default implementation
    }

    /// Callback function that is called after an item is bound to a store
    fn bound(&mut self) {
        //no-op by default
    }
    /// Generate a random ID in a given idmap (adds it to the map), Item must be bound
    fn generate_id(self, idmap: Option<&mut IdMap>) -> Self where Self: Sized {
        if let Some(intid) = self.get_intid() {
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
    fn get_idmap(&self) -> Option<&IdMap> {
        None
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap> {
        None
    }

    fn introspect_type(&self) -> &'static str;

    /// Adds an item to the store. Returns its internal id upon success
    /// This is a fairly low level method. You will likely want to use [`add`] instead.
    fn insert(&mut self, mut item: T) -> Result<IntId,StamError> {
        let intid = if let Some(intid) = item.get_intid() {
            intid
        } else {
            // item has no internal id yet, i.e. it is unbound
            // we generate an id and bind it now
            let intid = self.next_intid();
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
                idmap.data.insert(id.to_string(), item.get_intid().unwrap())
            });
        } else {
            item = item.generate_id(self.get_mut_idmap());
        }

        //add the resource
        self.get_mut_store().push(Some(Box::new(item)));

        self.inserted(intid);

        //sanity check to ensure no item can determine its own internal id that does not correspond with what's allocated
        assert_eq!(intid, self.get_store().len() as IntId - 1);

        Ok(intid)
    }

    /// Called after an item was inserted to the store
    /// Allows the store to do further bookkeeping
    /// like updating relation maps
    #[allow(unused_variables)]
    fn inserted(&mut self, intid: IntId) {
        //default implementation does nothing
    }

    fn add(mut self, item: T) -> Result<Self, StamError> where Self: Sized {
        self.insert(item)?;
        Ok(self)
    }

    /// Returns true if the store contains the item
    fn contains(&self, item: &T) -> bool {
        if let (Some(intid), Some(true)) = (item.get_intid(), self.owns(item)) {
            self.has(intid)
        } else if let Some(id) = item.get_id() {
            self.has_by_id(id)
        } else {
            false
        }
    }

    /// Retrieves the internal id for the item as it occurs in the store. The passed item and reference item may be distinct instances.
    fn find(&self, item: &T) -> Option<IntId> {
        if let (Some(intid), Some(true)) = (item.get_intid(), self.owns(item)) {
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
    fn has(&self, intid: IntId) -> bool {
        self.get_store().len() > intid as usize
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
        if let Some(idmap) = self.get_idmap() {
            if let Some(intid) = idmap.data.get(id) {
                self.get(*intid)
            } else {
                Err(StamError::IdError(id.to_string(), self.introspect_type()))
            }
        } else {
            Err(StamError::NoIdError(self.introspect_type()))
        }
    }

    /// Get a mutable reference to an item from the store by its global ID
    fn get_mut_by_id<'a>(&'a mut self, id: &str) -> Result<&'a mut T,StamError> {
        if let Some(idmap) = self.get_idmap() {
            if let Some(intid) = idmap.data.get(id) {
                self.get_mut(*intid)
            } else {
                Err(StamError::IdError(id.to_string(), self.introspect_type()))
            }
        } else {
            Err(StamError::NoIdError(self.introspect_type()))
        }
    }

    /// Get a reference to an item from the store by internal ID
    fn get(&self, intid: IntId) -> Result<&T,StamError> {
        if let Some(Some(item)) = self.get_store().get(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid,self.introspect_type()))
        }
    }

    /// Get a mutable reference to an item from the store by internal ID
    fn get_mut(&mut self, intid: IntId) -> Result<&mut T,StamError> {
        if let Some(Some(item)) = self.get_mut_store().get_mut(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid,"Store::get_mut")) //MAYBE TODO: self.introspect_type didn't work here (cannot borrow `*self` as immutable because it is also borrowed as mutable)
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
    fn next_intid(&self) -> IntId {
        self.get_store().len() as IntId
    }

    /// Return the internal id that was assigned to last inserted item
    fn last_intid(&self) -> IntId {
        (self.get_store().len() as IntId) - 1
    }

    /// This binds an item to the store *PRIOR* to it being actually added
    /// You should never need to call this directly (it can only be called once per item anyway).
    fn bind(&mut self, mut item: T) -> Result<T,StamError> {
        //we already pass the internal id this item will get upon the next insert()
        //so it knows its internal id immediate after construction
        if item.get_intid().is_some() {
            Err(StamError::AlreadyBound("bind()") )
        } else {
            item.set_intid(self.next_intid());
            item.bound();
            Ok(item)
        }
    }

    /// Get a reference to an item from the store by any ID
    /// If the item does not exist, None will be returned
    fn get_by_anyid<'a>(&self, anyid: &AnyId<'a>) -> Option<&T> {
        match anyid {
            AnyId::None => None,
            AnyId::IntId(intid) => self.get(*intid).ok(),
            AnyId::Id(Cow::Borrowed(id)) => self.get_by_id(id).ok(),
            AnyId::Id(Cow::Owned(id)) => self.get_by_id(id.as_str()).ok()
        }
    }

    fn get_by_anyid_or_err<'a>(&self, anyid: &AnyId<'a>) -> Result<&T, StamError> {
        match anyid {
            AnyId::None => Err(anyid.get_error("")),
            AnyId::IntId(intid) => self.get(*intid),
            AnyId::Id(Cow::Borrowed(id)) => self.get_by_id(id),
            AnyId::Id(Cow::Owned(id)) => self.get_by_id(id.as_str())
        }
    }

    /// Get a reference to an item from the store by any ID
    /// If the item does not exist, None will be returned
    fn get_mut_by_anyid<'a>(&mut self, anyid: &AnyId<'a>) -> Option<&mut T> {
        match anyid {
            AnyId::None => None,
            AnyId::IntId(intid) => self.get_mut(*intid).ok(),
            AnyId::Id(Cow::Borrowed(id)) => self.get_mut_by_id(id).ok(),
            AnyId::Id(Cow::Owned(id)) => self.get_mut_by_id(id.as_str()).ok()
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

#[derive(Debug,Clone)]
pub enum AnyId<'a> {
    None,
    IntId(IntId),
    Id(Cow<'a, str>)
}

impl<'a> AnyId<'a> {
    pub fn private(&self) -> bool { 
        if let Self::IntId(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_none(&self) -> bool {
        if let Self::None = self {
            true
        } else {
            false
        }
    }

    pub fn is_intid(&self) -> bool {
        match self {
            Self::IntId(_) => true,
            _ => false
        }
    }

    pub fn is_id(&self) -> bool {
        match self {
            Self::Id(_) => true,
            _ => false
        }
    }

    pub fn is_some(&self) -> bool {
        !self.is_none()
    }


    // raises an ID error or Unbound error
    pub fn get_error(&self, contextmsg: &'static str) -> StamError {
        match self {
            Self::IntId(intid) => StamError::IntIdError(*intid, contextmsg),
            Self::Id(id) => StamError::IdError(id.to_string(), contextmsg),
            Self::None => StamError::Unbound(contextmsg)
        }
    }

    pub fn to_string(self) -> Option<String> {
        if let Self::Id(Cow::Owned(s)) = self {
            Some(s)
        } else if let Self::Id(Cow::Borrowed(s)) = self {
            Some(s.to_string())
        } else {
            None
        }
    }
}

impl<'a> Default for AnyId<'a> {
    fn default() -> Self {
        Self::None
    }
}

impl<'a> From<&'a str> for AnyId<'a> {
    fn from(id: &'a str) -> Self {
        if !id.is_empty() {
            AnyId::Id(Cow::Borrowed(id))
        } else {
            AnyId::None
        }
    }
}
impl<'a> From<String> for AnyId<'a> {
    fn from(id: String) -> Self {
        if !id.is_empty() {
            AnyId::Id(Cow::Owned(id))
        } else {
            AnyId::None
        }
    }
}
impl<'a> From<IntId> for AnyId<'a> {
    fn from(intid: IntId) -> Self {
        AnyId::IntId(intid)
    }
}
impl<'a> From<usize> for AnyId<'a> {
    fn from(intid: usize) -> Self {
        AnyId::IntId(intid as IntId)
    }
}
impl<'a> From<Option<IntId>> for AnyId<'a> {
    fn from(intid: Option<IntId>) -> Self {
        if let Some(intid) = intid {
            AnyId::IntId(intid)
        } else {
            AnyId::None
        }
    }
}
impl<'a> From<Option<String>> for AnyId<'a> {
    fn from(id: Option<String>) -> Self {
        if let Some(id) = id {
            if !id.is_empty() {
                AnyId::Id(Cow::Owned(id))
            } else {
                AnyId::None
            }
        } else {
            AnyId::None
        }
    }
}

// this allows us to pass a reference to any stored item and get back the best AnyId for it
impl<'a> From<&'a dyn Storable> for AnyId<'a> {
    fn from(item: &'a dyn Storable) -> Self {
        if let Some(intid) = item.get_intid() {
            AnyId::IntId(intid as IntId)
        } else if let Some(id) = item.get_id() {
            AnyId::Id(id.into())
        } else {
            Self::None
        }
    }
}

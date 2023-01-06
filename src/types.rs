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
pub enum Cursor {
    /// Cursor relative to the start of a text. Has a value of 0 or higher
    BeginAlignedCursor(usize),
    /// Cursor relative to the end of a text. Has a value of 0 or lower. The last character of a text begins at EndAlignedCursor(-1) and ends at EndAlignedCursor(0)
    EndAlignedCursor(isize)
}


impl From<usize> for Cursor {
    fn from(cursor: usize) -> Self {
        Self::BeginAlignedCursor(cursor)
    }
}

impl TryFrom<isize> for Cursor {
    type Error = &'static str;
    fn try_from(cursor: isize) -> Result<Self,Self::Error> {
        if cursor > 0 {
            Err("Cursor is a signed integer and converts to EndAlignedCursor, expected a value <= 0. Conver from an unsigned integer for a normal BeginAlignedCursor")
        } else {
            Ok(Self::EndAlignedCursor(cursor))
        }
    }
}


/// A map mapping public IDs to internal ids, implemented as a HashMap.
/// Used to resolve public IDs to internal ones.
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

// ************** The following are high-level abstractions so we only have to implement a certain logic once ***********************

/// This trait is used on types that may have an internal numeric ID. Though these IDs are internal
/// the trait is public as outside implementations may used the internal ids during their lifetime, they should, however, never be serialised!
pub trait MayHaveIntId {
    /// Retrieve the internal (numeric) id. For any type T uses in StoreFor<T>, this may be None only in the initial
    /// stage when it is still unbounded to a store.
    fn get_intid(&self) -> Option<IntId> {
        None
    }

    /// Like [`Self::get_intid()`] but returns a [`StamError:Unbound`] error if there is no internal id.
    fn get_intid_or_err(&self) -> Result<IntId,StamError> {
        self.get_intid().ok_or(StamError::Unbound(None))
    }
}

/// This trait is used on types that may have an internal id that can be set.
pub(crate) trait SetIntId {
    /// Set the internal ID. May only be called once (though currently not enforced).
    fn set_intid(&mut self, intid: IntId) {
        //no-op in default implementation
    }
}

//^ -- the SetIntId trait is separate from MayHaveIntId because we don't want to expose it publicly.
//     internal ID setting is an internal business.

/// This trait is used on types that can have a public ID
pub trait MayHaveId: MayHaveIntId {
    /// Get the global ID
    fn get_id(&self) -> Option<&str> {
        None
    }
    fn get_id_or_err(&self) -> Result<&str,StamError> {
        self.get_id().ok_or(StamError::NoIdError(None))
    }

    /// Builder pattern to set the public Id
    fn with_id(self, id: String) -> Self where Self: Sized {
        //no-op
        self
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
pub(crate) trait StoreFor<T: MayHaveIntId + SetIntId + MayHaveId> {
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

    fn introspect_type(&self) -> &str;

    /// Add an item to the store. Returns its internal id upon success
    fn add(&mut self, mut item: T) -> Result<IntId,StamError> {
        let intid = self.get_store().len() as IntId;
        item.set_intid(intid);
        self.set_owner_of(&mut item);

        //insert a mapping from the global ID to the numeric ID in the map
        if let Some(id) = item.get_id() {
            //check if global ID does not already exist
            if self.get_by_id(id).is_ok() {
                return Err(StamError::DuplicateIdError(id.to_string(), Some(self.introspect_type().to_string())));
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

        Ok(intid)
    }

    /// Builder pattern, similar to add()
    fn store(mut self, item: T) -> Result<Self,StamError> where Self: Sized {
        if let Err(err) = self.add(item) {
            panic!("Unable to add: {:?}",err);
        }
        Ok(self)
    }

    /// Returns true if the store contains the item
    fn contains(&self, item: &T) -> bool {
        if let (Some(intid), Some(true)) = (item.get_intid(), self.is_owner_of(item)) {
            self.has(intid)
        } else if let Some(id) = item.get_id() {
            self.has_by_id(id)
        } else {
            false
        }
    }

    /// Retrieves the internal id for the item as it occurs in the store. The passed item and reference item may be distinct instances.
    fn find(&self, item: &T) -> Option<IntId> {
        if let (Some(intid), Some(true)) = (item.get_intid(), self.is_owner_of(item)) {
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
                Err(StamError::IdError(id.to_string(), Some(self.introspect_type().to_string())))
            }
        } else {
            Err(StamError::NoIdError(Some(self.introspect_type().to_string())))
        }
    }

    /// Get a mutable reference to an item from the store by its global ID
    fn get_mut_by_id<'a>(&'a mut self, id: &str) -> Result<&'a mut T,StamError> {
        if let Some(idmap) = self.get_idmap() {
            if let Some(intid) = idmap.data.get(id) {
                self.get_mut(*intid)
            } else {
                Err(StamError::IdError(id.to_string(), Some(self.introspect_type().to_string())))
            }
        } else {
            Err(StamError::NoIdError(Some(self.introspect_type().to_string())))
        }
    }

    /// Get a reference to an item from the store by internal ID
    fn get(&self, intid: IntId) -> Result<&T,StamError> {
        if let Some(Some(item)) = self.get_store().get(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid,Some(self.introspect_type().to_string())))
        }
    }

    /// Get a mutable reference to an item from the store by internal ID
    fn get_mut(&mut self, intid: IntId) -> Result<&mut T,StamError> {
        if let Some(Some(item)) = self.get_mut_store().get_mut(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid,Some("get_mut".to_string()))) //MAYBE TODO: self.introspect_type didn't work here
        }
    }

    /// Sets the store (self) as the owner of the item (may be a no-op if no ownership is recorded)
    fn set_owner_of(&self, item: &mut T) {
        //default implementation does nothing
    }

    fn is_owner_of(&self, item: &T) -> Option<bool> {
        //indicated unknown
        None
    }

    /// Iterate over the store
    fn iter<'a>(&'a self) -> Iter<'a, Option<Box<T>>>  {
        self.get_store().iter()
    }

    /// Iterate over the store, mutably
    fn iter_mut<'a>(&'a mut self) -> IterMut<'a, Option<Box<T>>>  {
        self.get_mut_store().iter_mut()
    }

    /// Get the item from the store if it already exists, if not, add it
    fn get_or_add(&mut self, item: T) -> Result<&T,StamError>  {
        if let Some(intid) = self.find(&item) {
            self.get(intid)
        } else {
            match self.add(item) {
                Ok(intid) => self.get(intid),
                Err(err) => Err(err)
            }
        }
    }
}

/// This trait is implemented by stores that convert a builder type to a normal type.
/// A Builder type (Builder*) converts a 'recipe' to an actual instance with properly resolved
/// references.
pub(crate) trait Build<FromType,ToType> {
    /// Builds an item of ToType (A Builder* type) from FromType and returns it
    /// Does not add it to the store yet, see [`Self::build_and_store()`] instead,
    /// However, it may already add necessary dependencies to the store.
    fn build(&mut self, item: FromType) -> Result<ToType,StamError>;
}

/// This trait is implemented by stores that convert a builder type to a normal type.
/// A Builder type (Builder*) converts a 'recipe' to an actual instance with properly resolved
/// references. This is a combined trait that does the build and adds it to the store.
pub(crate) trait BuildAndStore<FromType,ToType>: Build<FromType,ToType> + StoreFor<ToType>  where ToType: MayHaveIntId + SetIntId + MayHaveId {
    /// Builds an item and adds it to the store.
    /// May panic on error!
    fn build_and_store(mut self, item: FromType) -> Result<Self,StamError> where Self: Sized {
        //                                     V---- when there's an error, we wrap it error to give more information
        let newitem: ToType = self.build(item).map_err(|err| StamError::BuildError(Box::new(err),Some(self.introspect_type().to_string())))?;
        self.add(newitem).map_err(|err| StamError::StoreError(Box::new(err),Some(self.introspect_type().to_string())))?;
        Ok(self)
    }
}



use std::collections::HashMap;
use std::slice::{Iter,IterMut};
use crate::error::StamError;

/// Type for internal numeric IDs. This determines the size of the address space
pub type IntId = u32;

/// Type for offsets. This determines the size of the address space, use the platform maximum.
pub type CursorSize = usize;



// ************** The following are high-level abstractions so we only have to implement a certain logic once ***********************

/// This trait is used on types that (may) have an internal numeric ID
pub trait HasIntId {
    /// Retrieve the internal id. This may be None only in the initial stage when it is still unbounded to a store
    fn get_intid(&self) -> Option<IntId>;
    /// Set the internal ID 
    fn set_intid(&mut self, intid: IntId);
}

/// This trait is used on types that can have a global ID
pub trait HasId {
    /// Get the global ID
    fn get_id(&self) -> Option<&str> {
        None
    }

    fn with_id(self, id: String) -> Self where Self: Sized {
        //no-op
        self
    }
}


/// This trait is implemented on types that provide storage for a certain other generic type (T)
/// It requires the types to also implemnet GetStore<T> and HasIdMap<T>
pub trait StoreFor<T: HasIntId + HasId> {
    /// Get a reference to the entire store for the associated type
    fn get_store(&self) -> &Vec<T>;
    /// Get a mutable reference to the entire store for the associated type
    fn get_mut_store(&mut self) -> &mut Vec<T>;
    /// Get a reference to the id map for the associated type, mapping global ids to internal ids
    fn get_idmap(&self) -> Option<&HashMap<String,IntId>> {
        None
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    fn get_mut_idmap(&mut self) -> Option<&mut HashMap<String,IntId>> {
        None
    }


    /// Add an item to the store. Returns its internal id upon success
    fn add(&mut self, mut item: T) -> Result<IntId,StamError> {
        let intid = self.get_store().len() as IntId;
        item.set_intid(intid);
        self.set_owner_of(&mut item);

        //insert a mapping from the global ID to the numeric ID in the map
        if let Some(id) = item.get_id() {
            //check if global ID does not already exist
            if self.get_by_id(id).is_ok() {
                return Err(StamError::DuplicateIdError(id.to_string()));
            }

            self.get_mut_idmap().map(|idmap| {
            //                 v-- MAYBE TODO: optimise the id copy away
                idmap.insert(id.to_string(), item.get_intid().unwrap())
            });
        }

        //add the resource
        self.get_mut_store().push(item);

        Ok(intid)
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

    /// Retrievs a reference to the item as it occurs in the store. The passed item and reference item may be distinct instances.
    fn find<'a>(&'a self, item: &T) -> Option<&'a T> {
        if let (Some(intid), Some(true)) = (item.get_intid(), self.is_owner_of(item)) {
            self.get(intid).ok()
        } else if let Some(id) = item.get_id() {
            self.get_by_id(id).ok()
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
            idmap.contains_key(id)
        } else {
            false
        }
    }

    /// Get a reference to an item from the store by its global ID
    fn get_by_id<'a>(&'a self, id: &str) -> Result<&'a T,StamError> {
        if let Some(idmap) = self.get_idmap() {
            if let Some(intid) = idmap.get(id) {
                self.get(*intid)
            } else {
                Err(StamError::IdError(id.to_string()))
            }
        } else {
            Err(StamError::NoIdError)
        }
    }

    /// Get a mutable reference to an item from the store by its global ID
    fn get_mut_by_id<'a>(&'a mut self, id: &str) -> Result<&'a mut T,StamError> {
        if let Some(idmap) = self.get_idmap() {
            if let Some(intid) = idmap.get(id) {
                self.get_mut(*intid)
            } else {
                Err(StamError::IdError(id.to_string()))
            }
        } else {
            Err(StamError::NoIdError)
        }
    }

    /// Get a reference to an item from the store by internal ID
    fn get(&self, intid: IntId) -> Result<&T,StamError> {
        if let Some(item) = self.get_store().get(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid))
        }
    }

    /// Get a mutable reference to an item from the store by internal ID
    fn get_mut(&mut self, intid: IntId) -> Result<&mut T,StamError> {
        if let Some(item) = self.get_mut_store().get_mut(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid))
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
    fn iter<'a>(&'a self) -> Iter<'a, T>  {
        self.get_store().iter()
    }

    /// Iterate over the store, mutably
    fn iter_mut<'a>(&'a mut self) -> IterMut<'a, T>  {
        self.get_mut_store().iter_mut()
    }

    /// Get the item from the store if it already exists, if not, add it
    fn get_or_add(&mut self, item: T) -> Result<&T,StamError>  {
        if self.contains(&item) { //TODO: this check should be superfluous (find already does it) but I'm fighting the borrow checker if I remove it..
            let itemref = self.find(&item).unwrap();
            Ok(itemref)
        } else {
            match self.add(item) {
                Ok(intid) => self.get(intid),
                Err(err) => Err(err)
            }
        }
    }

}


use std::collections::HashMap;
use crate::error::StamError;

/// Type for internal numeric IDs. This determines the size of the address space
pub type IntId = u32;

/// Type for offsets. This determines the size of the address space, use the platform maximum.
pub type CursorSize = usize;




// ************** The following are high-level abstractions so we only have to implement a certain logic once ***********************

/// This trait is used on types that (may) have an internal numeric ID
pub trait HasIntId {
    fn get_intid(&self) -> Option<IntId>;
    fn set_intid(&mut self, intid: IntId);
}


/// This trait is used on types that may have a global ID
pub trait HasId {
    fn get_id(&self) -> Option<&str>;
}

pub trait OwnedBy<T> {
    fn set_owner(&mut self, owner: &T) {
        //no op by default, override when needed
    }
}

pub trait GetStore<T> {
    fn get_store(&self) -> &Vec<T>;
    fn get_mut_store(&mut self) -> &mut Vec<T>;
}

pub trait GetIdMap<T> {
    fn get_idmap(&self) -> &HashMap<T,IntId>;
    fn get_mut_idmap(&self) -> &mut HashMap<T,IntId>;
}



/// This trait is implemented on types that provide storage for a certain other generic type (T)
pub(crate) trait StoreFor<T: HasIntId + HasId>: GetStore<T> {
    fn add(&mut self, mut item: T, store: &mut Vec<T>, index: Option<&mut HashMap<String,IntId>>) -> Result<(),StamError> {
        item.set_intid(store.len() as IntId);
        //item.set_owner(self);

        //insert a mapping from the global ID to the numeric ID in the map
        if let (Some(index), Some(id)) = (index, item.get_id()) {
            //check if global ID does not already exis
            if let Err(err) = self.get_by_id(id, store, index) {
                return Err(StamError::DuplicateIdError(id.to_string()));
            }

            //               v-- MAYBE TODO: optimise the copy away
            index.insert(id.to_string(), item.get_intid().unwrap());
        }

        //add the resource
        store.push(item);

        Ok(())
    }

    fn get_by_id<'a>(&self, id: &str, store: &'a Vec<T>, index: &HashMap<String,IntId>) -> Result<&'a T,StamError> {
        if let Some(intid) = index.get(id) {
            self.get(*intid, store)
        } else {
            Err(StamError::IdError(id.to_string()))
        }
    }

    fn get_mut_by_id<'a>(&mut self, id: &str, store: &'a mut Vec<T>, index: &HashMap<String,IntId>) -> Result<&'a mut T,StamError> {
        if let Some(intid) = index.get(id) {
            self.get_mut(*intid, store)
        } else {
            Err(StamError::IdError(id.to_string()))
        }
    }

    fn get<'a>(&self, intid: IntId, store: &'a Vec<T>) -> Result<&'a T,StamError> {
        if let Some(item) = store.get(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid))
        }
    }

    fn get_mut<'a>(&mut self, intid: IntId, store: &'a mut Vec<T>) -> Result<&'a mut T,StamError> {
        if let Some(item) = store.get_mut(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid))
        }
    }
}

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

pub trait GetStore<T> {
    fn get_store(&self) -> &Vec<T>;
    fn get_mut_store(&mut self) -> &mut Vec<T>;
}

pub trait GetIdMap<T> {
    fn get_idmap(&self) -> &HashMap<String,IntId>;
    fn get_mut_idmap(&mut self) -> &mut HashMap<String,IntId>;
}

/// This trait is implemented on types that provide storage for a certain other generic type (T)
pub(crate) trait StoreFor<T>: GetStore<T>  + GetIdMap<T> where T: HasIntId + HasId {
    fn add(&mut self, mut item: T) -> Result<(),StamError> {
        item.set_intid(self.get_store().len() as IntId);
        self.own(&mut item);

        //insert a mapping from the global ID to the numeric ID in the map
        if let Some(id) = item.get_id() {
            //check if global ID does not already exis
            if self.get_by_id(id).is_ok() {
                return Err(StamError::DuplicateIdError(id.to_string()));
            }

            //               v-- MAYBE TODO: optimise the copy away
            self.get_mut_idmap().insert(id.to_string(), item.get_intid().unwrap());
        }

        //add the resource
        self.get_mut_store().push(item);

        Ok(())
    }

    fn get_by_id<'a>(&'a self, id: &str) -> Result<&'a T,StamError> {
        if let Some(intid) = self.get_idmap().get(id) {
            self.get(*intid)
        } else {
            Err(StamError::IdError(id.to_string()))
        }
    }

    fn get_mut_by_id<'a>(&'a mut self, id: &str) -> Result<&'a mut T,StamError> {
        if let Some(intid) = self.get_idmap().get(id) {
            self.get_mut(*intid)
        } else {
            Err(StamError::IdError(id.to_string()))
        }
    }

    fn get(&self, intid: IntId) -> Result<&T,StamError> {
        if let Some(item) = self.get_store().get(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid))
        }
    }

    fn get_mut(&mut self, intid: IntId) -> Result<&mut T,StamError> {
        if let Some(item) = self.get_mut_store().get_mut(intid as usize) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid))
        }
    }

    fn own(&self, item: &mut T) {
        //default implementation does nothing
    }

}

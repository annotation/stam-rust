use std::collections::HashMap;
use crate::error::StamError;

/// Type for internal numeric IDs. This determines the size of the address space
pub type IntId = u32;

/// Type for offsets. This determines the size of the address space, use the platform maximum.
pub type CursorSize = usize;


pub trait HasIntId {
    fn get_intid(&self) -> Option<IntId> {
        return self.intid;
    }
}

pub trait HasId {
    fn get_id(&self) -> Option<&str> {
        return self.id;
    }
}

pub trait StoreFor<T: HasIntId + HasId> {
    fn add(&mut self, item: T, store: &mut Vec<T>, index: &mut HashMap<T,IntId>) -> Result<(),StamError> {
        item.intid = Some(store.len());

        //check if global ID does not already exist
        if let Err(err) = self.get_by_id(&item.id) {
            return Err(StamError::DuplicateIdError(item.id));
        }

        //insert a mapping from the global ID to the numeric ID in the map
        let key = item.id.clone(); //MAYBE TODO: optimise the clone() away
        self.index.insert(key, item.intid);

        //add the resource
        self.resources.push(item);
    }

    fn get_by_id(&self, id: &str, store: &Vec<T>, index: &HashMap<T,IntId>) -> Result<&T,StamError> {
        if let Some(intid) = self.resource_index.get(id) {
            self.get_resource_int(intid)
        } else {
            Err(StamError::IdError(id))
        }
    }

    fn get_by_intid(&self, intid: IntId, store: &Vec<T>) -> Result<&T,StamError> {
        if let Some(item) = self.store.get(intid) {
            Ok(item)
        } else {
            Err(StamError::IntIdError(intid))
        }
    }
}

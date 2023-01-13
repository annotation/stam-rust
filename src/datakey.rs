use std::fmt;
use serde::{Serialize,Deserialize};
use serde::ser::{Serializer, SerializeStruct};
//use serde_json::Result;

use crate::types::*;
use crate::annotationdataset::{AnnotationDataSet,AnnotationDataSetHandle};

/// The DataKey class defines a vocabulary field, it 
/// belongs to a certain [`AnnotationDataSet`]. An `AnnotationData`
/// in turn makes reference to a DataKey and assigns it a value.
#[derive(Deserialize,Debug)]
pub struct DataKey {
    /// The Id is the name that identifies this key, it must be unique in the dataset to which it pertains
    #[serde(rename="@id")]
    id: String,

    //indexed: bool,  //TODO: handle later

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::keys that has the ownership. May be unbound (None) only during creation.
    #[serde(skip)]
    intid: Option<DataKeyHandle>,

    ///Refers to internal ID of the AnnotationDataSet (as owned by AnnotationStore) that owns this DataKey. May be unbound (None) only during creation.
    #[serde(skip)]
    pub(crate) part_of_set: Option<AnnotationDataSetHandle>
}

impl Serialize for DataKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> 
    where S: Serializer {
        let mut state = serializer.serialize_struct("DataKey",1)?;
        state.serialize_field("@type", "DataKey")?;
        state.serialize_field("@id", &self.id)?;
        state.end()
    }
}


#[derive(Clone,Copy,Debug,PartialEq,Eq,PartialOrd,Hash)]
pub struct DataKeyHandle(u16);
impl Handle for DataKeyHandle {
    fn new(intid: usize) -> Self { Self(intid as u16) }
    fn unwrap(&self) -> usize { self.0 as usize }
}

impl Storable for DataKey {
    type HandleType = DataKeyHandle;

    fn id(&self) -> Option<&str> { 
        Some(self.id.as_str())
    }
    fn handle(&self) -> Option<DataKeyHandle> { 
        self.intid
    }
    fn set_handle(&mut self, intid: DataKeyHandle) {
        self.intid = Some(intid);
    }
}

impl fmt::Display for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,"{}",self.as_str())
    }
}

impl PartialEq<str> for DataKey {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<DataKey> for str {
    fn eq(&self, other: &DataKey) -> bool {
        other.as_str() == self
    }
}

impl DataKey {
    ///Creates a new DataKey which you can add to an AnnotationDataSet using AnnotationDataSet.add_key()
    pub fn new(id: String) -> Self {
        Self { 
            id,
            intid: None,
            part_of_set: None
        }
    }

    /// Returns the global id that identifier the key. This is a bit shorter than using get_id()
    pub fn as_str(&self) -> &str {
        self.id.as_str()
    }

    /// Returns a handle to the [`AnnotationDataSet`]
    pub fn annotationset(&self) -> Option<AnnotationDataSetHandle> {
        self.part_of_set
    }
}

impl<'a> WrappedStorable<'a, DataKey, AnnotationDataSet> {
    /// Shortcut to return a reference to the dataset
    pub fn annotationset_as_ref(&'a self) -> &'a AnnotationDataSet {
        self.store()
    }
}

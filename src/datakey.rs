use sealed::sealed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use std::fmt;
//use serde_json::Result;

use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::error::StamError;
use crate::store::*;
use crate::types::*;

/// The DataKey class defines a vocabulary field, it
/// belongs to a certain [`AnnotationDataSet`]. An `AnnotationData`
/// in turn makes reference to a DataKey and assigns it a value.
#[derive(Deserialize, Debug)]
pub struct DataKey {
    /// The Id is the name that identifies this key, it must be unique in the dataset to which it pertains
    #[serde(rename = "@id")]
    id: String,

    //indexed: bool,  //TODO: handle later
    ///Internal numeric ID, corresponds with the index in the AnnotationStore::keys that has the ownership. May be unbound (None) only during creation.
    #[serde(skip)]
    intid: Option<DataKeyHandle>,

    ///Refers to internal ID of the AnnotationDataSet (as owned by AnnotationStore) that owns this DataKey. May be unbound (None) only during creation.
    #[serde(skip)]
    pub(crate) part_of_set: Option<AnnotationDataSetHandle>,
}

impl Serialize for DataKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DataKey", 1)?;
        state.serialize_field("@type", "DataKey")?;
        state.serialize_field("@id", &self.id)?;
        state.end()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataKeyHandle(u16);

#[sealed]
impl Handle for DataKeyHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u16)
    }
    fn unwrap(&self) -> usize {
        self.0 as usize
    }
}
// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> From<&DataKeyHandle> for Item<'a, DataKey> {
    fn from(handle: &DataKeyHandle) -> Self {
        Item::Handle(*handle)
    }
}
impl<'a> From<Option<&DataKeyHandle>> for Item<'a, DataKey> {
    fn from(handle: Option<&DataKeyHandle>) -> Self {
        if let Some(handle) = handle {
            Item::Handle(*handle)
        } else {
            Item::None
        }
    }
}
impl<'a> From<DataKeyHandle> for Item<'a, DataKey> {
    fn from(handle: DataKeyHandle) -> Self {
        Item::Handle(handle)
    }
}
impl<'a> From<Option<DataKeyHandle>> for Item<'a, DataKey> {
    fn from(handle: Option<DataKeyHandle>) -> Self {
        if let Some(handle) = handle {
            Item::Handle(handle)
        } else {
            Item::None
        }
    }
}

#[sealed]
impl TypeInfo for DataKey {
    fn typeinfo() -> Type {
        Type::DataKey
    }
}

#[sealed]
impl Storable for DataKey {
    type HandleType = DataKeyHandle;
    type StoreType = AnnotationDataSet;

    fn id(&self) -> Option<&str> {
        Some(self.id.as_str())
    }
    fn handle(&self) -> Option<DataKeyHandle> {
        self.intid
    }
    fn set_handle(&mut self, intid: DataKeyHandle) {
        self.intid = Some(intid);
    }

    fn with_id(mut self, id: String) -> Self {
        self.id = id;
        self
    }

    fn carries_id() -> bool {
        true
    }
}

impl PartialEq<DataKey> for DataKey {
    fn eq(&self, other: &DataKey) -> bool {
        self.id == other.id
    }
}

impl fmt::Display for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
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
            part_of_set: None,
        }
    }

    /// Returns the global id that identifies the key. This is a bit shorter than using get_id()
    pub fn as_str(&self) -> &str {
        self.id.as_str()
    }

    /// Returns a handle to the [`AnnotationDataSet`]
    pub fn annotationset(&self) -> Option<AnnotationDataSetHandle> {
        self.part_of_set
    }

    /// Writes a datakey to a STAM JSON string, with appropriate formatting
    pub fn to_json(&self) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        serde_json::to_string_pretty(&self).map_err(|e| {
            StamError::SerializationError(format!("Serializing datakey to string: {}", e))
        })
    }

    /// Writes a datakey to a STAM JSON string, without any indentation
    pub fn to_json_compact(&self) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        serde_json::to_string(&self).map_err(|e| {
            StamError::SerializationError(format!("Serializing datakey to string: {}", e))
        })
    }
}

impl<'store, 'slf> WrappedItem<'store, DataKey> {
    /// Shortcut to return a reference to the dataset
    pub fn set(&'slf self) -> &'store AnnotationDataSet {
        self.store()
    }

    /// Returns an iterator over all data ([`AnnotationData`]) that makes use of this key. The iterator returns the data as [`WrappedItem<AnnotationData>`].
    pub fn data(
        &'slf self,
    ) -> Option<impl Iterator<Item = WrappedItem<'store, AnnotationData>> + 'slf> {
        if let Some(vec) = self
            .store()
            .data_by_key(&self.handle().expect("key must have handle").into())
        {
            Some(
                vec.iter().filter_map(|data_handle| {
                    self.store().annotationdata(&Item::Handle(*data_handle))
                }),
            )
        } else {
            None
        }
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that makes use of this key.
    /// The iterator returns the annotations as [`WrappedItem<Annotation>`].
    /// Especially useful in combination with a call to  [`AnnotationDataSet.key()`] first.
    pub fn annotations(
        &'slf self,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = WrappedItem<'store, Annotation>> + 'slf> {
        if let Some(iter) = annotationstore.annotations_by_key(
            self.set().handle().expect("set must have handle"),
            self.handle().expect("key must have handle"),
        ) {
            Some(iter.filter_map(|a_handle| annotationstore.annotation(&Item::Handle(a_handle))))
        } else {
            None
        }
    }
}

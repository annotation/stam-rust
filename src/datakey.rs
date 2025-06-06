/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the low-level API for [`DataKey`]. It defines and implements the
//! struct, the handle, and things like serialisation, deserialisation to STAM JSON.

use datasize::DataSize;
use minicbor::{Decode, Encode};
use sealed::sealed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use std::fmt;
//use serde_json::Result;

use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::error::StamError;
use crate::store::*;
use crate::types::*;

/// The DataKey structure defines a *vocabulary field* or *feature*, as it is called in some
/// annotation paradigms. it belongs to a certain [`AnnotationDataSet`]. An
/// [`AnnotationData`](crate::AnnotationData) instance in turn makes reference to a DataKey and assigns it a value,
/// producing a full key/value pair.
#[derive(Deserialize, Debug, Clone, DataSize, Encode, Decode)]
pub struct DataKey {
    //indexed: bool,  //TODO: handle later
    ///Internal numeric ID, corresponds with the index in the AnnotationStore::keys that has the ownership. May be unbound (None) only during creation.
    #[serde(skip)]
    #[n(0)] //these macros are for cbor binary (de)serialisation
    intid: Option<DataKeyHandle>,

    /// The Id is the name (string) that identifies this key, it must be unique in the dataset to which it pertains
    #[serde(rename = "@id")]
    #[n(1)]
    id: String,
}

impl Serialize for DataKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DataKey", 2)?;
        state.serialize_field("@type", "DataKey")?;
        state.serialize_field("@id", &self.id)?;
        state.end()
    }
}

impl DataKey {
    /// Writes a datakey to STAM JSON string, with appropriate formatting
    pub fn to_json_string(&self) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        serde_json::to_string_pretty(self)
            .map_err(|e| StamError::SerializationError(format!("Writing key to string: {}", e)))
    }

    /// Writes a datakey to a JSON value
    pub fn to_json_value(&self) -> Result<serde_json::Value, StamError> {
        serde_json::to_value(self)
            .map_err(|e| StamError::SerializationError(format!("Producing Json Value: {}", e)))
    }
}

/// [Handle] to an instance of [`DataKey`] in the store ([`AnnotationDataSet`])
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, DataSize, Encode, Decode)]
#[cbor(transparent)]
pub struct DataKeyHandle(#[n(0)] u16);

#[sealed]
impl Handle for DataKeyHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u16)
    }
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}
// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> Request<DataKey> for DataKeyHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<DataKeyHandle>
    where
        S: StoreFor<DataKey>,
    {
        Some(*self)
    }
}
impl<'a> Request<DataKey> for Option<DataKeyHandle> {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<DataKeyHandle>
    where
        S: StoreFor<DataKey>,
    {
        *self
    }
    fn any(&self) -> bool {
        self.is_none()
    }
}

impl<'a> From<&DataKeyHandle> for BuildItem<'a, DataKey> {
    fn from(handle: &DataKeyHandle) -> Self {
        BuildItem::Handle(*handle)
    }
}
impl<'a> From<Option<&DataKeyHandle>> for BuildItem<'a, DataKey> {
    fn from(handle: Option<&DataKeyHandle>) -> Self {
        if let Some(handle) = handle {
            BuildItem::Handle(*handle)
        } else {
            BuildItem::None
        }
    }
}
impl<'a> From<DataKeyHandle> for BuildItem<'a, DataKey> {
    fn from(handle: DataKeyHandle) -> Self {
        BuildItem::Handle(handle)
    }
}
impl<'a> From<Option<DataKeyHandle>> for BuildItem<'a, DataKey> {
    fn from(handle: Option<DataKeyHandle>) -> Self {
        if let Some(handle) = handle {
            BuildItem::Handle(handle)
        } else {
            BuildItem::None
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
    type StoreHandleType = AnnotationDataSetHandle;
    type FullHandleType = (AnnotationDataSetHandle, DataKeyHandle);
    type StoreType = AnnotationDataSet;

    fn id(&self) -> Option<&str> {
        Some(self.id.as_str())
    }
    fn handle(&self) -> Option<DataKeyHandle> {
        self.intid
    }
    fn with_handle(mut self, intid: DataKeyHandle) -> Self {
        self.intid = Some(intid);
        self
    }

    fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    fn carries_id() -> bool {
        true
    }

    fn fullhandle(
        storehandle: Self::StoreHandleType,
        handle: Self::HandleType,
    ) -> Self::FullHandleType {
        (storehandle, handle)
    }

    fn merge(&mut self, other: Self) -> Result<(), StamError> {
        let intid = self.intid;
        *self = other;
        self.intid = intid; //retain same internal ID
        Ok(())
    }

    fn unbind(mut self) -> Self {
        self.intid = None;
        self
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
    ///Creates a new DataKey which you can add to an [`AnnotationDataSet`] using its [`StoreFor<DataKey>.add()`] method.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            intid: None,
        }
    }

    /// Returns the global id that identifies the key.
    pub fn as_str(&self) -> &str {
        self.id.as_str()
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

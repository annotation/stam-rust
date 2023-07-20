use datasize::DataSize;
use minicbor::{Decode, Encode};
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
#[derive(Deserialize, Debug, Clone, DataSize, Encode, Decode)]
pub struct DataKey {
    //indexed: bool,  //TODO: handle later
    ///Internal numeric ID, corresponds with the index in the AnnotationStore::keys that has the ownership. May be unbound (None) only during creation.
    #[serde(skip)]
    #[n(0)] //these macros are for cbor binary (de)serialisation
    intid: Option<DataKeyHandle>,

    /// The Id is the name that identifies this key, it must be unique in the dataset to which it pertains
    #[serde(rename = "@id")]
    #[n(1)]
    id: String,
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
impl<'a> ToHandle<DataKey> for DataKeyHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<DataKeyHandle>
    where
        S: StoreFor<DataKey>,
    {
        Some(*self)
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

    fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
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
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            intid: None,
        }
    }

    /// Returns the global id that identifies the key. This is a bit shorter than using get_id()
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

impl<'store, 'slf> ResultItem<'store, DataKey> {
    /// Shortcut to return a reference to the dataset
    pub fn set(&'slf self) -> &'store AnnotationDataSet {
        self.store()
    }

    /// Returns the global id that identifies the key. This is a bit shorter than using get_id()
    pub fn as_str(&'slf self) -> &'store str {
        self.as_ref().as_str()
    }

    /// Returns an iterator over all data ([`AnnotationData`]) that makes use of this key. The iterator returns the data as [`WrappedItem<AnnotationData>`].
    pub fn data(
        &'slf self,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'slf> {
        if let Some(vec) = self.store().data_by_key(self.handle()) {
            Some(
                vec.iter()
                    .filter_map(|data_handle| self.store().annotationdata(*data_handle)),
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
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + 'slf> {
        if let Some(iter) = annotationstore.annotations_by_key(
            self.set().handle().expect("set must have handle"),
            self.handle(),
        ) {
            Some(iter.filter_map(|a_handle| annotationstore.annotation(a_handle)))
        } else {
            None
        }
    }

    /// Returns the number of annotations that make use of this key.
    ///  Note: this method has suffix `_count` instead of `_len` because it is not O(1) but does actual counting (O(n) at worst).
    pub fn annotations_count(&'slf self, annotationstore: &'store AnnotationStore) -> usize {
        if let Some(iter) = annotationstore.annotations_by_key(
            self.set().handle().expect("set must have handle"),
            self.handle(),
        ) {
            iter.count()
        } else {
            0
        }
    }

    /// Tests whether two DataKeys are the same
    pub fn test(&'slf self, other: &BuildItem<DataKey>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }
}

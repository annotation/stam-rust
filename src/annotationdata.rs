/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the low-level API for [`AnnotationData`]. It defines and implements the
//! struct, the handle, and things like serialisation, deserialisation to STAM JSON.

//use Chrono::DateTime;
use datasize::DataSize;
use minicbor::{Decode, Encode};
use sealed::sealed;
use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
//use serde_json::Result;

use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::DataValue;
use crate::error::StamError;
use crate::store::*;
use crate::types::*;

/// AnnotationData holds the actual content of an annotation; a key/value pair. (the
/// term *feature* is regularly seen for this in certain annotation paradigms).
/// Annotation Data is deliberately decoupled from the actual [`Annotation`](crate::Annotation)
/// instances so multiple annotation instances can point to the same content
/// without causing any overhead in storage. Moreover, it facilitates indexing and
/// searching. The annotation data is part of an [`AnnotationDataSet`](crate::AnnotationDataSet), which
/// effectively defines a certain user-defined vocabulary.
///
/// Once instantiated, instances of this type are, by design, largely immutable.
/// The key and value can not be changed. Create a new AnnotationData and new Annotation for edits.
#[derive(Debug, Clone, DataSize, Encode, Decode)]
pub struct AnnotationData {
    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership
    #[n(0)]
    pub(crate) intid: Option<AnnotationDataHandle>,

    /// Public identifier
    #[n(1)]
    pub(crate) id: Option<String>,

    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    #[n(2)]
    pub(crate) key: DataKeyHandle,

    //Actual annotation value
    #[n(3)]
    value: DataValue,
}

/// [Handle] to an instance of [`AnnotationData`] in the store ([`AnnotationDataSet`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, DataSize, Encode, Decode)]
#[cbor(transparent)]
pub struct AnnotationDataHandle(#[n(0)] u32);

#[sealed]
impl Handle for AnnotationDataHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u32)
    }
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> Request<AnnotationData> for AnnotationDataHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<AnnotationDataHandle>
    where
        S: StoreFor<AnnotationData>,
    {
        Some(*self)
    }
}
impl From<AnnotationDataHandle> for BuildItem<'_, AnnotationData> {
    fn from(handle: AnnotationDataHandle) -> Self {
        Self::Handle(handle)
    }
}
impl From<&AnnotationDataHandle> for BuildItem<'_, AnnotationData> {
    fn from(handle: &AnnotationDataHandle) -> Self {
        Self::Handle(*handle)
    }
}

#[sealed]
impl TypeInfo for AnnotationData {
    fn typeinfo() -> Type {
        Type::AnnotationData
    }
}

#[sealed]
impl Storable for AnnotationData {
    type HandleType = AnnotationDataHandle;
    type StoreHandleType = AnnotationDataSetHandle;
    type FullHandleType = (AnnotationDataSetHandle, AnnotationDataHandle);
    type StoreType = AnnotationDataSet;

    fn handle(&self) -> Option<AnnotationDataHandle> {
        self.intid
    }

    fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    fn with_handle(mut self, intid: AnnotationDataHandle) -> Self {
        self.intid = Some(intid);
        self
    }
    fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
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
}

impl PartialEq<AnnotationData> for AnnotationData {
    fn eq(&self, other: &AnnotationData) -> bool {
        self.id.is_some()
            && self.id == other.id
            && self.key == other.key
            && self.value == other.value
    }
}

impl<'a> Serialize for ResultItem<'a, AnnotationData> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationData", 2)?;
        state.serialize_field("@type", "AnnotationData")?;
        if let Some(id) = self.id() {
            state.serialize_field("@id", id)?;
        } else {
            state.serialize_field(
                "@id",
                &self.as_ref().temp_id().expect("temp_id must succeed"),
            )?;
        }
        // we don't use self.key() because we have a partial ResultItem and it'll panic,
        // we need to take a small detour to get the key here
        let keyhandle = self.as_ref().key();

        let key = self.store().get(keyhandle).expect("key must exist");
        state.serialize_field("key", &key.id())?;
        state.serialize_field("value", self.as_ref().value())?;
        state.end()
    }
}

// This is just a newtype wrapping the one above, and used if one explicitly wants to serialize a set (needed if serialized from Annotation context)
pub(crate) struct AnnotationDataRefImpliedSet<'a>(pub(crate) ResultItem<'a, AnnotationData>);

impl<'a> Serialize for AnnotationDataRefImpliedSet<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationData", 2)?;
        state.serialize_field("@type", "AnnotationData")?;
        if let Some(id) = self.0.id() {
            state.serialize_field("@id", id)?;
        } else {
            state.serialize_field(
                "@id",
                &self.0.as_ref().temp_id().expect("temp_id must succeed"),
            )?;
        }
        state.serialize_field("key", &self.0.key().id())?;
        state.serialize_field("value", self.0.as_ref().value())?;
        state.end()
    }
}

impl<'a> Serialize for WrappedStore<'a, AnnotationData, AnnotationDataSet> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.store.len()))?;
        for data in self.store.iter() {
            if let Some(data) = data {
                seq.serialize_element(&ResultItem::new_partial(data, self.parent))?;
            }
        }
        seq.end()
    }
}

impl AnnotationData {
    /// Creates a new unbounded AnnotationData instance, you will likely never want to instantiate this directly, but via
    //// [`AnnotationDataSet::with_data()`] or indirectly [`AnnotationBuilder::with_data()`].
    pub(crate) fn new(id: Option<String>, key: DataKeyHandle, value: DataValue) -> Self {
        AnnotationData {
            id,
            key,
            value,
            intid: None,
        }
    }

    pub fn key(&self) -> DataKeyHandle {
        self.key
    }

    /// Get the value of this annotationdata. The value will be a DataValue instance. This will return an immutable reference.
    /// Note that there is no mutable variant nor a set_value(), values can deliberately only be set once at instantiation.
    /// Make a new AnnotationData if you want to change data.
    pub fn value(&self) -> &DataValue {
        &self.value
    }

    /// Writes an Annotation to one big STAM JSON string, with appropriate formatting
    pub fn to_json(&self, store: &AnnotationDataSet) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        let resultitem: ResultItem<Self> = ResultItem::new_partial(self, store);
        serde_json::to_string_pretty(&resultitem).map_err(|e| {
            StamError::SerializationError(format!("Writing annotation dataset to string: {}", e))
        })
    }
}

/// This is the builder for [`AnnotationData`]. It contains public IDs or handles that will be resolved.
/// This structure is usually not instantiated directly but via the [`AnnotationBuilder.with_data()`](crate::AnnotationBuilder::with_data), [`AnnotationDataSet.insert_data()`](crate::AnnotationDataSet::insert_data) or [`AnnotationDataSet.with_data()`](crate::AnnotationDataSet::with_data()) or [`AnnotationDataSet.build_insert_data()`](crate::AnnotationDataSet::build_insert_data()) methods.
/// It also does not have its own `build()` method but is resolved via the aforementioned methods.
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "AnnotationData")]
#[serde(from = "AnnotationDataJson")]
pub struct AnnotationDataBuilder<'a> {
    #[serde(rename = "@id")]
    pub(crate) id: BuildItem<'a, AnnotationData>,
    #[serde(rename = "set")]
    pub(crate) dataset: BuildItem<'a, AnnotationDataSet>,
    pub(crate) key: BuildItem<'a, DataKey>,
    pub(crate) value: DataValue,
}

impl<'a> Default for AnnotationDataBuilder<'a> {
    fn default() -> Self {
        Self {
            id: BuildItem::None,
            dataset: BuildItem::None,
            key: BuildItem::None,
            value: DataValue::Null,
        }
    }
}

impl<'a> AnnotationDataBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: BuildItem<'a, AnnotationData>) -> Self {
        self.id = id;
        self
    }

    pub fn id(&self) -> &BuildItem<AnnotationData> {
        &self.id
    }

    pub fn with_dataset(mut self, dataset: BuildItem<'a, AnnotationDataSet>) -> Self {
        self.dataset = dataset;
        self
    }

    pub fn dataset(&self) -> &BuildItem<AnnotationDataSet> {
        &self.dataset
    }

    pub fn with_key(mut self, key: BuildItem<'a, DataKey>) -> Self {
        self.key = key;
        self
    }

    pub fn key(&self) -> &BuildItem<DataKey> {
        &self.key
    }

    pub fn with_value(mut self, value: DataValue) -> Self {
        self.value = value;
        self
    }

    pub fn value(&self) -> &DataValue {
        &self.value
    }
}

/// Helper structure for deserialisation
#[derive(Deserialize)]
pub(crate) struct AnnotationDataJson {
    #[serde(rename = "@id")]
    id: Option<String>,
    set: Option<String>,
    key: Option<String>,
    value: Option<DataValue>,
}

impl<'a> From<AnnotationDataJson> for AnnotationDataBuilder<'a> {
    fn from(helper: AnnotationDataJson) -> Self {
        Self {
            id: helper.id.into(),
            dataset: helper.set.into(),
            key: helper.key.into(),
            value: helper.value.unwrap_or(DataValue::Null),
        }
    }
}

//use Chrono::DateTime;
use sealed::sealed;
use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
//use serde_json::Result;

use crate::annotation::Annotation;
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::error::StamError;
use crate::store::*;
use crate::types::*;

/// AnnotationData holds the actual content of an annotation; a key/value pair. (the
/// term *feature* is regularly seen for this in certain annotation paradigms).
/// Annotation Data is deliberately decoupled from the actual ``Annotation``
/// instances so multiple annotation instances can point to the same content
/// without causing any overhead in storage. Moreover, it facilitates indexing and
/// searching. The annotation data is part of an `AnnotationDataSet`, which
/// effectively defines a certain user-defined vocabulary.
///
/// Once instantiated, instances of this type are, by design, largely immutable.
/// The key and value can not be changed. Create a new AnnotationData and new Annotation for edits.
#[derive(Debug, Clone)]
pub struct AnnotationData {
    /// Public identifier
    id: Option<String>,

    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    pub(crate) key: DataKeyHandle,

    //Actual annotation value
    value: DataValue,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership
    intid: Option<AnnotationDataHandle>,
    ///Referers to internal ID of the AnnotationDataSet (as owned by AnnotationStore) that owns this DataKey
    pub(crate) part_of_set: Option<AnnotationDataSetHandle>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AnnotationDataHandle(u32);
#[sealed]
impl Handle for AnnotationDataHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u32)
    }
    fn unwrap(&self) -> usize {
        self.0 as usize
    }
}

// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> From<&AnnotationDataHandle> for Item<'a, AnnotationData> {
    fn from(handle: &AnnotationDataHandle) -> Self {
        Item::Handle(*handle)
    }
}
impl<'a> From<Option<&AnnotationDataHandle>> for Item<'a, AnnotationData> {
    fn from(handle: Option<&AnnotationDataHandle>) -> Self {
        if let Some(handle) = handle {
            Item::Handle(*handle)
        } else {
            Item::None
        }
    }
}
impl<'a> From<AnnotationDataHandle> for Item<'a, AnnotationData> {
    fn from(handle: AnnotationDataHandle) -> Self {
        Item::Handle(handle)
    }
}
impl<'a> From<Option<AnnotationDataHandle>> for Item<'a, AnnotationData> {
    fn from(handle: Option<AnnotationDataHandle>) -> Self {
        if let Some(handle) = handle {
            Item::Handle(handle)
        } else {
            Item::None
        }
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
    type StoreType = AnnotationDataSet;

    fn handle(&self) -> Option<AnnotationDataHandle> {
        self.intid
    }

    fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }
    fn set_handle(&mut self, intid: AnnotationDataHandle) {
        self.intid = Some(intid);
    }

    fn carries_id() -> bool {
        true
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

impl<'a> Serialize for WrappedItem<'a, AnnotationData> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationData", 2)?;
        state.serialize_field("@type", "AnnotationData")?;
        if let Some(id) = self.id() {
            state.serialize_field("@id", id)?;
        }
        state.serialize_field("key", &self.key().id())?;
        state.serialize_field("value", self.value())?;
        state.end()
    }
}

// This is just a newtype wrapping the one above, and used if one explicitly wants to serialize a set (needed if serialized from Annotation context)
pub(crate) struct AnnotationDataRefWithSet<'a>(pub(crate) WrappedItem<'a, AnnotationData>);

impl<'a> Serialize for AnnotationDataRefWithSet<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationData", 2)?;
        state.serialize_field("@type", "AnnotationData")?;
        state.serialize_field("@id", &self.0.id())?;
        state.serialize_field("key", &self.0.key().id())?;
        state.serialize_field("value", self.0.value())?;
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
                if let Ok(data) = self.parent.wrap(data) {
                    seq.serialize_element(&data)?;
                } else {
                    return Err(serde::ser::Error::custom(
                        "Unable to wrap annotationdata during serialization",
                    ));
                }
            }
        }
        seq.end()
    }
}

impl AnnotationData {
    /// Creates a new unbounded AnnotationData instance, you will likely never want to instantiate this directly, but via
    //// [`AnnotationDataSet::with_data()`] or indirectly [`AnnotationBuilder::with_data()`].
    pub fn new(id: Option<String>, key: DataKeyHandle, value: DataValue) -> Self {
        AnnotationData {
            id,
            key,
            value,
            intid: None,
            part_of_set: None,
        }
    }

    /// Returns an Annotation data builder to build new annotationdata
    pub fn builder<'a>() -> AnnotationDataBuilder<'a> {
        AnnotationDataBuilder::default()
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
        let wrapped: WrappedItem<Self> = WrappedItem::borrow(self, store)?;
        serde_json::to_string_pretty(&wrapped).map_err(|e| {
            StamError::SerializationError(format!("Writing annotation dataset to string: {}", e))
        })
    }
}

impl<'store, 'slf> WrappedItem<'store, AnnotationData> {
    /// Return a reference to the AnnotationDataSet that holds this data (and its key)
    pub fn set(&'slf self) -> &'store AnnotationDataSet {
        self.store()
    }

    pub fn key(&'slf self) -> WrappedItem<'store, DataKey> {
        self.store()
            .key(&Item::Handle(self.deref().key()))
            .expect("AnnotationData must always have a key at this point")
    }

    /// Returns an iterator over all annotations ([`Annotation`]) that makes use of this data.
    /// The iterator returns the annotations as [`WrappedItem<Annotation>`].
    /// Especially useful in combination with a call to  [`WrappedItem<AnnotationDataSet>.find_data()`] or [`AnnotationDataSet.annotationdata()`] first.
    pub fn annotations(
        &'slf self,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = WrappedItem<'store, Annotation>> + 'store> {
        if let Some(vec) = annotationstore.annotations_by_data(
            self.set().handle().expect("set must have handle"),
            self.handle().expect("data must have handle"),
        ) {
            Some(
                vec.iter()
                    .filter_map(|a_handle| annotationstore.annotation(&Item::Handle(*a_handle))),
            )
        } else {
            None
        }
    }

    /// Returns the number of annotations ([`Annotation`]) that make use of this data.
    pub fn annotations_len(&'slf self, annotationstore: &'store AnnotationStore) -> usize {
        if let Some(vec) = annotationstore.annotations_by_data(
            self.set().handle().expect("set must have handle"),
            self.handle().expect("data must have handle"),
        ) {
            vec.len()
        } else {
            0
        }
    }

    pub fn test(&self, key: Option<&Item<DataKey>>, operator: &DataOperator) -> bool {
        if key.is_none() || self.key().test(key.unwrap()) {
            self.value().test(operator)
        } else {
            false
        }
    }
}

/// This is the builder for `AnnotationData`. It contains public IDs or handles that will be resolved.
/// It is usually not instantiated directly but used via the [`AnnotationBuilder.with_data()`], [`AnnotationBuilder.insert_data()`] or [`AnnotationDataSet.with_data()`] methods.
/// It also does not have its own `build()` method but is resolved via the aforementioned methods.
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "AnnotationData")]
#[serde(from = "AnnotationDataJson")]
pub struct AnnotationDataBuilder<'a> {
    #[serde(rename = "@id")]
    pub(crate) id: Item<'a, AnnotationData>,
    #[serde(rename = "set")]
    pub(crate) annotationset: Item<'a, AnnotationDataSet>,
    pub(crate) key: Item<'a, DataKey>,
    pub(crate) value: DataValue,
}

impl<'a> Default for AnnotationDataBuilder<'a> {
    fn default() -> Self {
        Self {
            id: Item::None,
            annotationset: Item::None,
            key: Item::None,
            value: DataValue::Null,
        }
    }
}

impl<'a> AnnotationDataBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: Item<'a, AnnotationData>) -> Self {
        self.id = id;
        self
    }

    pub fn id(&self) -> &Item<AnnotationData> {
        &self.id
    }

    pub fn with_annotationset(mut self, annotationset: Item<'a, AnnotationDataSet>) -> Self {
        self.annotationset = annotationset;
        self
    }

    pub fn annotationset(&self) -> &Item<AnnotationDataSet> {
        &self.annotationset
    }

    pub fn with_key(mut self, key: Item<'a, DataKey>) -> Self {
        self.key = key;
        self
    }

    pub fn key(&self) -> &Item<DataKey> {
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
            annotationset: helper.set.into(),
            key: helper.key.into(),
            value: helper.value.unwrap_or(DataValue::Null),
        }
    }
}

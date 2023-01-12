//use Chrono::DateTime;
use serde::{Serialize,Deserialize};
use serde::ser::{Serializer, SerializeStruct};
//use serde_json::Result;

use crate::types::*;
use crate::annotationstore::AnnotationStore;
use crate::annotationdataset::{AnnotationDataSet,AnnotationDataSetHandle};
use crate::datakey::{DataKey,DataKeyHandle};
use crate::datavalue::DataValue;
use crate::error::StamError;




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
    pub(crate) part_of_set: Option<AnnotationDataSetHandle>
}

#[derive(Clone,Copy,Debug,PartialEq,Eq,PartialOrd,Hash)]
pub struct AnnotationDataHandle(u32);
impl Handle for AnnotationDataHandle {
    fn new(intid: usize) -> Self { Self(intid as u32) }
    fn unwrap(&self) -> usize { self.0 as usize }
}

impl Storable for AnnotationData {
    type HandleType = AnnotationDataHandle;

    fn handle(&self) -> Option<AnnotationDataHandle> { 
        self.intid
    }

    fn id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }

    fn with_id(mut self, id:String) ->  Self {
        self.id = Some(id);
        self
    }
    fn set_handle(&mut self, intid: AnnotationDataHandle) {
        self.intid = Some(intid);
    }
}


impl Serialize for AnnotationData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> 
    where S: Serializer {
        let mut state = serializer.serialize_struct("AnnotationData",2)?;
        state.serialize_field("@type", "AnnotationData")?;
        if let Some(id) = self.id() {
            state.serialize_field("@id", id)?;
        }
        state.serialize_field("value", self.value())?;
        state.end()
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
            part_of_set: None
        }
    }

    /// Return a reference to the AnnotationDataSet that holds this data (and its key) 
    pub fn dataset_as_ref<'a>(&self, annotationstore: &'a AnnotationStore) -> Result<&'a AnnotationDataSet,StamError> {
        if let Some(part_of_set) = self.part_of_set {
           annotationstore.get(part_of_set)
        } else {
            Err(StamError::Unbound("AnnotationData.get_dataset failed due to unbound part_of_set"))
        }
    }

    /// Return a reference to the DataKey used by this data
    pub fn key_as_ref<'a>(&self, dataset: &'a AnnotationDataSet) -> Result<&'a DataKey,StamError> {
        dataset.get(self.key())
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
}

/// This is the build recipe for `AnnotationData`. It contains public IDs or handles that will be resolved.
/// It is usually not instantiated directly but used via the [`AnnotationBuilder.with_data()`], [`AnnotationBuilder.insert_data()`] or [`AnnotationDataSet.with_data()`] methods.
#[derive(Deserialize,Debug)]
#[serde(tag="AnnotationData")]
#[serde(from="AnnotationDataJson")]
pub struct AnnotationDataBuilder {
    #[serde(rename="@id")]
    pub id: AnyId<AnnotationDataHandle>,
    #[serde(rename="set")]
    pub annotationset: AnyId<AnnotationDataSetHandle>,
    pub key: AnyId<DataKeyHandle>,
    pub value: DataValue,
}

impl Default for AnnotationDataBuilder {
    fn default() -> Self {
        Self {
            id: AnyId::None,
            annotationset: AnyId::None,
            key: AnyId::None,
            value: DataValue::Null,
        }
    }
}

/// Helper structure for deserialisation
#[derive(Deserialize)]
pub(crate) struct AnnotationDataJson {
    #[serde(rename="@id")]
    id: Option<String>,
    set: Option<String>,
    key: Option<String>,
    value: Option<DataValue>,
}

impl From<AnnotationDataJson> for AnnotationDataBuilder { 
    fn from(helper: AnnotationDataJson) -> Self {
        Self {
            id: helper.id.into(),
            annotationset: helper.set.into(),
            key: helper.key.into(),
            value: helper.value.unwrap_or(DataValue::Null),
        }
    }
}

//use Chrono::DateTime;
use std::collections::{HashSet, HashMap};
use std::borrow::Cow;
use serde::{Serialize,Deserialize};
use serde::ser::{Serializer, SerializeStruct};
//use serde_json::Result;

use crate::types::*;
use crate::annotationstore::AnnotationStore;
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
    key: IntId,

    //Actual annotation value
    value: DataValue,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership 
    intid: Option<IntId>,
    ///Referers to internal IDs of Annotation (as owned by an AnnotationStore) that use this dataset
    referenced_by: Vec<IntId>,
    ///Referers to internal ID of the AnnotationDataSet (as owned by AnnotationStore) that owns this DataKey
    part_of_set: Option<IntId>
}


impl MayHaveIntId for AnnotationData {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
}
impl SetIntId for AnnotationData {
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}

impl MayHaveId for AnnotationData {
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
    fn with_id(mut self, id:String) ->  Self {
        self.id = Some(id);
        self
    }
}


impl Serialize for AnnotationData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> 
    where S: Serializer {
        let mut state = serializer.serialize_struct("AnnotationData",1)?;
        state.serialize_field("@type", "AnnotationData")?;
        if let Some(id) = self.get_id() {
            state.serialize_field("@id", id)?;
        }
        state.end()
    }
}

impl AnnotationData {
    /// Create a new unbounded AnnotationData instance, you will likely want to use BuildAnnotationData::new() instead and pass it to AnnotationDataSet.build()
    pub fn new(id: Option<String>, key: IntId, value: DataValue) -> Self {
        AnnotationData {
            id,
            key,
            value,
            intid: None,
            referenced_by: Vec::new(),
            part_of_set: None
        }
    }

    /// Return a reference to the AnnotationDataSet that holds this data (and its key) 
    pub fn get_dataset<'a>(&self, annotationstore: &'a AnnotationStore) -> Option<&'a AnnotationDataSet> {
        if let Some(part_of_set) = self.part_of_set {
           annotationstore.get(part_of_set).ok()
        } else {
            None
        }
    }

    /// Return a reference to the DataKey used by this data
    pub fn get_key<'a>(&self, annotationstore: &'a AnnotationStore) -> Result<&'a DataKey,StamError> {
        if let Some(dataset) = self.get_dataset(annotationstore) {
            dataset.get(self.key)
        } else {
            Err(StamError::Unbound(None))
        }
    }

    /// Get the value of this annotationdata. The value will be a DataValue instance. This will return an immutable reference.
    /// Note that there is no mutable variant nor a set_value(), values can deliberately only be set once at instantiation. 
    /// Make a new AnnotationData if you want to change data.
    pub fn get_value(&self) -> &DataValue {
        &self.value
    }

    /// Returns an iterator over all the Annotations that reference this data
    pub fn referenced_by<'a>(&self, annotationstore: &'a AnnotationStore) {
        //TODO: implement
    }
}

/// This is the build recipe for AnnotationData. It contains references to public IDs that will be resolved
/// when the actual AnnotationData is build. The building is done by the build() method on the store that 
/// owns AnnotationData, i.e. an AnnotationDataSet.
pub struct BuildAnnotationData<'a> {
    ///The public ID for the AnnotationData. (This is a copy-on-work type)
    id: Cow<'a,str>,
    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    key: Cow<'a,str>,
    pub value: DataValue,
}

impl<'a> BuildAnnotationData<'a> {
    /// Build Annotation Data, parameters are str references, used new_owned() instead if you have Strings (saves a copy)
    pub fn new(id: &'a str, key: &'a str, value: DataValue) -> Self {
        Self {
            id: Cow::Borrowed(id),
            key: Cow::Borrowed(key),
            value
        }
    }

    /// Build Annotation Data, parameters are from owned strings
    pub fn new_owned(id: String, key: String, value: DataValue) -> Self {
        Self {
            id: Cow::Owned(id),
            key: Cow::Owned(key),
            value
        }
    }
}


// The following two implementations allows for building AnnotationData from BuildAnnotationData.
// This is done on the AnnotationDataSet that is the store for the AnnotationData.


impl<'a> Build<BuildAnnotationData<'a>,AnnotationData> for AnnotationDataSet {
    fn build(&mut self, item: BuildAnnotationData<'a>) -> Result<AnnotationData,StamError> {
        let key_intid = if let Ok::<&DataKey,_>(key) = self.get_by_id(&item.key)  {
            key.get_intid_or_err()?
        } else {
            let datakey = DataKey::new(item.key.to_string(),false);
            self.add(datakey)?
        };
        Ok(AnnotationData::new(Some(item.id.to_string()), key_intid, item.value))
    }
}

impl<'a> BuildAndStore<BuildAnnotationData<'a>,AnnotationData> for AnnotationDataSet {}



pub struct AnnotationDataSet {
    id: Option<String>,
    keys: Store<DataKey>,
    data: Store<AnnotationData>,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership 
    intid: Option<IntId>,

    key_idmap: IdMap,
    data_idmap: IdMap
}

impl MayHaveId for AnnotationDataSet {
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
    fn with_id(mut self, id: String) ->  Self {
        self.id = Some(id);
        self
    }
}

impl MayHaveIntId for AnnotationDataSet {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
}

impl SetIntId for AnnotationDataSet {
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}


impl StoreFor<DataKey> for AnnotationDataSet {
    fn get_store(&self) -> &Store<DataKey> {
        &self.keys
    }
    fn get_mut_store(&mut self) -> &mut Store<DataKey> {
        &mut self.keys
    }
    fn get_idmap(&self) -> Option<&IdMap> {
        Some(&self.key_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap> {
        Some(&mut self.key_idmap)
    }
    fn set_owner_of(&self, item: &mut DataKey) {
        item.part_of_set = self.get_intid();
    }
    fn is_owner_of(&self, item: &DataKey) -> Option<bool> {
        if item.part_of_set.is_none() || self.get_intid().is_none() {
            //ownership is unclear because one of both is unbound
            None
        } else {
            Some(item.part_of_set == self.get_intid())
        }
    }
    fn introspect_type(&self) -> &'static str {
        "DataKey in AnnotationDataSet"
    }
}

impl StoreFor<AnnotationData> for AnnotationDataSet {
    fn get_store(&self) -> &Store<AnnotationData> {
        &self.data
    }
    fn get_mut_store(&mut self) -> &mut Store<AnnotationData> {
        &mut self.data
    }
    fn get_idmap(&self) -> Option<&IdMap> {
        Some(&self.data_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap> {
        Some(&mut self.data_idmap)
    }
    fn set_owner_of(&self, item: &mut AnnotationData) {
        item.part_of_set = self.get_intid();
    }
    fn is_owner_of(&self, item: &AnnotationData) -> Option<bool> {
        if item.part_of_set.is_none() || self.get_intid().is_none() {
            //ownership is unclear because one of both is unbound
            None
        } else {
            Some(item.part_of_set == self.get_intid())
        }
    }
    fn introspect_type(&self) -> &'static str {
        "AnnotationData in AnnotationDataSet"
    }
}

impl Default for AnnotationDataSet {
    fn default() -> Self {
        Self {
            id: None,
            keys: Store::new(),
            data: Store::new(),
            intid: None,
            key_idmap: IdMap::new("K".to_string()),
            data_idmap: IdMap::new("D".to_string())
        }
    }
}


impl AnnotationDataSet {
    pub fn new() -> Self {
        Self::default()
    }

}

#[derive(Serialize,Deserialize)]
#[serde(tag = "@type", content="value")]
pub enum DataValue {
    ///No value
    Null,
    String(String),
    Bool(bool),
    Int(usize),
    Float(f64),
    //Datetime(chrono::DateTime), //TODO

    /// Value is an unordered set
    //Set(HashSet<DataValue>),

    //Value is an ordered list
    List(Vec<DataValue>)
}

impl From<&str> for DataValue {
    fn from(item: &str) -> Self {
        Self::String(item.to_string())
    }
}

impl From<String> for DataValue {
    fn from(item: String) -> Self {
        Self::String(item)
    }
}

impl From<f64> for DataValue {
    fn from(item: f64) -> Self {
        Self::Float(item)
    }
}

impl From<f32> for DataValue {
    fn from(item: f32) -> Self {
        Self::Float(item as f64)
    }
}

impl From<usize> for DataValue {
    fn from(item: usize) -> Self {
        Self::Int(item)
    }
}

impl From<u64> for DataValue {
    fn from(item: u64) -> Self {
        Self::Int(item as usize)
    }
}

impl From<u32> for DataValue {
    fn from(item: u32) -> Self {
        Self::Int(item as usize)
    }
}

impl From<u16> for DataValue {
    fn from(item: u16) -> Self {
        Self::Int(item as usize)
    }
}

impl From<u8> for DataValue {
    fn from(item: u8) -> Self {
        Self::Int(item as usize)
    }
}

impl From<bool> for DataValue {
    fn from(item: bool) -> Self {
        Self::Bool(item)
    }
}

impl From<Vec<DataValue>> for DataValue {
    fn from(item: Vec<DataValue>) -> Self {
        Self::List(item)
    }
}

/// The DataKey class defines a vocabulary field in STAM, it 
/// belongs to a certain `AnnotationDataSet`. An `AnnotationData`
/// in turn makes reference to a DataKey and assigns it a value.
#[derive(Deserialize)]
pub struct DataKey {
    /// The Id is the name that identifies this key, it must be unique in the dataset to which it pertains
    #[serde(rename="@id")]
    id: String,

    #[serde(skip)]
    indexed: bool,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::keys that has the ownership. May be unbound (None) only during creation.
    #[serde(skip)]
    intid: Option<IntId>,
    ///Refers to internal IDs of AnnotationData (as owned by an AnnotationDataSet)
    #[serde(skip)]
    referenced_by: Vec<IntId>,
    ///Refers to internal ID of the AnnotationDataSet (as owned by AnnotationStore) that owns this DataKey. May be unbound (None) only during creation.
    #[serde(skip)]
    part_of_set: Option<IntId>
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



impl MayHaveId for DataKey {
    fn get_id(&self) -> Option<&str> { 
        Some(self.id.as_str())
    }
}

impl MayHaveIntId for DataKey {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
}
impl SetIntId for DataKey {
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}

impl DataKey {
    ///Creates a new DataKey which you can add to an AnnotationDataSet using AnnotationDataSet.add_key()
    pub fn new(id: String, indexed: bool) -> Self {
        Self { 
            id,
            indexed,
            intid: None,
            referenced_by: Vec::new(),
            part_of_set: None
        }
    }

    pub fn get_dataset<'a>(&self, annotationstore: &'a AnnotationStore) -> Option<&'a AnnotationDataSet> {
        if let Some(part_of_set) = self.part_of_set {
           annotationstore.get(part_of_set).ok()
        } else {
            None
        }
    }
}

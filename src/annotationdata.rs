//use Chrono::DateTime;
use std::collections::{HashSet, HashMap};
use std::borrow::Cow;
use serde::{Serialize,Deserialize};
use serde::ser::{Serializer, SerializeStruct};
//use serde_json::Result;

use crate::types::*;
use crate::annotationstore::AnnotationStore;
use crate::error::StamError;



pub struct AnnotationData {
    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    id: Option<String>,
    key: IntId,
    pub value: DataValue,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership 
    intid: Option<IntId>,
    ///Referers to internal IDs of Annotation (as owned by an AnnotationStore) that use this dataset
    referenced_by: Vec<IntId>,
    ///Referers to internal ID of the AnnotationDataSet (as owned by AnnotationStore) that owns this DataKey
    part_of_set: Option<IntId>
}


impl HasIntId for AnnotationData {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}

impl HasId for AnnotationData {
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
    pub fn get_key<'a>(&self, annotationstore: &'a AnnotationStore) -> Option<&'a DataKey> {
        if let Some(dataset) = self.get_dataset(annotationstore) {
            dataset.get(self.key).ok()
        } else {
            None
        }
    }

    /// Returns an iterator over all the Annotations that reference this data
    pub fn referenced_by<'a>(&self, annotationstore: &'a AnnotationStore) {
        //TODO: implement
    }
}

pub struct BuildAnnotationData<'a> {
    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    id: Cow<'a,str>,
    key: Cow<'a,str>,
    pub value: DataValue,
}

impl<'a> BuildAnnotationData<'a> {
    pub fn new(id: &'a str, key: &'a str, value: DataValue) -> Self {
        Self {
            id: Cow::Borrowed(id),
            key: Cow::Borrowed(key),
            value
        }
    }

    pub fn new_owned(id: String, key: String, value: DataValue) -> Self {
        Self {
            id: Cow::Owned(id),
            key: Cow::Owned(key),
            value
        }
    }
}

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
    keys: Vec<DataKey>,
    data: Vec<AnnotationData>,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership 
    intid: Option<IntId>,

    key_idmap: IdMap,
    data_idmap: IdMap
}

impl HasId for AnnotationDataSet {
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
    fn with_id(mut self, id: String) ->  Self {
        self.id = Some(id);
        self
    }
}

impl HasIntId for AnnotationDataSet {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}


impl StoreFor<DataKey> for AnnotationDataSet {
    fn get_store(&self) -> &Vec<DataKey> {
        &self.keys
    }
    fn get_mut_store(&mut self) -> &mut Vec<DataKey> {
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
    fn get_store(&self) -> &Vec<AnnotationData> {
        &self.data
    }
    fn get_mut_store(&mut self) -> &mut Vec<AnnotationData> {
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
            keys: Vec::new(),
            data: Vec::new(),
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



impl HasId for DataKey {
    fn get_id(&self) -> Option<&str> { 
        Some(self.id.as_str())
    }
}

impl HasIntId for DataKey {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
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

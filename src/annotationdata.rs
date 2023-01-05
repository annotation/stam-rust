//use Chrono::DateTime;
use std::collections::{HashSet, HashMap};
use serde::{Deserialize, Serialize};
//use serde_json::Result;

use crate::types::*;
use crate::annotationstore::AnnotationStore;
use crate::error::StamError;



pub struct AnnotationData {
    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
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

impl HasId for AnnotationData {}


impl AnnotationData {
    /// Return a reference to the AnnotationDataSet that holds this data (and its key) 
    pub fn get_dataset<'a>(&self, annotationstore: &'a AnnotationStore) -> Option<&'a AnnotationDataSet> {
        if let Some(part_of_set) = self.part_of_set {
           annotationstore.get_dataset(part_of_set) 
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

pub struct AnnotationDataSet {
    id: Option<String>,
    keys: Vec<DataKey>,
    data: Vec<AnnotationData>,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership 
    intid: Option<IntId>,

    key_idmap: HashMap<String,IntId>
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
    fn get_idmap(&self) -> Option<&HashMap<String,IntId>> {
        Some(&self.key_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut HashMap<String,IntId>> {
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
}

impl StoreFor<AnnotationData> for AnnotationDataSet {
    fn get_store(&self) -> &Vec<AnnotationData> {
        &self.data
    }
    fn get_mut_store(&mut self) -> &mut Vec<AnnotationData> {
        &mut self.data
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
}

impl Default for AnnotationDataSet {
    fn default() -> Self {
        Self {
            id: None,
            keys: Vec::new(),
            data: Vec::new(),
            intid: None,
            key_idmap: HashMap::new()
        }
    }
}


impl AnnotationDataSet {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Serialize, Deserialize, Debug)]
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

/// The DataKey class defines a vocabulary field in STAM, it 
/// belongs to a certain `AnnotationDataSet`. An `AnnotationData`
/// in turn makes reference to a DataKey and assigns it a value.
#[derive(Serialize, Deserialize, Debug)]
pub struct DataKey {
    #[serde(rename="@id")]
    id: String,
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
           annotationstore.get_dataset(part_of_set) 
        } else {
            None
        }
    }
}

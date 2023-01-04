//use Chrono::DateTime;
use std::collections::{HashSet, HashMap};

use crate::types::*;
use crate::error::StamError;


pub struct AnnotationData {
    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    pub key: IntId,
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

impl GetId for AnnotationData {}


pub trait PartOfSet {
    fn get_set(&self) -> &AnnotationDataSet;
}

pub struct AnnotationDataSet {
    pub id: Option<String>,
    pub keys: Vec<DataKey>,
    pub data: Vec<AnnotationData>,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership 
    intid: Option<IntId>,

    key_idmap: HashMap<String,IntId>
}

impl GetId for AnnotationDataSet {
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
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

impl AnnotationDataSet {
    /// Add a new key to an annotation data set
    pub fn add_key(&mut self, key: DataKey) -> Result<(),StamError> {
        self.add(key)
    }
}

pub enum DataValue {
    ///No value
    Null,
    String(String),
    Bool(bool),
    Int(usize),
    Float(f64),
    //Datetime(chrono::DateTime), //TODO

    /// Value is an unordered set
    Set(HashSet<DataValue>),

    //Value is an ordered list
    List(Vec<DataValue>)
}

/// The DataKey class defines a vocabulary field in STAM, it 
/// belongs to a certain `AnnotationDataSet`. An `AnnotationData`
/// in turn makes reference to a DataKey and assigns it a value.
pub struct DataKey {
    pub id: String,
    pub indexed: bool,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::keys that has the ownership. May be unbound (None) only during creation.
    pub(crate) intid: Option<IntId>,
    ///Refers to internal IDs of AnnotationData (as owned by an AnnotationDataSet)
    pub(crate) referenced_by: Vec<IntId>,
    ///Refers to internal ID of the AnnotationDataSet (as owned by AnnotationStore) that owns this DataKey. May be unbound (None) only during creation.
    pub(crate) part_of_set: Option<IntId>
}

impl GetId for DataKey {
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
}

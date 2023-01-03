use Chrono::DateTime;
use std::collections::{HashSet, HashMap};

use crate::types::*;
use crate::error::StamError;


pub struct AnnotationData {
    pub id: Option<String>,

    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    pub key: IntId,
    pub value: DataValue,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership 
    pub(crate) intid: IntId,
    ///Referers to internal IDs of Annotation (as owned by an AnnotationStore) that use this dataset
    pub(crate) referenced_by: Vec<IntId>,
    ///Referers to internal ID of the AnnotationDataSet (as owned by AnnotationStore) that owns this DataKey
    pub(crate) part_of_set: IntId
}

pub struct AnnotationDataSet {
    pub id: Option<String>,
    pub keys: Vec<DataKey>,
    pub data: Vec<AnnotationData>,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership 
    pub(crate) intid: IntId,

    pub(crate) key_index: HashMap<String,IntId>
}

pub enum DataValue {
    ///No value
    Null,
    String(String),
    Bool(bool),
    Int(usize),
    Float(f64),
    Datetime(chrono::DateTime),

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


impl AnnotationDataSet {
    /// Add a new key to an annotation data set
    pub fn add_key(&mut self, key: DataKey) -> Result<(),StamError> {
        key.intid = Some(self.keys.len());
        key.part_of_set = Some(self.intid);

        //check if key does not already exist within this set
        if let Err(err) = self.key(&key.id) {
            return Err(StamError::DuplicateIdError(key.id));
        }

        //insert a mapping from the global ID to the numeric ID in the map
        let key = key.id.clone(); //MAYBE TODO: optimise the clone() away
        self.key_index.insert(key, key.intid.unwrap());

        //add the key
        self.keys.push(key);

        Ok(())
    }

    pub fn add_data(&mut self, data: AnnotationData) {
    }



}

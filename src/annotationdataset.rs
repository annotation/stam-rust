use std::borrow::Cow;
use std::fmt;
use serde::{Serialize,Deserialize};
use serde::ser::{Serializer, SerializeStruct};
//use serde_json::Result;

use crate::types::*;
use crate::annotationstore::AnnotationStore;
use crate::annotationdata::AnnotationData;
use crate::datakey::DataKey;
use crate::datavalue::DataValue;
use crate::error::StamError;

/// An `AnnotationDataSet` stores the keys [`DataKey`] and values
/// [`AnnotationData`] (which in turn encapsulates [`DataValue`]) that are used by annotations.
/// It effectively defines a certain vocabulary, i.e. key/value pairs. 
/// The `AnnotationDataSet` does not store the [`Annotation`] instances themselves, those are in
/// the `AnnotationStore`. The datasets themselves are also held by the `AnnotationStore`.
pub struct AnnotationDataSet {
    /// Public Id
    id: Option<String>,

    /// A store for [`DataKey`]
    keys: Store<DataKey>,
    /// A store for [`AnnotationData`], each makes *reference* to a [`DataKey`] (in this same `AnnotationDataSet`) and gives it a value  ([`DataValue`])
    data: Store<AnnotationData>,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership 
    intid: Option<IntId>,

    /// Maps public IDs to internal IDs for 
    key_idmap: IdMap,

    /// Maps public IDs to internal IDs for AnnotationData
    data_idmap: IdMap,

    key_data_map: RelationMap
}


impl Storable for AnnotationDataSet {
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
    fn with_id(mut self, id: String) ->  Self {
        self.id = Some(id);
        self
    }
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
}

impl MutableStorable for AnnotationDataSet {
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }

    /// Sets the ownership of all items in the store
    /// This ensure the part_of_set relation (backreference)
    /// is set right.
    fn bound(&mut self) {
        let intid = self.get_intid().expect("getting internal id");
        let datastore: &mut Store<AnnotationData> = self.get_mut_store();
        for data in datastore.iter_mut() {
            if let Some(data) = data {
                data.part_of_set = Some(intid);
            }
        }
        let keystore: &mut Store<DataKey> = self.get_mut_store();
        for key in keystore.iter_mut() {
            if let Some(key) = key {
                key.part_of_set = Some(intid);
            }
        }
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
    fn owns(&self, item: &DataKey) -> Option<bool> {
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
    fn owns(&self, item: &AnnotationData) -> Option<bool> {
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

    fn inserted(&mut self, intid: IntId) {
        // called after the item is inserted in the store
        // update the relation map
        let annotationdata: &AnnotationData = self.get(intid).expect("item must exist after insertion");

        self.key_data_map.data.entry(annotationdata.key).or_default().push(intid);
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
            data_idmap: IdMap::new("D".to_string()),
            key_data_map: RelationMap::new(),
        }
    }
}

impl AnnotationDataSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds new [`AnnotationData`] to the dataset, this should be
    /// Note: if you don't want to set an ID (first argument), you can just just pass "".into()
    pub fn with_data<'a>(mut self, id: AnyId<'a>, key: AnyId<'a>, value: DataValue) -> Result<Self, StamError> {
        self.insert_data(id,key,value,true)?;
        Ok(self)
    }

    /// Adds new [`AnnotationData`] to the dataset. Use [`with_data`] instead if you are using a regular builder pattern.
    /// Note: if you don't want to set an ID (first argument), you can just just pass "".into()
    pub fn insert_data<'a>(&mut self, id: AnyId<'a>, key: AnyId<'a>, value: DataValue, safety: bool) -> Result<IntId, StamError> {
        let annotationdata: Option<&AnnotationData> = self.get_by_anyid(&id);
        if annotationdata.is_some() {
            return Err(StamError::AlreadyExists(annotationdata.unwrap().get_intid_or_err()?, "Data with this ID already exists"));
        }
        if key.is_none() {
            return Err(StamError::IncompleteError("Key supplied to AnnotationDataSet.with_data() can not be None"));
        }

        let datakey: Option<&DataKey> = self.get_by_anyid(&key);
        let mut newkey = false;
        let key_intid = if let Some(datakey) = datakey {
            datakey.get_intid_or_err()?
        } else if key.is_id() {
            //datakey not found, create new one and add it to the store
            newkey = true;
            self.insert(DataKey::new(key.to_string().unwrap()))?
        } else {
            return Err(key.get_error("Datakey not found by AnnotationDataSet.with_data()"));
        };

        if !newkey && id.is_none() && safety {
            // there is a chance that this key and value combination already occurs, check it
            if let Some(dataitems) = self.key_data_map.data.get(&key_intid) {
                for intid in dataitems.iter() { //MAYBE TODO: this may get slow if there is a key with a lot of data values
                    let data: &AnnotationData = self.get(*intid).expect("getting item");
                    if data.get_value() == &value {
                        return Err(StamError::AlreadyExists(*intid, "Data with this exact key and value already exists"));
                    }
                }
            }
        }

        let public_id: Option<String> = id.to_string();

        self.insert(AnnotationData::new(public_id, key_intid, value))
    }


}

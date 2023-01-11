use serde::{Serialize,Deserialize};
use serde::ser::{Serializer, SerializeStruct};
use serde_with::serde_as;
//use serde_json::Result;

use crate::types::*;
use crate::annotationdata::{AnnotationData,AnnotationDataBuilder,AnnotationDataPointer};
use crate::datakey::{DataKey,DataKeyPointer};
use crate::datavalue::DataValue;
use crate::error::StamError;

/// An `AnnotationDataSet` stores the keys [`DataKey`] and values
/// [`AnnotationData`] (which in turn encapsulates [`DataValue`]) that are used by annotations.
/// It effectively defines a certain vocabulary, i.e. key/value pairs. 
/// The `AnnotationDataSet` does not store the [`Annotation`] instances themselves, those are in
/// the `AnnotationStore`. The datasets themselves are also held by the `AnnotationStore`.
#[serde_as]
#[derive(Deserialize)]
#[serde(try_from="AnnotationDataSetBuilder")]
pub struct AnnotationDataSet {
    /// Public Id
    id: Option<String>,

    /// A store for [`DataKey`]
    keys: Store<DataKey>,

    /// A store for [`AnnotationData`], each makes *reference* to a [`DataKey`] (in this same `AnnotationDataSet`) and gives it a value  ([`DataValue`])
    data: Store<AnnotationData>,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership 
    intid: Option<AnnotationDataSetPointer>,

    /// Maps public IDs to internal IDs for 
    key_idmap: IdMap<DataKeyPointer>,

    /// Maps public IDs to internal IDs for AnnotationData
    data_idmap: IdMap<AnnotationDataPointer>,

    key_data_map: RelationMap<DataKeyPointer,AnnotationDataPointer>
}


#[serde_as]
#[derive(Deserialize)]
pub struct AnnotationDataSetBuilder {
    #[serde(rename="@id")]
    pub id: Option<String>,
    pub keys: Vec<DataKey>,
    pub data: Option<Vec<AnnotationDataBuilder>>,
}

impl TryFrom<AnnotationDataSetBuilder> for AnnotationDataSet {
    type Error = StamError;

    fn try_from(other: AnnotationDataSetBuilder) -> Result<Self, StamError> {
        let mut set = Self {
            id: other.id,
            keys: Vec::with_capacity(other.keys.len()),
            data: if other.data.is_some() {
                Vec::with_capacity(other.data.as_ref().unwrap().len())
            } else {
                Vec::new()
            },
            ..Default::default()
        };
        for key in other.keys {
            set.insert(key)?;
        }
        if other.data.is_some() {
            for dataitem in other.data.unwrap() {
                set.build_insert_data(dataitem, true)?;
            }
        }
        Ok(set)
    }
}



#[derive(Clone,Copy,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct AnnotationDataSetPointer(u16);
impl Pointer for AnnotationDataSetPointer {
    fn new(intid: usize) -> Self { Self(intid as u16) }
    fn unwrap(&self) -> usize { self.0 as usize }
}

impl Storable for AnnotationDataSet {
    type PointerType = AnnotationDataSetPointer;

    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
    fn with_id(mut self, id: String) ->  Self {
        self.id = Some(id);
        self
    }
    fn get_pointer(&self) -> Option<Self::PointerType> { 
        self.intid
    }
}

impl MutableStorable for AnnotationDataSet {
    fn set_pointer(&mut self, pointer: AnnotationDataSetPointer) {
        self.intid = Some(pointer);
    }

    /// Sets the ownership of all items in the store
    /// This ensure the part_of_set relation (backreference)
    /// is set right.
    fn bound(&mut self) {
        let intid = self.get_pointer().expect("getting internal id");
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
    fn get_idmap(&self) -> Option<&IdMap<DataKeyPointer>> {
        Some(&self.key_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap<DataKeyPointer>> {
        Some(&mut self.key_idmap)
    }
    fn owns(&self, item: &DataKey) -> Option<bool> {
        if item.part_of_set.is_none() || self.get_pointer().is_none() {
            //ownership is unclear because one of both is unbound
            None
        } else {
            Some(item.part_of_set == self.get_pointer())
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
    fn get_idmap(&self) -> Option<&IdMap<AnnotationDataPointer>> {
        Some(&self.data_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap<AnnotationDataPointer>> {
        Some(&mut self.data_idmap)
    }
    fn owns(&self, item: &AnnotationData) -> Option<bool> {
        if item.part_of_set.is_none() || self.get_pointer().is_none() {
            //ownership is unclear because one of both is unbound
            None
        } else {
            Some(item.part_of_set == self.get_pointer())
        }
    }
    fn introspect_type(&self) -> &'static str {
        "AnnotationData in AnnotationDataSet"
    }

    fn inserted(&mut self, pointer: AnnotationDataPointer) {
        // called after the item is inserted in the store
        // update the relation map
        let annotationdata: &AnnotationData = self.get(pointer).expect("item must exist after insertion");

        self.key_data_map.insert(annotationdata.key, pointer);
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
    pub fn with_data(mut self, id: AnyId<AnnotationDataPointer>, key: AnyId<DataKeyPointer>, value: DataValue) -> Result<Self, StamError> {
        self.insert_data(id,key,value,true)?;
        Ok(self)
    }

    /// Adds new [`AnnotationData`] to the dataset. Use [`with_data`] instead if you are using a regular builder pattern.
    /// If the data already exists, this returns a pointer to the existing data and inserts nothing new
    ///
    /// Note: if you don't want to set an ID (first argument), you can just just pass "".into()
    pub fn insert_data(&mut self, id: AnyId<AnnotationDataPointer>, key: AnyId<DataKeyPointer>, value: DataValue, safety: bool) -> Result<AnnotationDataPointer, StamError> {
        let annotationdata: Option<&AnnotationData> = self.get_by_anyid(&id);
        if let Some(annotationdata) = annotationdata {
            //already exists, return as is
            return Ok(annotationdata.get_pointer().expect("item must have intid when in store"))
        }
        if key.is_none() {
            return Err(StamError::IncompleteError("Key supplied to AnnotationDataSet.with_data() can not be None"));
        }

        let datakey: Option<&DataKey> = self.get_by_anyid(&key);
        let mut newkey = false;
        let datakey_pointer = if let Some(datakey) = datakey {
            datakey.get_pointer_or_err()?
        } else if key.is_id() {
            //datakey not found, create new one and add it to the store
            newkey = true;
            self.insert(DataKey::new(key.to_string().unwrap()))?
        } else {
            return Err(key.get_error("Datakey not found by AnnotationDataSet.with_data()"));
        };

        if !newkey && id.is_none() && safety {
            // there is a chance that this key and value combination already occurs, check it
            if let Some(dataitems) = self.key_data_map.data.get(&datakey_pointer) {
                for intid in dataitems.iter() { //MAYBE TODO: this may get slow if there is a key with a lot of data values
                    let data: &AnnotationData = self.get(*intid).expect("getting item");
                    if data.get_value() == &value {
                        // Data with this exact key and value already exists, return it:
                        return Ok(data.get_pointer().expect("item must have intid if in store"));
                    }
                }
            }
        }

        let public_id: Option<String> = id.to_string();

        self.insert(AnnotationData::new(public_id, datakey_pointer, value))
    }

    /// Build and insert data into the dataset, similar to [`insert_data()`] and [`with_data()`], but takes a prepared `AnnotationDataBuilder` instead.
    pub fn build_insert_data(&mut self, dataitem: AnnotationDataBuilder, safety: bool) -> Result<AnnotationDataPointer, StamError> {
        self.insert_data(dataitem.id, dataitem.key, dataitem.value, safety)
    }

    /// Get an annotation pointer from an ID.
    /// Shortcut wraps arround get_pointer()
    pub fn resolve_data_id(&self, id: &str) -> Result<AnnotationDataPointer,StamError> {
        <Self as StoreFor<AnnotationData>>::resolve_id(&self, id)
    }

    /// Get an annotation pointer from an ID.
    /// Shortcut wraps arround get_pointer()
    pub fn resolve_key_id(&self, id: &str) -> Result<DataKeyPointer,StamError> {
        <Self as StoreFor<DataKey>>::resolve_id(&self, id)
    }

    ///Iteratest over all the data ([`AnnotationData`]) in this set, returns references
    pub fn iter_data(&self) -> StoreIter<AnnotationData> {
        <Self as StoreFor<AnnotationData>>::iter(&self)
    }

    ///Iteratest over all the keys in this set, returns references
    pub fn iter_keys(&self) -> StoreIter<DataKey> {
        <Self as StoreFor<DataKey>>::iter(&self)
    }
}

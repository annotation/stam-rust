use std::fs::File;
use std::io::{BufReader, BufWriter};

use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
//use serde_json::Result;

use crate::annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::DataValue;
use crate::error::StamError;
use crate::selector::{Selector, SelfSelector};
use crate::types::*;

/// An `AnnotationDataSet` stores the keys [`DataKey`] and values
/// [`AnnotationData`] (which in turn encapsulates [`DataValue`]) that are used by annotations.
/// It effectively defines a certain vocabulary, i.e. key/value pairs.
/// The `AnnotationDataSet` does not store the [`crate::annotation::Annotation`] instances themselves, those are in
/// the `AnnotationStore`. The datasets themselves are also held by the `AnnotationStore`.
#[serde_as]
#[derive(Deserialize)]
#[serde(try_from = "AnnotationDataSetBuilder")]
pub struct AnnotationDataSet {
    /// Public Id
    #[serde(rename = "@id")]
    id: Option<String>,

    /// A store for [`DataKey`]
    keys: Store<DataKey>,

    /// A store for [`AnnotationData`], each makes *reference* to a [`DataKey`] (in this same `AnnotationDataSet`) and gives it a value  ([`DataValue`])
    data: Store<AnnotationData>,

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership
    #[serde(skip)]
    intid: Option<AnnotationDataSetHandle>,

    /// Maps public IDs to internal IDs for
    #[serde(skip)]
    key_idmap: IdMap<DataKeyHandle>,

    /// Maps public IDs to internal IDs for AnnotationData
    #[serde(skip)]
    data_idmap: IdMap<AnnotationDataHandle>,

    #[serde(skip)]
    key_data_map: RelationMap<DataKeyHandle, AnnotationDataHandle>,

    #[serde(skip)]
    config: StoreConfig,
}

#[serde_as]
#[derive(Deserialize)]
pub struct AnnotationDataSetBuilder {
    #[serde(rename = "@id")]
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

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct AnnotationDataSetHandle(u16);
impl Handle for AnnotationDataSetHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u16)
    }
    fn unwrap(&self) -> usize {
        self.0 as usize
    }
}

impl Storable for AnnotationDataSet {
    type HandleType = AnnotationDataSetHandle;

    fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
    fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }
    fn handle(&self) -> Option<Self::HandleType> {
        self.intid
    }
    fn set_handle(&mut self, handle: AnnotationDataSetHandle) {
        self.intid = Some(handle);
    }

    /// Sets the ownership of all items in the store
    /// This ensure the part_of_set relation (backreference)
    /// is set right.
    fn bound(&mut self) {
        let intid = self.handle().expect("getting internal id");
        let datastore: &mut Store<AnnotationData> = self.store_mut();
        for data in datastore.iter_mut() {
            if let Some(data) = data {
                data.part_of_set = Some(intid);
            }
        }
        let keystore: &mut Store<DataKey> = self.store_mut();
        for key in keystore.iter_mut() {
            if let Some(key) = key {
                key.part_of_set = Some(intid);
            }
        }
    }
}

impl StoreFor<DataKey> for AnnotationDataSet {
    fn store(&self) -> &Store<DataKey> {
        &self.keys
    }
    fn store_mut(&mut self) -> &mut Store<DataKey> {
        &mut self.keys
    }
    fn idmap(&self) -> Option<&IdMap<DataKeyHandle>> {
        Some(&self.key_idmap)
    }
    fn idmap_mut(&mut self) -> Option<&mut IdMap<DataKeyHandle>> {
        Some(&mut self.key_idmap)
    }
    fn owns(&self, item: &DataKey) -> Option<bool> {
        if item.part_of_set.is_none() || self.handle().is_none() {
            //ownership is unclear because one of both is unbound
            None
        } else {
            Some(item.part_of_set == self.handle())
        }
    }
    fn introspect_type(&self) -> &'static str {
        "DataKey in AnnotationDataSet"
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    fn preremove(&mut self, handle: DataKeyHandle) -> Result<(), StamError> {
        if self.handle().is_some() {
            return Err(StamError::InUse("Refusing to remove datakey because its AnnotationDataSet is bound and we can't guarantee it's not used"));
        }
        if let Some(datavec) = self.key_data_map.data.get(handle.unwrap()) {
            if datavec.is_empty() {
                return Err(StamError::InUse("DataKey"));
            }
        }
        self.key_data_map.data.remove(handle.unwrap());
        Ok(())
    }

    fn config(&self) -> &StoreConfig {
        &self.config
    }
}

impl StoreFor<AnnotationData> for AnnotationDataSet {
    fn store(&self) -> &Store<AnnotationData> {
        &self.data
    }
    fn store_mut(&mut self) -> &mut Store<AnnotationData> {
        &mut self.data
    }
    fn idmap(&self) -> Option<&IdMap<AnnotationDataHandle>> {
        Some(&self.data_idmap)
    }
    fn idmap_mut(&mut self) -> Option<&mut IdMap<AnnotationDataHandle>> {
        Some(&mut self.data_idmap)
    }
    fn owns(&self, item: &AnnotationData) -> Option<bool> {
        if item.part_of_set.is_none() || self.handle().is_none() {
            //ownership is unclear because one of both is unbound
            None
        } else {
            Some(item.part_of_set == self.handle())
        }
    }
    fn introspect_type(&self) -> &'static str {
        "AnnotationData in AnnotationDataSet"
    }

    fn inserted(&mut self, handle: AnnotationDataHandle) -> Result<(), StamError> {
        // called after the item is inserted in the store
        // update the relation map
        let annotationdata: &AnnotationData =
            self.get(handle).expect("item must exist after insertion");

        self.key_data_map.insert(annotationdata.key, handle);
        Ok(())
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    fn preremove(&mut self, handle: AnnotationDataHandle) -> Result<(), StamError> {
        let data: &AnnotationData = self.get(handle)?;
        if self.handle().is_some() {
            return Err(StamError::InUse("Refusing to remove annotationdata because AnnotationDataSet is bound and we can't guarantee it's not used"));
        }
        self.key_data_map.remove(data.key, handle);
        Ok(())
    }

    fn config(&self) -> &StoreConfig {
        &self.config
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
            config: StoreConfig::default(),
        }
    }
}

impl Serialize for AnnotationDataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationDataSet", 4)?;
        state.serialize_field("@type", "AnnotationDataSet")?;
        if let Some(id) = self.id() {
            state.serialize_field("@id", id)?;
        }
        state.serialize_field("keys", &self.keys)?;
        let wrappedstore: WrappedStore<AnnotationData, Self> = self.wrappedstore();
        state.serialize_field("data", &wrappedstore)?;
        state.end()
    }
}

impl SelfSelector for AnnotationDataSet {
    /// Returns a selector to this resource
    fn self_selector(&self) -> Result<Selector, StamError> {
        if let Some(intid) = self.handle() {
            Ok(Selector::DataSetSelector(intid))
        } else {
            Err(StamError::Unbound("AnnotationDataSet::self_selector()"))
        }
    }
}

impl AnnotationDataSet {
    pub fn new() -> Self {
        Self::default()
    }

    ///Builds a new annotation store from [`AnnotationDataSetBuilder'].
    pub fn from_builder(builder: AnnotationDataSetBuilder) -> Result<Self, StamError> {
        let store: Self = builder.try_into()?;
        Ok(store)
    }

    /// Loads an AnnotationDataSet from a STAM JSON file
    /// The file must contain a single object which has "@type": "AnnotationDataSet"
    pub fn from_file(filename: &str) -> Result<Self, StamError> {
        let f = File::open(filename).map_err(|e| {
            StamError::IOError(e, "Reading AnnotationDataSet from file, open failed")
        })?;
        let reader = BufReader::new(f);
        let builder: AnnotationDataSetBuilder = serde_json::from_reader(reader)
            .map_err(|e| StamError::JsonError(e, "Reading AnnotationDataSet from file"))?;
        Self::from_builder(builder)
    }

    pub fn with_config(mut self, config: StoreConfig) -> Self {
        self.config = config;
        self
    }

    /// Adds new [`AnnotationData`] to the dataset, this should be
    /// Note: if you don't want to set an ID (first argument), you can just just pass "".into()
    pub fn with_data(
        mut self,
        id: AnyId<AnnotationDataHandle>,
        key: AnyId<DataKeyHandle>,
        value: DataValue,
    ) -> Result<Self, StamError> {
        self.insert_data(id, key, value, true)?;
        Ok(self)
    }

    /// Adds new [`AnnotationData`] to the dataset. Use [`Self.with_data()`] instead if you are using a regular builder pattern.
    /// If the data already exists, this returns a handle to the existing data and inserts nothing new.
    /// If the data is new, it returns a handle to the new data.
    ///
    /// Note: if you don't want to set an ID (first argument), you can just just pass "".into()
    pub fn insert_data(
        &mut self,
        id: AnyId<AnnotationDataHandle>,
        key: AnyId<DataKeyHandle>,
        value: DataValue,
        safety: bool,
    ) -> Result<AnnotationDataHandle, StamError> {
        let annotationdata: Option<&AnnotationData> = self.get_by_anyid(&id);
        if let Some(annotationdata) = annotationdata {
            //already exists, return as is
            return Ok(annotationdata
                .handle()
                .expect("item must have intid when in store"));
        }
        if key.is_none() {
            return Err(StamError::IncompleteError(
                "Key supplied to AnnotationDataSet.with_data() can not be None",
            ));
        }

        let datakey: Option<&DataKey> = self.get_by_anyid(&key);
        let mut newkey = false;
        let datakey_handle = if let Some(datakey) = datakey {
            datakey.handle_or_err()?
        } else if key.is_id() {
            //datakey not found, create new one and add it to the store
            newkey = true;
            self.insert(DataKey::new(key.to_string().unwrap()))?
        } else {
            return Err(key.error("Datakey not found by AnnotationDataSet.with_data()"));
        };

        if !newkey && id.is_none() && safety {
            // there is a chance that this key and value combination already occurs, check it
            if let Some(dataitems) = self.key_data_map.data.get(datakey_handle.unwrap()) {
                for intid in dataitems.iter() {
                    //MAYBE TODO: this may get slow if there is a key with a lot of data values
                    let data: &AnnotationData = self.get(*intid).expect("getting item");
                    if data.value() == &value {
                        // Data with this exact key and value already exists, return it:
                        return Ok(data.handle().expect("item must have intid if in store"));
                    }
                }
            }
        }

        let public_id: Option<String> = id.to_string();

        self.insert(AnnotationData::new(public_id, datakey_handle, value))
    }

    /// Build and insert data into the dataset, similar to [`Self.insert_data()`] and [`Self.with_data()`], but takes a prepared `AnnotationDataBuilder` instead.
    pub fn build_insert_data(
        &mut self,
        dataitem: AnnotationDataBuilder,
        safety: bool,
    ) -> Result<AnnotationDataHandle, StamError> {
        self.insert_data(dataitem.id, dataitem.key, dataitem.value, safety)
    }

    /// Get an annotation handle from an ID.
    pub fn resolve_data_id(&self, id: &str) -> Result<AnnotationDataHandle, StamError> {
        <Self as StoreFor<AnnotationData>>::resolve_id(self, id)
    }

    pub fn resolve_key_id(&self, id: &str) -> Result<DataKeyHandle, StamError> {
        <Self as StoreFor<DataKey>>::resolve_id(self, id)
    }

    ///Iterates over all the data ([`AnnotationData`]) in this set, returns references
    pub fn data(&self) -> StoreIter<AnnotationData> {
        <Self as StoreFor<AnnotationData>>::iter(self)
    }

    ///Iterates over all the keys in this set, returns references
    pub fn keys(&self) -> StoreIter<DataKey> {
        <Self as StoreFor<DataKey>>::iter(self)
    }

    /// Shortcut method to get an resource by any id
    pub fn key(&self, id: &AnyId<DataKeyHandle>) -> Option<&DataKey> {
        self.get_by_anyid(id)
    }

    /// Shortcut method to get a single annotation data instance resource by any id
    pub fn annotationdata(&self, id: &AnyId<AnnotationDataHandle>) -> Option<&AnnotationData> {
        self.get_by_anyid(id)
    }

    /// Returns data by key, does a lookup in the reverse index and returns a reference to it.
    pub fn data_by_key(&self, key_handle: DataKeyHandle) -> Option<&Vec<AnnotationDataHandle>> {
        self.key_data_map.get(key_handle)
    }

    /// Sets the ID of the dataset in a builder pattern
    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// Writes a dataset to a STAM JSON file, with appropriate formatting
    pub fn to_file(&self, filename: &str) -> Result<(), StamError> {
        let f = File::create(filename)
            .map_err(|e| StamError::IOError(e, "Writing dataset from file, open failed"))?;
        let writer = BufWriter::new(f);
        serde_json::to_writer_pretty(writer, &self).map_err(|e| {
            StamError::SerializationError(format!("Writing dataset to file: {}", e))
        })?;
        Ok(())
    }

    /// Writes a dataset to a STAM JSON file, without any indentation
    pub fn to_file_compact(&self, filename: &str) -> Result<(), StamError> {
        let f = File::create(filename)
            .map_err(|e| StamError::IOError(e, "Writing dataset from file, open failed"))?;
        let writer = BufWriter::new(f);
        serde_json::to_writer(writer, &self).map_err(|e| {
            StamError::SerializationError(format!("Writing dataset to file: {}", e))
        })?;
        Ok(())
    }

    //Returns the number of keys in the store (deletions are not substracted)
    pub fn keys_len(&self) -> usize {
        self.keys.len()
    }

    //Returns the number of data items in the store (deletions are not substracted)
    pub fn data_len(&self) -> usize {
        self.data.len()
    }
}

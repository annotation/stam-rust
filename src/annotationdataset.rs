use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use sealed::sealed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
//use serde_json::Result;

use crate::annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
use crate::config::{Config, SerializeMode};
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

    /// Is this annotation dataset stored stand-off in an external file?
    #[serde(skip)]
    include: Option<String>,

    /// Flags if set has changed, if so, they need to be reserialised if stored via the include mechanism
    #[serde(skip)]
    changed: Arc<RwLock<bool>>, //this is modified via internal mutability

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

    /// Configuration
    #[serde(skip)]
    config: Config,
}

#[derive(Deserialize)]
pub struct AnnotationDataSetBuilder {
    #[serde(rename = "@id")]
    pub id: Option<String>,
    pub keys: Option<Vec<DataKey>>, //this is an Option because it can be omitted if @include is used
    pub data: Option<Vec<AnnotationDataBuilder>>,
    #[serde(rename = "@include")]
    pub(crate) include: Option<String>,
    #[serde(skip)]
    pub(crate) mode: SerializeMode,
    #[serde(skip)]
    pub(crate) workdir: Option<PathBuf>,
}

impl TryFrom<AnnotationDataSetBuilder> for AnnotationDataSet {
    type Error = StamError;

    fn try_from(builder: AnnotationDataSetBuilder) -> Result<Self, StamError> {
        let mut set = Self {
            id: builder.id,
            keys: if builder.keys.is_some() {
                Vec::with_capacity(builder.keys.as_ref().unwrap().len())
            } else {
                Vec::new()
            },
            data: if builder.data.is_some() {
                Vec::with_capacity(builder.data.as_ref().unwrap().len())
            } else {
                Vec::new()
            },
            ..Default::default()
        };
        if builder.keys.is_some() {
            for key in builder.keys.unwrap() {
                set.insert(key)?;
            }
        }
        if builder.data.is_some() {
            for dataitem in builder.data.unwrap() {
                set.build_insert_data(dataitem, true)?;
            }
        }
        if let Some(filename) = &builder.include {
            set.include = Some(filename.to_string());
            if builder.mode == SerializeMode::AllowInclude {
                set = set.with_file(filename, builder.workdir.as_ref().map(|x| x.as_path()))?;
            }
        }
        Ok(set)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct AnnotationDataSetHandle(u16);
#[sealed]
impl Handle for AnnotationDataSetHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u16)
    }
    fn unwrap(&self) -> usize {
        self.0 as usize
    }
}

#[sealed]
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

#[sealed]
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

    #[allow(unused_variables)]
    fn inserted(&mut self, handle: DataKeyHandle) -> Result<(), StamError> {
        // called after the key is inserted in the store
        if let Ok(mut changed) = self.changed.write() {
            *changed = true;
        }
        Ok(())
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
        if let Ok(mut changed) = self.changed.write() {
            *changed = true;
        }
        Ok(())
    }
}

#[sealed]
impl Configurable for AnnotationDataSet {
    fn config(&self) -> &Config {
        &self.config
    }

    fn set_config(&mut self, config: Config) -> &mut Self {
        self.config = config;
        self
    }
}

#[sealed]
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
        if let Ok(mut changed) = self.changed.write() {
            *changed = true;
        }
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
        if let Ok(mut changed) = self.changed.write() {
            *changed = true;
        }
        Ok(())
    }
}

impl Default for AnnotationDataSet {
    fn default() -> Self {
        Self {
            id: None,
            keys: Store::new(),
            data: Store::new(),
            intid: None,
            include: None,
            changed: Arc::new(RwLock::new(false)),
            key_idmap: IdMap::new("K".to_string()),
            data_idmap: IdMap::new("D".to_string()),
            key_data_map: RelationMap::new(),
            config: Config::default(),
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
        if self.include.is_some() && self.config.serialize_mode() == SerializeMode::AllowInclude
        //                                      ^-- we need type annotations for the compiler, doesn't really matter which we use
        {
            let filename = self.include.as_ref().unwrap();
            if self.id() != Some(&filename) && self.id.is_some() {
                state.serialize_field("@id", &self.id().unwrap())?;
            }
            state.serialize_field("@include", &filename)?;

            //if there are any changes, we write to the standoff file
            if let Ok(changed) = self.changed.read() {
                if *changed {
                    //we trigger the standoff flag, this is the only way we can parametrize the serializer
                    let result = self.to_file(&filename); //this reinvokes this function after setting config.standoff_include
                    result.map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
                }
            }
            if let Ok(mut changed) = self.changed.write() {
                //reset
                *changed = false;
            }
        } else {
            if self.id().is_some() {
                state.serialize_field("@id", &self.id().unwrap())?;
            }
            state.serialize_field("keys", &self.keys)?;
            let wrappedstore: WrappedStore<AnnotationData, Self> = self.wrappedstore();
            state.serialize_field("data", &wrappedstore)?;
        }
        state.end()
    }
}

impl SelfSelector for AnnotationDataSet {
    /// Returns a selector to this resource
    fn selector(&self) -> Result<Selector, StamError> {
        if let Some(intid) = self.handle() {
            Ok(Selector::DataSetSelector(intid))
        } else {
            Err(StamError::Unbound("AnnotationDataSet::self_selector()"))
        }
    }
}

impl PartialEq<AnnotationDataSet> for AnnotationDataSet {
    fn eq(&self, other: &AnnotationDataSet) -> bool {
        self.id.is_some()
            && self.id == other.id
            && self.keys == other.keys
            && self.data == other.data
    }
}

impl AnnotationDataSetBuilder {
    /// Loads an AnnotationDataSet from a STAM JSON file, as a builder
    /// The file must contain a single object which has "@type": "AnnotationDataSet"
    /// If `include` is true, the file will be included via the `@include` mechanism, and is kept external upon serialization
    /// If `workdir` is set, the file will be searched for in the workdir if needed
    pub fn from_file(
        filename: &str,
        workdir: Option<&Path>,
        include: bool,
    ) -> Result<Self, StamError> {
        let reader = open_file_reader(filename, workdir)?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);
        let mut result: Result<AnnotationDataSetBuilder, _> =
            serde_path_to_error::deserialize(deserializer);
        if result.is_ok() && include {
            let result = result.as_mut().unwrap();
            result.include = Some(filename.to_string()); //we use the original filename, not the one we found
            result.mode = SerializeMode::NoInclude;
            result.workdir = workdir.map(|x| x.to_owned())
        }
        result.map_err(|e| {
            StamError::JsonError(
                e,
                filename.to_string(),
                "Reading AnnotationDataSet from file",
            )
        })
    }

    /// Loads an AnnotationDataSet from a STAM JSON string
    /// The string must contain a single object which has "@type": "AnnotationDataSet"
    pub fn from_str(string: &str) -> Result<Self, StamError> {
        let deserializer = &mut serde_json::Deserializer::from_str(string);
        let result: Result<AnnotationDataSetBuilder, _> =
            serde_path_to_error::deserialize(deserializer);
        result.map_err(|e| {
            StamError::JsonError(
                e,
                string.to_string(),
                "Reading AnnotationDataSet from string",
            )
        })
    }
}

impl AnnotationDataSet {
    pub fn new() -> Self {
        Self::default()
    }

    ///Builds a new annotation store from [`AnnotationDataSetBuilder'].
    pub fn from_builder(builder: AnnotationDataSetBuilder) -> Result<Self, StamError> {
        let set: Self = builder.try_into()?;
        Ok(set)
    }

    /// Loads an AnnotationDataSet from a STAM JSON file
    /// The file must contain a single object which has "@type": "AnnotationDataSet"
    /// If `include` is true, the file will be included via the `@include` mechanism, and is kept external upon serialization
    /// If `workdir` is set, the file will be searched for in the workdir if needed
    pub fn from_file(
        filename: &str,
        workdir: Option<&Path>,
        include: bool,
    ) -> Result<Self, StamError> {
        let builder = AnnotationDataSetBuilder::from_file(filename, workdir, include)?;
        Self::from_builder(builder)
    }

    /// Loads an AnnotationDataSet from a STAM JSON string
    /// The string must contain a single object which has "@type": "AnnotationDataSet"
    pub fn from_str(string: &str) -> Result<Self, StamError> {
        let builder = AnnotationDataSetBuilder::from_str(string)?;
        Self::from_builder(builder)
    }

    /// Loads an AnnotationDataSet from a STAM JSON file and merges it into the current     one.
    /// The file must contain a single object which has "@type": "AnnotationDataSet"
    /// The ID will be ignored (existing one takes precendence).
    /// If `workdir` is set, the file will be searched for in the workdir if needed
    pub fn with_file(mut self, filename: &str, workdir: Option<&Path>) -> Result<Self, StamError> {
        let builder = AnnotationDataSetBuilder::from_file(filename, workdir, false)?;
        self.merge_from_builder(builder)?;
        Ok(self)
    }

    /// Writes an AnnotationStore to one big STAM JSON string, with appropriate formatting
    pub fn to_json(&self) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        serde_json::to_string_pretty(&self).map_err(|e| {
            StamError::SerializationError(format!("Writing annotation dataset to string: {}", e))
        })
    }

    /// Writes an AnnotationStore to one big STAM JSON string, without any indentation
    pub fn to_json_compact(&self) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        serde_json::to_string(&self).map_err(|e| {
            StamError::SerializationError(format!("Writing annotation dataset to string: {}", e))
        })
    }

    /// Merge another AnnotationDataSet, represented by an AnnotationDataSetBuilder, into this one.
    /// It's a fairly low-level function which you often don't need directly, use `AnnotationDataSet.with_file()` instead.
    pub fn merge_from_builder(
        &mut self,
        builder: AnnotationDataSetBuilder,
    ) -> Result<&mut Self, StamError> {
        //this function is very much like TryFrom<AnnotationDataSetBuilder> for AnnotationDataSet

        if self.id.is_none() && builder.id.is_some() {
            self.id = builder.id;
        }
        if builder.keys.is_some() {
            for key in builder.keys.unwrap() {
                self.insert(key)?;
            }
        }
        if builder.data.is_some() {
            for dataitem in builder.data.unwrap() {
                self.build_insert_data(dataitem, true)?;
            }
        }
        Ok(self)
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

    /// Finds the [`AnnotationData'] in the annotation dataset. Returns one match.
    pub fn find_data(
        &self,
        key: AnyId<DataKeyHandle>,
        value: &DataValue,
    ) -> Option<&AnnotationData> {
        if key.is_none() {
            None
        } else {
            let datakey: Option<&DataKey> = self.get_by_anyid(&key);
            if let Some(datakey) = datakey {
                let datakey_handle = datakey.handle().expect("key must be bound at this point");
                if let Some(dataitems) = self.key_data_map.data.get(datakey_handle.unwrap()) {
                    for intid in dataitems.iter() {
                        //MAYBE TODO: this may get slow if there is a key with a lot of data values
                        let data: &AnnotationData = self.get(*intid).expect("getting item");
                        if data.value() == value {
                            // Data with this exact key and value already exists, return it:
                            return Some(data);
                        }
                    }
                }
                None
            } else {
                None
            }
        }
    }

    /// Adds new [`AnnotationData`] to the dataset. Use [`Self.with_data()`] instead if you are using a regular builder pattern.
    /// If the data already exists, this returns a handle to the existing data and inserts nothing new.
    /// If the data is new, it returns a handle to the new data.
    ///
    /// Note: if you don't want to set an ID (first argument), you can just pass AnyId::None or "".into()
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
            if let Some(data) = self.find_data(AnyId::Handle(datakey_handle), &value) {
                return Ok(data.handle().expect("item must have intid if in store"));
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
        let writer = open_file_writer(filename, self.config().workdir())?;
        self.config.set_serialize_mode(SerializeMode::NoInclude); //set standoff mode, what we're about the write is the standoff file
        let result = serde_json::to_writer_pretty(writer, &self)
            .map_err(|e| StamError::SerializationError(format!("Writing dataset to file: {}", e)));
        self.config.set_serialize_mode(SerializeMode::AllowInclude); //reset
        result?;
        Ok(())
    }

    /// Writes a dataset to a STAM JSON file, without any indentation
    pub fn to_file_compact(&self, filename: &str) -> Result<(), StamError> {
        let writer = open_file_writer(filename, self.config().workdir())?;
        self.config.set_serialize_mode(SerializeMode::NoInclude); //set standoff mode, what we're about the write is the standoff file
        let result = serde_json::to_writer(writer, &self)
            .map_err(|e| StamError::SerializationError(format!("Writing dataset to file: {}", e)));
        self.config.set_serialize_mode(SerializeMode::AllowInclude); //reset
        result?;
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

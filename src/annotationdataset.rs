use std::sync::{Arc, RwLock};

use sealed::sealed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
//use serde_json::Result;

use crate::annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
use crate::annotationstore::AnnotationStore;
use crate::config::{get_global_config, Config, Configurable, SerializeMode};
#[cfg(feature = "csv")]
use crate::csv::FromCsv;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::DataValue;
use crate::error::StamError;
use crate::file::*;
use crate::json::{FromJson, ToJson};
use crate::selector::{Selector, SelfSelector};
use crate::store::*;
use crate::types::*;

/// An `AnnotationDataSet` stores the keys [`DataKey`] and values
/// [`AnnotationData`] (which in turn encapsulates [`DataValue`]) that are used by annotations.
/// It effectively defines a certain vocabulary, i.e. key/value pairs.
/// The `AnnotationDataSet` does not store the [`crate::annotation::Annotation`] instances themselves, those are in
/// the `AnnotationStore`. The datasets themselves are also held by the `AnnotationStore`.
#[derive(Deserialize, Debug)]
#[serde(try_from = "AnnotationDataSetBuilder")]
pub struct AnnotationDataSet {
    /// Public Id
    #[serde(rename = "@id")]
    id: Option<String>,

    /// A store for [`DataKey`]
    keys: Store<DataKey>,

    /// A store for [`AnnotationData`], each makes *reference* to a [`DataKey`] (in this same `AnnotationDataSet`) and gives it a value  ([`DataValue`])
    data: Store<AnnotationData>,

    /// Is this annotation dataset stored stand-off in an external file via @include? This holds the filename
    #[serde(skip)]
    filename: Option<String>,

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
    #[serde(skip, default = "get_global_config")]
    config: Config,
}

#[derive(Deserialize, Default)]
pub struct AnnotationDataSetBuilder<'a> {
    #[serde(rename = "@id")]
    pub(crate) id: Option<String>,
    pub(crate) keys: Option<Vec<DataKey>>, //this is an Option because it can be omitted if @include is used
    pub(crate) data: Option<Vec<AnnotationDataBuilder<'a>>>,
    #[serde(rename = "@include")]
    pub(crate) filename: Option<String>,

    #[serde(skip)]
    pub(crate) mode: SerializeMode,

    /// Configuration
    #[serde(skip, default = "get_global_config")]
    pub(crate) config: Config,
}

impl<'a> TryFrom<AnnotationDataSetBuilder<'a>> for AnnotationDataSet {
    type Error = StamError;

    fn try_from(builder: AnnotationDataSetBuilder<'a>) -> Result<Self, StamError> {
        debug(&builder.config, || {
            format!("TryFrom<AnnotationDataSetBuilder for AnnotationDataSet>: Creation of AnnotationDataSet from builder")
        });
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
            //note: includes have to be resolved in a later stage via [`AnnotationStore.process_includes()`]
            //      we don't do it here as we don't have state information from the deserializer (believe me, I tried)
            filename: builder.filename,
            config: builder.config,
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
        debug(&set.config, || {
            format!("TryFrom<AnnotationDataSetBuilder for AnnotationDataSet>: done")
        });
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

// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> From<&AnnotationDataSetHandle> for Item<'a, AnnotationDataSet> {
    fn from(handle: &AnnotationDataSetHandle) -> Self {
        Item::Handle(*handle)
    }
}
impl<'a> From<Option<&AnnotationDataSetHandle>> for Item<'a, AnnotationDataSet> {
    fn from(handle: Option<&AnnotationDataSetHandle>) -> Self {
        if let Some(handle) = handle {
            Item::Handle(*handle)
        } else {
            Item::None
        }
    }
}
impl<'a> From<AnnotationDataSetHandle> for Item<'a, AnnotationDataSet> {
    fn from(handle: AnnotationDataSetHandle) -> Self {
        Item::Handle(handle)
    }
}
impl<'a> From<Option<AnnotationDataSetHandle>> for Item<'a, AnnotationDataSet> {
    fn from(handle: Option<AnnotationDataSetHandle>) -> Self {
        if let Some(handle) = handle {
            Item::Handle(handle)
        } else {
            Item::None
        }
    }
}

#[sealed]
impl TypeInfo for AnnotationDataSet {
    fn typeinfo() -> Type {
        Type::AnnotationDataSet
    }
}

#[sealed]
impl<'a> TypeInfo for AnnotationDataSetBuilder<'a> {
    fn typeinfo() -> Type {
        Type::AnnotationDataSet
    }
}

#[sealed]
impl Storable for AnnotationDataSet {
    type HandleType = AnnotationDataSetHandle;
    type StoreType = AnnotationStore;

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

    fn carries_id() -> bool {
        true
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

    fn store_typeinfo() -> &'static str {
        "DataKey in AnnotationDataSet"
    }

    #[allow(unused_variables)]
    fn inserted(&mut self, handle: DataKeyHandle) -> Result<(), StamError> {
        // called after the key is inserted in the store
        self.mark_changed();
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
        self.mark_changed();
        Ok(())
    }
}

impl Configurable for AnnotationDataSet {
    fn config(&self) -> &Config {
        &self.config
    }

    fn config_mut(&mut self) -> &mut Config {
        &mut self.config
    }

    fn set_config(&mut self, config: Config) -> &mut Self {
        self.config = config;
        self
    }
}

#[sealed]
impl AssociatedFile for AnnotationDataSet {
    /// Get the filename for stand-off file specified using @include (if any)
    fn filename(&self) -> Option<&str> {
        self.filename.as_ref().map(|x| x.as_str())
    }

    /// Get the filename for stand-off file specified using @include (if any)
    fn set_filename(&mut self, filename: &str) -> &mut Self {
        self.filename = Some(filename.into());
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
    fn store_typeinfo() -> &'static str {
        "AnnotationData in AnnotationDataSet"
    }

    fn inserted(&mut self, handle: AnnotationDataHandle) -> Result<(), StamError> {
        // called after the item is inserted in the store
        // update the relation map
        let annotationdata: &AnnotationData = self
            .get(&handle.into())
            .expect("item must exist after insertion");

        self.key_data_map.insert(annotationdata.key, handle);
        self.mark_changed();
        Ok(())
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    fn preremove(&mut self, handle: AnnotationDataHandle) -> Result<(), StamError> {
        let data: &AnnotationData = self.get(&handle.into())?;
        if self.handle().is_some() {
            return Err(StamError::InUse("Refusing to remove annotationdata because AnnotationDataSet is bound and we can't guarantee it's not used"));
        }
        self.key_data_map.remove(data.key, handle);
        self.mark_changed();
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
            filename: None,
            changed: Arc::new(RwLock::new(false)),
            key_idmap: IdMap::new("K".to_string()),
            data_idmap: IdMap::new("D".to_string()),
            key_data_map: RelationMap::new(),
            config: get_global_config(),
        }
    }
}

impl ToJson for AnnotationDataSet {}

impl Serialize for AnnotationDataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationDataSet", 4)?;
        state.serialize_field("@type", "AnnotationDataSet")?;
        if self.filename.is_some() && self.config.serialize_mode() == SerializeMode::AllowInclude
        //                                      ^-- we need type annotations for the compiler, doesn't really matter which we use
        {
            let filename = self.filename.as_ref().unwrap();
            if self.id() != Some(&filename) && self.id.is_some() {
                state.serialize_field("@id", &self.id().unwrap())?;
            }
            state.serialize_field("@include", &filename)?;

            //if there are any changes, we write to the standoff file
            if self.changed() {
                //we trigger the standoff flag, this is the only way we can parametrize the serializer
                let result = self.to_json_file(&filename, self.config()); //this reinvokes this function after setting config.standoff_include
                result.map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
                self.mark_unchanged();
            }
        } else {
            if self.id().is_some() {
                state.serialize_field("@id", &self.id().unwrap())?;
            }
            state.serialize_field("keys", &self.keys)?;
            let wrappedstore: WrappedStore<AnnotationData, Self> = self.wrap_store();
            state.serialize_field("data", &wrappedstore)?;
        }
        state.end()
    }
}

#[sealed]
impl ChangeMarker for AnnotationDataSet {
    fn change_marker(&self) -> &Arc<RwLock<bool>> {
        &self.changed
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

impl<'j, 'a> FromJson<'j> for AnnotationDataSetBuilder<'a> {
    /// Loads an AnnotationDataSet from a STAM JSON file, as a builder
    /// The file must contain a single object which has "@type": "AnnotationDataSet"
    /// If `include` is true, the file will be included via the `@include` mechanism, and is kept external upon serialization
    /// If `workdir` is set, the file will be searched for in the workdir if needed
    fn from_json_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "AnnotationDataSetBuilder::from_json_file: filename={}",
                filename
            )
        });
        let reader = open_file_reader(filename, &config)?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);
        let mut result: Result<AnnotationDataSetBuilder<'a>, _> =
            serde_path_to_error::deserialize(deserializer);
        if result.is_ok() {
            let builder = result.as_mut().unwrap();
            builder.filename = Some(filename.to_string()); //we use the original filename, not the one we found
            builder.mode = SerializeMode::NoInclude;
            builder.config = config;
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
    fn from_json_str(string: &str, config: Config) -> Result<Self, StamError> {
        let deserializer = &mut serde_json::Deserializer::from_str(string);
        let mut result: Result<AnnotationDataSetBuilder<'a>, _> =
            serde_path_to_error::deserialize(deserializer);
        if result.is_ok() {
            let builder = result.as_mut().unwrap();
            builder.config = config;
        }
        result.map_err(|e| {
            StamError::JsonError(
                e,
                string.to_string(),
                "Reading AnnotationDataSet from string",
            )
        })
    }
}

impl<'j> FromJson<'j> for AnnotationDataSet {
    /// Loads an AnnotationDataSet from a STAM JSON file
    /// The file must contain a single object which has "@type": "AnnotationDataSet"
    /// If `workdir` is set, the file will be searched for in the workdir if needed
    fn from_json_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "AnnotationDataSet::from_json_file: filename={:?} config={:?}",
                filename, config
            )
        });
        AnnotationDataSetBuilder::from_json_file(filename, config)?.build()
    }

    /// Loads an AnnotationDataSet from a STAM JSON string
    /// The string must contain a single object which has "@type": "AnnotationDataSet"
    fn from_json_str(string: &str, config: Config) -> Result<Self, StamError> {
        AnnotationDataSetBuilder::from_json_str(string, config)?.build()
    }
}

impl<'a> AnnotationDataSetBuilder<'a> {
    /// Start a new builder to build an [`AnnotationDataSet`]. You can chain various with_* methods and subsequently call `build()` to produce the actual [`AnnotationDataSet`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads an AnnotationDataSetBuilder from file (STAM JSON or other supported format).
    /// For STAM JSON, the file must contain a single object which has "@type": "AnnotationDataSet"
    pub fn from_file(filename: &str, mut config: Config) -> Result<Self, StamError> {
        #[cfg(feature = "csv")]
        if filename.ends_with("csv") || config.dataformat == DataFormat::Csv {
            config.dataformat = DataFormat::Csv;
            return AnnotationDataSetBuilder::from_csv_file(filename, config);
        }

        AnnotationDataSetBuilder::from_json_file(filename, config)
    }

    /// Set the public ID for the [`AnnotationDataSet`] that will be built.
    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// Adds multiple keys at once. This can only be called once (it will override any prior set keys)
    pub fn with_keys(mut self, keys: Vec<DataKey>) -> Self {
        self.keys = Some(keys);
        self
    }

    /// Adds a data key. This can be called multiple times.
    pub fn with_key(mut self, key: DataKey) -> Self {
        if let Some(keys) = self.keys.as_mut() {
            keys.push(key);
        }
        self
    }

    /// Adds multiple data builder at once. This can only be called once (it will override any prior set keys)
    pub fn with_data_builders(mut self, data: Vec<AnnotationDataBuilder<'a>>) -> Self {
        self.data = Some(data);
        self
    }

    /// Adds a data builder. This can be called multiple times.
    pub fn with_data(mut self, data: AnnotationDataBuilder<'a>) -> Self {
        if let Some(datavec) = self.data.as_mut() {
            datavec.push(data);
        }
        self
    }

    /// Set the filename for the [`AnnotationDataSet`] that will be built. This does not load from file. Use [`AnnotationDataSet::from_file()`] instead of this builder if that's what you want.
    pub fn with_filename(mut self, filename: &str) -> Self {
        if filename.is_empty() {
            self.filename = None
        } else {
            self.filename = Some(filename.to_string());
        }
        self
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Final builder method, constructs the ['AnnotationDataSet`], consuming the builder
    pub fn build(self) -> Result<AnnotationDataSet, StamError> {
        debug(&self.config, || {
            format!("AnnotationDataSetBuilder::build()")
        });
        self.try_into()
    }
}

impl AnnotationDataSet {
    pub fn new(config: Config) -> Self {
        AnnotationDataSet {
            config,
            ..Self::default()
        }
    }

    /// Returns an Annotation dataset builder to build new annotation datasets
    pub fn builder<'a>() -> AnnotationDataSetBuilder<'a> {
        AnnotationDataSetBuilder::default()
    }

    /// Loads an AnnotationDataSet from file (STAM JSON or other supported format) and merges it into the current one.
    /// For STAM JSON, the file must contain a single object which has "@type": "AnnotationDataSet"
    /// The ID will be ignored (existing one takes precendence).
    pub fn with_file(mut self, filename: &str, mut config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "AnnotationDataSet.with_file: filename={:?} config={:?}",
                filename, config
            )
        });

        #[cfg(feature = "csv")]
        if filename.ends_with("csv") {
            config.dataformat = DataFormat::Csv;
            let builder = AnnotationDataSetBuilder::from_csv_file(filename, config)?;
            self.merge_from_builder(builder)?;
            return Ok(self);
        }

        let builder = AnnotationDataSetBuilder::from_json_file(filename, config)?;
        self.merge_from_builder(builder)?;
        Ok(self)
    }

    /// Loads an AnnotationDataSet from file (STAM JSON or other supported format).
    /// For STAM JSON, the file must contain a single object which has "@type": "AnnotationDataSet"
    pub fn from_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "AnnotationDataSet.from_file: filename={:?} config={:?}",
                filename, config
            )
        });
        AnnotationDataSetBuilder::from_file(filename, config)?.build()
    }

    /// Merge another AnnotationDataSet, represented by an AnnotationDataSetBuilder, into this one.
    /// It's a fairly low-level function which you often don't need directly, use `AnnotationDataSet.with_file()` instead.
    pub fn merge_from_builder(
        &mut self,
        builder: AnnotationDataSetBuilder,
    ) -> Result<&mut Self, StamError> {
        //this function is very much like TryFrom<AnnotationDataSetBuilder> for AnnotationDataSet
        debug(self.config(), || {
            format!("AnnotationDataSet.merge_from_builder")
        });

        //only override ID if we had none yet
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
        id: Item<AnnotationData>,
        key: Item<DataKey>,
        value: DataValue,
    ) -> Result<Self, StamError> {
        debug(self.config(), || format!("AnnotationDataSet.with_data"));
        self.insert_data(id, key, value, true)?;
        Ok(self)
    }

    /// Finds the [`AnnotationData'] in the annotation dataset. Returns one match.
    pub fn find_data(&self, key: Item<DataKey>, value: &DataValue) -> Option<&AnnotationData> {
        if key.is_none() {
            None
        } else {
            let datakey: Option<&DataKey> = self.key(&key);
            if let Some(datakey) = datakey {
                let datakey_handle = datakey.handle().expect("key must be bound at this point");
                if let Some(dataitems) = self.key_data_map.data.get(datakey_handle.unwrap()) {
                    for datahandle in dataitems.iter() {
                        //MAYBE TODO: this may get slow if there is a key with a lot of data values
                        let data: &AnnotationData =
                            self.get(&datahandle.into()).expect("getting item");
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
    pub fn insert_data<'a>(
        &mut self,
        id: Item<'a, AnnotationData>,
        key: Item<'a, DataKey>,
        value: DataValue,
        safety: bool,
    ) -> Result<AnnotationDataHandle, StamError> {
        debug(self.config(), || {
            format!(
                "AnnotationDataSet.insert_data: id={:?} key={:?} value={:?}",
                id, key, value
            )
        });

        let annotationdata: Result<&AnnotationData, _> = self.get(&id);
        if let Ok(annotationdata) = annotationdata {
            //already exists, return as is
            return Ok(annotationdata
                .handle()
                .expect("item must have intid when in store"));
        }
        if key.is_none() {
            return Err(StamError::IncompleteError(
                format!(
                    "id={:?} key={:?} value={:?}",
                    annotationdata.map(|data| data.id()),
                    key,
                    value
                ),
                "Key supplied to AnnotationDataSet.insert_data() (or with_data()) can not be None",
            ));
        }

        let datakey: Result<&DataKey, _> = self.get(&key);
        let mut newkey = false;
        let datakey_handle = if let Ok(datakey) = datakey {
            datakey.handle_or_err()?
        } else if key.is_id() {
            //datakey not found, create new one and add it to the store
            newkey = true;
            self.insert(DataKey::new(key.to_string().unwrap()))?
        } else {
            return Err(key.error("Datakey not found by AnnotationDataSet.insert_data()"));
        };

        if !newkey && id.is_none() && safety {
            // there is a chance that this key and value combination already occurs, check it
            if let Some(data) = self.find_data(Item::from(datakey_handle), &value) {
                return Ok(data.handle().expect("item must have intid if in store"));
            }
        }

        let public_id: Option<String> = id.to_string();

        let result = self.insert(AnnotationData::new(public_id, datakey_handle, value));
        debug(self.config(), || {
            format!("AnnotationDataSet.insert_data: done, result={:?}", result)
        });
        result
    }

    /// Build and insert data into the dataset, similar to [`Self.insert_data()`] and [`Self.with_data()`], but takes a prepared `AnnotationDataBuilder` instead.
    pub fn build_insert_data(
        &mut self,
        dataitem: AnnotationDataBuilder,
        safety: bool,
    ) -> Result<AnnotationDataHandle, StamError> {
        self.insert_data(dataitem.id, dataitem.key, dataitem.value, safety)
    }

    ///Iterates over all the data ([`AnnotationData`]) in this set, returns references
    pub fn data(&self) -> StoreIter<AnnotationData> {
        <Self as StoreFor<AnnotationData>>::iter(self)
    }

    ///Iterates over all the keys in this set, returns references
    pub fn keys(&self) -> StoreIter<DataKey> {
        <Self as StoreFor<DataKey>>::iter(self)
    }

    /// Shortcut method to get a key
    pub fn key(&self, id: &Item<DataKey>) -> Option<&DataKey> {
        self.get(id.into()).ok()
    }

    /// Method to retrieve an annotation from the store.
    ///
    /// Returns a reference to [`Annotation`] that is wrapped in a fat pointer ([`WrappedItem<Annotation>`]) that also contains reference to the store
    /// and which is immediately implements various methods for working with the type.
    /// If you need a more performant low-level method, use `StoreFor<T>::get()` instead.
    pub fn annotationdata(
        &self,
        annotationdata: &Item<AnnotationData>,
    ) -> Option<WrappedItem<AnnotationData>> {
        self.get(annotationdata)
            .map(|x| x.wrap_in(self).expect("wrap must succeed"))
            .ok()
    }

    /// Returns data by key, does a lookup in the reverse index and returns a reference to it.
    pub fn data_by_key(&self, key: &Item<DataKey>) -> Option<&Vec<AnnotationDataHandle>> {
        if let Some(key_handle) = key.to_handle(self) {
            self.key_data_map.get(key_handle)
        } else {
            None
        }
    }

    /// Sets the ID of the dataset in a builder pattern
    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// Writes a    //Returns the number of keys in the store (deletions are not substracted)
    pub fn keys_len(&self) -> usize {
        self.keys.len()
    }

    //Returns the number of data items in the store (deletions are not substracted)
    pub fn data_len(&self) -> usize {
        self.data.len()
    }
}

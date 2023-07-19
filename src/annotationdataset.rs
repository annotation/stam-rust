use std::sync::{Arc, RwLock};

use datasize::{data_size, DataSize};
use sealed::sealed;
use serde::de::DeserializeSeed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
//use serde_json::Result;

use crate::annotation::Annotation;
use crate::annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
use crate::annotationstore::AnnotationStore;
use crate::config::{Config, Configurable, SerializeMode};
#[cfg(feature = "csv")]
use crate::csv::FromCsv;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::error::StamError;
use crate::file::*;
use crate::json::{FromJson, ToJson};
use crate::selector::{Selector, SelfSelector};
use crate::store::*;
use crate::types::*;
use std::fmt::Debug;

/// An `AnnotationDataSet` stores the keys [`DataKey`] and values
/// [`AnnotationData`] (which in turn encapsulates [`DataValue`]) that are used by annotations.
/// It effectively defines a certain vocabulary, i.e. key/value pairs.
/// The `AnnotationDataSet` does not store the [`crate::annotation::Annotation`] instances themselves, those are in
/// the `AnnotationStore`. The datasets themselves are also held by the `AnnotationStore`.
#[derive(Debug, Clone, DataSize)]
pub struct AnnotationDataSet {
    /// Public Id
    id: Option<String>,

    /// A store for [`DataKey`]
    keys: Store<DataKey>,

    /// A store for [`AnnotationData`], each makes *reference* to a [`DataKey`] (in this same `AnnotationDataSet`) and gives it a value  ([`DataValue`])
    data: Store<AnnotationData>,

    /// Is this annotation dataset stored stand-off in an external file via @include? This holds the filename
    filename: Option<String>,

    /// Flags if set has changed, if so, they need to be reserialised if stored via the include mechanism
    changed: Arc<RwLock<bool>>, //this is modified via internal mutability

    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership
    intid: Option<AnnotationDataSetHandle>,

    /// Maps public IDs to internal IDs for
    key_idmap: IdMap<DataKeyHandle>,

    /// Maps public IDs to internal IDs for AnnotationData
    data_idmap: IdMap<AnnotationDataHandle>,

    key_data_map: RelationMap<DataKeyHandle, AnnotationDataHandle>,

    /// Configuration
    #[data_size(skip)]
    config: Config,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, DataSize)]
pub struct AnnotationDataSetHandle(u16);
#[sealed]
impl Handle for AnnotationDataSetHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u16)
    }
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

//these I couldn't solve nicely using generics:

impl<'a> ToHandle<AnnotationDataSet> for AnnotationDataSetHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<AnnotationDataSetHandle>
    where
        S: StoreFor<AnnotationDataSet>,
    {
        Some(*self)
    }
}

impl From<AnnotationDataSetHandle> for BuildItem<'_, AnnotationDataSet> {
    fn from(handle: AnnotationDataSetHandle) -> Self {
        Self::Handle(handle)
    }
}
impl From<&AnnotationDataSetHandle> for BuildItem<'_, AnnotationDataSet> {
    fn from(handle: &AnnotationDataSetHandle) -> Self {
        Self::Handle(*handle)
    }
}

#[sealed]
impl TypeInfo for AnnotationDataSet {
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
    fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
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

    fn set_id(&mut self, id: Option<String>) {
        self.id = id;
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
        if let Some(datavec) = self.key_data_map.data.get(handle.as_usize()) {
            if datavec.is_empty() {
                return Err(StamError::InUse("DataKey"));
            }
        }
        self.key_data_map.data.remove(handle.as_usize());
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
        let annotationdata: &AnnotationData =
            self.get(handle).expect("item must exist after insertion");

        self.key_data_map.insert(annotationdata.key, handle);
        self.mark_changed();
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
        self.mark_changed();
        Ok(())
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
            if self.id() != Some(&filename) {
                if let Some(id) = self.id() {
                    state.serialize_field("@id", id)?;
                }
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
            if let Some(id) = self.id() {
                state.serialize_field("@id", id)?;
            } else if let Ok(id) = self.temp_id() {
                state.serialize_field("@id", id.as_str())?;
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

impl FromJson for AnnotationDataSet {
    /// Loads an AnnotationDataSet from a STAM JSON file, as a builder
    /// The file must contain a single object which has "@type": "AnnotationDataSet"
    /// If `include` is true, the file will be included via the `@include` mechanism, and is kept external upon serialization
    /// If `workdir` is set, the file will be searched for in the workdir if needed
    fn from_json_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!("AnnotationDataSet::from_json_file: filename={}", filename)
        });
        let reader = open_file_reader(filename, &config)?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);

        let mut annotationset: AnnotationDataSet =
            AnnotationDataSet::new(config).with_filename(filename); //we use the original filename, not the one we found

        DeserializeAnnotationDataSet::new(&mut annotationset)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(annotationset)
    }

    /// Loads an AnnotationDataSet from a STAM JSON string
    /// The string must contain a single object which has "@type": "AnnotationDataSet"
    fn from_json_str(string: &str, config: Config) -> Result<Self, StamError> {
        let deserializer = &mut serde_json::Deserializer::from_str(string);

        let mut annotationset: AnnotationDataSet = AnnotationDataSet::new(config);

        DeserializeAnnotationDataSet::new(&mut annotationset)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(annotationset)
    }

    /// Merges an AnnotationDataSet from a STAM JSON file into the current one
    /// The file must contain a single object which has "@type": "AnnotationDataSet"
    fn merge_json_file(&mut self, filename: &str) -> Result<(), StamError> {
        debug(self.config(), || {
            format!("AnnotationStore::from_json_file: filename={:?}", filename)
        });
        let reader = open_file_reader(filename, self.config())?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);

        DeserializeAnnotationDataSet::new(self)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(())
    }

    /// Merges an AnnotationDataSet from a STAM JSON string into the current one
    /// The string must contain a single object which has "@type": "AnnotationDataSet"
    fn merge_json_str(&mut self, string: &str) -> Result<(), StamError> {
        debug(self.config(), || {
            format!("AnnotationStore::from_json_str: string={:?}", string)
        });
        let deserializer = &mut serde_json::Deserializer::from_str(string);

        DeserializeAnnotationDataSet::new(self)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(())
    }
}

impl AnnotationDataSet {
    pub fn new(config: Config) -> Self {
        Self {
            id: None,
            keys: Store::new(),
            data: Store::new(),
            intid: None,
            filename: None,
            changed: Arc::new(RwLock::new(false)),
            key_idmap: IdMap::new("K".to_string()).with_resolve_temp_ids(config.strip_temp_ids()),
            data_idmap: IdMap::new("D".to_string()).with_resolve_temp_ids(config.strip_temp_ids()),
            key_data_map: RelationMap::new(),
            config,
        }
    }

    /// Loads an AnnotationDataSet from file (STAM JSON or other supported format).
    /// For STAM JSON, the file must contain a single object which has "@type": "AnnotationDataSet"
    pub fn from_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "AnnotationDataSet.with_file: filename={:?} config={:?}",
                filename, config
            )
        });

        #[cfg(feature = "csv")]
        if filename.ends_with("csv") {
            return Self::from_csv_file(filename, config);
        }
        Self::from_json_file(filename, config)
    }

    /// Merge another AnnotationDataSet from file (STAM JSON or other supported format).
    /// Note: The ID and filename of the set will not be overwritten if already set,
    ///       reserialising the store will produce a single new set.
    pub fn with_file(mut self, filename: &str) -> Result<Self, StamError> {
        #[cfg(feature = "csv")]
        if filename.ends_with("csv") || self.config().dataformat == DataFormat::Csv {
            if self.keys_len() == 0 {
                return Self::from_csv_file(filename, self.config().clone());
            }
            todo!("Merging CSV files for AnnotationDataSet is not supported yet");
            //TODO
        }

        self.merge_json_file(filename)?;

        Ok(self)
    }

    /// Adds new [`AnnotationData`] to the dataset, this should be
    pub fn with_data<'a>(
        mut self,
        key: impl Into<BuildItem<'a, DataKey>>,
        value: impl Into<DataValue> + std::fmt::Debug,
    ) -> Result<Self, StamError> {
        debug(self.config(), || format!("AnnotationDataSet.with_data"));
        self.insert_data(BuildItem::None, key, value, true)?;
        Ok(self)
    }

    /// Adds new [`AnnotationData`] to the dataset, this should be
    pub fn with_data_with_id<'a>(
        mut self,
        key: impl Into<BuildItem<'a, DataKey>>,
        value: impl Into<DataValue> + std::fmt::Debug,
        id: impl Into<BuildItem<'a, AnnotationData>>,
    ) -> Result<Self, StamError> {
        debug(self.config(), || format!("AnnotationDataSet.with_data"));
        self.insert_data(id, key, value, true)?;
        Ok(self)
    }

    /// Finds the [`AnnotationData'] in the annotation dataset. Returns one match.
    /// This is a low level method, use the similar [`WrappedItem<AnnotationDataSet>.find_data()`] for a higher level version.
    pub fn data_by_value(
        &self,
        key: impl ToHandle<DataKey>,
        value: &DataValue,
    ) -> Option<&AnnotationData> {
        let datakey: Option<&DataKey> = self.key(key).map(|key| key.as_ref());
        if let Some(datakey) = datakey {
            let datakey_handle = datakey.handle().expect("key must be bound at this point");
            if let Some(dataitems) = self.key_data_map.data.get(datakey_handle.as_usize()) {
                for datahandle in dataitems.iter() {
                    //MAYBE TODO: this may get slow if there is a key with a lot of data values
                    let data: &AnnotationData = self.get(*datahandle).expect("getting item");
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

    /// Adds new [`AnnotationData`] to the dataset. Use [`Self.with_data()`] instead if you are using a regular builder pattern.
    /// If the data already exists, this returns a handle to the existing data and inserts nothing new.
    /// If the data is new, it returns a handle to the new data.
    ///
    /// The `safety` parameter should be set to `true` in normal circumstances, it will check if the data already exists
    /// if it has no ID, and reuse that. If set to `false`, data will be inserted as a duplicate.
    ///
    /// Note: if you don't want to set an ID (first argument), you can just pass AnyId::None or "".into()
    pub fn insert_data<'a>(
        &mut self,
        id: impl Into<BuildItem<'a, AnnotationData>>,
        key: impl Into<BuildItem<'a, DataKey>>,
        value: impl Into<DataValue> + std::fmt::Debug,
        safety: bool,
    ) -> Result<AnnotationDataHandle, StamError> {
        let id = id.into();
        let key = key.into();

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
            if id.is_none() {
                return Err(StamError::HandleError(
                    "AnnotationData Hsupplied to AnnotationDataSet.insert_data() (often via with_data()) was not found in this set",
                ));
            } else {
                return Err(StamError::IncompleteError(
                    format!(
                        "id={:?} key={:?} value={:?} current_set={:?}",
                        id,
                        key,
                        value,
                        self.id().unwrap_or("(no id)")
                    ),
                    "Key supplied to AnnotationDataSet.insert_data() (often via with_data()) can not be None",
                ));
            }
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

        let value = value.into();

        if !newkey && id.is_none() && safety {
            // there is a chance that this key and value combination already occurs, check it
            if let Some(data) = self.data_by_value(datakey_handle, &value) {
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

    ///Returns an iterator over all the data ([`AnnotationData`]) in this set, the iterator returns references as [`WrappedItem<AnnotationData`].
    pub fn data(&self) -> StoreIter<AnnotationData> {
        <Self as StoreFor<AnnotationData>>::iter(self)
    }

    /// Returns an iterator over all the keys ([`DataKey`]) in this set, the iterator in returns references as [`WrappedItem<DataKey>`]
    pub fn keys(&self) -> StoreIter<DataKey> {
        <Self as StoreFor<DataKey>>::iter(self)
    }

    /// Retrieve a key in this set
    pub fn key(&self, key: impl ToHandle<DataKey>) -> Option<ResultItem<DataKey>> {
        self.get(key)
            .map(|x| x.wrap_in(self).expect("wrap must succeed"))
            .ok()
    }

    /// Retrieve a [`AnnotationData`] in this set
    ///
    /// Returns a reference to [`AnnotationData`] that is wrapped in a fat pointer
    /// ([`WrappedItem<AnnotationData>`]) that also contains reference to the store and which is
    /// immediately implements various methods for working with the type. If you need a more
    /// performant low-level method, use `StoreFor<T>::get()` instead.
    pub fn annotationdata<'a>(
        &'a self,
        annotationdata: impl ToHandle<AnnotationData>,
    ) -> Option<ResultItem<'a, AnnotationData>> {
        self.get(annotationdata)
            .map(|x| x.wrap_in(self).expect("wrap must succeed"))
            .ok()
    }

    /// Returns data by key, does a lookup in the reverse index and returns a reference to it.
    /// This is a low-level method. use [`WrappedItem<DataKey>.data()`] instead.
    pub fn data_by_key(&self, key: impl ToHandle<DataKey>) -> Option<&Vec<AnnotationDataHandle>> {
        if let Some(key_handle) = key.to_handle(self) {
            self.key_data_map.get(key_handle)
        } else {
            None
        }
    }

    /// Sets the ID of the dataset in a builder pattern
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
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

    pub fn meminfo(&self) -> usize {
        data_size(self)
    }

    pub fn shrink_to_fit(&mut self) {
        self.keys.shrink_to_fit();
        self.data.shrink_to_fit();
        self.key_data_map.shrink_to_fit(true);
    }

    /// Strip public identifiers from annotation data
    /// This will not affect any internal references but will render any references from external sources impossible.
    pub fn strip_data_ids(&mut self) {
        for data in self.data.iter_mut() {
            if let Some(data) = data {
                data.set_id(None);
            }
        }
        self.data_idmap =
            IdMap::new("D".to_string()).with_resolve_temp_ids(self.config().strip_temp_ids());
    }
}

impl<'store, 'slf> ResultItem<'store, AnnotationDataSet> {
    /// Returns [`AnnotationData'] in the annotation dataset that matches they key and value.
    /// Returns a single match, use `Self::find_data()` for a more extensive search.
    pub fn data_by_value(
        &self,
        key: impl ToHandle<DataKey>,
        value: &DataValue,
    ) -> Option<ResultItem<'store, AnnotationData>> {
        self.as_ref()
            .data_by_value(key, value)
            .map(|annotationdata| annotationdata.wrap_in(self.as_ref()).unwrap())
    }

    /// Finds the [`AnnotationData'] in the annotation dataset. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use `test_data` instead.
    ///
    /// Provide `key`  as an Options, if set to `None`, all keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    pub fn find_data<'a>(
        &'slf self,
        key: Option<impl ToHandle<DataKey>>,
        value: DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'store: 'slf,
        'a: 'store,
    {
        let mut key_handle: Option<DataKeyHandle> = None; //this means 'any' in this context
        if let Some(key) = key {
            key_handle = key.to_handle(self.as_ref());
            if key_handle.is_none() {
                //requested key doesn't exist, bail out early, we won't find anything at all
                return None;
            }
        };
        Some(self.as_ref().data().filter_map(move |annotationdata| {
            if (key_handle.is_none() || key_handle.unwrap() == annotationdata.key().handle())
                && annotationdata.as_ref().value().test(&value)
            {
                Some(annotationdata)
            } else {
                None
            }
        }))
    }

    /// Tests if the dataset has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored.
    pub fn test_data<'a>(
        &'slf self,
        key: Option<impl ToHandle<DataKey>>,
        value: DataOperator<'a>,
    ) -> bool {
        match self.find_data(key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Returns all annotations that use this dataset. Use
    /// [`Self.annotations_metadata()`] instead if you are looking for annotations that reference
    /// the dataset as a whole via a DataSetSelector. These are also included here, though.
    pub fn annotations(
        &'slf self,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + '_> {
        if let Some(iter) = self.store().annotations_by_annotationset(self.handle()) {
            Some(iter.filter_map(|a_handle| self.store().annotation(a_handle)))
        } else {
            None
        }
    }

    /// Tests whether two AnnotationDataSets are the same
    pub fn test(&'slf self, other: impl ToHandle<AnnotationDataSet>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }

    /// This only returns annotations that directly point at the resource, i.e. are metadata for it. It does not include annotations that
    /// point at a text in the resource, use [`Self.annotations_by_resource()`] instead for those.
    pub fn annotations_metadata(
        &'slf self,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + '_> {
        if let Some(vec) = self
            .store()
            .annotations_by_annotationset_metadata(self.handle())
        {
            Some(
                vec.iter()
                    .filter_map(|a_handle| self.store().annotation(*a_handle)),
            )
        } else {
            None
        }
    }
}

////////////////////// Deserialisation

#[derive(Debug)]
pub(crate) struct DeserializeAnnotationDataSet<'a> {
    store: &'a mut AnnotationDataSet,
}

impl<'a> DeserializeAnnotationDataSet<'a> {
    pub fn new(store: &'a mut AnnotationDataSet) -> Self {
        Self { store }
    }
}

/// Top-level seeded deserializer that serializes into the state (the store)
impl<'de> DeserializeSeed<'de> for DeserializeAnnotationDataSet<'_> {
    // This implementation adds onto the AnnotationDataSet passed as state, it does not return any data of itself
    type Value = ();

    fn deserialize<D>(mut self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let visitor = AnnotationDataSetVisitor {
            store: &mut self.store,
        };
        deserializer.deserialize_map(visitor)?;
        Ok(())
    }
}

struct AnnotationDataSetVisitor<'a> {
    store: &'a mut AnnotationDataSet,
}

impl<'de> serde::de::Visitor<'de> for AnnotationDataSetVisitor<'_> {
    // This implementation adds onto the AnnotationDataSet passed as state, it does not return any data of itself
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a map for AnnotationDataSet")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "@id" => {
                    let id: String = map.next_value()?;
                    if self.store.id.is_none() {
                        self.store.id = Some(id);
                    }
                }
                "@type" => {
                    let tp: String = map.next_value()?;
                    if tp != "AnnotationDataSet" {
                        return Err(<A::Error as serde::de::Error>::custom(format!(
                            "Expected type AnnotationDataSet, got {tp}"
                        )));
                    }
                }
                "@include" => {
                    let filename: String = map.next_value()?;
                    self.store
                        .merge_json_file(filename.as_str())
                        .map_err(|e| -> A::Error { serde::de::Error::custom(e) })?;
                    if self.store.filename.is_none() {
                        self.store.filename = Some(filename);
                    }
                }
                "keys" => {
                    // handle the next value in a streaming manner
                    map.next_value_seed(DeserializeKeys { store: self.store })?;
                }
                "data" => {
                    // handle the next value in a streaming manner
                    map.next_value_seed(DeserializeData { store: self.store })?;
                }
                _ => {
                    eprintln!(
                        "Notice: Ignoring unknown key '{key}' whilst parsing AnnotationDataSet"
                    );
                    map.next_value()?; //read and discard the value
                }
            }
        }

        Ok(())
    }
}

struct DeserializeKeys<'a> {
    store: &'a mut AnnotationDataSet,
}

impl<'de> DeserializeSeed<'de> for DeserializeKeys<'_> {
    type Value = ();

    fn deserialize<D>(mut self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let visitor = KeysVisitor { store: self.store };
        deserializer.deserialize_seq(visitor)?;
        Ok(())
    }
}

struct KeysVisitor<'a> {
    store: &'a mut AnnotationDataSet,
}

impl<'de> serde::de::Visitor<'de> for KeysVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a list of annotations")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        loop {
            let key: Option<DataKey> = seq.next_element()?;
            if let Some(key) = key {
                self.store
                    .insert(key)
                    .map_err(|e| -> A::Error { serde::de::Error::custom(e) })?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

struct DeserializeData<'a> {
    store: &'a mut AnnotationDataSet,
}

impl<'de> DeserializeSeed<'de> for DeserializeData<'_> {
    type Value = ();

    fn deserialize<D>(mut self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let visitor = DataVisitor { store: self.store };
        deserializer.deserialize_seq(visitor)?;
        Ok(())
    }
}

struct DataVisitor<'a> {
    store: &'a mut AnnotationDataSet,
}

impl<'de> serde::de::Visitor<'de> for DataVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a list of annotationdata")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        loop {
            let databuilder: Option<AnnotationDataBuilder> = seq.next_element()?;
            if let Some(databuilder) = databuilder {
                self.store
                    .build_insert_data(databuilder, false) //safety disabled, data duplicates allowed at this stage (=faster)
                    .map_err(|e| -> A::Error { serde::de::Error::custom(e) })?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

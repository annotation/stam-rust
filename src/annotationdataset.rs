/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the low-level API for [`AnnotationDataSet`]. It defines and implements the
//! struct, the handle, and things like serialisation, deserialisation to STAM JSON.

use std::sync::{Arc, RwLock};

use datasize::{data_size, DataSize};
use minicbor::{Decode, Encode};
use sealed::sealed;
use serde::de::DeserializeSeed;
use serde::ser::{SerializeStruct, Serializer};
use serde::Serialize;

use crate::annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
use crate::annotationstore::AnnotationStore;
use crate::config::{Config, Configurable, SerializeMode};
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
use std::fmt::Debug;

/// An `AnnotationDataSet` stores the keys [`DataKey`] and values
/// [`AnnotationData`] (which in turn encapsulates [`DataValue`]) that are used by annotations.
/// It effectively defines a certain vocabulary, i.e. key/value pairs.
/// The `AnnotationDataSet` does not store the [`Annotation`](crate::annotation::Annotation) instances, those are in
/// the [`AnnotationStore`]. The datasets themselves are also held by the [`AnnotationStore`].
///
/// See the top-level documentation for [`AnnotationStore`] for a complete usage example on instantiating a data set.
#[derive(Debug, Clone, DataSize, Encode, Decode)]
pub struct AnnotationDataSet {
    ///Internal numeric ID, corresponds with the index in the AnnotationStore::datasets that has the ownership
    #[n(0)]
    intid: Option<AnnotationDataSetHandle>,

    /// Public Id
    #[n(1)]
    id: Option<String>,

    /// A store for [`DataKey`]
    #[n(2)]
    keys: Store<DataKey>,

    /// A store for [`AnnotationData`], each makes *reference* to a [`DataKey`] (in this same `AnnotationDataSet`) and gives it a value  ([`DataValue`])
    #[n(3)]
    data: Store<AnnotationData>,

    /// Is this annotation dataset stored stand-off in an external file via @include? This holds the filename
    #[n(4)]
    filename: Option<String>,

    /// Flags if set has changed, if so, they need to be reserialised if stored via the include mechanism
    #[cbor(skip)] // this used to be n(5)
    changed: Arc<RwLock<bool>>, //this is modified via internal mutability

    /// Maps public IDs to internal IDs for
    #[n(6)]
    key_idmap: IdMap<DataKeyHandle>,

    /// Maps public IDs to internal IDs for AnnotationData
    #[n(7)]
    data_idmap: IdMap<AnnotationDataHandle>,

    #[n(8)]
    /// Map data keys to all their values. Sorted by definition due to the way it is constructed.
    key_data_map: RelationMap<DataKeyHandle, AnnotationDataHandle>,

    /// Configuration
    #[data_size(skip)]
    #[n(9)]
    pub(crate) config: Config,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, DataSize, Encode, Decode)]
#[cbor(transparent)]
pub struct AnnotationDataSetHandle(#[n(0)] u16);

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

impl<'a> Request<AnnotationDataSet> for AnnotationDataSetHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<AnnotationDataSetHandle>
    where
        S: StoreFor<AnnotationDataSet>,
    {
        Some(*self)
    }
}
impl<'a> Request<AnnotationDataSet> for Option<AnnotationDataSetHandle> {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<AnnotationDataSetHandle>
    where
        S: StoreFor<AnnotationDataSet>,
    {
        *self
    }

    fn any(&self) -> bool {
        self.is_none()
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
    type StoreHandleType = ();
    type FullHandleType = Self::HandleType;
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
    fn with_handle(mut self, handle: AnnotationDataSetHandle) -> Self {
        self.intid = Some(handle);
        self
    }
    fn carries_id() -> bool {
        true
    }

    fn fullhandle(
        _storehandle: Self::StoreHandleType,
        handle: Self::HandleType,
    ) -> Self::FullHandleType {
        handle
    }

    fn merge(&mut self, other: Self) -> Result<(), StamError> {
        let merge = self.config.merge;
        self.config.merge = true; //enable merge mode for underlying keys and data
        for key in other.keys {
            if let Some(key) = key {
                self.insert(key.unbind())?;
            }
        }
        for data in other.data {
            if let Some(data) = data {
                self.insert(data.unbind())?;
            }
        }
        self.config.merge = merge; //reset merge mode
        Ok(())
    }

    fn unbind(mut self) -> Self {
        self.intid = None;
        self
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

    fn store_typeinfo() -> &'static str {
        "DataKey in AnnotationDataSet"
    }
}

impl private::StoreCallbacks<DataKey> for AnnotationDataSet {
    #[allow(unused_variables)]
    fn inserted(&mut self, handle: DataKeyHandle) -> Result<(), StamError> {
        // called after the key is inserted in the store
        self.mark_changed();
        Ok(())
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    /// This does *NOT* take into consideration any annotations point to or make use of this key!
    /// Use [`AnnotationStore::remove_key()`] instead.
    fn preremove(&mut self, handle: DataKeyHandle) -> Result<(), StamError> {
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

    /// Set the filename for stand-off file specified using @include (if any)
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
    fn store_typeinfo() -> &'static str {
        "AnnotationData in AnnotationDataSet"
    }
}

impl private::StoreCallbacks<AnnotationData> for AnnotationDataSet {
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
    /// This does *NOT* take into consideration any annotations point to or make use of this data!
    /// Use [`AnnotationStore::remove_data()`] instead.
    fn preremove(&mut self, handle: AnnotationDataHandle) -> Result<(), StamError> {
        let data: &AnnotationData = self.get(handle)?;
        self.key_data_map.remove(data.key, handle);
        self.mark_changed();
        Ok(())
    }
}

impl AnnotationDataSet {
    /// Writes a Dataset to one big STAM JSON string, with appropriate formatting
    pub fn to_json_string(&self) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        serde_json::to_string_pretty(self).map_err(|e| {
            StamError::SerializationError(format!("Writing annotationdataset to string: {}", e))
        })
    }
}
impl ToJson for AnnotationDataSet {}

impl WrappableStore<AnnotationData> for AnnotationDataSet {}

impl Serialize for AnnotationDataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationDataSet", 4)?;
        state.serialize_field("@type", "AnnotationDataSet")?;
        if self.filename.is_some() && self.config.serialize_mode() == SerializeMode::AllowInclude {
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
                let filename = get_filepath(filename, self.config.workdir())
                    .expect("get_filepath must succeed");
                let result = self.to_json_file(&filename.to_string_lossy(), self.config()); //this reinvokes this function after setting config.standoff_include
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
            let wrappedstore: WrappedStore<AnnotationData, Self> = self.wrap_store(None);
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
    fn to_selector(&self) -> Result<Selector, StamError> {
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

        let mut dataset: AnnotationDataSet = AnnotationDataSet::new(config).with_filename(filename); //we use the original filename, not the one we found

        DeserializeAnnotationDataSet::new(&mut dataset)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(dataset)
    }

    /// Loads an AnnotationDataSet from a STAM JSON string
    /// The string must contain a single object which has "@type": "AnnotationDataSet"
    fn from_json_str(string: &str, config: Config) -> Result<Self, StamError> {
        let deserializer = &mut serde_json::Deserializer::from_str(string);

        let mut dataset: AnnotationDataSet = AnnotationDataSet::new(config);

        DeserializeAnnotationDataSet::new(&mut dataset)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(dataset)
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
    /// The configuration is typically obtained via [`AnnotationStore::new_config()`]
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

    /// Finds the [`AnnotationData`] in the annotation dataset. Returns one match.
    /// This is a low level method, use the similar [`crate::ResultItem<AnnotationDataSet>.data_by_value()`] for a higher level version.
    pub fn data_by_value(
        &self,
        key: impl Request<DataKey>,
        value: &DataValue,
    ) -> Option<&AnnotationData> {
        let datakey: Option<&DataKey> = self.key(key).map(|key| key);
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

    /// Adds new [`AnnotationData`] to the dataset. Use [`Self::with_data()`] instead if you are using a regular builder pattern.
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
                    "AnnotationData supplied to AnnotationDataSet.insert_data() (often via with_data()) was not found in this set",
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
                    "Key supplied to AnnotationDataSet.insert_data() (often via with_data()) can not be None. This may also mean that the data ID did not resolve to an existing item!",
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

    /// Build and insert data into the dataset, similar to [`Self::insert_data()`] and
    /// [`Self::with_data()`], but takes a prepared [`AnnotationDataBuilder`] instead.
    pub fn build_insert_data(
        &mut self,
        dataitem: AnnotationDataBuilder,
        safety: bool,
    ) -> Result<AnnotationDataHandle, StamError> {
        self.insert_data(dataitem.id, dataitem.key, dataitem.value, safety)
    }

    ///Returns an iterator over all the data ([`AnnotationData`]) in this set, the iterator returns references to [`AnnotationData`].
    pub fn data(&self) -> StoreIter<AnnotationData> {
        <Self as StoreFor<AnnotationData>>::iter(self)
    }

    /// Returns an iterator over all the keys ([`DataKey`]) in this set, the iterator in returns references to [`DataKey`]
    pub fn keys(&self) -> StoreIter<DataKey> {
        <Self as StoreFor<DataKey>>::iter(self)
    }

    /// Retrieve a key in this set
    pub fn key(&self, key: impl Request<DataKey>) -> Option<&DataKey> {
        self.get(key).map(|x| x).ok()
    }

    /// Retrieve a [`AnnotationData`] in this set
    pub fn annotationdata(
        &self,
        annotationdata: impl Request<AnnotationData>,
    ) -> Option<&AnnotationData> {
        self.get(annotationdata).ok()
    }

    /// Returns data by key, does a lookup in the reverse index and returns a reference to it.
    /// This is a low-level method. use [`ResultItem<DataKey>.data()`] instead.
    pub fn data_by_key(&self, key: impl Request<DataKey>) -> Option<&Vec<AnnotationDataHandle>> {
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
        self.data_idmap.shrink_to_fit();
        self.key_idmap.shrink_to_fit();
    }

    /// Strip public identifiers from annotation data
    /// This will not affect any internal references but will render any references from external sources impossible.
    pub fn strip_data_ids(&mut self) {
        for data in self.data.iter_mut() {
            if let Some(data) = data {
                data.id = None;
            }
        }
        self.data_idmap =
            IdMap::new("D".to_string()).with_resolve_temp_ids(self.config().strip_temp_ids());
    }
}

#[derive(Debug, Default)]
pub struct AnnotationDataSetBuilder<'a> {
    /// Public Id
    id: Option<String>,

    databuilders: Vec<AnnotationDataBuilder<'a>>,

    /// Is this annotation dataset stored stand-off in an external file via @include? This holds the filename
    filename: Option<String>,
}

impl<'a> AnnotationDataSetBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Associate a public identifier with the resource. Use this to make a new DataSet.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set the filename associated with the dataset.
    /// It will be loaded from file as long as you leave id unset.
    pub fn with_filename(mut self, filename: impl Into<String>) -> Self {
        self.filename = Some(filename.into());
        self
    }

    pub fn with_key(mut self, key: impl Into<BuildItem<'a, DataKey>>) -> Self {
        let databuilder = AnnotationDataBuilder::new().with_key(key.into());
        self.databuilders.push(databuilder);
        self
    }

    pub fn with_key_value(
        mut self,
        key: impl Into<BuildItem<'a, DataKey>>,
        value: impl Into<DataValue>,
    ) -> Self {
        let databuilder = AnnotationDataBuilder::new()
            .with_key(key.into())
            .with_value(value.into());
        self.databuilders.push(databuilder);
        self
    }

    pub fn with_key_value_id(
        mut self,
        key: impl Into<BuildItem<'a, DataKey>>,
        value: impl Into<DataValue>,
        id: impl Into<BuildItem<'a, AnnotationData>>,
    ) -> Self {
        let databuilder = AnnotationDataBuilder::new()
            .with_key(key.into())
            .with_value(value.into())
            .with_id(id.into());
        self.databuilders.push(databuilder);
        self
    }

    pub fn with_data(mut self, databuilder: AnnotationDataBuilder<'a>) -> Self {
        self.databuilders.push(databuilder);
        self
    }

    ///Builds a new [`AnnotationDataSet`] from [`AnnotationDataSetBuilder`], consuming the latter
    pub(crate) fn build(self, config: Config) -> Result<AnnotationDataSet, StamError> {
        debug(&config, || {
            format!(
                "AnnotationDataSetBuilder::build: id={:?}, filename={:?}, workdir={:?}",
                self.id,
                self.filename,
                config.workdir(),
            )
        });
        let mut dataset = if let Some(id) = self.id {
            AnnotationDataSet::new(config).with_id(id)
        } else if let Some(filename) = self.filename {
            AnnotationDataSet::from_file(filename.as_str(), config)?
        } else {
            return Err(StamError::OtherError("AnnotationDataSetBuilder.build(): No filename or ID specified for AnnotationDataSet."));
        };
        for databuilder in self.databuilders {
            dataset.build_insert_data(databuilder, true)?;
        }
        Ok(dataset)
    }
}

impl AnnotationStore {
    /// Builds and adds an [`AnnotationDataSet']
    pub fn with_dataset(mut self, builder: AnnotationDataSetBuilder) -> Result<Self, StamError> {
        self.add_dataset(builder)?;
        Ok(self)
    }

    pub fn add_dataset(
        &mut self,
        builder: AnnotationDataSetBuilder,
    ) -> Result<AnnotationDataSetHandle, StamError> {
        debug(self.config(), || {
            format!("AnnotationStore.add_dataset: builder={:?}", builder)
        });
        self.insert(builder.build(self.new_config())?)
    }
}

////////////////////// Deserialisation

#[derive(Debug)]
pub(crate) struct DeserializeAnnotationDataSet<'a> {
    dataset: &'a mut AnnotationDataSet,
}

impl<'a> DeserializeAnnotationDataSet<'a> {
    pub fn new(dataset: &'a mut AnnotationDataSet) -> Self {
        Self { dataset }
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
            dataset: &mut self.dataset,
        };
        deserializer.deserialize_map(visitor)?;
        Ok(())
    }
}

struct AnnotationDataSetVisitor<'a> {
    dataset: &'a mut AnnotationDataSet,
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
                    if self.dataset.id.is_none() {
                        self.dataset.id = Some(id);
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
                    self.dataset
                        .merge_json_file(filename.as_str())
                        .map_err(|e| -> A::Error { serde::de::Error::custom(e) })?;
                    if self.dataset.filename.is_none() {
                        self.dataset.filename = Some(filename);
                    }
                }
                "keys" => {
                    // handle the next value in a streaming manner
                    map.next_value_seed(DeserializeKeys {
                        store: self.dataset,
                    })?;
                }
                "data" => {
                    // handle the next value in a streaming manner
                    map.next_value_seed(DeserializeData {
                        dataset: self.dataset,
                    })?;
                }
                _ => {
                    eprintln!(
                        "Notice: Ignoring unknown key '{key}' whilst parsing AnnotationDataSet"
                    );
                    let _value: Self::Value = map.next_value()?; //read and discard the value
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

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
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
    dataset: &'a mut AnnotationDataSet,
}

impl<'de> DeserializeSeed<'de> for DeserializeData<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let visitor = DataVisitor {
            dataset: self.dataset,
        };
        deserializer.deserialize_seq(visitor)?;
        Ok(())
    }
}

struct DataVisitor<'a> {
    dataset: &'a mut AnnotationDataSet,
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
        let pre_length = self.dataset.data_len();
        loop {
            let databuilder: Option<AnnotationDataBuilder> = seq.next_element()?;
            if let Some(mut databuilder) = databuilder {
                let handle_from_temp_id = if self.dataset.config().strip_temp_ids() {
                    if let BuildItem::Id(s) = &databuilder.id {
                        resolve_temp_id(s.as_str())
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(handle) = handle_from_temp_id {
                    //strip the temporary public ID, it maps to a handle directly
                    databuilder.id = BuildItem::None;
                    // temporary public IDs are deserialized exactly
                    // as they were serialized. So if there were any gaps,
                    // we need to deserialize these too:
                    if self.dataset.data_len() > handle + pre_length {
                        return Err(serde::de::Error::custom(
                            "unable to resolve temporary public identifiers for annotation data",
                        ));
                    } else if handle > self.dataset.data_len() {
                        // expand the gaps, though this wastes memory if ensures that all references
                        // are valid without explicitly storing public identifiers.
                        self.dataset.data.resize_with(handle, Default::default);
                    }
                }
                self.dataset
                    .build_insert_data(databuilder, false) //safety disabled, data duplicates allowed at this stage (=faster)
                    .map_err(|e| -> A::Error { serde::de::Error::custom(e) })?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

use datasize::data_size;
use minicbor::{Decode, Encode};
use sealed::sealed;
use serde::de::DeserializeSeed;
use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::Serialize;
use smallvec::{smallvec, SmallVec};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::annotation::{Annotation, AnnotationBuilder, AnnotationHandle, AnnotationsJson};
use crate::annotationdata::AnnotationDataHandle;
use crate::annotationdataset::{
    AnnotationDataSet, AnnotationDataSetHandle, DeserializeAnnotationDataSet,
};
use crate::config::{Config, Configurable};
#[cfg(feature = "csv")]
use crate::csv::FromCsv;
use crate::datakey::DataKeyHandle;
use crate::error::*;
use crate::file::*;
use crate::json::{FromJson, ToJson};
use crate::resources::{DeserializeTextResource, TextResource, TextResourceHandle};
use crate::selector::{Offset, OffsetMode, Selector, SelectorBuilder};
use crate::store::*;
use crate::textselection::{ResultTextSelection, TextSelection, TextSelectionHandle};
use crate::types::*;

/// An Annotation Store is an unordered collection of annotations, resources and
/// annotation data sets. It can be seen as the *root* of the *graph model* and the glue
/// that holds everything together. It is the entry point for any stam model.
#[derive(Debug, Encode, Decode)]
pub struct AnnotationStore {
    #[n(0)] //these macros are field index numbers for cbor binary (de)serialisation
    pub(crate) id: Option<String>,

    /// Configuration with some minor interior mutability
    #[n(1)]
    pub(crate) config: Config,

    #[n(2)]
    pub(crate) annotations: Store<Annotation>,
    #[n(3)]
    pub(crate) annotationsets: Store<AnnotationDataSet>,
    #[n(4)]
    pub(crate) resources: Store<TextResource>,

    /// Links to annotations by ID.
    #[n(50)]
    pub(crate) annotation_idmap: IdMap<AnnotationHandle>,
    /// Links to resources by ID.
    #[n(51)]
    pub(crate) resource_idmap: IdMap<TextResourceHandle>,
    /// Links to datasets by ID.
    #[n(52)]
    pub(crate) dataset_idmap: IdMap<AnnotationDataSetHandle>,

    //reverse indices:
    /// Reverse index for AnnotationDataSet => AnnotationData => Annotation. Stores IntIds.
    /// The map is always sorted due to how it is constructed, no explicit sorting needed
    #[n(100)]
    pub(crate) dataset_data_annotation_map:
        TripleRelationMap<AnnotationDataSetHandle, AnnotationDataHandle, AnnotationHandle>,

    // Note there is no AnnotationDataSet => DataKey => Annotation map, that relationship
    // can be rsolved by the AnnotationDataSet::key_data_map in combination with the above dataset_data_annotation_map
    //
    /// This is the reverse index for text, it maps TextResource => TextSelection => Annotation
    /// The map is always sorted due to how it is constructed, no explicit sorting needed
    #[n(101)]
    pub(crate) textrelationmap:
        TripleRelationMap<TextResourceHandle, TextSelectionHandle, AnnotationHandle>,

    /// Reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`Selector::ResourceSelector`]), i.e. metadata
    /// The map is always sorted due to how it is constructed, no explicit sorting needed
    #[n(102)]
    pub(crate) resource_annotation_map: RelationMap<TextResourceHandle, AnnotationHandle>,

    /// Reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`Selector::DataSetSelector`]), i.e. metadata
    /// The map is always sorted due to how it is constructed, no explicit sorting needed
    #[n(103)]
    pub(crate) dataset_annotation_map: RelationMap<AnnotationDataSetHandle, AnnotationHandle>,

    /// Reverse index for annotations that reference other annotations
    /// The map is always sorted due to how it is constructed, no explicit sorting needed
    /// The choice for a BTreeMap here is more for memory-reduction (key may be sparse)
    #[n(104)]
    pub(crate) annotation_annotation_map: RelationBTreeMap<AnnotationHandle, AnnotationHandle>,

    /// path associated with this store
    #[n(200)]
    pub(crate) filename: Option<PathBuf>,

    #[cfg(feature = "csv")]
    /// path associated with the stand-off files holding annotations (only used for STAM CSV)
    #[n(201)]
    pub(crate) annotations_filename: Option<PathBuf>,
}

#[sealed]
impl TypeInfo for AnnotationStore {
    fn typeinfo() -> Type {
        Type::AnnotationStore
    }
}

//An AnnotationStore is a StoreFor TextResource
#[sealed]
impl StoreFor<TextResource> for AnnotationStore {
    /// Get a reference to the entire store for the associated type
    fn store(&self) -> &Store<TextResource> {
        &self.resources
    }
    /// Get a mutable reference to the entire store for the associated type
    fn store_mut(&mut self) -> &mut Store<TextResource> {
        &mut self.resources
    }
    /// Get a reference to the id map for the associated type, mapping global ids to internal ids
    fn idmap(&self) -> Option<&IdMap<TextResourceHandle>> {
        Some(&self.resource_idmap)
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    fn idmap_mut(&mut self) -> Option<&mut IdMap<TextResourceHandle>> {
        Some(&mut self.resource_idmap)
    }
    fn store_typeinfo() -> &'static str {
        "TextResource in AnnotationStore"
    }
}

impl private::StoreCallbacks<TextResource> for AnnotationStore {
    /// Called prior to inserting an item into to the store
    /// If it returns an error, the insert will be cancelled.
    /// Allows for bookkeeping such as inheriting configuration
    /// parameters from parent to the item
    #[allow(unused_variables)]
    fn preinsert(&self, item: &mut TextResource) -> Result<(), StamError> {
        item.set_config(self.config.clone());
        Ok(())
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    fn preremove(&mut self, handle: TextResourceHandle) -> Result<(), StamError> {
        if let Some(annotations) = self.resource_annotation_map.data.get(handle.as_usize()) {
            if !annotations.is_empty() {
                return Err(StamError::InUse("TextResource"));
            }
        }
        self.resource_annotation_map.data.remove(handle.as_usize());
        Ok(())
    }
}

impl ToJson for AnnotationStore {}

//An AnnotationStore is a StoreFor Annotation
#[sealed]
impl StoreFor<Annotation> for AnnotationStore {
    fn store(&self) -> &Store<Annotation> {
        &self.annotations
    }
    fn store_mut(&mut self) -> &mut Store<Annotation> {
        &mut self.annotations
    }
    fn idmap(&self) -> Option<&IdMap<AnnotationHandle>> {
        Some(&self.annotation_idmap)
    }
    fn idmap_mut(&mut self) -> Option<&mut IdMap<AnnotationHandle>> {
        Some(&mut self.annotation_idmap)
    }
    fn store_typeinfo() -> &'static str {
        "Annotation in AnnotationStore"
    }
}

impl private::StoreCallbacks<Annotation> for AnnotationStore {
    fn inserted(&mut self, handle: AnnotationHandle) -> Result<(), StamError> {
        // called after the item is inserted in the store
        // updates the relation map, this is where most of the reverse indexing happens
        // that facilitate search at later stages

        // note: a normal self.get() doesn't cut it here because then all of self will be borrowed for 'a and we have problems with the mutable reference later
        //       now at least the borrow checker knows self.annotations is distinct
        //       the other option would be to dp annotation.clone(), at a slightly higher cost which we don't want here
        let annotation = self
            .annotations
            .get(handle.as_usize())
            .unwrap()
            .as_ref()
            .unwrap();

        debug(self.config(), || {
            format!("StoreFor<Annotation in AnnotationStore>.inserted: Indexing annotation")
        });

        for (dataset, data) in annotation.data() {
            self.dataset_data_annotation_map
                .insert(*dataset, *data, handle);
        }

        let mut multitarget = false;
        // first we handle the simple singular targets, and determine if we need to do more
        match annotation.target() {
            Selector::DataSetSelector(dataset_handle) => {
                if self.config.dataset_annotation_map {
                    self.dataset_annotation_map.insert(*dataset_handle, handle);
                }
            }
            Selector::ResourceSelector(res_handle) => {
                if self.config.resource_annotation_map {
                    self.resource_annotation_map.insert(*res_handle, handle);
                }
            }
            Selector::AnnotationSelector(a_handle, offset) => {
                if self.config.annotation_annotation_map {
                    if offset.is_some() {
                        multitarget = true; //we also want to populate the textrelationmap, this is handled in the multitarget block
                    } else {
                        self.annotation_annotation_map.insert(*a_handle, handle);
                    }
                }
            }
            Selector::TextSelector(res_handle, textselection_handle, _) => {
                if self.config.textrelationmap {
                    self.textrelationmap
                        .insert(*res_handle, *textselection_handle, handle);
                }
            }
            _ => {
                multitarget = true;
            }
        }

        //intermediate structure to gather text relations that should later be added to the reverse index
        let mut extend_textrelationmap: SmallVec<
            [(TextResourceHandle, TextSelectionHandle, AnnotationHandle); 1],
        > = SmallVec::new();

        // if needed, we handle more complex situations where there are multiple targets
        if multitarget {
            let mut target_resources: Vec<(TextResourceHandle, AnnotationHandle)> = Vec::new();
            let mut target_annotations: Vec<(AnnotationHandle, AnnotationHandle)> = Vec::new();
            let mut target_datasets: Vec<(AnnotationDataSetHandle, AnnotationHandle)> = Vec::new();

            for selector in annotation.target().iter(self, false) {
                if self.config.textrelationmap {
                    for (res_handle, tsel_handle) in self.textselections_by_selector(&selector) {
                        extend_textrelationmap.push((res_handle, tsel_handle, handle));
                    }
                }
                if self.config.annotation_annotation_map {
                    if let Selector::AnnotationSelector(a_handle, _) = selector.as_ref() {
                        target_annotations.push((*a_handle, handle));
                    }
                }
                match selector.as_ref() {
                    Selector::ResourceSelector(res_handle) => {
                        if self.config.resource_annotation_map {
                            target_resources.push((*res_handle, handle));
                        }
                    }
                    Selector::DataSetSelector(set_handle) => {
                        if self.config.dataset_annotation_map {
                            target_datasets.push((*set_handle, handle));
                        }
                    }
                    _ => {}
                };
            }

            if self.config.resource_annotation_map {
                self.resource_annotation_map
                    .extend(target_resources.iter().map(|(x, y)| (*x, *y)).into_iter());
            }

            if self.config.textrelationmap {
                //now we add the gathered textselection to the textrelationmap (we needed this buffer because we couldn't have a mutable and immutable reference at once before)
                self.textrelationmap
                    .extend(extend_textrelationmap.into_iter());
            }
        }

        Ok(())
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    fn preremove(&mut self, handle: AnnotationHandle) -> Result<(), StamError> {
        let annotation: &Annotation = self.get(handle)?;
        let resource_handle: Option<TextResourceHandle> = match annotation.target() {
            Selector::ResourceSelector(res_handle) => Some(*res_handle),
            _ => None,
        };
        let annotationset_handle: Option<AnnotationDataSetHandle> = match annotation.target() {
            Selector::DataSetSelector(annotationset_handle) => Some(*annotationset_handle),
            _ => None,
        };
        let annotation_handle: Option<AnnotationHandle> = match annotation.target() {
            Selector::AnnotationSelector(annotation_handle, _) => Some(*annotation_handle),
            _ => None,
        };

        if let Some(resource_handle) = resource_handle {
            self.resource_annotation_map.remove(resource_handle, handle);
        }
        if let Some(annotationset_handle) = annotationset_handle {
            self.dataset_annotation_map
                .remove(annotationset_handle, handle);
        }
        if let Some(annotation_handle) = annotation_handle {
            self.annotation_annotation_map
                .remove(annotation_handle, handle);
        }

        Ok(())
    }
}

//An AnnotationStore is a StoreFor AnnotationDataSet
#[sealed]
impl StoreFor<AnnotationDataSet> for AnnotationStore {
    fn store(&self) -> &Store<AnnotationDataSet> {
        &self.annotationsets
    }
    fn store_mut(&mut self) -> &mut Store<AnnotationDataSet> {
        &mut self.annotationsets
    }
    fn idmap(&self) -> Option<&IdMap<AnnotationDataSetHandle>> {
        Some(&self.dataset_idmap)
    }
    fn idmap_mut(&mut self) -> Option<&mut IdMap<AnnotationDataSetHandle>> {
        Some(&mut self.dataset_idmap)
    }
    fn store_typeinfo() -> &'static str {
        "AnnotationDataSet in AnnotationStore"
    }
}

impl private::StoreCallbacks<AnnotationDataSet> for AnnotationStore {
    /// Called prior to inserting an item into to the store
    /// If it returns an error, the insert will be cancelled.
    /// Allows for bookkeeping such as inheriting configuration
    /// parameters from parent to the item
    #[allow(unused_variables)]
    fn preinsert(&self, item: &mut AnnotationDataSet) -> Result<(), StamError> {
        item.set_config(self.config.clone());
        Ok(())
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    fn preremove(&mut self, handle: AnnotationDataSetHandle) -> Result<(), StamError> {
        if let Some(annotations) = self.dataset_annotation_map.data.get(handle.as_usize()) {
            if !annotations.is_empty() {
                return Err(StamError::InUse("AnnotationDataSet"));
            }
        }
        self.dataset_annotation_map.data.remove(handle.as_usize());
        Ok(())
    }
}

impl WrappableStore<Annotation> for AnnotationStore {}

impl Serialize for AnnotationStore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationStore", 2)?;
        state.serialize_field("@type", "AnnotationStore")?;
        if let Some(id) = self.id() {
            state.serialize_field("@id", id)?;
        }
        state.serialize_field("resources", &self.resources)?;
        state.serialize_field("annotationsets", &self.annotationsets)?;
        let wrappedstore: WrappedStore<Annotation, Self> = self.wrap_store();
        state.serialize_field("annotations", &wrappedstore)?;
        state.end()
    }
}

impl<'a> Serialize for WrappedStore<'a, Annotation, AnnotationStore> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.store.len()))?;
        for data in self.store.iter() {
            if let Some(data) = data {
                seq.serialize_element(&data.as_resultitem(self.parent, self.parent))?;
            }
        }
        seq.end()
    }
}

impl FromJson for AnnotationStore {
    /// Loads an AnnotationStore from a STAM JSON file
    /// The file must contain a single object which has "@type": "AnnotationStore"
    fn from_json_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!("AnnotationStore::from_json_file: filename={:?}", filename)
        });
        let reader = open_file_reader(filename, &config)?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);

        let mut store: AnnotationStore = AnnotationStore::new(config).with_filename(filename);

        DeserializeAnnotationStore::new(&mut store)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(store)
    }

    /// Loads an AnnotationStore from a STAM JSON string
    /// The string must contain a single object which has "@type": "AnnotationStore"
    fn from_json_str(string: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!("AnnotationStore::from_json_str: string={:?}", string)
        });
        let deserializer = &mut serde_json::Deserializer::from_str(string);

        let mut store: AnnotationStore = AnnotationStore::new(config);

        DeserializeAnnotationStore::new(&mut store)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(store)
    }

    /// Merges an AnnotationStore from a STAM JSON file into the current one
    /// The file must contain a single object which has "@type": "AnnotationStore"
    fn merge_json_file(&mut self, filename: &str) -> Result<(), StamError> {
        debug(self.config(), || {
            format!("AnnotationStore::from_json_file: filename={:?}", filename)
        });
        let reader = open_file_reader(filename, self.config())?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);

        DeserializeAnnotationStore::new(self)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(())
    }

    /// Merges an AnnotationStore from a STAM JSON string into the current one
    /// The string must contain a single object which has "@type": "AnnotationStore"
    fn merge_json_str(&mut self, string: &str) -> Result<(), StamError> {
        debug(self.config(), || {
            format!("AnnotationStore::from_json_str: string={:?}", string)
        });
        let deserializer = &mut serde_json::Deserializer::from_str(string);

        DeserializeAnnotationStore::new(self)
            .deserialize(deserializer)
            .map_err(|e| StamError::DeserializationError(e.to_string()))?;

        Ok(())
    }
}

impl Configurable for AnnotationStore {
    /// Return the associated configuration
    fn config(&self) -> &Config {
        &self.config
    }

    fn config_mut(&mut self) -> &mut Config {
        &mut self.config
    }

    /// Sets the configuration, this will also overwrite all underlying configurations for annotation data sets and resources!
    fn set_config(&mut self, config: Config) -> &mut Self {
        self.config = config;
        self.propagate_full_config();
        self
    }
}

impl Default for AnnotationStore {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl AnnotationStore {
    ///Creates a new empty annotation store with a default configuraton, add the [`AnnotationStore.with_config()`] to provide a custom one
    pub fn new(config: Config) -> Self {
        AnnotationStore {
            id: None,
            annotations: Vec::new(),
            annotationsets: Vec::new(),
            resources: Vec::new(),
            annotation_idmap: IdMap::new("A".to_string())
                .with_resolve_temp_ids(config.strip_temp_ids()),
            resource_idmap: IdMap::new("R".to_string())
                .with_resolve_temp_ids(config.strip_temp_ids()),
            dataset_idmap: IdMap::new("S".to_string())
                .with_resolve_temp_ids(config.strip_temp_ids()),
            dataset_data_annotation_map: TripleRelationMap::new(),
            dataset_annotation_map: RelationMap::new(),
            resource_annotation_map: RelationMap::new(),
            annotation_annotation_map: RelationBTreeMap::new(),
            textrelationmap: TripleRelationMap::new(),
            config,
            filename: None,
            annotations_filename: None,
        }
    }

    /// Loads an AnnotationStore from a file (STAM JSON or another supported format)
    /// The file must contain a single object which has "@type": "AnnotationStore"
    pub fn from_file(filename: &str, mut config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "AnnotationStore::from_file: filename={:?} config={:?}",
                filename, config
            )
        });
        //extract work directory add add it to the config (if it does not already specify a working directory)
        if config.workdir().is_none() {
            let mut workdir: PathBuf = filename.into();
            workdir.pop();
            if !workdir.to_str().expect("path to string").is_empty() {
                debug(&config, || {
                    format!("AnnotationStore::from_file: set workdir to {:?}", workdir)
                });
                config.workdir = Some(workdir);
            }
        }

        if filename.ends_with("cbor") || config.dataformat == DataFormat::CBOR {
            config.dataformat = DataFormat::CBOR;
            return AnnotationStore::from_cbor_file(filename, config);
        }

        #[cfg(feature = "csv")]
        if filename.ends_with("csv") || config.dataformat == DataFormat::Csv {
            config.dataformat = DataFormat::Csv;
            return AnnotationStore::from_csv_file(filename, config);
        }

        AnnotationStore::from_json_file(filename, config)
    }

    /// Loads an AnnotationStore from a STAM JSON string
    /// The string must contain a single object which has "@type": "AnnotationStore"
    pub fn from_str(string: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "AnnotationStore::from_str: string={:?} config={:?}",
                string, config
            )
        });
        AnnotationStore::from_json_str(string, config)
    }

    /// Merge another annotation store STAM JSON file into this one
    /// Note: The ID and filename of the store will not be overwritten if already set,
    ///       reserialising the store will produce a single new store.
    pub fn with_file(mut self, filename: &str) -> Result<Self, StamError> {
        #[cfg(feature = "csv")]
        if filename.ends_with("csv") || self.config().dataformat == DataFormat::Csv {
            if self.annotations.is_empty()
                && self.resources.is_empty()
                && self.annotationsets.is_empty()
            {
                let mut config = self.config.clone();
                config.dataformat = DataFormat::Csv;
                return AnnotationStore::from_csv_file(filename, config);
            }
            todo!("Merging CSV files for AnnotationStore is not supported yet");
        }

        self.merge_json_file(filename)?;

        Ok(self)
    }

    /// Returns the filename associated with this annotation store for storage of annotations
    /// Only used for STAM CSV, not for STAM JSON.
    pub fn annotations_filename(&self) -> Option<&Path> {
        self.annotations_filename.as_ref().map(|x| x.as_path())
    }

    /// Load a JSON file containing an array of annotations in STAM JSON
    /// TODO: this is currently not efficient as it holds all annotation builders in memory first
    pub fn annotate_from_file(&mut self, filename: &str) -> Result<&mut Self, StamError> {
        let reader = open_file_reader(filename, self.config())?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);
        let result: Result<AnnotationsJson, _> = serde_path_to_error::deserialize(deserializer);
        let result = result.map_err(|e| {
            StamError::JsonError(e, filename.to_string(), "Reading annotations from file")
        })?;
        for annotation in result.0.into_iter() {
            self.annotate(annotation.into())?;
        }
        Ok(self)
    }

    /// Write the annotation store and all files below it to file (STAM JSON or other supported formats likes STAM CSV)
    /// The filetype is determined by the extension.
    pub fn to_file(&mut self, filename: &str) -> Result<(), StamError> {
        if self.filename() != Some(filename) {
            self.set_filename(filename);
        }
        self.save()
    }

    /// Changes the output dataformat, this function will set the external files with appropriate filenames (extensions) that are to be written on serialisation
    /// They will be derived from the existing filenames, if any.
    /// End user should just use [`AnnotationStore.set_filename`] with a recognized extension (json, csv)
    pub(crate) fn set_dataformat(&mut self, dataformat: DataFormat) -> Result<(), StamError> {
        if dataformat != DataFormat::CBOR {
            //process the children

            for resource in self.resources.iter_mut() {
                if let Some(resource) = resource.as_mut() {
                    if resource.config().dataformat != dataformat {
                        let mut basename = if let Some(basename) =
                            resource.filename_without_extension()
                        {
                            basename.to_owned()
                        } else if let Some(id) = resource.id() {
                            sanitize_id_to_filename(id)
                        } else {
                            return Err(StamError::SerializationError(format!(
                            "Unable to infer a filename for resource {:?}. Has neither filename nor ID.",
                            resource
                        )));
                        };

                        if basename.find("/").is_none() {
                            if let Some(workdir) = resource.config().workdir() {
                                if workdir.ends_with("/") {
                                    basename = String::from(workdir.to_str().expect("valid utf-8"))
                                        + &basename;
                                } else {
                                    basename = String::from(workdir.to_str().expect("valid utf-8"))
                                        + "/"
                                        + &basename;
                                }
                            }
                        }

                        //always prefer external plain text for CSV
                        #[cfg(feature = "csv")]
                        if dataformat == DataFormat::Csv {
                            resource.set_filename(format!("{}.txt", basename).as_str());
                            resource.mark_changed()
                        }
                    }
                }
            }
            for annotationset in self.annotationsets.iter_mut() {
                if let Some(annotationset) = annotationset.as_mut() {
                    if annotationset.config().dataformat != dataformat {
                        let mut basename = if let Some(basename) =
                            annotationset.filename_without_extension()
                        {
                            basename.to_owned()
                        } else if let Some(id) = annotationset.id() {
                            sanitize_id_to_filename(id)
                        } else {
                            return Err(StamError::SerializationError(format!(
                        "Unable to infer a filename for annotationset. Has neither filename nor ID.",
                    )));
                        };

                        if basename.find("/").is_none() {
                            if let Some(workdir) = annotationset.config().workdir() {
                                if workdir.ends_with("/") {
                                    basename = String::from(workdir.to_str().expect("valid utf-8"))
                                        + &basename;
                                } else {
                                    basename = String::from(workdir.to_str().expect("valid utf-8"))
                                        + "/"
                                        + &basename;
                                }
                            }
                        }

                        if let DataFormat::Json { .. } = dataformat {
                            annotationset.set_filename(
                                format!("{}.annotationset.stam.json", basename).as_str(),
                            );
                            annotationset.mark_changed()
                        }

                        #[cfg(feature = "csv")]
                        if dataformat == DataFormat::Csv {
                            annotationset.set_filename(
                                format!("{}.annotationset.stam.csv", basename).as_str(),
                            );
                            annotationset.mark_changed()
                        }
                    }
                }
            }
        }

        let basename = if let Some(basename) = self.filename_without_extension() {
            basename.to_owned()
        } else if let Some(id) = self.id() {
            sanitize_id_to_filename(id)
        } else {
            return Err(StamError::SerializationError(format!(
                "Unable to infer a filename for AnnotationStore. Has neither filename nor ID.",
            )));
        };

        if let DataFormat::Json { .. } = dataformat {
            // we don't use set_filename because that might recurse back to us
            self.filename = Some(format!("{}.store.stam.json", basename).into());
        } else if let DataFormat::CBOR = dataformat {
            // we don't use set_filename because that might recurse back to us
            self.filename = Some(format!("{}.store.stam.cbor", basename).into());
        }

        #[cfg(feature = "csv")]
        if let DataFormat::Csv = dataformat {
            self.filename = Some(format!("{}.store.stam.csv", basename).into());
            self.annotations_filename = Some(format!("{}.annotations.stam.csv", basename).into());
        }

        self.update_config(|config| config.dataformat = dataformat);

        Ok(())
    }

    /// Shortcut to write an AnnotationStore to a STAM JSON file, writes to the same file as was loaded.
    /// Returns an error if no filename was associated yet.
    /// Use [`AnnotationStore.to_file`] instead if you want to write elsewhere.
    ///
    /// Note: If multiple stores were loaded and merged, this will write all merged results in place of the first loaded store!
    pub fn save(&self) -> Result<(), StamError> {
        debug(self.config(), || format!("AnnotationStore.save"));
        if self.filename.is_some() {
            match self.config().dataformat {
                DataFormat::Json { .. } => {
                    self.to_json_file(self.filename().unwrap(), self.config()) //may produce 1 or multiple files
                }
                DataFormat::CBOR { .. } => {
                    self.to_cbor_file(self.filename().unwrap()) //always one file
                }
                #[cfg(feature = "csv")]
                DataFormat::Csv => self.to_csv_files(self.filename().unwrap()), //always produces multiple files
            }
        } else {
            Err(StamError::SerializationError(
                "No filename associated with the store".to_owned(),
            ))
        }
    }

    fn to_cbor_file(&self, filename: &str) -> Result<(), StamError> {
        debug(self.config(), || {
            format!("{}.to_cbor_file: filename={:?}", Self::typeinfo(), filename)
        });
        let writer = open_file_writer(filename, self.config())?;
        let writer = minicbor::encode::write::Writer::new(writer);
        minicbor::encode(self, writer)
            .map_err(|e| StamError::SerializationError(format!("{}", e)))?;
        Ok(())
    }

    fn from_cbor_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!("AnnotationStore::from_json_file: filename={:?}", filename)
        });
        let mut reader = open_file_reader(filename, &config)?;
        let mut buffer: Vec<u8> = Vec::new(); //will hold the entire CBOR file!!!
        reader
            .read_to_end(&mut buffer)
            .map_err(|e| StamError::DeserializationError(format!("{}", e)))?;
        let mut store: AnnotationStore = minicbor::decode(&buffer)
            .map_err(|e| StamError::DeserializationError(format!("{}", e)))?;
        // the supplied configuration is largely discarded in favour of the loaded one from file, but we do copy some settings:
        store.config.debug = config.debug;
        store.config.shrink_to_fit = config.shrink_to_fit;
        if config.shrink_to_fit() {
            store.shrink_to_fit(true);
        }
        Ok(store)
    }

    /// Propagate the entire configuration to all children, will overwrite customized configurations
    fn propagate_full_config(&mut self) {
        if self.resources_len() > 0 || self.datasets_len() > 0 {
            let config = self.config().clone();
            for resource in self.resources.iter_mut() {
                if let Some(resource) = resource.as_mut() {
                    resource.set_config(config.clone());
                }
            }
            for annotationset in self.annotationsets.iter_mut() {
                if let Some(annotationset) = annotationset.as_mut() {
                    annotationset.set_config(config.clone());
                }
            }
        }
        self.annotation_idmap
            .set_resolve_temp_ids(self.config().strip_temp_ids());
        self.resource_idmap
            .set_resolve_temp_ids(self.config().strip_temp_ids());
        self.dataset_idmap
            .set_resolve_temp_ids(self.config().strip_temp_ids());
    }

    /// Recursively update the configurate for self and all children, the actual update is in a closure
    fn update_config<F>(&mut self, f: F)
    where
        F: Fn(&mut Config),
    {
        f(self.config_mut());

        for resource in self.resources.iter_mut() {
            if let Some(resource) = resource.as_mut() {
                f(resource.config_mut());
            }
        }
        for annotationset in self.annotationsets.iter_mut() {
            if let Some(annotationset) = annotationset.as_mut() {
                f(annotationset.config_mut());
            }
        }
    }

    /// Returns the ID of the annotation store (if any)
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Sets the ID of the annotation store in a builder pattern
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Shortcut method that calls add_resource under the hood and returns a reference to it
    pub fn add_resource_from_file(
        &mut self,
        filename: &str,
    ) -> Result<TextResourceHandle, StamError> {
        let resource = TextResource::from_file(filename, self.config().clone())?;
        self.insert(resource)
    }

    /// Get an annotation handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_annotation_id(&self, id: &str) -> Result<AnnotationHandle, StamError> {
        <AnnotationStore as StoreFor<Annotation>>::resolve_id(self, id)
    }

    /// Get an annotation dataset handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_dataset_id(&self, id: &str) -> Result<AnnotationDataSetHandle, StamError> {
        <AnnotationStore as StoreFor<AnnotationDataSet>>::resolve_id(self, id)
    }

    /// Get an annotation dataset handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_resource_id(&self, id: &str) -> Result<TextResourceHandle, StamError> {
        <AnnotationStore as StoreFor<TextResource>>::resolve_id(self, id)
    }

    /// Returns the number of annotations in the store (deletions are not substracted)
    pub fn annotations_len(&self) -> usize {
        self.annotations.len()
    }

    /// Returns the number of resources  in the store (deletions are not substracted)
    pub fn resources_len(&self) -> usize {
        self.resources.len()
    }

    /// Returns the number of datasets in the store (deletions are not substracted)
    pub fn datasets_len(&self) -> usize {
        self.annotationsets.len()
    }

    /// Builds a [`Selector`] based on its [`SelectorBuilder`], this will produce an error if the selected resource does not exist.
    /// This is a low-level method that you shouldn't need to call yourself.
    pub fn selector(&mut self, item: SelectorBuilder) -> Result<Selector, StamError> {
        match item {
            SelectorBuilder::ResourceSelector(id) => {
                let resource: &TextResource = self.get(id)?;
                Ok(Selector::ResourceSelector(resource.handle_or_err()?))
            }
            SelectorBuilder::TextSelector(res_id, offset) => {
                let resource: &mut TextResource = self.get_mut(res_id)?;
                let textselection = resource.textselection_by_offset(&offset)?;
                let textselection_handle: TextSelectionHandle =
                    if let Some(textselection_handle) = textselection.handle() {
                        //we already have a handle so the textselection already exists
                        textselection_handle
                    } else {
                        //new, insert... (it's important never to insert the same one twice!)
                        resource.insert(textselection)?
                    };
                //note: insertion into reverse indices will happen after annotation insertion
                Ok(Selector::TextSelector(
                    resource.handle_or_err()?,
                    textselection_handle,
                    offset.mode(),
                ))
            }
            SelectorBuilder::AnnotationSelector(a_id, offset) => {
                if let Some(offset) = offset {
                    let target_annotation: &Annotation = self.get(&a_id)?;
                    let target_annotation_handle = target_annotation.handle_or_err()?;
                    if let Some(parent_textselection) =
                        target_annotation.target().textselection(self)
                    {
                        let resource_handle = target_annotation
                            .target()
                            .resource_handle()
                            .expect("selector must have resource");
                        let textselection =
                            parent_textselection.textselection_by_offset(&offset)?;
                        let resource: &mut TextResource = self.get_mut(resource_handle)?;
                        let textselection_handle = if let Some(textselection_handle) =
                            resource.known_textselection(&textselection.into())?
                        {
                            //we already have a handle so the textselection already exists
                            textselection_handle
                        } else {
                            //new, insert... (it's important never to insert the same one twice!)
                            resource.insert(textselection)?
                        };
                        return Ok(Selector::AnnotationSelector(
                            target_annotation_handle,
                            Some((
                                resource.handle_or_err()?,
                                textselection_handle,
                                offset.mode(),
                            )),
                        ));
                    }
                }
                let target_annotation: &Annotation = self.get(&a_id)?;
                Ok(Selector::AnnotationSelector(
                    target_annotation.handle_or_err()?,
                    None,
                ))
            }
            SelectorBuilder::DataSetSelector(id) => {
                let resource: &AnnotationDataSet = self.get(id)?;
                Ok(Selector::DataSetSelector(resource.handle_or_err()?))
            }
            SelectorBuilder::MultiSelector(v) => {
                Ok(Selector::MultiSelector(self.subselectors(v, true)?))
            }
            SelectorBuilder::DirectionalSelector(v) => {
                Ok(Selector::DirectionalSelector(self.subselectors(v, false)?))
            }
            SelectorBuilder::CompositeSelector(v) => {
                Ok(Selector::CompositeSelector(self.subselectors(v, true)?))
            }
        }
    }

    /// Auxiliary function for `selector() `
    pub(crate) fn subselectors(
        &mut self,
        builders: Vec<SelectorBuilder>,
        textual_order: bool,
    ) -> Result<Vec<Selector>, StamError> {
        let mut tmp = Vec::with_capacity(builders.len());
        for builder in builders {
            if builder.is_complex() {
                return Err(StamError::WrongSelectorType(
                    "Complex selectors may not be nested",
                ));
            }
            let selector = self.selector(builder)?;
            tmp.push(selector);
        }
        if tmp.len() == 1 {
            //shortcut (but in this edge-case there's little point of a complex selector at all)
            return Ok(tmp);
        }

        if textual_order {
            tmp.sort_unstable_by(|a, b| match (a, b) {
                (Selector::TextSelector(res, tsel, _), Selector::TextSelector(res2, tsel2, _))
                | (
                    Selector::AnnotationSelector(_, Some((res, tsel, _))),
                    Selector::AnnotationSelector(_, Some((res2, tsel2, _))),
                )
                | (
                    Selector::AnnotationSelector(_, Some((res, tsel, _))),
                    Selector::TextSelector(res2, tsel2, _),
                )
                | (
                    Selector::TextSelector(res, tsel, _),
                    Selector::AnnotationSelector(_, Some((res2, tsel2, _))),
                ) => {
                    if res == res2 {
                        let resource: &TextResource =
                            self.get(*res).expect("resource must resolve");
                        let textselection: &TextSelection =
                            resource.get(*tsel).expect("textselection must resolve");
                        let textselection2: &TextSelection =
                            resource.get(*tsel2).expect("textselection must resolve");
                        textselection.cmp(textselection2)
                    } else {
                        res.cmp(res2)
                    }
                }
                (
                    Selector::AnnotationSelector(annotation, None),
                    Selector::AnnotationSelector(annotation2, None),
                ) => annotation.cmp(annotation2),
                (
                    Selector::AnnotationSelector(_, None),
                    Selector::AnnotationSelector(_, Some(_)),
                ) => Ordering::Greater,
                (
                    Selector::AnnotationSelector(_, Some(_)),
                    Selector::AnnotationSelector(_, None),
                ) => Ordering::Less,
                (Selector::ResourceSelector(res), Selector::ResourceSelector(res2)) => {
                    res.cmp(res2)
                }
                (Selector::DataSetSelector(dataset), Selector::DataSetSelector(dataset2)) => {
                    dataset.cmp(dataset2)
                }
                //some canonical ordering for selectors
                (Selector::TextSelector(..), _) => Ordering::Less,
                (_, Selector::TextSelector(..)) => Ordering::Greater,
                (Selector::ResourceSelector(..), _) => Ordering::Less,
                (_, Selector::ResourceSelector(..)) => Ordering::Greater,
                (Selector::DataSetSelector(..), _) => Ordering::Less,
                (_, Selector::DataSetSelector(..)) => Ordering::Greater,
                // catch-all for anything that shouldn't occur at this point anyway:
                (a, b) => panic!("Unable to compare order for selector {:?} vs {:?}", a, b),
            });
        }

        let mut results = Vec::with_capacity(tmp.len());
        for selector in tmp {
            //we may be able to merge things into an internal ranged selector, conserving memory
            //we also need to ensure selectors are inserted in textual order
            if let Some(last) = results.last_mut() {
                let mut substitute: Option<Selector> = None;
                match (&last, &selector) {
                    (
                        Selector::TextSelector(res, tsel, OffsetMode::BeginBegin),
                        Selector::TextSelector(res2, tsel2, OffsetMode::BeginBegin),
                    ) => {
                        if res == res2 && tsel2.as_usize() == tsel.as_usize() + 1 {
                            substitute = Some(Selector::RangedTextSelector {
                                resource: *res,
                                begin: *tsel,
                                end: *tsel2,
                            });
                        }
                    }
                    (
                        Selector::RangedTextSelector {
                            resource,
                            begin,
                            end,
                        },
                        Selector::TextSelector(res2, tsel2, OffsetMode::BeginBegin),
                    ) => {
                        if resource == res2 && tsel2.as_usize() == end.as_usize() + 1 {
                            substitute = Some(Selector::RangedTextSelector {
                                resource: *resource,
                                begin: *begin,
                                end: *tsel2,
                            });
                        }
                    }
                    (
                        Selector::AnnotationSelector(annotation, None),
                        Selector::AnnotationSelector(annotation2, None),
                    ) => {
                        if annotation2.as_usize() == annotation.as_usize() + 1 {
                            substitute = Some(Selector::RangedAnnotationSelector {
                                begin: *annotation,
                                end: *annotation2,
                                with_text: false,
                            });
                        }
                    }
                    (
                        Selector::RangedAnnotationSelector {
                            begin,
                            end,
                            with_text: false,
                        },
                        Selector::AnnotationSelector(annotation, None),
                    ) => {
                        if annotation.as_usize() == end.as_usize() + 1 {
                            substitute = Some(Selector::RangedAnnotationSelector {
                                begin: *begin,
                                end: *annotation,
                                with_text: false,
                            });
                        }
                    }
                    (
                        Selector::AnnotationSelector(annotation, Some(_)),
                        Selector::AnnotationSelector(annotation2, Some(_)),
                    ) => {
                        if annotation2.as_usize() == annotation.as_usize() + 1 {
                            //we can only merge annotations that reference the entire underlying annotation's text and not a subpart of it
                            if let (Some(offset), Some(offset2)) = (
                                last.offset_with_mode(self, Some(OffsetMode::BeginEnd)),
                                selector.offset_with_mode(self, Some(OffsetMode::BeginEnd)),
                            ) {
                                if offset.begin == Cursor::BeginAligned(0)
                                    && offset2.begin == Cursor::BeginAligned(0)
                                    && offset.end == Cursor::EndAligned(0)
                                    && offset2.end == Cursor::EndAligned(0)
                                {
                                    substitute = Some(Selector::RangedAnnotationSelector {
                                        begin: *annotation,
                                        end: *annotation2,
                                        with_text: true,
                                    });
                                }
                            }
                        }
                    }
                    (
                        Selector::RangedAnnotationSelector {
                            begin,
                            end,
                            with_text: true,
                        },
                        Selector::AnnotationSelector(annotation, Some(_)),
                    ) => {
                        if annotation.as_usize() == end.as_usize() + 1 {
                            //we can only merge annotations that reference the entire underlying annotation's text and not a subpart of it
                            if let Some(offset) =
                                selector.offset_with_mode(self, Some(OffsetMode::BeginEnd))
                            {
                                if offset.begin == Cursor::BeginAligned(0)
                                    && offset.end == Cursor::EndAligned(0)
                                {
                                    substitute = Some(Selector::RangedAnnotationSelector {
                                        begin: *begin,
                                        end: *annotation,
                                        with_text: true,
                                    });
                                }
                            }
                        }
                    }
                    _ => {
                        //nothing to do for others, not mergable
                    }
                }
                if let Some(substitute) = substitute {
                    *last = substitute;
                    continue; //prevent reaching the push below
                }
            }
            results.push(selector);
        }
        Ok(results)
    }

    /// Low-level method to retrieve  [`TextSelection`] handles given a specific selector.
    pub(crate) fn textselections_by_selector<'store>(
        &'store self,
        selector: &Selector,
    ) -> SmallVec<[(TextResourceHandle, TextSelectionHandle); 2]> {
        match selector {
            Selector::TextSelector(res_handle, tsel_handle, _)
            | Selector::AnnotationSelector(_, Some((res_handle, tsel_handle, _))) => {
                smallvec!((*res_handle, *tsel_handle))
            }
            Selector::RangedTextSelector {
                resource,
                begin,
                end,
            } => {
                let mut results = SmallVec::with_capacity(end.as_usize() - begin.as_usize());
                for i in begin.as_usize()..=end.as_usize() {
                    results.push((*resource, TextSelectionHandle(i as u32)));
                }
                results
            }
            Selector::RangedAnnotationSelector {
                begin,
                end,
                with_text: true,
            } => {
                let mut results = SmallVec::with_capacity(end.as_usize() - begin.as_usize());
                for i in begin.as_usize()..=end.as_usize() {
                    let annotation: &Annotation = self
                        .get(AnnotationHandle::new(i))
                        .expect("handle must be valid");
                    if let (Some(res_handle), Some(tsel_handle)) = (
                        annotation.target().resource_handle(),
                        annotation.target().textselection_handle(),
                    ) {
                        results.push((res_handle, tsel_handle));
                    }
                }
                results
            }
            Selector::MultiSelector(subselectors)
            | Selector::CompositeSelector(subselectors)
            | Selector::DirectionalSelector(subselectors) => {
                let mut results = SmallVec::with_capacity(subselectors.len());
                for subselector in subselectors {
                    results.extend(self.textselections_by_selector(subselector).into_iter())
                }
                results
            }
            _ => SmallVec::new(),
        }
    }

    /// Returns length for each of the reverse indices
    ///  - dataset_data_annotation_map
    ///  - textrelationmap
    ///  - resource_annotation_map
    ///  - dataset_annotation_map
    ///  - annotation_annotation_map
    ///  - resource id map
    ///  - dataset id map
    ///  - annotation id map
    pub fn index_len(&self) -> (usize, usize, usize, usize, usize, usize, usize, usize) {
        (
            self.dataset_data_annotation_map.len(),
            self.textrelationmap.len(),
            self.resource_annotation_map.len(),
            self.dataset_annotation_map.len(),
            self.annotation_annotation_map.len(),
            self.resource_idmap.len(),
            self.dataset_idmap.len(),
            self.annotation_idmap.len(),
        )
    }

    /// Returns total counts for each of the reverse indices
    ///  - dataset_data_annotation_map
    ///  - textrelationmap
    ///  - resource_annotation_map
    ///  - dataset_annotation_map
    ///  - annotation_annotation_map
    pub fn index_totalcount(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.dataset_data_annotation_map.totalcount(),
            self.textrelationmap.totalcount(),
            self.resource_annotation_map.totalcount(),
            self.dataset_annotation_map.totalcount(),
            self.annotation_annotation_map.totalcount(),
        )
    }

    /// Returns estimated lower-bound for memory consumption for each of the reverse indices
    ///  - dataset_data_annotation_map
    ///  - textrelationmap
    ///  - resource_annotation_map
    ///  - dataset_annotation_map
    ///  - annotation_annotation_map
    ///  - resource id map
    ///  - dataset id map
    ///  - annotation id map
    pub fn index_meminfo(&self) -> (usize, usize, usize, usize, usize, usize, usize, usize) {
        (
            self.dataset_data_annotation_map.meminfo(),
            self.textrelationmap.meminfo(),
            self.resource_annotation_map.meminfo(),
            self.dataset_annotation_map.meminfo(),
            self.annotation_annotation_map.meminfo(),
            self.resource_idmap.meminfo(),
            self.dataset_idmap.meminfo(),
            self.annotation_idmap.meminfo(),
        )
    }

    pub fn annotations_meminfo(&self) -> usize {
        data_size(&self.annotations)
    }

    /// Returns partcial count for each of the triple reverse indices, does not count the deepest layer
    ///  - dataset_data_annotation_map
    ///  - textrelationmap
    pub fn index_partialcount(&self) -> (usize, usize) {
        (
            self.dataset_data_annotation_map.partialcount(),
            self.textrelationmap.partialcount(),
        )
    }

    /// Re-allocates data structures to minimize memory consumption
    pub fn shrink_to_fit(&mut self, recursive: bool) {
        if recursive {
            for resource in self.resources.iter_mut() {
                if let Some(resource) = resource {
                    resource.shrink_to_fit();
                }
            }
            for annotationset in self.annotationsets.iter_mut() {
                if let Some(annotationset) = annotationset {
                    annotationset.shrink_to_fit();
                }
            }
        }
        self.annotationsets.shrink_to_fit();
        self.resources.shrink_to_fit();
        self.annotations.shrink_to_fit();
        self.annotation_annotation_map.shrink_to_fit(true);
        self.dataset_annotation_map.shrink_to_fit(true);
        self.resource_annotation_map.shrink_to_fit(true);
        self.dataset_idmap.shrink_to_fit();
        self.annotation_idmap.shrink_to_fit();
        self.resource_idmap.shrink_to_fit();
        self.textrelationmap.shrink_to_fit(true);
    }

    /// This reindexes all elements, effectively performing garbage collection
    /// and freeing any deleted items from memory permanently. You can only run
    /// this on a fully owned AnnotationStore. Many data structures will be reallocated
    /// from scratch so this is a fairly costly operation (and not everywhere as efficient
    /// as it could be). Fortunately, there is often little reason to call this method,
    /// serializing (e.g. to STAM JSON) and deserializing the data is usually preferred.
    ///
    /// WARNING: This operation may invalidate any/all outstanding handles!
    ///          Ensure you reobtain any handles anew after this operation.
    ///
    /// Although Rust's borrowing rules ensure there can be no external references alive
    /// during reindexing, this does not apply to the various handles that this library
    /// publishes.
    pub fn reindex(mut self) -> Self {
        let remap_annotations: Vec<(AnnotationHandle, isize)> = self.annotations.gaps();
        self.annotations = self.annotations.reindex(&remap_annotations);
        let remap_resources: Vec<(TextResourceHandle, isize)> = self.resources.gaps();
        self.resources = self.resources.reindex(&remap_resources);
        let remap_annotationsets: Vec<(AnnotationDataSetHandle, isize)> =
            self.annotationsets.gaps();
        self.annotationsets = self.annotationsets.reindex(&remap_annotationsets);
        if !remap_annotations.is_empty() {
            self.annotation_annotation_map = self
                .annotation_annotation_map
                .reindex(&remap_annotations, &remap_annotations);
            self.annotation_idmap.reindex(&remap_annotations);
        }
        if !remap_resources.is_empty() {
            self.resource_idmap.reindex(&remap_resources);
        }
        if !remap_annotationsets.is_empty() {
            self.dataset_idmap.reindex(&remap_annotationsets);
        }
        if !remap_annotations.is_empty() || !remap_resources.is_empty() {
            self.resource_annotation_map = self
                .resource_annotation_map
                .reindex(&remap_resources, &remap_annotations);
            self.textrelationmap =
                self.textrelationmap
                    .reindex(&remap_resources, &Vec::new(), &remap_annotations);
        }
        if !remap_annotations.is_empty() || !remap_annotationsets.is_empty() {
            self.dataset_annotation_map = self
                .dataset_annotation_map
                .reindex(&remap_annotationsets, &remap_annotations);
        }
        //TODO: TextSelections (inside resources) are currently not reindexed yet
        //TODO: Keys and Annotationdata (inside annotationsets) are currently not reindexed yet
        self
    }

    /// Strip public identifiers from annotations.
    /// This will not affect any internal references but will render any references from external sources impossible.
    pub fn strip_annotation_ids(&mut self) {
        for annotation in self.annotations.iter_mut() {
            if let Some(annotation) = annotation {
                annotation.id = None;
            }
        }
        self.annotation_idmap =
            IdMap::new("A".to_string()).with_resolve_temp_ids(self.config().strip_temp_ids())
    }

    /// Strip public identifiers from annotation data.
    /// This will not affect any internal references but will render any references from external sources impossible.
    pub fn strip_data_ids(&mut self) {
        for annotationset in self.annotationsets.iter_mut() {
            if let Some(annotationset) = annotationset {
                annotationset.strip_data_ids();
            }
        }
    }
}

#[sealed]
impl AssociatedFile for AnnotationStore {
    //Set the associated filename for this annotation store. Also sets the working directory. Builder pattern.
    fn with_filename(mut self, filename: &str) -> Self {
        self.set_filename(filename);
        self
    }

    //Set the associated filename for this annotation store. Also resets the working directory accordingly if needed.
    fn set_filename(&mut self, filename: &str) -> &mut Self {
        debug(self.config(), || {
            format!(
                "AnnotationStore.set_filename: {}, current dataformat={:?}",
                filename,
                self.config().dataformat
            )
        });
        self.filename = Some(filename.into());
        if let Some(mut workdir) = self.filename.clone() {
            workdir.pop();
            if !workdir.to_str().expect("path to string").is_empty() {
                debug(self.config(), || {
                    format!("AnnotationStore.set_filename: workdir={:?}", workdir)
                });
                let workdir = &workdir;
                self.update_config(|config| config.workdir = Some(workdir.clone()));
            }
        }

        if self.filename().unwrap().ends_with(".json") {
            if let DataFormat::Json { .. } = self.config.dataformat {
                //nothing to do
            } else {
                debug(&self.config, || {
                    format!("AnnotationStore.set_filename: Changing dataformat to JSON")
                });
                self.set_dataformat(DataFormat::Json { compact: false })
                    .unwrap_or_default(); //ignores errors!
            }
        }

        #[cfg(feature = "csv")]
        if self.filename().unwrap().ends_with(".csv") && self.config.dataformat != DataFormat::Csv {
            debug(&self.config, || {
                format!("AnnotationStore.set_filename: Changing dataformat to CSV")
            });
            self.set_dataformat(DataFormat::Csv).unwrap_or_default(); //ignores errors!
        }

        if self.filename().unwrap().ends_with(".cbor") && self.config.dataformat != DataFormat::CBOR
        {
            debug(&self.config, || {
                format!("AnnotationStore.set_filename: Changing dataformat to CBOR")
            });
            self.set_dataformat(DataFormat::CBOR).unwrap_or_default(); //ignores errors!
        }

        self
    }

    /// Returns the filename associated with this annotation store
    fn filename(&self) -> Option<&str> {
        self.filename
            .as_ref()
            .map(|x| x.to_str().expect("valid utf-8"))
    }
}

// low-level search API (private)
impl AnnotationStore {
    /// Returns all annotations that reference any text selection in the resource.
    /// This is a low-level method, use [`ResultItem<TextResource>.annotations()`] instead for higher-level access.
    ///
    /// Use [`Self.annotations_by_resource_metadata()`] instead if you are looking for annotations that reference the resource as is
    pub(crate) fn annotations_by_resource<'store>(
        &'store self,
        resource_handle: TextResourceHandle,
    ) -> Option<impl Iterator<Item = AnnotationHandle> + 'store> {
        if let Some(textselection_annotationmap) =
            self.textrelationmap.data.get(resource_handle.as_usize())
        {
            Some(
                textselection_annotationmap
                    .data
                    .iter()
                    .flat_map(|v| v.iter().copied()), //copies only the handles (cheap)
            )
        } else {
            None
        }
    }

    /// This only returns annotations that directly point at the resource, i.e. are metadata for it. It does not include annotations that
    /// This is a low-level method, use [`ResultItem<TextResource>.annotations(_metadata`] instead for higher-level access.
    ///
    /// point at a text in the resource, use [`Self.annotations_by_resource()`] instead for those.
    pub(crate) fn annotations_by_resource_metadata(
        &self,
        resource_handle: TextResourceHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.resource_annotation_map.get(resource_handle)
    }

    /// Find all annotations with a particular textselection. This is a quick lookup in the reverse index and returns a reference to a vector.
    /// This is a low-level method, use [`ResultItem<TextSelection>.annotations()`] instead for higher-level access.
    pub(crate) fn annotations_by_textselection(
        &self,
        resource_handle: TextResourceHandle,
        textselection: &TextSelection,
    ) -> Option<&Vec<AnnotationHandle>> {
        if let Some(handle) = textselection.handle() {
            // existing textselection. Quick lookup in the reverse
            // index. Returns a reference to a vector.
            self.textrelationmap.get(resource_handle, handle)
        } else {
            // we can just cast a TextSelection into an offset and see if it exists as existing textselection
            self.annotations_by_offset(resource_handle, &textselection.into())
        }
    }

    /*
    pub fn annotations_by_textselection_operator(
        &self,
        resource_handle: TextResourceHandle,
        operator: &TextSelectionOperator,
    ) -> Option<impl Iterator<Item = AnnotationHandle>> {
        //TODO: implement
        panic!("annotations_by_textselection_operator() not implemented yet");
    }
    */

    /// Find all annotations with a particular offset (exact). This is a lookup in the reverse index and returns a reference to a vector.
    /// This is  a low-level method.
    pub(crate) fn annotations_by_offset<'a>(
        &'a self,
        resource_handle: TextResourceHandle,
        offset: &Offset,
    ) -> Option<&'a Vec<AnnotationHandle>> {
        if let Some(resource) = self.get(resource_handle).ok() {
            if let Ok(Some(textselection_handle)) = resource.known_textselection(&offset) {
                return self
                    .textrelationmap
                    .get(resource_handle, textselection_handle);
            }
        };
        None
    }

    /// Find all annotations that overlap with a particular offset.
    /*
    pub fn annotations_by_offset_operator(
        &self,
        resource_handle: TextResourceHandle,
        offset: &TextRelationOperator,
    ) -> Option<impl Iterator<Item = AnnotationHandle>> {
        let resource: Option<&TextResource> = self.get(&resource_handle.into()).ok();
        resource?;
        if let Ok(textselection) = resource.unwrap().textselection(&offset.offset()) {
            //TODO: implement
            panic!("annotations_by_offset_overlap() not implemented yet");
        } else {
            None
        }
    }
    */

    /// Find all annotations referenced by the specified annotation (i.e. annotations that point AT the specified annotation). This is a lookup in the reverse index and returns a reference to a vector
    ///
    /// This is a low-level function, use [`ResultItem<Annotation>.annotations()`] instead.
    /// Use [`ResultItem<annotation>.annotations_in_targets()`] if you are looking for the annotations that an annotation points at.
    pub(crate) fn annotations_by_annotation_reverse(
        &self,
        annotation_handle: AnnotationHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.annotation_annotation_map.get(annotation_handle)
    }

    /// Returns all annotations that reference any keys/data in an annotationset
    /// Use [`Self.annotations_by_annotationset_metadata()`] instead if you are looking for annotations that reference the dataset as is
    /// This is a low-level method. Use [`ResultItem<AnnotationDataSet>.annotations()`] instead.
    pub(crate) fn annotations_by_dataset(
        &self,
        dataset_handle: AnnotationDataSetHandle,
    ) -> Option<impl Iterator<Item = AnnotationHandle> + '_> {
        if let Some(data_annotationmap) = self
            .dataset_data_annotation_map
            .data
            .get(dataset_handle.as_usize())
        {
            Some(
                data_annotationmap
                    .data
                    .iter()
                    .flat_map(|v| v.iter().copied()), //copies only the handles (cheap)
            )
        } else {
            None
        }
    }

    /// Find all annotations referenced by the specified annotationset. This is a lookup in the reverse index and returns a reference to a vector.
    /// This only returns annotations that directly point at the dataset, i.e. are metadata for it.
    /// This is a low-level method. Use [`ResultItem<AnnotationDataSet>.annotations_metadata()`] instead.
    pub(crate) fn annotations_by_dataset_metadata(
        &self,
        dataset_handle: AnnotationDataSetHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.dataset_annotation_map.get(dataset_handle)
    }

    /// Find all annotations referenced by data. This is a lookup in the reverse index and returns a reference to it.
    /// This is a low-level method. Use [`ResultItem<AnnotationData>.annotations()`] instead.
    pub(crate) fn annotations_by_data_indexlookup(
        &self,
        dataset_handle: AnnotationDataSetHandle,
        data_handle: AnnotationDataHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.dataset_data_annotation_map
            .get(dataset_handle, data_handle)
    }

    /// Find all annotations referenced by key
    /// This allocates and returns a set because it needs to ensure there are no duplicates
    /// This is a low-level method.
    pub(crate) fn annotations_by_key(
        &self,
        dataset_handle: AnnotationDataSetHandle,
        datakey_handle: DataKeyHandle,
    ) -> BTreeSet<AnnotationHandle> {
        let dataset: Option<&AnnotationDataSet> = self.get(dataset_handle).ok();
        if let Some(dataset) = dataset {
            if let Some(data) = dataset.data_by_key(datakey_handle) {
                data.iter()
                    .filter_map(move |dataitem| {
                        self.annotations_by_data_indexlookup(dataset_handle, *dataitem)
                    })
                    .flat_map(|v| v.iter().copied()) //(only the handles are copied)
                    .collect()
            } else {
                BTreeSet::new()
            }
        } else {
            BTreeSet::new()
        }
    }

    pub(crate) fn annotations_by_data_search(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> BTreeSet<AnnotationHandle> {
        if let Some((test_set_handle, test_key_handle)) = self.find_data_request_resolver(set, key)
        {
        }
    }
}

///////////////////////////////////////////////// Custom deserialisation with serde

#[derive(Debug)]
struct DeserializeAnnotationStore<'a> {
    store: &'a mut AnnotationStore,
}

impl<'a> DeserializeAnnotationStore<'a> {
    pub fn new(store: &'a mut AnnotationStore) -> Self {
        Self { store }
    }
}

/// Top-level seeded deserializer that serializes into the state (the store)
impl<'de> DeserializeSeed<'de> for DeserializeAnnotationStore<'_> {
    // This implementation adds onto the AnnotationStore passed as state, it does not return any data of itself
    type Value = ();

    fn deserialize<D>(mut self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let visitor = AnnotationStoreVisitor {
            store: &mut self.store,
        };
        deserializer.deserialize_map(visitor)?;
        Ok(())
    }
}

struct AnnotationStoreVisitor<'a> {
    store: &'a mut AnnotationStore,
}

impl<'de> serde::de::Visitor<'de> for AnnotationStoreVisitor<'_> {
    // This implementation adds onto the AnnotationStore passed as state, it does not return any data of itself
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a map")
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
                    if tp != "AnnotationStore" {
                        return Err(<A::Error as serde::de::Error>::custom(format!(
                            "Expected type AnnotationStore, got {tp}"
                        )));
                    }
                }
                "annotations" => {
                    // handle the next value in a streaming manner
                    map.next_value_seed(DeserializeAnnotations { store: self.store })?;
                }
                "resources" => {
                    // handle the next value in a streaming manner
                    map.next_value_seed(DeserializeResources { store: self.store })?;
                }
                "annotationsets" => {
                    // handle the next value in a streaming manner
                    map.next_value_seed(DeserializeAnnotationDataSets { store: self.store })?;
                }
                _ => {
                    eprintln!(
                        "Notice: Ignoring unknown key '{key}' whilst parsing AnnotationStore"
                    );
                    map.next_value()?; //read and discard the value
                }
            }
        }

        if self.store.config.shrink_to_fit {
            self.store.shrink_to_fit(true);
        }

        Ok(())
    }
}

struct DeserializeAnnotations<'a> {
    store: &'a mut AnnotationStore,
}

impl<'de> DeserializeSeed<'de> for DeserializeAnnotations<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let visitor = AnnotationsVisitor { store: self.store };
        deserializer.deserialize_seq(visitor)?;
        Ok(())
    }
}

struct AnnotationsVisitor<'a> {
    store: &'a mut AnnotationStore,
}

impl<'de> serde::de::Visitor<'de> for AnnotationsVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a list of annotations")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        loop {
            let annotationbuilder: Option<AnnotationBuilder> = seq.next_element()?;
            if let Some(mut annotationbuilder) = annotationbuilder {
                let handle_from_temp_id = if self.store.config().strip_temp_ids() {
                    if let BuildItem::Id(s) = &annotationbuilder.id {
                        resolve_temp_id(s.as_str())
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(handle) = handle_from_temp_id {
                    //strip the temporary public ID, it maps to a handle directly
                    annotationbuilder.id = BuildItem::None;
                    // temporary public IDs are deserialized exactly
                    // as they were serialized. So if there were any gaps,
                    // we need to deserialize these too:
                    if self.store.annotations_len() > handle {
                        return Err(serde::de::Error::custom("unable to resolve temporary public identifiers, did you start with an empty store? It won't work otherwise."));
                    } else if handle > self.store.annotations_len() {
                        // expand the gaps, though this wastes memory if ensures that all references
                        // are valid without explicitly storing public identifiers.
                        self.store.annotations.resize_with(handle, Default::default);
                    }
                }
                self.store
                    .annotate(annotationbuilder)
                    .map_err(|e| -> A::Error { serde::de::Error::custom(e) })?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

struct DeserializeResources<'a> {
    store: &'a mut AnnotationStore,
}

impl<'de> DeserializeSeed<'de> for DeserializeResources<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let visitor = ResourcesVisitor { store: self.store };
        deserializer.deserialize_seq(visitor)?;
        Ok(())
    }
}

struct ResourcesVisitor<'a> {
    store: &'a mut AnnotationStore,
}

impl<'de> serde::de::Visitor<'de> for ResourcesVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a list of resources")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        loop {
            if let Some(resource) =
                seq.next_element_seed(DeserializeTextResource::new(&self.store.config))?
            {
                self.store
                    .insert(resource)
                    .map_err(|e| -> A::Error { serde::de::Error::custom(e) })?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

struct DeserializeAnnotationDataSets<'a> {
    store: &'a mut AnnotationStore,
}

impl<'de> DeserializeSeed<'de> for DeserializeAnnotationDataSets<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let visitor = AnnotationDataSetsVisitor { store: self.store };
        deserializer.deserialize_seq(visitor)?;
        Ok(())
    }
}

struct AnnotationDataSetsVisitor<'a> {
    store: &'a mut AnnotationStore,
}

impl<'de> serde::de::Visitor<'de> for AnnotationDataSetsVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a list of datasets")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        loop {
            let mut annotationset: AnnotationDataSet =
                AnnotationDataSet::new(self.store.config.clone());
            if seq
                .next_element_seed(DeserializeAnnotationDataSet::new(&mut annotationset))?
                .is_some()
            {
                self.store
                    .insert(annotationset)
                    .map_err(|e| -> A::Error { serde::de::Error::custom(e) })?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

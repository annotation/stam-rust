use sealed::sealed;
use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::slice::Iter;

use crate::annotation::{Annotation, AnnotationBuilder, AnnotationHandle, AnnotationsJson};
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{
    AnnotationDataSet, AnnotationDataSetBuilder, AnnotationDataSetHandle,
};
use crate::config::{get_global_config, set_global_config, Config, Configurable, SerializeMode};
#[cfg(feature = "csv")]
use crate::csv::FromCsv;
use crate::datakey::{DataKey, DataKeyHandle};
use crate::error::*;
use crate::file::*;
use crate::json::{FromJson, ToJson};
use crate::resources::{TextResource, TextResourceBuilder, TextResourceHandle};
use crate::selector::{
    AncestorVec, Offset, Selector, SelectorBuilder, SelectorIter, SelectorIterItem,
};
use crate::store::*;
use crate::textselection::{
    TextRelationOperator, TextSelection, TextSelectionHandle, TextSelectionOperator,
};
use crate::types::*;

/// An Annotation Store is an unordered collection of annotations, resources and
/// annotation data sets. It can be seen as the *root* of the *graph model* and the glue
/// that holds everything together. It is the entry point for any stam model.
#[derive(Deserialize)]
#[serde(try_from = "AnnotationStoreBuilder")]
pub struct AnnotationStore {
    pub(crate) id: Option<String>,

    /// Configuration with interior mutability
    pub(crate) config: Config,

    pub(crate) annotations: Store<Annotation>,
    pub(crate) annotationsets: Store<AnnotationDataSet>,
    pub(crate) resources: Store<TextResource>,

    /// Links to annotations by ID.
    pub(crate) annotation_idmap: IdMap<AnnotationHandle>,
    /// Links to resources by ID.
    pub(crate) resource_idmap: IdMap<TextResourceHandle>,
    /// Links to datasets by ID.
    pub(crate) dataset_idmap: IdMap<AnnotationDataSetHandle>,

    //reverse indices:
    /// Reverse index for AnnotationDataSet => AnnotationData => Annotation. Stores IntIds.
    pub(crate) dataset_data_annotation_map:
        TripleRelationMap<AnnotationDataSetHandle, AnnotationDataHandle, AnnotationHandle>,

    // Note there is no AnnotationDataSet => DataKey => Annotation map, that relationship
    // can be rsolved by the AnnotationDataSet::key_data_map in combination with the above dataset_data_annotation_map
    //
    /// This is the reverse index for text, it maps TextResource => TextSelection => Annotation
    pub(crate) textrelationmap:
        TripleRelationMap<TextResourceHandle, TextSelectionHandle, AnnotationHandle>,

    /// Reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`Selector::ResourceSelector`]), i.e. metadata
    pub(crate) resource_annotation_map: RelationMap<TextResourceHandle, AnnotationHandle>,

    /// Reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`Selector::DataSetSelector`]), i.e. metadata
    pub(crate) dataset_annotation_map: RelationMap<AnnotationDataSetHandle, AnnotationHandle>,

    /// Reverse index for annotations that reference other annotations
    pub(crate) annotation_annotation_map: RelationMap<AnnotationHandle, AnnotationHandle>,

    /// path associated with this store
    pub(crate) filename: Option<PathBuf>,

    #[cfg(feature = "csv")]
    /// path associated with the stand-off files holding annotations (only used for STAM CSV)
    pub(crate) annotations_filename: Option<PathBuf>,
}

#[derive(Deserialize, Default)]
pub struct AnnotationStoreBuilder<'a> {
    #[serde(rename = "@id")]
    pub(crate) id: Option<String>,

    pub(crate) annotationsets: Vec<AnnotationDataSetBuilder<'a>>,

    pub(crate) annotations: Vec<AnnotationBuilder<'a>>,

    pub(crate) resources: Vec<TextResourceBuilder>,

    #[serde(skip, default = "get_global_config")]
    pub(crate) config: Config,

    #[serde(skip)]
    pub(crate) filename: Option<PathBuf>,

    #[serde(skip)]
    pub(crate) annotations_filename: Option<PathBuf>,
}

impl<'a> TryFrom<AnnotationStoreBuilder<'a>> for AnnotationStore {
    type Error = StamError;

    fn try_from(builder: AnnotationStoreBuilder) -> Result<Self, StamError> {
        debug(&builder.config, || {
            format!("TryFrom<AnnotationStoreBuilder for AnnotationStore>: Creation of AnnotationStore from builder")
        });
        let mut store = Self {
            id: builder.id,
            annotationsets: Vec::with_capacity(builder.annotationsets.len()),
            annotations: Vec::with_capacity(builder.annotations.len()),
            resources: Vec::with_capacity(builder.resources.len()),
            config: builder.config,
            ..Default::default()
        };
        if let Some(filename) = builder.filename {
            //also sets working directory
            store.set_filename(
                filename
                    .as_path()
                    .to_str()
                    .expect("filename must be valid utf-8"),
            );
        }
        debug(&store.config, || {
            format!(
                "TryFrom<AnnotationStoreBuilder for AnnotationStore>: Adding {} annotation sets",
                builder.annotationsets.len()
            )
        });
        for annotationset in builder.annotationsets {
            let mut annotationset: AnnotationDataSet = annotationset.try_into()?;
            if annotationset.filename().is_some() {
                //we have an @include statement to resolve
                let filename = annotationset.filename().unwrap().to_owned();
                let id = annotationset.id().map(|x| x.to_string());
                debug(&store.config, || {
                    format!(
                        "TryFrom<AnnotationStoreBuilder for AnnotationStore>: Resolving @include for AnnotationDataSet: filename={:?} id={:?}",
                        filename,
                        id
                    )
                });
                if id.is_some() {
                    //create and override with the ID we already had
                    annotationset = AnnotationDataSet::from_file(&filename, store.config.clone())?
                        .with_id(id.unwrap());
                } else {
                    annotationset = AnnotationDataSet::from_file(&filename, store.config.clone())?;
                }
            }
            store.insert(annotationset)?;
        }
        debug(&store.config, || {
            format!(
                "TryFrom<AnnotationStoreBuilder for AnnotationStore>: Adding {} resources",
                builder.resources.len()
            )
        });
        for resource in builder.resources {
            let mut resource: TextResource = resource.try_into()?;
            if resource.filename().is_some() {
                //we have an @include statement to resolve
                let filename = resource.filename().unwrap().to_owned();
                let id = resource.id().map(|x| x.to_string());
                debug(&store.config, || {
                    format!(
                        "TryFrom<AnnotationStoreBuilder for AnnotationStore>: Resolving @include for TextResource: filename={:?} id={:?}",
                        filename,
                        id
                    )
                });
                if id.is_some() {
                    //create and override with the ID we already had
                    resource = TextResource::from_file(&filename, store.config.clone())?
                        .with_id(id.unwrap());
                } else {
                    resource = TextResource::from_file(&filename, store.config.clone())?;
                }
            }
            store.insert(resource)?;
        }
        debug(&store.config, || {
            format!(
                "TryFrom<AnnotationStoreBuilder for AnnotationStore>: Adding {} annotations",
                builder.annotations.len()
            )
        });
        for annotation in builder.annotations {
            store.annotate(annotation)?;
        }
        Ok(store)
    }
}

#[sealed]
impl TypeInfo for AnnotationStore {
    fn typeinfo() -> Type {
        Type::AnnotationStore
    }
}

#[sealed]
impl<'a> TypeInfo for AnnotationStoreBuilder<'a> {
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
        if let Some(annotations) = self.resource_annotation_map.data.get(handle.unwrap()) {
            if !annotations.is_empty() {
                return Err(StamError::InUse("TextResource"));
            }
        }
        self.resource_annotation_map.data.remove(handle.unwrap());
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

    fn inserted(&mut self, handle: AnnotationHandle) -> Result<(), StamError> {
        // called after the item is inserted in the store
        // updates the relation map, this is where most of the reverse indexing happens
        // that facilitate search at later stages

        // note: a normal self.get() doesn't cut it here because then all of self will be borrowed for 'a and we have problems with the mutable reference later
        //       now at least the borrow checker knows self.annotations is distinct
        //       the other option would be to dp annotation.clone(), at a slightly higher cost which we don't want here
        let annotation = self
            .annotations
            .get(handle.unwrap())
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
                        multitarget = true;
                    } else {
                        self.annotation_annotation_map.insert(*a_handle, handle);
                    }
                }
            }
            Selector::TextSelector(res_handle, offset) => {
                if self.config.textrelationmap {
                    // note: a normal self.get() doesn't cut it here because of the borrow checker
                    //       now at least the borrow checker knows self.resources is distinct from self and self.annotations
                    let resource: &mut TextResource = self
                        .resources
                        .get_mut(res_handle.unwrap())
                        .unwrap()
                        .as_mut()
                        .unwrap();
                    let textselection = resource.textselection(offset)?;
                    let textselection_handle: TextSelectionHandle =
                        if let Some(textselection_handle) = textselection.handle() {
                            //already exists
                            textselection_handle
                        } else {
                            //new, insert... (it's important never to insert the same one twice!)
                            resource.insert(textselection)?
                        };
                    self.textrelationmap
                        .insert(*res_handle, textselection_handle, handle);
                }
            }
            _ => {
                multitarget = true;
            }
        }

        //intermediate structure to gather text relations that should later be added to the reverse index
        let mut extend_textrelationmap: SmallVec<
            [(TextResourceHandle, TextSelection, AnnotationHandle); 1],
        > = SmallVec::new();

        // if needed, we handle more complex situations where there are multiple targets
        if multitarget {
            if self.config.dataset_annotation_map {
                let target_datasets: Vec<(AnnotationDataSetHandle, AnnotationHandle)> = self
                    .annotationsets_by_annotation(annotation)
                    .map(|targetitem| {
                        (
                            targetitem
                                .handle()
                                .expect("annotationset must have a handle"),
                            handle,
                        )
                    })
                    .collect();
                self.dataset_annotation_map
                    .extend(target_datasets.into_iter());
            }

            if self.config.annotation_annotation_map {
                let target_annotations: Vec<(AnnotationHandle, AnnotationHandle)> = self
                    .annotations_by_annotation(annotation, false, false)
                    .map(|targetitem| {
                        (
                            targetitem.handle().expect("annotation must have a handle"),
                            handle,
                        )
                    })
                    .collect();
                self.annotation_annotation_map
                    .extend(target_annotations.into_iter());
            }

            let target_resources: Vec<(TextResourceHandle, AnnotationHandle)> = self
                .resources_by_annotation(annotation)
                .map(|targetitem| {
                    let res_handle = targetitem.handle().expect("resource must have a handle");
                    if self.config.textrelationmap {
                        //process relative offset (note that this essentially duplicates 'iter_target_textselection` but
                        //it allows us to combine two things in one and save an iteration.
                        match self.textselection(
                            targetitem.selector(),
                            Box::new(targetitem.ancestors().iter().map(|x| x.as_ref())),
                        ) {
                            Ok(textselection) => {
                                extend_textrelationmap.push((res_handle, textselection, handle))
                            }
                            Err(err) => panic!("Error resolving relative text: {}", err), //TODO: panic is too strong here! handle more nicely
                        }
                    }
                    (res_handle, handle)
                })
                .collect();

            if self.config.resource_annotation_map {
                self.resource_annotation_map
                    .extend(target_resources.iter().map(|(x, y)| (*x, *y)).into_iter());
            }

            if self.config.textrelationmap {
                //now we add the gathered textselection to the textrelationmap
                self.textrelationmap.extend(
                    extend_textrelationmap
                        .iter()
                        .map(|(res_handle, textselection, handle)| {
                            let resource: &mut TextResource = self
                                .resources
                                .get_mut(res_handle.unwrap())
                                .unwrap()
                                .as_mut()
                                .unwrap();
                            let textselection_handle: TextSelectionHandle =
                                if let Some(textselection_handle) = textselection.handle() {
                                    //we already have handle, don't insert anew
                                    textselection_handle
                                } else {
                                    match resource.has_textselection(&textselection.into()) {
                                        Ok(Some(found_textselectionhandle)) => {
                                            //we have just been inserted and found a handle
                                            found_textselectionhandle
                                        }
                                        Ok(None) => {
                                            //new, insert... (it's important never to insert the same one twice!)
                                            resource
                                                .insert(*textselection)
                                                .expect("insertion should succeed")
                                        }
                                        Err(err) => {
                                            panic!("Error resolving textselection: {}", err)
                                        }
                                    }
                                };
                            (*res_handle, textselection_handle, *handle)
                        })
                        .into_iter(),
                );
            }
        }

        Ok(())
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    fn preremove(&mut self, handle: AnnotationHandle) -> Result<(), StamError> {
        let annotation: &Annotation = self.get(&handle.into())?;
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
        if let Some(annotations) = self.dataset_annotation_map.data.get(handle.unwrap()) {
            if !annotations.is_empty() {
                return Err(StamError::InUse("AnnotationDataSet"));
            }
        }
        self.dataset_annotation_map.data.remove(handle.unwrap());
        Ok(())
    }
}

//impl<'a> Add<NewAnnotation<'a>,Annotation> for AnnotationStore

impl Default for AnnotationStore {
    fn default() -> Self {
        AnnotationStore {
            id: None,
            annotations: Vec::new(),
            annotationsets: Vec::new(),
            resources: Vec::new(),
            annotation_idmap: IdMap::new("A".to_string()),
            resource_idmap: IdMap::new("R".to_string()),
            dataset_idmap: IdMap::new("S".to_string()),
            dataset_data_annotation_map: TripleRelationMap::new(),
            dataset_annotation_map: RelationMap::new(),
            resource_annotation_map: RelationMap::new(),
            annotation_annotation_map: RelationMap::new(),
            textrelationmap: TripleRelationMap::new(),
            config: get_global_config(),
            filename: None,
            annotations_filename: None,
        }
    }
}

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
                if let Ok(data) = self.parent.wrap(data) {
                    seq.serialize_element(&data)?;
                } else {
                    return Err(serde::ser::Error::custom(
                        "Unable to wrap annotationdata during serialization",
                    ));
                }
            }
        }
        seq.end()
    }
}

impl<'j, 'a> FromJson<'j> for AnnotationStoreBuilder<'a> {
    /// Loads an AnnotationStore from a STAM JSON file
    /// The file must contain a single object which has "@type": "AnnotationStore"
    fn from_json_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!("AnnotationStoreBuilder::from_file: filename={:?}", filename)
        });
        set_global_config(config.clone()); //we need this trick to inject state into serde's deserializer
        let reader = open_file_reader(filename, &config)?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);
        let mut result: Result<AnnotationStoreBuilder, _> =
            serde_path_to_error::deserialize(deserializer);
        if result.is_ok() {
            //keep track of the filename we loaded (its directory is searched for @include files when applicable)
            let builder = result.as_mut().unwrap();
            builder.filename = Some(filename.into());
            builder.config = config;
        }
        result.map_err(|e| {
            StamError::JsonError(e, filename.to_string(), "Reading annotationstore from file")
        })
    }

    /// Loads an AnnotationStore from a STAM JSON string
    /// The string must contain a single object which has "@type": "AnnotationStore"
    fn from_json_str(string: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!("AnnotationStoreBuilder::from_json: string={:?}", string)
        });
        let deserializer = &mut serde_json::Deserializer::from_str(string);
        let mut result: Result<AnnotationStoreBuilder, _> =
            serde_path_to_error::deserialize(deserializer);
        if result.is_ok() {
            let builder = result.as_mut().unwrap();
            builder.config = config;
        }
        result.map_err(|e| {
            StamError::JsonError(e, string.to_string(), "Reading annotationstore from string")
        })
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
        set_global_config(config.clone()); //we need this trick to inject state for serde's deserializer
        self.config = config;
        self.propagate_full_config();
        self
    }
}

impl<'a> AnnotationStoreBuilder<'a> {
    /// Start a new [`AnnotationStoreBuilder`] to build an [`AnnotationStore`]. Chain various `with_*()` methods and call `build()` in the end fo produce the actual [`AnnotationStore`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the public ID for the [`AnnotationStore`] that will be built.
    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// Add an [`AnnotationDataSet`] to the store (in the form of a builder). Can be called multiple times.
    pub fn with_annotationset(mut self, annotationset: AnnotationDataSetBuilder<'a>) -> Self {
        self.annotationsets.push(annotationset);
        self
    }

    /// Adds multiple annotation sets at once, this can only be called once as it will overwrite existing builders.
    pub fn with_annotationsets(
        mut self,
        annotationsets: Vec<AnnotationDataSetBuilder<'a>>,
    ) -> Self {
        self.annotationsets = annotationsets;
        self
    }

    /// Add an [`TextResource`] to the store (in the form of a builder). Can be called multiple times.
    pub fn with_resource(mut self, resource: TextResourceBuilder) -> Self {
        self.resources.push(resource);
        self
    }

    /// Adds multiple resource builders at once, this can only be called once as it will overwrite existing builders.
    pub fn with_resources(mut self, resources: Vec<TextResourceBuilder>) -> Self {
        self.resources = resources;
        self
    }

    /// Adds an annotation builder, can be called multiple times.
    pub fn with_annotation(mut self, annotation: AnnotationBuilder<'a>) -> Self {
        self.annotations.push(annotation);
        self
    }

    /// Adds multiple annotation builders, this can only be called once as it will overwrite existing annotation builders.
    pub fn with_annotations(mut self, annotations: Vec<AnnotationBuilder<'a>>) -> Self {
        self.annotations = annotations;
        self
    }

    /// Set the filename for the [`AnnotationStore`] that will be built. This does not load from file. Use [`AnnotationStore::from_file()`] instead of this builder if that's what you want.
    pub fn with_filename(mut self, filename: &str) -> Self {
        if filename.is_empty() {
            self.filename = None
        } else {
            self.filename = Some(filename.into());
        }
        self
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    ///Builds a new annotation store from [`AnnotationStoreBuilder'].
    pub fn build(self) -> Result<AnnotationStore, StamError> {
        debug(&self.config, || format!("AnnotationStore::from_builder"));
        self.try_into()
    }
}

impl AnnotationStore {
    ///Creates a new empty annotation store with a default configuraton, add the [`AnnotationStore.with_config()`] to provide a custom one
    pub fn new() -> Self {
        AnnotationStore::default()
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
        set_global_config(config.clone());
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

        #[cfg(feature = "csv")]
        if filename.ends_with("csv") || config.dataformat == DataFormat::Csv {
            config.dataformat = DataFormat::Csv;
            return AnnotationStoreBuilder::from_csv_file(filename, config)?.build();
        }

        AnnotationStoreBuilder::from_json_file(filename, config)?.build()
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
        set_global_config(config.clone());
        AnnotationStoreBuilder::from_json_str(string, config)?.build()
    }

    /// Merge another annotation store STAM JSON file into this one
    pub fn with_file(mut self, filename: &str) -> Result<Self, StamError> {
        #[cfg(feature = "csv")]
        if filename.ends_with("csv") || self.config().dataformat == DataFormat::Csv {
            let mut config = self.config.clone();
            config.dataformat = DataFormat::Csv;
            return AnnotationStoreBuilder::from_csv_file(filename, config)?.build();
        }

        let builder = AnnotationStoreBuilder::from_json_file(filename, self.config.clone())?;
        self.merge_from_builder(builder)?;
        Ok(self)
    }

    /// Returns the filename associated with this annotation store for storage of annotations
    /// Only used for STAM CSV, not for STAM JSON.
    pub fn annotations_filename(&self) -> Option<&Path> {
        self.annotations_filename.as_ref().map(|x| x.as_path())
    }

    /// Load a JSON file containing an array of annotations in STAM JSON
    pub fn annotate_from_file(&mut self, filename: &str) -> Result<&mut Self, StamError> {
        let reader = open_file_reader(filename, self.config())?; //TODO!
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
        //first the children
        for resource in self.resources.iter_mut() {
            if let Some(resource) = resource.as_mut() {
                if resource.config().dataformat != dataformat {
                    let mut basename = if let Some(basename) = resource.filename_without_extension()
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
                        annotationset
                            .set_filename(format!("{}.annotationset.stam.json", basename).as_str());
                        annotationset.mark_changed()
                    }

                    #[cfg(feature = "csv")]
                    if dataformat == DataFormat::Csv {
                        annotationset
                            .set_filename(format!("{}.annotationset.stam.csv", basename).as_str());
                        annotationset.mark_changed()
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
        }

        #[cfg(feature = "csv")]
        if let DataFormat::Csv = dataformat {
            self.filename = Some(format!("{}.store.stam.csv", basename).into());
            self.annotations_filename = Some(format!("{}.annotations.stam.csv", basename).into());
        }

        self.update_config(|config| config.dataformat = dataformat);
        Ok(())
    }

    /// Merge another annotation store, represented by a builder, into this one
    /// It's a fairly low-level function which you often don't need directly, use `AnnotationStore.with_file()` instead.
    pub fn merge_from_builder(
        &mut self,
        builder: AnnotationStoreBuilder,
    ) -> Result<&mut Self, StamError> {
        debug(self.config(), || {
            format!("AnnotationStore.merge_from_builder")
        });
        //this is very much like TryFrom<AnnotationStoreBuilder> for AnnotationStore
        for dataset in builder.annotationsets {
            if let Some(dataset_id) = dataset.id.as_ref() {
                if let Ok(basedataset) = <AnnotationStore as StoreFor<AnnotationDataSet>>::get_mut(
                    self,
                    &dataset_id.into(),
                ) {
                    basedataset.merge_from_builder(dataset)?;
                    //done, skip the rest
                    continue;
                }
            }
            //otherwise
            let dataset: AnnotationDataSet = dataset.try_into()?;
            self.insert(dataset)?;
        }
        for resource in builder.resources {
            let resource: TextResource = resource.try_into()?;
            self.insert(resource)?;
        }
        for annotation in builder.annotations {
            self.annotate(annotation)?;
        }
        Ok(self)
    }

    /// Shortcut to write an AnnotationStore to a STAM JSON file, writes to the same file as was loaded.
    /// Returns an error if no filename was associated yet.
    /// Use [`AnnotationStore.to_file`] instead if you want to write elsewhere.
    pub fn save(&self) -> Result<(), StamError> {
        debug(self.config(), || format!("AnnotationStore.save"));
        if self.filename.is_some() {
            match self.config().dataformat {
                DataFormat::Json { .. } => {
                    self.to_json_file(self.filename().unwrap(), self.config()) //may produce 1 or multiple files
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

    /// Propagate the entire configuration to all children, will overwrite customized configurations
    fn propagate_full_config(&mut self) {
        if self.resources_len() > 0 || self.annotationsets_len() > 0 {
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
    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
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

    /// Returns an iterator over all annotations in the store
    pub fn annotations<'a>(&'a self) -> StoreIter<'a, Annotation> {
        self.iter()
    }

    /// Returns an iterator over all resources in the store
    pub fn resources<'a>(&'a self) -> StoreIter<'a, TextResource> {
        self.iter()
    }

    /// Returns an iterator over all annotationsets in the store
    pub fn annotationsets<'a>(&'a self) -> StoreIter<'a, AnnotationDataSet> {
        self.iter()
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
    pub fn annotationsets_len(&self) -> usize {
        self.annotationsets.len()
    }

    /// Returns the resource the annotation points to
    /// Returns a WrongSelectorType error if the annotation does not point at any resource.
    pub fn resource_for<'a>(
        &'a self,
        annotation_handle: AnnotationHandle,
    ) -> Result<&'a TextResource, StamError> {
        let annotation: &Annotation = self.get(&annotation_handle.into())?;
        match annotation.target() {
            Selector::TextSelector(res_id, _) | Selector::ResourceSelector(res_id) => {
                let resource: &TextResource = self.get(&res_id.into())?;
                Ok(resource)
            }
            Selector::AnnotationSelector(a_id, _) => self.resource_for(*a_id),
            _ => Err(StamError::WrongSelectorType("resource_for()")),
        }
    }

    /// Iterates over the resources this annotation points to
    pub fn resources_by_annotation<'a>(
        &'a self,
        annotation: &'a Annotation,
    ) -> TargetIter<'a, TextResource> {
        let selector_iter: SelectorIter<'a> = annotation.target().iter(self, true, true);
        //                                                                         ^ -- we track ancestors because it is needed to resolve relative offsets
        TargetIter {
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterates over the annotations this annotation points to directly
    /// Use [`annotations_by_annotation_reverse'] if you want to find the annotations this resource is pointed by.
    pub fn annotations_by_annotation<'a>(
        &'a self,
        annotation: &'a Annotation,
        recursive: bool,
        track_ancestors: bool,
    ) -> TargetIter<'a, Annotation> {
        let selector_iter: SelectorIter<'a> =
            annotation.target().iter(self, recursive, track_ancestors);
        TargetIter {
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterates over the annotation data sets this annotation points to (only the ones it points to directly using DataSetSelector, i.e. as metadata)
    pub fn annotationsets_by_annotation<'a>(
        &'a self,
        annotation: &'a Annotation,
    ) -> TargetIter<'a, AnnotationDataSet> {
        let selector_iter: SelectorIter<'a> = annotation.target().iter(self, true, false);
        TargetIter {
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterate over all resources with text selections this annotation refers to
    pub fn textselections_by_annotation<'a>(
        &'a self,
        annotation: &'a Annotation,
    ) -> Box<dyn Iterator<Item = (TextResourceHandle, TextSelection)> + 'a> {
        Box::new(self.resources_by_annotation(annotation).map(|targetitem| {
            //process offset relative offset
            let res_handle = targetitem.handle().expect("resource must have a handle");
            match self.textselection(
                targetitem.selector(),
                Box::new(targetitem.ancestors().iter().map(|x| x.as_ref())),
            ) {
                Ok(textselection) => (res_handle, textselection),
                Err(err) => panic!("Error resolving relative text: {}", err), //TODO: panic is too strong here! handle more nicely
            }
        }))
    }

    /// Iterates over all text slices this annotation refers to
    pub fn text_by_annotation<'a>(
        &'a self,
        annotation: &'a Annotation,
    ) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        Box::new(
            self.textselections_by_annotation(annotation)
                .map(|(reshandle, selection)| {
                    let resource: &TextResource =
                        self.get(&reshandle.into()).expect("resource must exist");
                    resource
                        .text_by_textselection(&selection)
                        .expect("selection must be valid")
                }),
        )
    }

    /// Returns all annotations that reference any text selection in the resource.
    /// Use [`Self.annotations_by_resource_metadata()`] instead if you are looking for annotations that reference the resource as is
    pub fn annotations_by_resource(
        &self,
        resource_handle: TextResourceHandle,
    ) -> Option<Box<dyn Iterator<Item = AnnotationHandle> + '_>> {
        if let Some(textselection_annotationmap) =
            self.textrelationmap.data.get(resource_handle.unwrap())
        {
            Some(Box::new(
                textselection_annotationmap
                    .data
                    .iter()
                    .flat_map(|v| v.iter().copied()), //copies only the handles (cheap)
            ))
        } else {
            None
        }
    }

    /// Find all annotations with a particular textselection. This is a lookup in the reverse index and returns a reference to a vector.
    /// This only returns annotations that directly point at the resource, i.e. are metadata for it. It does not include annotations that
    /// point at a text in the resource, use [`Self.annotations_by_resource()`] instead for those.
    pub fn annotations_by_resource_metadata(
        &self,
        resource_handle: TextResourceHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.resource_annotation_map.get(resource_handle)
    }

    /// Find all annotations with a particular textselection. This is a lookup in the reverse index and returns a reference to a vector.
    pub fn annotations_by_textselection(
        &self,
        resource_handle: TextResourceHandle,
        textselection: &TextSelection,
    ) -> Option<&Vec<AnnotationHandle>> {
        if let Some(handle) = textselection.handle() {
            //existing textselection
            self.textrelationmap.get(resource_handle, handle)
        } else {
            //we can just cast a TextSelection into an offset and see if it exists as existing textselection
            self.annotations_by_offset(resource_handle, &textselection.into())
        }
    }

    /// Find all annotations with a particular textselection. This is a lookup in the reverse index and returns a reference to a vector.
    pub fn annotations_by_textselection_handle(
        &self,
        resource_handle: TextResourceHandle,
        textselection_handle: TextSelectionHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.textrelationmap
            .get(resource_handle, textselection_handle)
    }

    pub fn annotations_by_textselection_operator(
        &self,
        resource_handle: TextResourceHandle,
        operator: &TextSelectionOperator,
    ) -> Option<Box<dyn Iterator<Item = AnnotationHandle>>> {
        //TODO: implement
        panic!("annotations_by_textselection_operator() not implemented yet");
    }

    /// Find all annotations with a particular offset (exact). This is a lookup in the reverse index and returns a reference to a vector.
    pub fn annotations_by_offset<'a>(
        &'a self,
        resource_handle: TextResourceHandle,
        offset: &Offset,
    ) -> Option<&'a Vec<AnnotationHandle>> {
        if let Some(resource) = self.resource(&Item::from(resource_handle)) {
            if let Ok(textselection) = resource.textselection(&offset) {
                if let Some(textselection_handle) = textselection.handle() {
                    return self
                        .textrelationmap
                        .get(resource_handle, textselection_handle);
                }
            }
        };
        None
    }

    /// Find all annotations that overlap with a particular offset.
    pub fn annotations_by_offset_operator(
        &self,
        resource_handle: TextResourceHandle,
        offset: &TextRelationOperator,
    ) -> Option<Box<dyn Iterator<Item = AnnotationHandle>>> {
        let resource: Option<&TextResource> = self.get(&resource_handle.into()).ok();
        resource?;
        if let Ok(textselection) = resource.unwrap().textselection(&offset.offset()) {
            //TODO: implement
            panic!("annotations_by_offset_overlap() not implemented yet");
        } else {
            None
        }
    }

    /// Find all annotations referenced by the specified annotation (i.e. annotations that point AT the specified annotation). This is a lookup in the reverse index and returns a reference to a vector
    /// Use [`Self.annotations_by_annotation()`] instead if you are looking for the annotations that an annotation points at.
    pub fn annotations_by_annotation_reverse(
        &self,
        annotation_handle: AnnotationHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.annotation_annotation_map.get(annotation_handle)
    }

    /// Returns all annotations that reference any keys/data in an annotationset
    /// Use [`Self.annotations_by_annotationset_metadata()`] instead if you are looking for annotations that reference the dataset as is
    pub fn annotations_by_annotationset(
        &self,
        annotationset_handle: AnnotationDataSetHandle,
    ) -> Option<Box<dyn Iterator<Item = AnnotationHandle> + '_>> {
        if let Some(data_annotationmap) = self
            .dataset_data_annotation_map
            .data
            .get(annotationset_handle.unwrap())
        {
            Some(Box::new(
                data_annotationmap
                    .data
                    .iter()
                    .flat_map(|v| v.iter().copied()), //copies only the handles (cheap)
            ))
        } else {
            None
        }
    }

    /// Find all annotations referenced by the specified annotationset. This is a lookup in the reverse index and returns a reference to a vector.
    /// This only returns annotations that directly point at the dataset, i.e. are metadata for it.
    pub fn annotations_by_annotationset_metadata(
        &self,
        annotationset_handle: AnnotationDataSetHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.dataset_annotation_map.get(annotationset_handle)
    }

    /// Find all annotations reference by data. This is a lookup in the reverse index and return a reference.
    pub fn annotations_by_data(
        &self,
        annotationset_handle: AnnotationDataSetHandle,
        data_handle: AnnotationDataHandle,
    ) -> Option<&Vec<AnnotationHandle>> {
        self.dataset_data_annotation_map
            .get(annotationset_handle, data_handle)
    }

    /// Find all annotations referenced by key
    pub fn annotations_by_key(
        &self,
        annotationset_handle: AnnotationDataSetHandle,
        datakey_handle: DataKeyHandle,
    ) -> Option<Box<dyn Iterator<Item = AnnotationHandle> + '_>> {
        let dataset: Option<&AnnotationDataSet> = self.get(&annotationset_handle.into()).ok();
        if let Some(dataset) = dataset {
            if let Some(data) = dataset.data_by_key(&datakey_handle.into()) {
                Some(Box::new(
                    data.iter()
                        .filter_map(move |dataitem| {
                            self.annotations_by_data(annotationset_handle, *dataitem)
                        })
                        .flat_map(|v| v.iter().copied()), //(only the handles are copied)
                ))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Retrieve a [`TextSelection`] given a specific TextSelector. Does not work with other more complex selectors, use for instance [`AnnotationStore::textselections_by_annotation`] instead for those.
    ///
    /// If multiple AnnotationSelectors are involved, they can be passed as subselectors
    /// and will further refine the TextSelection, but this is usually not invoked directly but via [`AnnotationStore::textselections_by_annotation`]
    pub fn textselection<'b>(
        &self,
        selector: &Selector,
        subselectors: Box<impl Iterator<Item = &'b Selector>>,
    ) -> Result<TextSelection, StamError> {
        match selector {
            Selector::TextSelector(res_id, offset) => {
                let resource: &TextResource = self.get(&res_id.into())?;
                let mut textselection = resource.textselection(offset)?;
                for selector in subselectors {
                    if let Selector::AnnotationSelector(_a_id, Some(suboffset)) = selector {
                        //each annotation selector selects a subslice of the previous textselection
                        let begin = textselection.absolute_cursor(&suboffset.begin)?;
                        let end = textselection.absolute_cursor(&suboffset.end)?;
                        textselection = TextSelection {
                            intid: None,
                            begin,
                            end,
                        };
                    }
                }
                Ok(textselection)
            }
            _ => Err(StamError::WrongSelectorType(
                "selector for Annotationstore::text_selection() must be a TextSelector",
            )),
        }
    }

    /// Iterate over the data for the specified annotation, returning `(&DataKey, &AnnotationData, &AnnotationDataSet)` tuples
    pub fn data_by_annotation<'a>(&'a self, annotation: &'a Annotation) -> AnnotationDataIter<'a> {
        AnnotationDataIter {
            store: self,
            iter: annotation.data(),
        }
    }

    /// Builds a [`Selector`] based on its [`SelectorBuilder`], this will produce an error if the selected resource does not exist.
    pub fn selector(&mut self, item: SelectorBuilder) -> Result<Selector, StamError> {
        match item {
            SelectorBuilder::ResourceSelector(id) => {
                let resource: &TextResource = self.get(&id.into())?;
                Ok(Selector::ResourceSelector(resource.handle_or_err()?))
            }
            SelectorBuilder::TextSelector(res_id, offset) => {
                let resource: &TextResource = self.get(&res_id.into())?;
                Ok(Selector::TextSelector(resource.handle_or_err()?, offset))
            }
            SelectorBuilder::AnnotationSelector(a_id, offset) => {
                let annotation: &Annotation = self.get(&a_id.into())?;
                Ok(Selector::AnnotationSelector(
                    annotation.handle_or_err()?,
                    offset,
                ))
            }
            SelectorBuilder::DataSetSelector(id) => {
                let resource: &AnnotationDataSet = self.get(&id.into())?;
                Ok(Selector::DataSetSelector(resource.handle_or_err()?))
            }
            SelectorBuilder::MultiSelector(v) => {
                let mut new_v = Vec::with_capacity(v.len());
                for builder in v {
                    if builder.is_complex() {
                        return Err(StamError::WrongSelectorType(
                            "Complex selectors may not be nested",
                        ));
                    }
                    new_v.push(self.selector(builder)?);
                }
                Ok(Selector::MultiSelector(new_v))
            }
            SelectorBuilder::DirectionalSelector(v) => {
                let mut new_v = Vec::with_capacity(v.len());
                for builder in v {
                    if builder.is_complex() {
                        return Err(StamError::WrongSelectorType(
                            "Complex selectors may not be nested",
                        ));
                    }
                    new_v.push(self.selector(builder)?);
                }
                Ok(Selector::DirectionalSelector(new_v))
            }
            SelectorBuilder::CompositeSelector(v) => {
                let mut new_v = Vec::with_capacity(v.len());
                for builder in v {
                    if builder.is_complex() {
                        return Err(StamError::WrongSelectorType(
                            "Complex selectors may not be nested",
                        ));
                    }
                    new_v.push(self.selector(builder)?);
                }
                Ok(Selector::CompositeSelector(new_v))
            }
        }
    }

    /// Shortcut method to get an annotation
    pub fn annotation(&self, annotation: &Item<Annotation>) -> Option<&Annotation> {
        self.get(annotation).ok()
    }

    /// Shortcut method to get an annotationset
    pub fn annotationset(
        &self,
        annotationset: &Item<AnnotationDataSet>,
    ) -> Option<&AnnotationDataSet> {
        self.get(annotationset).ok()
    }

    /// Shortcut method to get a resource
    pub fn resource(&self, resource: &Item<TextResource>) -> Option<&TextResource> {
        self.get(resource).ok()
    }

    /// Shortcut method to get an annotationset (mutable) by any id
    pub fn annotationset_mut(
        &mut self,
        annotationset: &Item<AnnotationDataSet>,
    ) -> Option<&mut AnnotationDataSet> {
        self.get_mut(annotationset).ok()
    }

    /// Shortcut method to get an annotation by any id
    pub fn annotation_mut(&mut self, annotation: &Item<Annotation>) -> Option<&mut Annotation> {
        self.get_mut(annotation).ok()
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
        let mut update_global_config = false;
        if let Some(mut workdir) = self.filename.clone() {
            workdir.pop();
            if !workdir.to_str().expect("path to string").is_empty() {
                debug(self.config(), || {
                    format!("AnnotationStore.set_filename: workdir={:?}", workdir)
                });
                let workdir = &workdir;
                self.update_config(|config| config.workdir = Some(workdir.clone()));
                update_global_config = true;
            }
        }

        //check if the dataformat changed, and update all other files accordingly then (via set_dataformat())
        #[cfg(feature = "csv")]
        match self.config.dataformat {
            //OLD dataformat
            DataFormat::Csv => {
                if self.filename().unwrap().ends_with(".json") {
                    debug(&self.config, || {
                        format!("AnnotationStore.set_filename: Changing dataformat to JSON")
                    });
                    self.set_dataformat(DataFormat::Json { compact: false })
                        .unwrap_or_default(); //ignores errors!
                }
            }
            DataFormat::Json { .. } => {
                if self.filename().unwrap().ends_with(".csv") {
                    debug(&self.config, || {
                        format!("AnnotationStore.set_filename: Changing dataformat to CSV")
                    });
                    self.set_dataformat(DataFormat::Csv).unwrap_or_default(); //ignores errors!
                }
            }
        }
        if update_global_config {
            set_global_config(self.config.clone());
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

pub struct AnnotationDataIter<'a> {
    store: &'a AnnotationStore,
    iter: Iter<'a, (AnnotationDataSetHandle, AnnotationDataHandle)>,
}

impl<'a> Iterator for AnnotationDataIter<'a> {
    type Item = (&'a DataKey, &'a AnnotationData, &'a AnnotationDataSet);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some((annotationset_handle, annotationdata_intid)) => {
                let annotationset: &AnnotationDataSet = self
                    .store
                    .get(&annotationset_handle.into())
                    .expect("Getting dataset for annotation");
                let annotationdata: &AnnotationData = annotationset
                    .get(&annotationdata_intid.into())
                    .expect("Getting annotationdata for annotation");
                let datakey: &DataKey = annotationset
                    .get(&annotationdata.key().into())
                    .expect("Getting datakey for annotation");
                Some((datakey, annotationdata, annotationset))
            }
            None => None,
        }
    }
}

pub struct TargetIter<'a, T> {
    pub(crate) iter: SelectorIter<'a>,
    _phantomdata: PhantomData<T>,
}

impl<'a, T> TargetIter<'a, T> {
    pub fn new(iter: SelectorIter<'a>) -> Self {
        Self {
            iter,
            _phantomdata: PhantomData,
        }
    }
}

pub struct TargetIterItem<'a, T> {
    pub(crate) item: &'a T,
    pub(crate) selectoriteritem: SelectorIterItem<'a>,
}

impl<'a, T> Deref for TargetIterItem<'a, T> {
    type Target = T;
    fn deref(&self) -> &'a T {
        self.item
    }
}

impl<'a, T> TargetIterItem<'a, T> {
    pub fn depth(&self) -> usize {
        self.selectoriteritem.depth()
    }
    pub fn selector<'b>(&'b self) -> &'b Cow<'a, Selector> {
        self.selectoriteritem.selector()
    }
    pub fn ancestors<'b>(&'b self) -> &'b AncestorVec<'a> {
        self.selectoriteritem.ancestors()
    }
    pub fn is_leaf(&self) -> bool {
        self.selectoriteritem.is_leaf()
    }
}

impl<'a> Iterator for TargetIter<'a, TextResource> {
    type Item = TargetIterItem<'a, TextResource>;

    fn next(&mut self) -> Option<Self::Item> {
        let selectoritem = self.iter.next();
        if let Some(selectoritem) = selectoritem {
            match selectoritem.selector().as_ref() {
                Selector::TextSelector(res_id, _) | Selector::ResourceSelector(res_id) => {
                    let resource: &TextResource = self
                        .iter
                        .store
                        .get(&res_id.into())
                        .expect("Resource must exist");
                    Some(TargetIterItem {
                        item: resource,
                        selectoriteritem: selectoritem,
                    })
                }
                _ => self.next(),
            }
        } else {
            None
        }
    }
}

impl<'a> Iterator for TargetIter<'a, AnnotationDataSet> {
    type Item = TargetIterItem<'a, AnnotationDataSet>;

    fn next(&mut self) -> Option<Self::Item> {
        let selectoritem = self.iter.next();
        if let Some(selectoritem) = selectoritem {
            match selectoritem.selector().as_ref() {
                Selector::DataSetSelector(set_id) => {
                    let annotationset: &AnnotationDataSet = self
                        .iter
                        .store
                        .get(&set_id.into())
                        .expect("Dataset must exist");
                    Some(TargetIterItem {
                        item: annotationset,
                        selectoriteritem: selectoritem,
                    })
                }
                _ => self.next(),
            }
        } else {
            None
        }
    }
}

impl<'a> Iterator for TargetIter<'a, Annotation> {
    type Item = TargetIterItem<'a, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {
        let selectoritem = self.iter.next();
        if let Some(selectoritem) = selectoritem {
            match selectoritem.selector().as_ref() {
                Selector::AnnotationSelector(a_id, _) => {
                    let annotation: &Annotation = self
                        .iter
                        .store
                        .get(&a_id.into())
                        .expect("Annotation must exist");
                    Some(TargetIterItem {
                        item: annotation,
                        selectoriteritem: selectoritem,
                    })
                }
                _ => self.next(),
            }
        } else {
            None
        }
    }
}

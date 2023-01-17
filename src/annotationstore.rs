use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::marker::PhantomData;
use std::ops::Deref;

use crate::annotation::{Annotation, AnnotationBuilder, AnnotationHandle};
use crate::annotationdata::AnnotationDataHandle;
use crate::annotationdataset::{
    AnnotationDataSet, AnnotationDataSetBuilder, AnnotationDataSetHandle,
};
use crate::resources::{TextResource, TextResourceBuilder, TextResourceHandle};
use crate::selector::{Offset, Selector, SelectorIter, SelectorIterItem, SelectorKind};
use crate::textselection::{TextRelationMap, TextSelection};

use crate::error::*;
use crate::types::*;

/// An Annotation Store is an unordered collection of annotations, resources and
/// annotation data sets. It can be seen as the *root* of the *graph model* and the glue
/// that holds everything together. It is the entry point for any stam model.
#[serde_as]
#[derive(Deserialize)]
#[serde(try_from = "AnnotationStoreBuilder")]
pub struct AnnotationStore {
    id: Option<String>,
    configuration: Configuration,
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
    dataset_data_annotation_map:
        TripleRelationMap<AnnotationDataSetHandle, AnnotationDataHandle, AnnotationHandle>,

    // Note there is no AnnotationDataSet => DataKey => Annotation map, that relationship
    // can be rsolved by the AnnotationDataSet::key_data_map in combination with the above dataset_data_annotation_map
    /// This is the reverse index for text, it maps TextResource => TextSelection => Annotation
    textrelationmap: TextRelationMap,

    /// Reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`Selector::ResourceSelector`]), i.e. metadata
    resource_annotation_map: RelationMap<TextResourceHandle, AnnotationHandle>,

    /// Reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`Selector::DataSetSelector`]), i.e. metadata
    dataset_annotation_map: RelationMap<AnnotationDataSetHandle, AnnotationHandle>,

    /// Reverse index for annotations that reference other annotations
    annotation_annotation_map: RelationMap<AnnotationHandle, AnnotationHandle>,
}

/// This holds the configuration for the annotationstore
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Configuration {
    /// Enable/disable the reverse index for text, it maps TextResource => TextSelection => Annotation
    pub textrelationmap: bool,
    /// Enable/disable reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`Selector::ResourceSelector`]), i.e. metadata
    pub resource_annotation_map: bool,
    /// Enable/disable reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`Selector::DataSetSelector`]), i.e. metadata
    pub dataset_annotation_map: bool,
    /// Enable/disable index for annotations that reference other annotations
    pub annotation_annotation_map: bool,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            textrelationmap: true,
            resource_annotation_map: true,
            dataset_annotation_map: true,
            annotation_annotation_map: true,
        }
    }
}

impl Configuration {
    pub fn new() -> Self {
        Self::default()
    }
}

#[serde_as]
#[derive(Deserialize)]
pub struct AnnotationStoreBuilder {
    #[serde(rename = "@id")]
    pub id: Option<String>,
    #[serde_as(as = "serde_with::OneOrMany<_>")]
    pub annotationsets: Vec<AnnotationDataSetBuilder>,
    #[serde_as(as = "serde_with::OneOrMany<_>")]
    pub annotations: Vec<AnnotationBuilder>,
    #[serde_as(as = "serde_with::OneOrMany<_>")]
    pub resources: Vec<TextResourceBuilder>,
    #[serde(skip)]
    pub configuration: Configuration,
}

impl TryFrom<AnnotationStoreBuilder> for AnnotationStore {
    type Error = StamError;

    fn try_from(builder: AnnotationStoreBuilder) -> Result<Self, StamError> {
        let mut store = Self {
            id: builder.id,
            annotationsets: Vec::with_capacity(builder.annotationsets.len()),
            annotations: Vec::with_capacity(builder.annotations.len()),
            resources: Vec::with_capacity(builder.resources.len()),
            ..Default::default()
        };
        for dataset in builder.annotationsets {
            let dataset: AnnotationDataSet = dataset.try_into()?;
            store.insert(dataset)?;
        }
        for resource in builder.resources {
            let resource: TextResource = resource.try_into()?;
            store.insert(resource)?;
        }
        for annotation in builder.annotations {
            store.annotate(annotation)?;
        }
        Ok(store)
    }
}

//An AnnotationStore is a StoreFor TextResource
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
    fn introspect_type(&self) -> &'static str {
        "TextResource in AnnotationStore"
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

//An AnnotationStore is a StoreFor Annotation
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
    fn introspect_type(&self) -> &'static str {
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

        for (dataset, data) in annotation.data() {
            self.dataset_data_annotation_map
                .insert(*dataset, *data, handle);
        }

        let mut multitarget = false;
        // first we handle the simple singular targets, and determine if we need to do more
        match annotation.target() {
            Selector::DataSetSelector(dataset_handle) => {
                if self.configuration.dataset_annotation_map {
                    self.dataset_annotation_map.insert(*dataset_handle, handle);
                }
            }
            Selector::ResourceSelector(res_handle) => {
                if self.configuration.resource_annotation_map {
                    self.resource_annotation_map.insert(*res_handle, handle);
                }
            }
            Selector::AnnotationSelector(a_handle, offset) => {
                if self.configuration.annotation_annotation_map {
                    if offset.is_some() {
                        multitarget = true;
                    } else {
                        self.annotation_annotation_map.insert(*a_handle, handle);
                    }
                }
            }
            Selector::TextSelector(res_handle, offset) => {
                if self.configuration.textrelationmap {
                    let resource: &TextResource = self.get(*res_handle)?;
                    let textselection = resource.text_selection(&offset)?;
                    self.textrelationmap
                        .insert(*res_handle, textselection, handle);
                }
            }
            _ => {
                multitarget = true;
            }
        }

        // if needed, we handle more complex situations where there are multiple targets
        if multitarget {
            if self.configuration.dataset_annotation_map {
                let target_datasets: Vec<(AnnotationDataSetHandle, AnnotationHandle)> = self
                    .iter_target_annotationsets(annotation)
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

            if self.configuration.annotation_annotation_map {
                let target_annotations: Vec<(AnnotationHandle, AnnotationHandle)> = self
                    .iter_target_annotations(annotation, false, false)
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

            let mut extend_textrelationmap: Vec<(
                TextResourceHandle,
                TextSelection,
                AnnotationHandle,
            )> = Vec::new();
            let target_resources: Vec<(TextResourceHandle, AnnotationHandle)> = self
                .iter_target_resources(annotation)
                .map(|targetitem| {
                    let res_handle = targetitem.handle().expect("resource must have a handle");
                    if self.configuration.textrelationmap {
                        //process offset relative offset (note that this essentially duplicates 'iter_target_textselection` but
                        //it allows us to combine two things in one and save an iteration.
                        match self
                            .text_selection(targetitem.selector(), Some(targetitem.ancestors()))
                        {
                            Ok(textselection) => {
                                extend_textrelationmap.push((res_handle, textselection, handle))
                            }
                            Err(err) => panic!("Error resolving relative text: {}", err), //TODO: panic is too strong here! handle more nicely
                        }
                    }
                    (res_handle, handle)
                })
                .collect();

            if self.configuration.resource_annotation_map {
                self.resource_annotation_map
                    .extend(target_resources.iter().map(|(x, y)| (*x, *y)).into_iter());
            }

            if self.configuration.textrelationmap {
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
    fn introspect_type(&self) -> &'static str {
        "AnnotationDataSet in AnnotationStore"
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
            textrelationmap: TextRelationMap::new(),
            configuration: Configuration::default(),
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
        let wrappedstore: WrappedStore<Annotation, Self> = self.wrappedstore();
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

impl AnnotationStore {
    ///Creates a new empty annotation store
    pub fn new() -> Self {
        AnnotationStore::default()
    }

    /// Sets a configuration for the annotation store. The configuration determines what
    /// reverse indices are built.
    pub fn with_configuration(&mut self, configuration: Configuration) {
        self.configuration = configuration;
    }

    ///Builds a new annotation store from [`AnnotationStoreBuilder'].
    pub fn from_builder(builder: AnnotationStoreBuilder) -> Result<Self, StamError> {
        let store: Self = builder.try_into()?;
        Ok(store)
    }

    /// Loads an AnnotationStore from a STAM JSON file
    /// The file must contain a single object which has "@type": "AnnotationStore"
    pub fn from_file(filename: &str) -> Result<Self, StamError> {
        let f = File::open(filename)
            .map_err(|e| StamError::IOError(e, "Reading annotationstore from file, open failed"))?;
        let reader = BufReader::new(f);
        let builder: AnnotationStoreBuilder = serde_json::from_reader(reader)
            .map_err(|e| StamError::JsonError(e, "Reading annotationstore from file"))?;
        Self::from_builder(builder)
    }

    /// Writes an AnnotationStore to a STAM JSON file, with appropriate formatting
    pub fn to_file(&self, filename: &str) -> Result<(), StamError> {
        let f = File::create(filename)
            .map_err(|e| StamError::IOError(e, "Writing annotationstore from file, open failed"))?;
        let writer = BufWriter::new(f);
        serde_json::to_writer_pretty(writer, &self).map_err(|e| {
            StamError::SerializationError(format!("Writing annotationstore to file: {}", e))
        })?;
        Ok(())
    }

    /// Writes an AnnotationStore to a STAM JSON file, without any indentation
    pub fn to_file_compact(&self, filename: &str) -> Result<(), StamError> {
        let f = File::create(filename)
            .map_err(|e| StamError::IOError(e, "Writing annotationstore from file, open failed"))?;
        let writer = BufWriter::new(f);
        serde_json::to_writer(writer, &self).map_err(|e| {
            StamError::SerializationError(format!("Writing annotationstore to file: {}", e))
        })?;
        Ok(())
    }

    /// Returns the ID of the annotation store (if any)
    pub fn id(&self) -> Option<&str> {
        self.id.as_ref().map(|x| &**x)
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
        let resource = TextResource::from_file(filename)?;
        self.insert(resource)
    }

    /// Get an annotation handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_annotation_id(&self, id: &str) -> Result<AnnotationHandle, StamError> {
        <AnnotationStore as StoreFor<Annotation>>::resolve_id(&self, id)
    }

    /// Get an annotation dataset handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_dataset_id(&self, id: &str) -> Result<AnnotationDataSetHandle, StamError> {
        <AnnotationStore as StoreFor<AnnotationDataSet>>::resolve_id(&self, id)
    }

    /// Get an annotation dataset handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_resource_id(&self, id: &str) -> Result<TextResourceHandle, StamError> {
        <AnnotationStore as StoreFor<TextResource>>::resolve_id(&self, id)
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

    /// Returns the resource the annotation points to
    /// Returns a WrongSelectorType error if the annotation does not point at any resource.
    pub fn resource_for<'a>(
        &'a self,
        annotation_handle: AnnotationHandle,
    ) -> Result<&'a TextResource, StamError> {
        let annotation: &Annotation = self.get(annotation_handle)?;
        match annotation.target() {
            Selector::TextSelector(res_id, _) | Selector::ResourceSelector(res_id) => {
                let resource: &TextResource = self.get(*res_id)?;
                Ok(resource)
            }
            Selector::AnnotationSelector(a_id, _) => self.resource_for(*a_id),
            _ => Err(StamError::WrongSelectorType("resource_for()")),
        }
    }

    /// Iterates over the resources this annotation points to
    pub fn iter_target_resources<'a>(
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
    pub fn iter_target_annotations<'a>(
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
    pub fn iter_target_annotationsets<'a>(
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
    pub fn iter_target_textselection<'a>(
        &'a self,
        annotation: &'a Annotation,
    ) -> Box<dyn Iterator<Item = (TextResourceHandle, TextSelection)> + 'a> {
        Box::new(self.iter_target_resources(annotation).map(|targetitem| {
            //process offset relative offset
            let res_handle = targetitem.handle().expect("resource must have a handle");
            match self.text_selection(targetitem.selector(), Some(targetitem.ancestors())) {
                Ok(textselection) => (res_handle, textselection),
                Err(err) => panic!("Error resolving relative text: {}", err), //TODO: panic is too strong here! handle more nicely
            }
        }))
    }

    /// Find all annotations with a particular textselection. This is a lookup in the reverse index and returns a reference to a vector.
    /// This only returns annotations that directly point at the resource, i.e. are metadata for it. It does not include annotations that
    /// point at a text in the resource, use [`iter_annotations_by_resource`] instead for those.
    pub fn annotations_by_resource_metadata<'a>(
        &'a self,
        resource_handle: TextResourceHandle,
    ) -> Option<&'a Vec<AnnotationHandle>> {
        self.resource_annotation_map
            .data
            .get(resource_handle.unwrap())
    }

    /// Find all annotations with a particular textselection. This is a lookup in the reverse index and returns a reference to a vector.
    pub fn annotations_by_textselection<'a>(
        &'a self,
        resource_handle: TextResourceHandle,
        textselection: &TextSelection,
    ) -> Option<&'a Vec<AnnotationHandle>> {
        self.textrelationmap
            .get_by_textselection(resource_handle, textselection)
    }

    /// Find all annotations with a particular offset (exact). This is a lookup in the reverse index and returns a reference to a vector.
    pub fn annotations_by_offset<'a>(
        &'a self,
        resource_handle: TextResourceHandle,
        offset: &Offset,
    ) -> Option<&'a Vec<AnnotationHandle>> {
        let resource: Option<&TextResource> = self.get(resource_handle).ok();
        if resource.is_none() {
            return None;
        }
        if let Ok(textselection) = resource.unwrap().text_selection(&offset) {
            self.textrelationmap
                .get_by_textselection(resource_handle, &textselection)
        } else {
            None
        }
    }

    /// Find all annotations referenced by the specified annotation (i.e. annotations that point AT the specified annotation). This is a lookup in the reverse index and returns a reference to a vector
    /// Use [`iter_target_annotation`] instead if you are looking for the annotations that an annotation points at.
    pub fn annotations_by_annotation<'a>(
        &'a self,
        annotation_handle: AnnotationHandle,
    ) -> Option<&'a Vec<AnnotationHandle>> {
        self.annotation_annotation_map
            .data
            .get(annotation_handle.unwrap())
    }

    /// Find all annotations referenced by the specified annotationset. This is a lookup in the reverse index and returns a reference to a vector.
    /// This only returns annotations that directly point at the dataset, i.e. are metadata for it.
    pub fn annotations_by_annotationset_metadata<'a>(
        &'a self,
        annotationset_handle: AnnotationDataSetHandle,
    ) -> Option<&'a Vec<AnnotationHandle>> {
        self.dataset_annotation_map
            .data
            .get(annotationset_handle.unwrap())
    }

    /// Retrieve a [`TextSelection`] given a specific TextSelector. Does not work with other more complex selectors, use [`iter_text_selection`] instead for those.
    ///
    /// If multiple AnnotationSelectors are involved, they can be passed as subselectors
    /// and will further refine the TextSelection, but this is usually not invoked directly but via [`iter_text_selection`]
    pub fn text_selection(
        &self,
        selector: &Selector,
        subselectors: Option<&Vec<&Selector>>,
    ) -> Result<TextSelection, StamError> {
        match selector {
            Selector::TextSelector(res_id, offset) => {
                let resource: &TextResource = self.get(*res_id)?;
                let mut textselection = resource.text_selection(offset)?;
                if let Some(subselectors) = subselectors {
                    for selector in subselectors.iter() {
                        if let Selector::AnnotationSelector(_a_id, Some(offset)) = selector {
                            //each annotation selector selects a subslice of the previous textselection
                            let text = resource.text_of(&textselection);
                            textselection = TextSelection {
                                beginbyte: textselection.beginbyte
                                    + textselection.resolve_cursor(text, &offset.begin)?,
                                endbyte: textselection.beginbyte
                                    + textselection.resolve_cursor(text, &offset.end)?,
                            };
                        }
                    }
                }
                Ok(textselection)
            }
            _ => Err(StamError::WrongSelectorType(
                "selector for Annotationstore::text_selection() must be a TextSelector",
            )),
        }
    }
}

pub struct TargetIter<'a, T>
where
    T: Storable,
{
    iter: SelectorIter<'a>,
    _phantomdata: PhantomData<T>,
}

pub struct TargetIterItem<'a, T> {
    item: &'a T,
    selectoriteritem: SelectorIterItem<'a>,
}

impl<'a, T> Deref for TargetIterItem<'a, T>
where
    T: Storable,
{
    type Target = T;
    fn deref(&self) -> &'a T {
        self.item
    }
}

impl<'a, T> TargetIterItem<'a, T> {
    pub fn depth(&self) -> usize {
        self.selectoriteritem.depth()
    }
    pub fn selector(&self) -> &Selector {
        self.selectoriteritem.deref()
    }
    pub fn ancestors<'b>(&'b self) -> &'b Vec<&'a Selector> {
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
            match &*selectoritem {
                Selector::TextSelector(res_id, _) | Selector::ResourceSelector(res_id) => {
                    let resource: &TextResource =
                        self.iter.store.get(*res_id).expect("Resource must exist");
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
            match &*selectoritem {
                Selector::DataSetSelector(set_id) => {
                    let annotationset: &AnnotationDataSet =
                        self.iter.store.get(*set_id).expect("Dataset must exist");
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
            match &*selectoritem {
                Selector::AnnotationSelector(a_id, _) => {
                    let annotation: &Annotation =
                        self.iter.store.get(*a_id).expect("Annotation must exist");
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

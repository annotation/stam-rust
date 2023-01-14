use std::io::{BufReader,BufWriter};
use std::fs::File;
use serde::{Serialize,Deserialize};
use serde::ser::{Serializer,SerializeStruct,SerializeSeq};
use serde_with::serde_as;

use crate::resources::{TextResource,TextResourceHandle,TextResourceBuilder}; 
use crate::annotation::{Annotation,AnnotationHandle,AnnotationBuilder};
use crate::annotationdataset::{AnnotationDataSet,AnnotationDataSetHandle,AnnotationDataSetBuilder};
use crate::annotationdata::AnnotationDataHandle;
use crate::textselection::TextRelationMap;
use crate::selector::Selector;

use crate::types::*;
use crate::error::*;

/// An Annotation Store is an unordered collection of annotations, resources and
/// annotation data sets. It can be seen as the *root* of the *graph model* and the glue
/// that holds everything together. It is the entry point for any stam model.
#[serde_as]
#[derive(Deserialize)]
#[serde(try_from="AnnotationStoreBuilder")]
pub struct AnnotationStore {
    id: Option<String>,
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
    dataset_data_annotation_map: TripleRelationMap<AnnotationDataSetHandle, AnnotationDataHandle, AnnotationHandle>,


    // Note there is no AnnotationDataSet => DataKey => Annotation map, that relationship
    // can be rsolved by the AnnotationDataSet::key_data_map in combination with the above dataset_data_annotation_map

    /// This is the reverse index for text, it maps TextResource => TextSelection => Annotation
    textrelationmap: TextRelationMap,

    /// Reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`Selector::ResourceSelector`]), i.e. metadata
    resource_annotation_map: RelationMap<TextResourceHandle,AnnotationHandle>,

    /// Reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`Selector::DataSetSelector`]), i.e. metadata
    dataset_annotation_map: RelationMap<AnnotationDataSetHandle,AnnotationHandle>,

    /// Reverse index for annotations that reference other annotations
    annotation_annotation_map: RelationMap<AnnotationHandle,AnnotationHandle>
}

#[serde_as]
#[derive(Deserialize)]
pub struct AnnotationStoreBuilder {
    #[serde(rename="@id")]
    pub id: Option<String>,
    #[serde_as(as = "serde_with::OneOrMany<_>")]
    pub annotationsets: Vec<AnnotationDataSetBuilder>,
    #[serde_as(as = "serde_with::OneOrMany<_>")]
    pub annotations: Vec<AnnotationBuilder>,
    #[serde_as(as = "serde_with::OneOrMany<_>")]
    pub resources: Vec<TextResourceBuilder>
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
    fn preremove(&mut self, handle: TextResourceHandle) -> Result<(),StamError> {
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

    fn inserted(&mut self, handle: AnnotationHandle) {
        // called after the item is inserted in the store
        // update the relation map

        // note: a normal self.get() doesn't cut it here because then all of self will be borrowed for 'a and we have problems with the mutable reference later
        //       now at least the borrow checker knows self.annotations is distinct
        //       the other option would be to dp annotation.clone(), at a slightly higher cost which we don't want here
        let annotation = self.annotations.get(handle.unwrap()).unwrap().as_ref().unwrap();

        for (dataset, data) in annotation.data() {
            self.dataset_data_annotation_map.insert(*dataset,*data,handle);
        }

        match annotation.target() {
            Selector::DataSetSelector(dataset_intid) => {
                self.dataset_annotation_map.insert(*dataset_intid, handle);
            },
            Selector::ResourceSelector(res_intid) => {
                self.resource_annotation_map.insert(*res_intid, handle);
            },
            Selector::AnnotationSelector( a_handle, .. ) => {
                self.annotation_annotation_map.insert(*a_handle, handle);
            },
            _ => {
                //TODO: implement
            }
        }
    }

    /// called before the item is removed from the store
    /// updates the relation maps, no need to call manually
    fn preremove(&mut self, handle: AnnotationHandle) -> Result<(),StamError> {
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
            Selector::AnnotationSelector(annotation_handle,_) => Some(*annotation_handle),
            _ => None,
        };

        if let Some(resource_handle) = resource_handle {
            self.resource_annotation_map.remove(resource_handle, handle);
        }
        if let Some(annotationset_handle) = annotationset_handle {
            self.dataset_annotation_map.remove(annotationset_handle, handle);
        }
        if let Some(annotation_handle) = annotation_handle {
            self.annotation_annotation_map.remove(annotation_handle, handle);
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
    fn preremove(&mut self, handle: AnnotationDataSetHandle) -> Result<(),StamError> {
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
            textrelationmap: TextRelationMap::new()
        }
    }
}

impl Serialize for AnnotationStore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> 
    where S: Serializer {
        let mut state = serializer.serialize_struct("AnnotationStore",2)?;
        state.serialize_field("@type", "AnnotationStore")?;
        if let Some(id) = self.id() {
            state.serialize_field("@id", id)?;
        }
        state.serialize_field("resources", &self.resources)?;
        state.serialize_field("annotationsets", &self.annotationsets)?;
        let wrappedstore: WrappedStore<Annotation,Self> = self.wrappedstore();
        state.serialize_field("annotations", &wrappedstore )?;
        state.end()
    }
}

impl<'a> Serialize for WrappedStore<'a, Annotation,AnnotationStore> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> 
    where S: Serializer {
        let mut seq = serializer.serialize_seq(Some(self.store.len()))?;
        for data in self.store.iter() {
            if let Some(data) = data {
                if let Ok(data) = self.parent.wrap(data) {
                    seq.serialize_element(&data)?;
                } else {
                    return Err(serde::ser::Error::custom("Unable to wrap annotationdata during serialization"));
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

    ///Builds a new annotation store from [`AnnotationStoreBuilder'].
    pub fn from_builder(builder: AnnotationStoreBuilder) -> Result<Self,StamError> {
        let store: Self = builder.try_into()?;
        Ok(store)
    }

    /// Loads an AnnotationStore from a STAM JSON file
    /// The file must contain a single object which has "@type": "AnnotationStore"
    pub fn from_file(filename: &str) -> Result<Self,StamError> {
        let f = File::open(filename).map_err(|e| StamError::IOError(e, "Reading annotationstore from file, open failed"))?;
        let reader = BufReader::new(f);
        let builder: AnnotationStoreBuilder = serde_json::from_reader(reader).map_err(|e| StamError::JsonError(e, "Reading annotationstore from file"))?;
        Self::from_builder(builder)
    }

    /// Writes an AnnotationStore to a STAM JSON file, with appropriate formatting
    pub fn to_file(&self, filename: &str) -> Result<(),StamError> {
        let f = File::create(filename).map_err(|e| StamError::IOError(e, "Writing annotationstore from file, open failed"))?;
        let writer = BufWriter::new(f);
        serde_json::to_writer_pretty(writer, &self).map_err(|e| StamError::SerializationError(format!("Writing annotationstore to file: {}", e)))?;
        Ok(())
    }

    /// Writes an AnnotationStore to a STAM JSON file, without any indentation
    pub fn to_file_compact(&self, filename: &str) -> Result<(),StamError> {
        let f = File::create(filename).map_err(|e| StamError::IOError(e, "Writing annotationstore from file, open failed"))?;
        let writer = BufWriter::new(f);
        serde_json::to_writer(writer, &self).map_err(|e| StamError::SerializationError(format!("Writing annotationstore to file: {}", e)))?;
        Ok(())
    }


    /// Returns the ID of the annotation store (if any)
    pub fn id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }

    /// Sets the ID of the annotation store in a builder pattern
    pub fn with_id(mut self, id: String) ->  Self {
        self.id = Some(id);
        self
    }

    /// Shortcut method that calls add_resource under the hood and returns a reference to it
    pub fn add_resource_from_file(&mut self, filename: &str) -> Result<TextResourceHandle,StamError> {
        let resource = TextResource::from_file(filename)?;
        self.insert(resource)
    }


    /// Get an annotation handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_annotation_id(&self, id: &str) -> Result<AnnotationHandle,StamError> {
        <AnnotationStore as StoreFor<Annotation>>::resolve_id(&self, id)
    }

    /// Get an annotation dataset handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_dataset_id(&self, id: &str) -> Result<AnnotationDataSetHandle,StamError> {
        <AnnotationStore as StoreFor<AnnotationDataSet>>::resolve_id(&self, id)
    }

    /// Get an annotation dataset handle from an ID.
    /// Shortcut wraps arround get_handle()
    pub fn resolve_resource_id(&self, id: &str) -> Result<TextResourceHandle,StamError> {
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
}

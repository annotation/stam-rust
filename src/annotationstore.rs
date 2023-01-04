use crate::resources::TextResource;
use crate::annotation::Annotation;
use crate::annotationdata::AnnotationDataSet;

use crate::types::*;
use crate::error::*;

use std::collections::HashMap;

pub struct AnnotationStore {
    id: Option<String>,
    pub annotations: Vec<Annotation>,
    pub datasets: Vec<AnnotationDataSet>,
    pub resources: Vec<TextResource>,

    /// Links to annotations by ID.
    pub(crate) annotation_idmap: HashMap<String,IntId>,
    /// Links to resources by ID.
    pub(crate) resource_idmap: HashMap<String,IntId>
}

impl HasId for AnnotationStore { 
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
    fn with_id(mut self, id: String) ->  Self {
        self.id = Some(id);
        self
    }
}

//An AnnotationStore is a StoreFor TextResource
impl StoreFor<TextResource> for AnnotationStore {
    /// Get a reference to the entire store for the associated type
    fn get_store(&self) -> &Vec<TextResource> {
        &self.resources
    }
    /// Get a mutable reference to the entire store for the associated type
    fn get_mut_store(&mut self) -> &mut Vec<TextResource> {
        &mut self.resources
    }
    /// Get a reference to the id map for the associated type, mapping global ids to internal ids
    fn get_idmap(&self) -> Option<&HashMap<String,IntId>> {
        Some(&self.resource_idmap)
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    fn get_mut_idmap(&mut self) -> Option<&mut HashMap<String,IntId>> {
        Some(&mut self.resource_idmap)
    }
}

//An AnnotationStore is a StoreFor Annotation
impl StoreFor<Annotation> for AnnotationStore {
    fn get_store(&self) -> &Vec<Annotation> {
        &self.annotations
    }
    fn get_mut_store(&mut self) -> &mut Vec<Annotation> {
        &mut self.annotations
    }
    fn get_idmap(&self) -> Option<&HashMap<String,IntId>> {
        Some(&self.annotation_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut HashMap<String,IntId>> {
        Some(&mut self.annotation_idmap)
    }
}

impl Default for AnnotationStore {
    fn default() -> Self {
        AnnotationStore {
            id: None,
            annotations: Vec::new(),
            datasets: Vec::new(),
            resources: Vec::new(),
            annotation_idmap: HashMap::new(),
            resource_idmap: HashMap::new(),
        }
    }
}

impl AnnotationStore {
    pub fn new() -> Self {
        AnnotationStore::default()
    }


    /// Add an Annotation to the annotation store and returns a reference to it.
    /// If you don't need the reference back, just use add() instead
    pub fn add_annotation(&mut self, annotation: Annotation) -> Result<&Annotation,StamError> {
        match self.add(annotation) {
            Ok(intid) => {
                self.get(intid)
            },
            Err(err) => Err(err)
        }
    }

    /// Get an annotation by global ID, returns None if it does not exist
    pub fn get_annotation(&self, id: &str) -> Option<&Annotation> {
        self.get_by_id(id).ok()
    }

    /// Add a TextResource to the annotation store and returns a reference to it
    /// If you don't need the reference back, just use add() instead
    pub fn add_resource(&mut self, resource: TextResource) -> Result<&TextResource,StamError> {
        match self.add(resource) {
            Ok(intid) => {
                self.get(intid)
            },
            Err(err) => Err(err)
        }
    }

    /// Shortcut method that calls add_resource under the hood and returns a reference to it
    pub fn add_resource_from_file(&mut self, filename: &str) -> Result<&TextResource,StamError> {
        let resource = TextResource::from_file(filename)?;
        self.add_resource(resource)
    }

    /// Get a resource by global Id, returns None if it does not exist
    pub fn get_resource(&self, id: &str) -> Option<&TextResource> {
        self.get_by_id(id).ok()
    }

    pub fn get_dataset(&self, intid: IntId) -> Option<&AnnotationDataSet> {
        self.datasets.get(intid as usize)
    }

    pub fn get_mut_dataset(&mut self, intid: IntId) -> Option<&mut AnnotationDataSet> {
        self.datasets.get_mut(intid as usize)
    }
}
    

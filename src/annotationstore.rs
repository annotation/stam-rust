use crate::resources::TextResource;
use crate::annotation::Annotation;
use crate::annotationdata::AnnotationDataSet;

use crate::types::*;
use crate::error::*;

use std::collections::HashMap;

pub struct AnnotationStore {
    pub id: Option<String>,
    pub annotations: Vec<Annotation>,
    pub datasets: Vec<AnnotationDataSet>,
    pub resources: Vec<TextResource>,

    /// Links to annotations by ID.
    pub(crate) annotation_idmap: HashMap<String,IntId>,
    /// Links to resources by ID.
    pub(crate) resource_idmap: HashMap<String,IntId>
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

impl AnnotationStore {
    /// Add an Annotation to the annotation store.
    pub fn add_annotation(&mut self, annotation: Annotation) -> Result<(),StamError> {
        self.add(annotation)
    }

    /// Add a Resource to the annotation store
    pub fn get_annotation(&self, id: &str) -> Result<&Annotation, StamError> {
        self.get_by_id(id)
    }

    pub fn get_annotation_int(&self, id: IntId) -> Result<&Annotation, StamError> {
        self.get(id)
    }

    /// Add a TextResource to the annotation store.
    pub fn add_resource(&mut self, resource: TextResource) -> Result<(),StamError> {
        self.add(resource)
    }

    /// Shortcut method that calls add_resource under the hood
    pub fn add_resource_from_file(&mut self, filename: &str) -> Result<(),StamError> {
        let resource = TextResource::from_file(filename)?;
        self.add_resource(resource)
    }

    pub fn get_resource(&self, id: &str) -> Result<&TextResource, StamError> {
        self.get_by_id(id)
    }

    pub fn get_resource_int(&self, id: IntId) -> Result<&TextResource, StamError> {
        self.get(id)
    }

}
    

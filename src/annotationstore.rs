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
    pub(crate) annotation_index: HashMap<String,IntId>,
    /// Links to resources by ID.
    pub(crate) resource_index: HashMap<String,IntId>
}


impl AnnotationStore {
    /// Add an Annotation to the annotation store.
    pub fn add_annotation(&mut self, annotation: Annotation) -> Result<(),StamError> {
        
    }

    /// Add a TextResource to the annotation store.
    pub fn add_resource(&mut self, resource: TextResource) -> Result<(),StamError> {
        //get and assign next numeric ID to the resource
        resource.intid = Some(self.resources.len());

        //check if global ID does not already exist
        if let Err(err) = resource.get_resource(&resource.id) {
            return Err(StamError::DuplicateIdError(resource.id));
        }

        //insert a mapping from the global ID to the numeric ID in the map
        let key = resource.id.clone(); //MAYBE TODO: optimise the clone() away
        self.annotation_index.insert(key, resource.intid);

        //add the resource
        self.resources.push(resource);

        Ok(())
    }

    /// Shortcut method that calls add_resource under the hood
    pub fn add_resource_from_file(&mut self, filename: &str) -> Result<(),StamError> {
        let mut resource = TextResource::from_file(filename)?;
        self.add_resource(resource)
    }

    pub fn get_resource(&self, id: &str) -> Result<&TextResource, StamError> {
        if let Some(intid) = self.resource_index.get(id) {
            self.get_resource_int(intid)
        } else {
            Err(StamError::IdError(id))
        }
    }

    pub fn get_resource_int(&self, id: IntId) -> Result<&TextResource, StamError> {
        if let Some(resource) = self.resources.get(id) {
            Ok(resource)
        } else {
            Err(StamError::IntIdError(id))
        }
    }


}
    

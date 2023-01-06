use crate::resources::TextResource;
use crate::annotation::Annotation;
use crate::annotationdata::AnnotationDataSet;
use crate::selector::Selector;

use crate::types::*;
use crate::error::*;

pub struct AnnotationStore {
    id: Option<String>,
    pub annotations: Store<Annotation>,
    pub datasets: Store<AnnotationDataSet>,
    pub resources: Store<TextResource>,

    /// Links to annotations by ID.
    pub(crate) annotation_idmap: IdMap,
    /// Links to resources by ID.
    pub(crate) resource_idmap: IdMap,
    /// Links to datasets by ID.
    pub(crate) dataset_idmap: IdMap,
}


impl MayHaveIntId for AnnotationStore {}
impl MayHaveId for AnnotationStore { 
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
    fn get_store(&self) -> &Store<TextResource> {
        &self.resources
    }
    /// Get a mutable reference to the entire store for the associated type
    fn get_mut_store(&mut self) -> &mut Store<TextResource> {
        &mut self.resources
    }
    /// Get a reference to the id map for the associated type, mapping global ids to internal ids
    fn get_idmap(&self) -> Option<&IdMap> {
        Some(&self.resource_idmap)
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap> {
        Some(&mut self.resource_idmap)
    }
    fn introspect_type(&self) -> &'static str {
        "TextResource in AnnotationStore"
    }
}

//An AnnotationStore is a StoreFor Annotation
impl StoreFor<Annotation> for AnnotationStore {
    fn get_store(&self) -> &Store<Annotation> {
        &self.annotations
    }
    fn get_mut_store(&mut self) -> &mut Store<Annotation> {
        &mut self.annotations
    }
    fn get_idmap(&self) -> Option<&IdMap> {
        Some(&self.annotation_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap> {
        Some(&mut self.annotation_idmap)
    }
    fn introspect_type(&self) -> &'static str {
        "Annotation in AnnotationStore"
    }
}

//An AnnotationStore is a StoreFor AnnotationDataSet
impl StoreFor<AnnotationDataSet> for AnnotationStore {
    fn get_store(&self) -> &Store<AnnotationDataSet> {
        &self.datasets
    }
    fn get_mut_store(&mut self) -> &mut Store<AnnotationDataSet> {
        &mut self.datasets
    }
    fn get_idmap(&self) -> Option<&IdMap> {
        Some(&self.dataset_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap> {
        Some(&mut self.dataset_idmap)
    }
    fn introspect_type(&self) -> &'static str {
        "AnnotationDataSet in AnnotationStore"
    }
}



impl Default for AnnotationStore {
    fn default() -> Self {
        AnnotationStore {
            id: None,
            annotations: Vec::new(),
            datasets: Vec::new(),
            resources: Vec::new(),
            annotation_idmap: IdMap::new("A".to_string()),
            resource_idmap: IdMap::new("R".to_string()),
            dataset_idmap: IdMap::new("S".to_string())
        }
    }
}

impl AnnotationStore {
    pub fn new() -> Self {
        AnnotationStore::default()
    }

    /// Shortcut method that calls add_resource under the hood and returns a reference to it
    pub fn add_resource_from_file(&mut self, filename: &str) -> Result<IntId,StamError> {
        let resource = TextResource::from_file(filename)?;
        self.add(resource)
    }


}
    

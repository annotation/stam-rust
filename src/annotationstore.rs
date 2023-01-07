use crate::resources::TextResource; use crate::annotation::Annotation;
use crate::annotationdata::{AnnotationDataSet,AnnotationData,DataKey};
use crate::selector::{Selector,ApplySelector};

use crate::types::*;
use crate::error::*;

/// An Annotation Store is an unordered collection of annotations, resources and
/// annotation data sets. It can be seen as the *root* of the *graph model* and the glue
/// that holds everything together. It is the entry point for any stam model.
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

    fn set_owner_of(&self, item: &mut AnnotationDataSet) {
        //at this point we need to set our ownership of all items we store
        //as earlier we had no internal id yet.
        item.set_ownership();
    }
}

impl Add<AnnotationDataSet,AnnotationDataSet> for AnnotationStore {
    fn intake(&mut self, item: AnnotationDataSet) -> Result<AnnotationDataSet,StamError> {
        self.bind(item)
    }
}
impl Add<Annotation,Annotation> for AnnotationStore {
    fn intake(&mut self, item: Annotation) -> Result<Annotation,StamError> {
        self.bind(item)
    }
}
impl Add<TextResource,TextResource> for AnnotationStore {
    fn intake(&mut self, item: TextResource) -> Result<TextResource,StamError> {
        self.bind(item)
    }
}

//impl<'a> Add<NewAnnotation<'a>,Annotation> for AnnotationStore 


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
        self.insert(resource)
    }



}
    

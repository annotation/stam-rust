use crate::resources::{TextResource,TextResourcePointer}; 
use crate::annotation::{Annotation,AnnotationPointer};
use crate::annotationdataset::{AnnotationDataSet,AnnotationDataSetPointer};
use crate::annotationdata::AnnotationDataPointer;
use crate::selector::Selector;

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
    pub(crate) annotation_idmap: IdMap<AnnotationPointer>,
    /// Links to resources by ID.
    pub(crate) resource_idmap: IdMap<TextResourcePointer>,
    /// Links to datasets by ID.
    pub(crate) dataset_idmap: IdMap<AnnotationDataSetPointer>,

    //reverse indices:

    /// Reverse index for AnnotationDataSet => AnnotationData => Annotation. Stores IntIds.
    dataset_data_annotation_map: TripleRelationMap<AnnotationDataSetPointer, AnnotationDataPointer, AnnotationPointer>,


    // Note there is no AnnotationDataSet => DataKey => Annotation map, that relationship
    // can be rsolved by the AnnotationDataSet::key_data_map in combination with the above dataset_data_annotation_map

    //TODO
    //resource_text_annotation_map: RelationMap<TextResource,TextSelection,Annotation>,

    /// Reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`Selector::ResourceSelector`]), i.e. metadata
    resource_annotation_map: RelationMap<TextResourcePointer,AnnotationPointer>,

    /// Reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`Selector::DataSetSelector`]), i.e. metadata
    dataset_annotation_map: RelationMap<AnnotationDataSetPointer,AnnotationPointer>,

    /// Reverse index for annotations that reference other annotations
    annotation_annotation_map: RelationMap<AnnotationPointer,AnnotationPointer>
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
    fn get_idmap(&self) -> Option<&IdMap<TextResourcePointer>> {
        Some(&self.resource_idmap)
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap<TextResourcePointer>> {
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
    fn get_idmap(&self) -> Option<&IdMap<AnnotationPointer>> {
        Some(&self.annotation_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap<AnnotationPointer>> {
        Some(&mut self.annotation_idmap)
    }
    fn introspect_type(&self) -> &'static str {
        "Annotation in AnnotationStore"
    }

    fn inserted(&mut self, pointer: AnnotationPointer) {
        // called after the item is inserted in the store
        // update the relation map

        // note: a normal self.get() doesn't cut it here because then all of self will be borrowed for 'a and we have problems with the mutable reference later
        //       now at least the borrow checker knows self.annotations is distinct
        //       the other option would be to dp annotation.clone(), at a slightly higher cost which we don't want here
        let annotation = self.annotations.get(pointer.unwrap()).unwrap().as_ref().unwrap();

        for (dataset, data) in annotation.iter_data() {
            self.dataset_data_annotation_map.insert(*dataset,*data,pointer);
        }

        match annotation.target {
            Selector::DataSetSelector(dataset_intid) => {
                self.dataset_annotation_map.insert(dataset_intid, pointer);
            },
            Selector::ResourceSelector(res_intid) => {
                self.resource_annotation_map.insert(res_intid, pointer);
            },
            Selector::AnnotationSelector( a_pointer, .. ) => {
                self.annotation_annotation_map.insert(a_pointer, pointer);
            },
            _ => {
                //TODO: implement
            }
        }
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
    fn get_idmap(&self) -> Option<&IdMap<AnnotationDataSetPointer>> {
        Some(&self.dataset_idmap)
    }
    fn get_mut_idmap(&mut self) -> Option<&mut IdMap<AnnotationDataSetPointer>> {
        Some(&mut self.dataset_idmap)
    }
    fn introspect_type(&self) -> &'static str {
        "AnnotationDataSet in AnnotationStore"
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
            dataset_idmap: IdMap::new("S".to_string()),
            dataset_data_annotation_map: TripleRelationMap::new(),
            dataset_annotation_map: RelationMap::new(),
            resource_annotation_map: RelationMap::new(),
            annotation_annotation_map: RelationMap::new()
        }
    }
}

impl AnnotationStore {
    pub fn new() -> Self {
        AnnotationStore::default()
    }

    pub fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
    pub fn with_id(mut self, id: String) ->  Self {
        self.id = Some(id);
        self
    }

    /// Shortcut method that calls add_resource under the hood and returns a reference to it
    pub fn add_resource_from_file(&mut self, filename: &str) -> Result<TextResourcePointer,StamError> {
        let resource = TextResource::from_file(filename)?;
        self.insert(resource)
    }

    /*
    /// Shortcut method to get annotations
    pub fn get_annotation(&self, intid: AnnotationPointer) -> Result<&Annotation,StamError> {
        self.annotations.get(intid as usize)
              .ok_or_else(|| StamError::IntIdError(intid, "get_annotation()"))
              .map(|x| x.as_ref().expect("item was deleted").as_ref())
    }
    */
}
    

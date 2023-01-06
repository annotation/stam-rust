use std::borrow::Cow;

use crate::types::*;
use crate::error::*;
use crate::annotationdata::{BuildAnnotationData,AnnotationDataSet,AnnotationData};
use crate::annotationstore::AnnotationStore;
use crate::selector::{Selector,BuildSelector};

/// `Annotation` represents a particular *instance of annotation* and is the central
/// concept of the model. They can be considered the primary nodes of the graph model. The
/// instance of annotation is strictly decoupled from the *data* or key/value of the
/// annotation ([`AnnotationData`]). After all, multiple instances can be annotated
/// with the same label (multiple annotations may share the same annotation data).
/// Moreover, an `Annotation` can have multiple annotation data associated. 
/// The result is that multiple annotations with the exact same content require less storage
/// space, and searching and indexing is facilitated.  
pub struct Annotation {
    /// Public identifier for this annotation
    id: Option<String>,

    /// Reference to the annotation data (may be multiple) that describe(s) this annotation, the first ID refers to an AnnotationDataSet as owned by the AnnotationStore, the second to an AnnotationData instance as owned by that set
    pub data: Vec<(IntId,IntId)>,

    /// Determines selection target
    pub target: Selector,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership. 
    intid: Option<IntId>,
    ///Referers to internal IDs of Annotations (as owned by an AnnotationStore) that reference this Annotation (via an AnnotationSelector)
    referenced_by: Vec<IntId>
}

impl MayHaveId for Annotation {
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
}

impl MayHaveIntId for Annotation {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
}

impl SetIntId for Annotation {
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}

/// This is the build recipe for `Annotation`. It contains references to public IDs that will be resolved
/// when the actual AnnotationData is build. The building is done by passing the `BuildAnnotation` to [`AnnotationDataSet::build()`].
pub struct BuildAnnotation<'a> {
    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    id: Cow<'a,str>,
    existingdata: Vec<(Cow<'a,str>,Cow<'a,str>)>,
    newdata: Vec<(Cow<'a,str>, BuildAnnotationData<'a>)>,
    target: BuildSelector<'a>
}

impl<'a> BuildAnnotation<'a> {
    pub fn new(id: &'a str, target: BuildSelector<'a>) -> Self {
        Self {
            id: Cow::Borrowed(id),
            target,
            existingdata: Vec::new(),
            newdata: Vec::new(),
        }
    }

    pub fn new_owned(id: String, target: BuildSelector<'a>) -> Self {
        Self {
            id: Cow::Owned(id),
            target,
            existingdata: Vec::new(),
            newdata: Vec::new(),
        }
    }

    pub fn with_data(mut self, dataset: &'a str, id: &'a str) -> Self {
        self.existingdata.push((Cow::Borrowed(dataset), Cow::Borrowed(id)));
        self
    }

    pub fn with_new_data(mut self, dataset: &'a str, data: BuildAnnotationData<'a> ) -> Self {
        self.newdata.push((Cow::Borrowed(dataset), data));
        self
    }

}

impl<'a> Build<BuildAnnotation<'a>,Annotation> for AnnotationStore {
    fn build(&mut self, item: BuildAnnotation<'a>) -> Result<Annotation,StamError> {
        let mut data = Vec::with_capacity(item.newdata.len() + item.existingdata.len());

        //gather references to existing AnnotationData
        for (dataset_id, annotationdata_id) in item.existingdata {
            let dataset: &AnnotationDataSet = self.get_by_id(&dataset_id)?;
            let adata: &AnnotationData = dataset.get_by_id(&annotationdata_id)?;
            data.push((dataset.get_intid_or_err()?, adata.get_intid_or_err()?));
        }

        //build new AnnotationData on the fly
        for (dataset_id, buildannotationdata) in item.newdata {
            let dataset: &mut AnnotationDataSet = self.get_mut_by_id(&dataset_id)?;
            let adata: AnnotationData = dataset.build(buildannotationdata)?;
            data.push((dataset.get_intid_or_err()?, adata.get_intid_or_err()?));
        }

        let target: Selector = self.build(item.target)?;

        Ok(Annotation::new(Some(item.id.to_string()), target, data))
    }
}

impl<'a> BuildAndStore<BuildAnnotation<'a>,Annotation> for AnnotationStore {}

impl Annotation {
    /// Create a new unbounded Annotation instance, you will likely want to use BuildAnnotation::new() instead and pass it to AnnotationStore.build()
    pub fn new(id: Option<String>, target: Selector, data: Vec<(IntId,IntId)>) -> Self {
        Annotation {
            id,
            data,
            target,
            intid: None,
            referenced_by: Vec::new(),
        }
    }

}


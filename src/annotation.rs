use crate::types::*;
use crate::selector::Selector;

pub struct Annotation {
    /// Public identifier for this annotation
    id: Option<String>,

    /// Reference to the annotation data (may be multiple) that describe(s) this annotation, the first ID refers to an AnnotationDataSet as owned by the AnnotationStore, the second to an AnnotationData instance as owned by that set
    pub data: Vec<(IntId,IntId)>,
    pub target: Selector,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership. 
    intid: Option<IntId>,
    ///Referers to internal IDs of Annotations (as owned by an AnnotationStore) that reference this Annotation (via an AnnotationSelector)
    referenced_by: Vec<IntId>
}

impl HasId for Annotation {
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
}

impl HasIntId for Annotation {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}

impl Annotation {
    //pub fn iter_data(&self) -> &AnnotationData {
    //}
}

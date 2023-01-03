use crate::types::*;
use crate::annotationdata::*;
use crate::selector::Selector;

pub struct Annotation {
    /// Public identifier for this annotation
    pub id: Option<String>,

    /// Reference to the annotation data (may be multiple) that describe(s) this annotation, the first ID refers to an AnnotationDataSet as owned by the AnnotationStore, the second to an AnnotationData instance as owned by that set
    pub data: Vec<(IntId,IntId)>,
    pub target: Selector,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership 
    pub(crate) intid: IntId,
    ///Referers to internal IDs of Annotations (as owned by an AnnotationStore) that reference this Annotation (via an AnnotationSelector)
    pub(crate) referenced_by: Vec<IntId>
}

impl Annotation {
    pub fn iter_data(&self) -> &AnnotationData {
    }
}

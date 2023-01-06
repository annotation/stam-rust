use crate::types::*;
use crate::error::*;
use crate::resources::TextResource;
use crate::annotationstore::AnnotationStore;

pub struct Offset {
    pub begin: Cursor,
    pub end: Cursor
}


/// A `Selector` identifies the target of an annotation and the part of the
/// target that the annotation applies to. Selectors can be considered the labelled edges of the graph model, tying all nodes together.
/// There are multiple types of selectors, all captured in this enum.
///
/// You usually do not instantiate these directly but via [`BuildSelector`].
pub enum Selector {
    /// Refers to a [`TextResource`] as a whole (as opposed to a text fragment inside it), as owned by an AnnotationStore.
    /// Annotations using this selector can be considered metadata of a text
    ResourceSelector(IntId),
    /// Refers to an Annotation (as owned by the AnnotationStore) and optionally a *relative* text selection offset in it
    AnnotationSelector { annotation: IntId, offset: Option<Offset> },
    /// Refers to the TextResource (as owned by the AnnotationStore) an an offset in it
    TextSelector { resource: IntId, offset: Offset },
    /// Refers to an [`AnnotationDataSet`] as owned by an AnnotationStore
    /// Annotations using this selector can be considered metadata.
    DataSetSelector(IntId),
    /// Combines selectors
    MultiSelector(Vec<Selector>),
    /// Combines selectors and expresseds a direction between two or more selectors in the exact order specified (from -> to)
    DirectionalSelector(Vec<Selector>)
}

// Selectors don't carry IDs so we implement the defaults that all return None  
// (MAYBE TODO: see if we can get rid of this alltogether)
impl MayHaveIntId for Selector {}
impl MayHaveId for Selector {}

pub enum BuildSelector<'a> {
    ResourceSelector(&'a str),
    AnnotationSelector { annotation: &'a str, begin: i64, end: i64 },
    TextSelector { resource: &'a str, begin: i64, end: i64 },
    DataSetSelector(&'a str),
    MultiSelector(Vec<BuildSelector<'a>>),
    DirectionalSelector(Vec<BuildSelector<'a>>)
}


impl<'a> Build<BuildSelector<'a>,Selector> for AnnotationStore {
    /// Builds a [`Selector`] based on its [`BuildSelector`] recipe
    fn build(&mut self, item: BuildSelector<'a>) -> Result<Selector,StamError> {
        match item {
            BuildSelector::ResourceSelector(res_id) => {
                let resource: &TextResource = self.get_by_id(res_id)?;
                Ok(Selector::ResourceSelector(resource.get_intid_or_err()?))
            },
            _ => {
                panic!("not implemented yet");
            }
        }
    }
}

use crate::types::*;
use crate::error::*;
use crate::resources::TextResource;
use crate::annotationstore::AnnotationStore;

pub struct Offset {
    pub begin: Cursor,
    pub end: Cursor
}

pub enum Selector {
    /// Refers to a Text Resource as a whole, as owned by an AnnotationStore
    ResourceSelector(IntId),
    /// Refers to an Annotation (as owned by the AnnotationStore) and optionall a *relative* offset in it
    AnnotationSelector { annotation: IntId, offset: Option<Offset> },
    /// Refers to the TextResource (as owned by the AnnotationStore) an an offset in it
    TextSelector { resource: IntId, offset: Offset },
    /// Refers to a DataSet as owned by an AnnotationStore
    DataSetSelector(IntId),
    /// Combines selectors
    MultiSelector(Vec<Selector>),
    /// Combines selectors and expresseds a direction between two or more selectors in the exact order specified (from -> to)
    DirectionalSelector(Vec<Selector>)
}

//We don't carry IDs so we implement the defaults only that all return None
impl HasIntId for Selector {}
impl HasId for Selector {}

pub enum BuildSelector<'a> {
    ResourceSelector(&'a str),
    AnnotationSelector { annotation: &'a str, begin: i64, end: i64 },
    TextSelector { resource: &'a str, begin: i64, end: i64 },
    DataSetSelector(&'a str),
    MultiSelector(Vec<BuildSelector<'a>>),
    DirectionalSelector(Vec<BuildSelector<'a>>)
}


impl<'a> Build<BuildSelector<'a>,Selector> for AnnotationStore {
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

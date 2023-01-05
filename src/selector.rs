use crate::types::*;

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

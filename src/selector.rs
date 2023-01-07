use crate::types::*;
use crate::error::*;
use crate::resources::TextResource;
use crate::annotation::Annotation;
use crate::annotationdata::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;

/// Text selection offset. Specifies begin and end offsets to select a range of a text, via two [`Cursor`] instances.
/// The end-point is non-inclusive.
#[derive(Debug)]
pub struct Offset {
    pub begin: Cursor,
    pub end: Cursor
}


impl Offset {
    pub fn new(begin: Cursor, end: Cursor) -> Self {
        Offset {
            begin,
            end
        }
    }


    /// Shortcut constructor to create a simple begin-aligned offset
    pub fn simple(begin: usize, end: usize) -> Self {
        Offset {
            begin: Cursor::BeginAligned(begin),
            end: Cursor::BeginAligned(end)
        }
    }
}

impl Default for Offset {
    /// The default constructor selects the text as a whole
    fn default() -> Self {
        Offset {
            begin: Cursor::BeginAligned(0),
            end: Cursor::EndAligned(0),
        }
    }
}


/// A `Selector` identifies the target of an annotation and the part of the
/// target that the annotation applies to. Selectors can be considered the labelled edges of the graph model, tying all nodes together.
/// There are multiple types of selectors, all captured in this enum.
///
/// You usually do not instantiate these directly but via [`BuildSelector`].
#[derive(Debug)]
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

pub enum NewSelector<'a> {
    ResourceSelector(&'a str),
    AnnotationSelector { annotation: &'a str, offset: Option<Offset> },
    TextSelector { resource: &'a str, offset: Offset },
    DataSetSelector(&'a str),
    MultiSelector(Vec<NewSelector<'a>>),
    DirectionalSelector(Vec<NewSelector<'a>>)
}


impl<'a> AnnotationStore {
    /// Builds a [`Selector`] based on its [`NewSelector`] recipe
    pub fn selector(&mut self, item: NewSelector<'a>) -> Result<Selector,StamError> {
        match item {
            NewSelector::ResourceSelector(res_id) => {
                let resource: &TextResource = self.get_by_id(res_id)?;
                Ok(Selector::ResourceSelector(resource.get_intid_or_err()?))
            },
            NewSelector::TextSelector { resource: res_id, offset } => {
                let resource: &TextResource = self.get_by_id(res_id)?;
                Ok(Selector::TextSelector { resource: resource.get_intid_or_err()?, offset } )
            },
            NewSelector::AnnotationSelector { annotation: a_id, offset } => {
                let annotation: &Annotation = self.get_by_id(a_id)?;
                Ok(Selector::AnnotationSelector { annotation: annotation.get_intid_or_err()?, offset } )
            },
            _ => {
                panic!("not implemented yet");
            }
        }
    }
}

/// This trait is implemented by types that can return a Selector to themselves
pub trait SelfSelector {
    /// Returns a selector that points to this resouce
    fn self_selector(&self) -> Result<Selector,StamError>;
}

impl SelfSelector for TextResource {
    /// Returns a selector to this resource
    fn self_selector(&self) -> Result<Selector,StamError> {
        if let Some(intid) = self.get_intid() {
            Ok(Selector::ResourceSelector(intid))
        } else {
            Err(StamError::Unbound(Some(format!("new_selector failed"))))
        }
    }
}

impl SelfSelector for AnnotationDataSet {
    /// Returns a selector to this resource
    fn self_selector(&self) -> Result<Selector,StamError> {
        if let Some(intid) = self.get_intid() {
            Ok(Selector::DataSetSelector(intid))
        } else {
            Err(StamError::Unbound(Some(format!("new_selector failed"))))
        }
    }
}

impl SelfSelector for Annotation {
    /// Returns a selector to this resource
    fn self_selector(&self) -> Result<Selector,StamError> {
        if let Some(intid) = self.get_intid() {
            Ok(Selector::AnnotationSelector { annotation: intid, offset: Some(Offset::default()) })
        } else {
            Err(StamError::Unbound(Some(format!("new_selector failed"))))
        }
    }
}

/// This trait is implemented by types to which a selector can be applied, returning as a result a reference to type T 
pub trait ApplySelector<T: ?Sized> {
    /// Apply a selector
    /// Raises a [`StamError::WrongSelectorType`] if the selector of the passed type does not apply to this resource
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a T,StamError>;
}

impl ApplySelector<TextResource> for AnnotationStore {
    /// Retrieve a reference to the resource ([`TextResource`]) the selector points to.
    /// Raises a [`StamError::WrongSelectorType`] if the selector does not point to a resource.
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a TextResource,StamError> {
        match selector {
            Selector::ResourceSelector(int_id) | Selector::TextSelector { resource: int_id, .. } => {
                let resource: &TextResource = self.get(*int_id)?;
                Ok(resource)
            },
            _ => {
                Err(StamError::WrongSelectorType(Some(format!("Selector of type {:?} has no get_resource()", selector))))
            }
        }
    }
}

impl ApplySelector<str> for TextResource {
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a str,StamError> {
        match selector {
            Selector::TextSelector { resource: int_id, offset } => {
                if self.get_intid() != Some(*int_id) {
                    Err(StamError::WrongSelectorTarget(Some(format!("Can not apply selector {:?} on a target it does not reference", selector))))
                } else {
                    Ok(self.get_text_slice(offset)?)
                }
            },
            _ => {
                Err(StamError::WrongSelectorType(Some(format!("Selector of type {:?} has no get_resource()", selector))))
            }
        }
    }
}

impl ApplySelector<AnnotationDataSet> for AnnotationStore {
    /// Retrieve a reference to the annotation data set ([`AnnotationDataSet`]) the selector points to.
    /// Raises a [`StamError::WrongSelectorType`] if the selector does not point to a resource.
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a AnnotationDataSet,StamError> {
        match selector {
            Selector::DataSetSelector(int_id) => {
                let dataset: &AnnotationDataSet = self.get(*int_id)?;
                Ok(dataset)
            },
            _ => {
                Err(StamError::WrongSelectorType(Some(format!("Selector of type {:?} has no get_resource()", selector))))
            }
        }
    }
}

impl ApplySelector<Annotation> for AnnotationStore {
    /// Retrieve a reference to the annotation ([`Annotation`]) the selector points to.
    /// Raises a [`StamError::WrongSelectorType`] if the selector does not point to a resource.
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a Annotation,StamError> {
        match selector {
            Selector::AnnotationSelector { annotation: int_id, .. } => {
                let annotation: &Annotation = self.get(*int_id)?;
                Ok(annotation)
            },
            _ => {
                Err(StamError::WrongSelectorType(Some(format!("Selector of type {:?} has no get_resource()", selector))))
            }
        }
    }
}

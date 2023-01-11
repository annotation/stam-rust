use serde::Deserialize;

use crate::types::*;
use crate::error::*;
use crate::resources::{TextResource,TextResourcePointer};
use crate::annotation::{Annotation,AnnotationPointer};
use crate::annotationdataset::{AnnotationDataSet,AnnotationDataSetPointer};
use crate::annotationstore::AnnotationStore;

/// Text selection offset. Specifies begin and end offsets to select a range of a text, via two [`Cursor`] instances.
/// The end-point is non-inclusive.
#[derive(Debug,Clone,Deserialize,PartialEq)]
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


    /// Shortcut constructor to create a simple begin-aligned offset (less boilerplate)
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
/// The selector can be applied to an annotationstore via [`AnnotationStore.select()`] and which will return actual references.
///
/// You usually do not instantiate these directly but via [`SelectorBuilder`].
#[derive(Debug,Clone,PartialEq)]
pub enum Selector {
    /// Refers to a [`TextResource`] as a whole (as opposed to a text fragment inside it), as owned by an AnnotationStore.
    /// Annotations using this selector can be considered metadata of a text
    ResourceSelector(TextResourcePointer),
    /// Refers to an Annotation (as owned by the AnnotationStore) and optionally a *relative* text selection offset in it
    AnnotationSelector(AnnotationPointer,  Option<Offset>),
    /// Refers to the TextResource (as owned by the AnnotationStore) an an offset in it
    TextSelector(TextResourcePointer, Offset),
    /// Refers to an [`AnnotationDataSet`] as owned by an AnnotationStore
    /// Annotations using this selector can be considered metadata.
    DataSetSelector(AnnotationDataSetPointer),
    /// Combines selectors
    MultiSelector(Vec<Selector>),
    /// Combines selectors and expresseds a direction between two or more selectors in the exact order specified (from -> to)
    DirectionalSelector(Vec<Selector>)
}


/// A `SelectorBuilder` is a recipe that, when applied, identifies the target of an annotation and the part of the
/// target that the annotation applies to. They produce a `Selector` and you can do so via [`Annotationstore.selector`].
///
/// A `SelectorBuilder` can refer to anything and is not validated yet, a `Selector` is and should not fail. 
///
/// There are multiple types of selectors, all captured in this enum.
#[derive(Debug,Clone,Deserialize)]
pub enum SelectorBuilder {
    ResourceSelector(AnyId<TextResourcePointer>),
    AnnotationSelector( AnyId<AnnotationPointer>, Option<Offset> ),
    TextSelector( AnyId<TextResourcePointer>, Offset ),
    DataSetSelector(AnyId<AnnotationDataSetPointer>),
    MultiSelector(Vec<SelectorBuilder>),
    DirectionalSelector(Vec<SelectorBuilder>)
}


impl<'a> AnnotationStore {
    /// Builds a [`Selector`] based on its [`SelectorBuilder`], this will produce an error if the selected resource does not exist.
    pub fn selector(&mut self, item: SelectorBuilder) -> Result<Selector,StamError> {
        match item {
            SelectorBuilder::ResourceSelector(id) => {
                let resource: &TextResource = self.get_by_anyid_or_err(&id)?;
                Ok(Selector::ResourceSelector(resource.get_pointer_or_err()?))
            },
            SelectorBuilder::TextSelector(res_id, offset) => {
                let resource: &TextResource = self.get_by_anyid_or_err(&res_id)?;
                Ok(Selector::TextSelector(resource.get_pointer_or_err()?, offset ) )
            },
            SelectorBuilder::AnnotationSelector(a_id, offset) => {
                let annotation: &Annotation = self.get_by_anyid_or_err(&a_id)?;
                Ok(Selector::AnnotationSelector( annotation.get_pointer_or_err()?, offset ) )
            },
            _ => {
                panic!("not implemented yet")
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
        if let Some(intid) = self.get_pointer() {
            Ok(Selector::ResourceSelector(intid))
        } else {
            Err(StamError::Unbound("TextResource::self_selector()"))
        }
    }
}

impl SelfSelector for AnnotationDataSet {
    /// Returns a selector to this resource
    fn self_selector(&self) -> Result<Selector,StamError> {
        if let Some(intid) = self.get_pointer() {
            Ok(Selector::DataSetSelector(intid))
        } else {
            Err(StamError::Unbound("AnnotationDataSet::self_selector()"))
        }
    }
}

impl SelfSelector for Annotation {
    /// Returns a selector to this resource
    fn self_selector(&self) -> Result<Selector,StamError> {
        if let Some(pointer) = self.get_pointer() {
            Ok(Selector::AnnotationSelector(pointer,Some(Offset::default()) ))
        } else {
            Err(StamError::Unbound("Annotation::self_selector()"))
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
            Selector::ResourceSelector(resource_pointer) | Selector::TextSelector(resource_pointer, .. ) => {
                let resource: &TextResource = self.get(*resource_pointer)?;
                Ok(resource)
            },
            _ => {
                Err(StamError::WrongSelectorType("Annotationstore::select() expected a ResourceSelector or TextSelector, got another"))
            }
        }
    }
}

impl ApplySelector<str> for TextResource {
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a str,StamError> {
        match selector {
            Selector::TextSelector(resource_pointer, offset) => {
                if self.get_pointer() != Some(*resource_pointer) {
                    Err(StamError::WrongSelectorTarget("TextResource:select() can not apply selector meant for another TextResource"))
                } else {
                    Ok(self.get_text_slice(offset)?)
                }
            },
            _ => {
                Err(StamError::WrongSelectorType("TextResource::select() expected a TextSelector, got another"))
            }
        }
    }
}

impl ApplySelector<str> for AnnotationStore {
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a str,StamError> {
        match selector {
            Selector::TextSelector { .. } => {
                let resource: &TextResource = self.select(selector)?;
                let text = resource.select(selector)?;
                Ok(text)
            },
            _ => {
                Err(StamError::WrongSelectorType("AnnotationStore::select() expected a TextSelector, got another"))
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
                Err(StamError::WrongSelectorType("AnnotationStore::select() expected a DataSetSelector, got another"))
            }
        }
    }
}

impl ApplySelector<Annotation> for AnnotationStore {
    /// Retrieve a reference to the annotation ([`Annotation`]) the selector points to.
    /// Raises a [`StamError::WrongSelectorType`] if the selector does not point to a resource.
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a Annotation,StamError> {
        match selector {
            Selector::AnnotationSelector(annotation_pointer, .. ) => {
                let annotation: &Annotation = self.get(*annotation_pointer)?;
                Ok(annotation)
            },
            _ => {
                Err(StamError::WrongSelectorType("AnnotationStore::select() expected an AnnotationSelector, got another"))
            }
        }
    }
}

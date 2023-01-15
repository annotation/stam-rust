use std::ops::Deref;
use serde::{Serialize,Deserialize};
use serde_with::serde_as;
use serde::ser::{Serializer, SerializeStruct};

use crate::types::*;
use crate::error::*;
use crate::resources::{TextResource,TextResourceHandle};
use crate::annotation::{Annotation,AnnotationHandle};
use crate::annotationdataset::{AnnotationDataSet,AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;

/// Text selection offset. Specifies begin and end offsets to select a range of a text, via two [`Cursor`] instances.
/// The end-point is non-inclusive.
#[serde_as]
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

impl Serialize for Offset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> 
    where S: Serializer {
        let mut state = serializer.serialize_struct("AnnotationData",2)?;
        state.serialize_field("@type", "Offset")?;
        state.serialize_field("begin", &self.begin)?;
        state.serialize_field("end", &self.end)?;
        state.end()
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
    ResourceSelector(TextResourceHandle),
    /// Refers to an Annotation (as owned by the AnnotationStore) and optionally a *relative* text selection offset in it
    AnnotationSelector(AnnotationHandle,  Option<Offset>),
    /// Refers to the TextResource (as owned by the AnnotationStore) an an offset in it
    TextSelector(TextResourceHandle, Offset),
    /// Refers to an [`AnnotationDataSet`] as owned by an AnnotationStore
    /// Annotations using this selector can be considered metadata.
    DataSetSelector(AnnotationDataSetHandle),
    /// Combines selectors
    MultiSelector(Vec<Selector>),
    /// Combines selectors and expresseds a direction between two or more selectors in the exact order specified (from -> to)
    DirectionalSelector(Vec<Selector>)
}

impl Selector {
    /// Returns a [`SelectorKind`]
    pub fn kind(&self) -> SelectorKind {
        self.into()
    }

}

pub enum SelectorKind {
    ResourceSelector,
    AnnotatinoSelector,
    TextSelector,
    DataSetSelector,
    MultiSelector,
    DirectionalSelector
}

impl From<&Selector> for SelectorKind {
    fn from(selector: &Selector) -> Self {
        match selector {
            Selector::ResourceSelector(_) => Self::ResourceSelector,
            Selector::AnnotationSelector(_,_) => Self::AnnotatinoSelector,
            Selector::TextSelector(_,_) => Self::TextSelector,
            Selector::DataSetSelector(_) => Self::DataSetSelector,
            Selector::MultiSelector(_) => Self::MultiSelector,
            Selector::DirectionalSelector(_) => Self::DirectionalSelector,
        }
    }
}

/// A `SelectorBuilder` is a recipe that, when applied, identifies the target of an annotation and the part of the
/// target that the annotation applies to. They produce a `Selector` and you can do so via [`Annotationstore.selector`].
///
/// A `SelectorBuilder` can refer to anything and is not validated yet, a `Selector` is and should not fail. 
///
/// There are multiple types of selectors, all captured in this enum.
#[derive(Debug,Clone,Deserialize)]
#[serde(from="SelectorJson")]
pub enum SelectorBuilder {
    ResourceSelector(AnyId<TextResourceHandle>),
    AnnotationSelector( AnyId<AnnotationHandle>, Option<Offset> ),
    TextSelector( AnyId<TextResourceHandle>, Offset ),
    DataSetSelector(AnyId<AnnotationDataSetHandle>),
    MultiSelector(Vec<SelectorBuilder>),
    DirectionalSelector(Vec<SelectorBuilder>)
}

/// Helper structure for Json deserialisation, we need named fields for the serde tag macro to work
#[serde_as]
#[derive(Debug,Clone,Deserialize)]
#[serde(tag="@type")]
enum SelectorJson {
    ResourceSelector { resource: AnyId<TextResourceHandle> },
    AnnotationSelector { annotation: AnyId<AnnotationHandle>, offset: Option<Offset> },
    TextSelector { resource: AnyId<TextResourceHandle>, offset: Offset },
    DataSetSelector { dataset: AnyId<AnnotationDataSetHandle> },
    MultiSelector(Vec<SelectorBuilder>),
    DirectionalSelector(Vec<SelectorBuilder>)
}


impl From<SelectorJson> for SelectorBuilder { 
    fn from(helper: SelectorJson) -> Self {
        match helper {
            SelectorJson::ResourceSelector { resource: res } => Self::ResourceSelector(res),
            SelectorJson::TextSelector { resource: res, offset: o } => Self::TextSelector(res, o),
            SelectorJson::AnnotationSelector { annotation: a, offset: o } => Self::AnnotationSelector(a, o),
            SelectorJson::DataSetSelector { dataset: s } => Self::DataSetSelector(s),
            SelectorJson::MultiSelector(v) => Self::MultiSelector(v),
            SelectorJson::DirectionalSelector(v) => Self::DirectionalSelector(v)
        }
    }
}



impl<'a> AnnotationStore {
    /// Builds a [`Selector`] based on its [`SelectorBuilder`], this will produce an error if the selected resource does not exist.
    pub fn selector(&mut self, item: SelectorBuilder) -> Result<Selector,StamError> {
        match item {
            SelectorBuilder::ResourceSelector(id) => {
                let resource: &TextResource = self.get_by_anyid_or_err(&id)?;
                Ok(Selector::ResourceSelector(resource.handle_or_err()?))
            },
            SelectorBuilder::TextSelector(res_id, offset) => {
                let resource: &TextResource = self.get_by_anyid_or_err(&res_id)?;
                Ok(Selector::TextSelector(resource.handle_or_err()?, offset ) )
            },
            SelectorBuilder::AnnotationSelector(a_id, offset) => {
                let annotation: &Annotation = self.get_by_anyid_or_err(&a_id)?;
                Ok(Selector::AnnotationSelector( annotation.handle_or_err()?, offset ) )
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
        if let Some(intid) = self.handle() {
            Ok(Selector::ResourceSelector(intid))
        } else {
            Err(StamError::Unbound("TextResource::self_selector()"))
        }
    }
}

impl SelfSelector for AnnotationDataSet {
    /// Returns a selector to this resource
    fn self_selector(&self) -> Result<Selector,StamError> {
        if let Some(intid) = self.handle() {
            Ok(Selector::DataSetSelector(intid))
        } else {
            Err(StamError::Unbound("AnnotationDataSet::self_selector()"))
        }
    }
}

impl SelfSelector for Annotation {
    /// Returns a selector to this resource
    fn self_selector(&self) -> Result<Selector,StamError> {
        if let Some(handle) = self.handle() {
            Ok(Selector::AnnotationSelector(handle,Some(Offset::default()) ))
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
            Selector::ResourceSelector(resource_handle) | Selector::TextSelector(resource_handle, .. ) => {
                let resource: &TextResource = self.get(*resource_handle)?;
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
            Selector::TextSelector(resource_handle, offset) => {
                if self.handle() != Some(*resource_handle) {
                    Err(StamError::WrongSelectorTarget("TextResource:select() can not apply selector meant for another TextResource"))
                } else {
                    Ok(self.text_slice(offset)?)
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
            Selector::AnnotationSelector(annotation_handle, .. ) => {
                let annotation: &Annotation = self.get(*annotation_handle)?;
                Ok(annotation)
            },
            _ => {
                Err(StamError::WrongSelectorType("AnnotationStore::select() expected an AnnotationSelector, got another"))
            }
        }
    }
}

/// This is a smart pointer that encapsulates both a selector and the annotationstore in which it can be resolved
pub struct WrappedSelector<'a> {
    selector: &'a Selector,
    store: &'a AnnotationStore
}

impl<'a> Deref for WrappedSelector<'a> {
    type Target = Selector;

    fn deref(&self) -> &Self::Target {
        self.selector
    }
}

impl<'a> WrappedSelector<'a> {
    pub(crate) fn new(selector: &'a Selector, store: &'a AnnotationStore) -> Self {
        WrappedSelector {
            selector,
            store
        }
    }

    pub(crate) fn store(&'a self)  -> &'a AnnotationStore {
        self.store
    }
}


impl<'a> Serialize for WrappedSelector<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        match self.selector {
            Selector::ResourceSelector(res_handle) => {
                let textresource: Result<&TextResource,_> = self.store().get(*res_handle); 
                if let Ok(textresource) = textresource {
                    let mut state = serializer.serialize_struct("Selector", 2)?;
                    state.serialize_field("@type","ResourceSelector")?;
                    if textresource.id().is_none() {
                        return Err(serde::ser::Error::custom("Selector target must have an ID"));
                    }
                    state.serialize_field("resource", &textresource.id())?;
                    state.end()
                } else {
                    return Err(serde::ser::Error::custom("Unable to resolve resource for ResourceSelector during serialization"));
                }
            },
            Selector::TextSelector(res_handle, offset) => {
                let textresource: Result<&TextResource,_> = self.store().get(*res_handle); 
                if let Ok(textresource) = textresource {
                    let mut state = serializer.serialize_struct("Selector", 3)?;
                    state.serialize_field("@type","TextSelector")?;
                    if textresource.id().is_none() {
                        return Err(serde::ser::Error::custom("Selector target must have an ID"));
                    }
                    state.serialize_field("resource", &textresource.id())?;
                    state.serialize_field("offset", offset)?;
                    state.end()
                } else {
                    return Err(serde::ser::Error::custom("Unable to resolve resource for ResourceSelector during serialization"));
                }
            },
            _ => panic!("Serialiser for this selector not implemented yet") //TODO
        }
    }
}

/// Iterator that returns all Selectors under a particular selector
pub struct SelectorIter<'a> {
    selector: &'a Selector, //we keep the root item out of subiterstack to save ourselves the Vec<> allocation
    subiterstack: Vec<SelectorIter<'a>>,
    store: &'a AnnotationStore,
    depth: usize
}

impl<'a> Iterator for SelectorIter<'a>  {
    type Item = &'a Selector;

    fn next(&mut self) -> Option<Self::Item> {
        if self.subiterstack.is_empty() {
            match self.selector {
                Selector::TextSelector(res_handle, offset ) => {
                    None
                },
                Selector::ResourceSelector(res_handle)=> {
                    None
                },
                Selector::AnnotationSelector(a_handle, offset) => {
                    if offset.is_none() {
                        None
                    } else {
                        let annotation: &Annotation = self.store.get(*a_handle).expect("referenced annotation must exist");
                        self.subiterstack.push(
                            SelectorIter {
                                selector: annotation.target(),
                                subiterstack: Vec::new(),
                                store: self.store,
                                depth: self.depth + 1,
                            }
                        );
                        self.next() //recursion
                    }
                },
                Selector::MultiSelector(v) | Selector::DirectionalSelector(v) => {
                    for subselector in v.iter() {
                        self.subiterstack.push(
                            SelectorIter {
                                selector: subselector,
                                subiterstack: Vec::new(),
                                store: self.store,
                                depth: self.depth + 1,
                            }
                        );
                    }
                    self.next() //recursion
                },
                _ => None, //TODO: implement others!
            }
        } else {
            let result = self.subiterstack.last_mut().unwrap().next();
            if result.is_none() {
                self.subiterstack.pop();
                if self.subiterstack.is_empty() {
                    None
                } else {
                    self.next() //recursion
                }
            } else {
                result
            }
        }
    }
}


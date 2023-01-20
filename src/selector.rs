use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::ops::Deref;

use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdataset::AnnotationDataSetHandle;
use crate::annotationstore::AnnotationStore;
use crate::error::*;
use crate::resources::{TextResource, TextResourceHandle};
use crate::types::*;

/// Text selection offset. Specifies begin and end offsets to select a range of a text, via two [`Cursor`] instances.
/// The end-point is non-inclusive.
#[serde_as]
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Offset {
    pub begin: Cursor,
    pub end: Cursor,
}

impl Offset {
    pub fn new(begin: Cursor, end: Cursor) -> Self {
        Offset { begin, end }
    }

    /// Shortcut constructor to create a simple begin-aligned offset (less boilerplate)
    pub fn simple(begin: usize, end: usize) -> Self {
        Offset {
            begin: Cursor::BeginAligned(begin),
            end: Cursor::BeginAligned(end),
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
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationData", 2)?;
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
#[derive(Debug, Clone, PartialEq)]
pub enum Selector {
    /// Refers to a [`TextResource`] as a whole (as opposed to a text fragment inside it), as owned by an AnnotationStore.
    /// Annotations using this selector can be considered metadata of a text
    ResourceSelector(TextResourceHandle),
    /// Refers to an Annotation (as owned by the AnnotationStore) and optionally a *relative* text selection offset in it
    AnnotationSelector(AnnotationHandle, Option<Offset>),
    /// Refers to the TextResource (as owned by the AnnotationStore) an an offset in it
    TextSelector(TextResourceHandle, Offset),
    /// Refers to an [`AnnotationDataSet`] as owned by an AnnotationStore
    /// Annotations using this selector can be considered metadata.
    DataSetSelector(AnnotationDataSetHandle),
    /// Combines selectors
    MultiSelector(Vec<Selector>),
    /// Combines selectors and expresseds a direction between two or more selectors in the exact order specified (from -> to)
    DirectionalSelector(Vec<Selector>),
}

impl Selector {
    /// Returns a [`SelectorKind`]
    pub fn kind(&self) -> SelectorKind {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorKind {
    ResourceSelector,
    AnnotationSelector,
    TextSelector,
    DataSetSelector,
    MultiSelector,
    DirectionalSelector,
}

impl From<&Selector> for SelectorKind {
    fn from(selector: &Selector) -> Self {
        match selector {
            Selector::ResourceSelector(_) => Self::ResourceSelector,
            Selector::AnnotationSelector(_, _) => Self::AnnotationSelector,
            Selector::TextSelector(_, _) => Self::TextSelector,
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
#[derive(Debug, Clone, Deserialize)]
#[serde(from = "SelectorJson")]
pub enum SelectorBuilder {
    ResourceSelector(AnyId<TextResourceHandle>),
    AnnotationSelector(AnyId<AnnotationHandle>, Option<Offset>),
    TextSelector(AnyId<TextResourceHandle>, Offset),
    DataSetSelector(AnyId<AnnotationDataSetHandle>),
    MultiSelector(Vec<SelectorBuilder>),
    DirectionalSelector(Vec<SelectorBuilder>),
}

/// Helper structure for Json deserialisation, we need named fields for the serde tag macro to work
#[serde_as]
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "@type")]
enum SelectorJson {
    ResourceSelector {
        resource: AnyId<TextResourceHandle>,
    },
    AnnotationSelector {
        annotation: AnyId<AnnotationHandle>,
        offset: Option<Offset>,
    },
    TextSelector {
        resource: AnyId<TextResourceHandle>,
        offset: Offset,
    },
    DataSetSelector {
        dataset: AnyId<AnnotationDataSetHandle>,
    },
    MultiSelector(Vec<SelectorBuilder>),
    DirectionalSelector(Vec<SelectorBuilder>),
}

impl From<SelectorJson> for SelectorBuilder {
    fn from(helper: SelectorJson) -> Self {
        match helper {
            SelectorJson::ResourceSelector { resource: res } => Self::ResourceSelector(res),
            SelectorJson::TextSelector {
                resource: res,
                offset: o,
            } => Self::TextSelector(res, o),
            SelectorJson::AnnotationSelector {
                annotation: a,
                offset: o,
            } => Self::AnnotationSelector(a, o),
            SelectorJson::DataSetSelector { dataset: s } => Self::DataSetSelector(s),
            SelectorJson::MultiSelector(v) => Self::MultiSelector(v),
            SelectorJson::DirectionalSelector(v) => Self::DirectionalSelector(v),
        }
    }
}

impl<'a> AnnotationStore {
    /// Builds a [`Selector`] based on its [`SelectorBuilder`], this will produce an error if the selected resource does not exist.
    pub fn selector(&mut self, item: SelectorBuilder) -> Result<Selector, StamError> {
        match item {
            SelectorBuilder::ResourceSelector(id) => {
                let resource: &TextResource = self.get_by_anyid_or_err(&id)?;
                Ok(Selector::ResourceSelector(resource.handle_or_err()?))
            }
            SelectorBuilder::TextSelector(res_id, offset) => {
                let resource: &TextResource = self.get_by_anyid_or_err(&res_id)?;
                Ok(Selector::TextSelector(resource.handle_or_err()?, offset))
            }
            SelectorBuilder::AnnotationSelector(a_id, offset) => {
                let annotation: &Annotation = self.get_by_anyid_or_err(&a_id)?;
                Ok(Selector::AnnotationSelector(
                    annotation.handle_or_err()?,
                    offset,
                ))
            }
            _ => {
                panic!("not implemented yet")
            }
        }
    }
}

/// This trait is implemented by types that can return a Selector to themselves
pub trait SelfSelector {
    /// Returns a selector that points to this resouce
    fn self_selector(&self) -> Result<Selector, StamError>;
}

/// This trait is implemented by types to which a selector can be applied, returning as a result a reference to type T
pub trait ApplySelector<T: ?Sized> {
    /// Apply a selector
    /// Raises a [`StamError::WrongSelectorType`] if the selector of the passed type does not apply to this resource
    fn select<'a>(&'a self, selector: &Selector) -> Result<&'a T, StamError>;
}

/// This is a smart pointer that encapsulates both a selector and the annotationstore in which it can be resolved
pub struct WrappedSelector<'a> {
    selector: &'a Selector,
    store: &'a AnnotationStore,
}

impl<'a> Deref for WrappedSelector<'a> {
    type Target = Selector;

    fn deref(&self) -> &Self::Target {
        self.selector
    }
}

impl<'a> WrappedSelector<'a> {
    pub(crate) fn new(selector: &'a Selector, store: &'a AnnotationStore) -> Self {
        WrappedSelector { selector, store }
    }

    pub(crate) fn store(&'a self) -> &'a AnnotationStore {
        self.store
    }
}

impl<'a> Serialize for WrappedSelector<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.selector {
            Selector::ResourceSelector(res_handle) => {
                let textresource: Result<&TextResource, _> = self.store().get(*res_handle);
                if let Ok(textresource) = textresource {
                    let mut state = serializer.serialize_struct("Selector", 2)?;
                    state.serialize_field("@type", "ResourceSelector")?;
                    if textresource.id().is_none() {
                        return Err(serde::ser::Error::custom("Selector target must have an ID"));
                    }
                    state.serialize_field("resource", &textresource.id())?;
                    state.end()
                } else {
                    return Err(serde::ser::Error::custom(
                        "Unable to resolve resource for ResourceSelector during serialization",
                    ));
                }
            }
            Selector::TextSelector(res_handle, offset) => {
                let textresource: Result<&TextResource, _> = self.store().get(*res_handle);
                if let Ok(textresource) = textresource {
                    let mut state = serializer.serialize_struct("Selector", 3)?;
                    state.serialize_field("@type", "TextSelector")?;
                    if textresource.id().is_none() {
                        return Err(serde::ser::Error::custom("Selector target must have an ID"));
                    }
                    state.serialize_field("resource", &textresource.id())?;
                    state.serialize_field("offset", offset)?;
                    state.end()
                } else {
                    return Err(serde::ser::Error::custom(
                        "Unable to resolve resource for ResourceSelector during serialization",
                    ));
                }
            }
            _ => panic!("Serialiser for this selector not implemented yet"), //TODO
        }
    }
}

impl Selector {
    /// Returns an iterator that yields all Selectors under a particular selector, including the selcetor in question as well.
    /// The parameter `recurse_annotation` determines whether an AnnotationSelector will be resolved recursively or not (finding all it points at)
    pub fn iter<'a>(
        &'a self,
        store: &'a AnnotationStore,
        recurse_annotation: bool,
        track_ancestors: bool,
    ) -> SelectorIter<'a> {
        SelectorIter {
            selector: Some(self),
            ancestors: Vec::new(),
            subiterstack: Vec::new(),
            recurse_annotation,
            track_ancestors,
            store,
            depth: 0,
        }
    }
}

/// Iterator that returns the selector itself, plus all selectors under it (recursively)
pub struct SelectorIter<'a> {
    selector: Option<&'a Selector>, //we keep the root item out of subiterstack to save ourselves the Vec<> allocation
    ancestors: Vec<&'a Selector>,
    subiterstack: Vec<SelectorIter<'a>>,
    recurse_annotation: bool,
    track_ancestors: bool,
    pub(crate) store: &'a AnnotationStore,
    pub(crate) depth: usize,
}

pub struct SelectorIterItem<'a> {
    ancestors: Vec<&'a Selector>,
    selector: &'a Selector,
    depth: usize,
    leaf: bool,
}

impl<'a> Deref for SelectorIterItem<'a> {
    type Target = Selector;
    fn deref(&self) -> &'a Selector {
        self.selector
    }
}

impl<'a> SelectorIterItem<'a> {
    pub fn depth(&self) -> usize {
        self.depth
    }
    pub fn ancestors<'b>(&'b self) -> &'b Vec<&'a Selector> {
        &self.ancestors
    }
    pub fn is_leaf(&self) -> bool {
        self.leaf
    }
}

impl<'a> Iterator for SelectorIter<'a> {
    type Item = SelectorIterItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        //note: Vec::new() should be cheap as Vec only allocates on push!

        if self.subiterstack.is_empty() {
            if let Some(selector) = self.selector {
                let mut leaf = true;
                match selector {
                    Selector::AnnotationSelector(a_handle, offset) => {
                        if self.recurse_annotation {
                            leaf = false;
                            let annotation: &Annotation = self
                                .store
                                .get(*a_handle)
                                .expect("referenced annotation must exist");
                            self.subiterstack.push(SelectorIter {
                                selector: Some(annotation.target()),
                                ancestors: if self.track_ancestors {
                                    let mut ancestors = self.ancestors.clone(); //MAYBE TODO: the clones are fairly expensive
                                    ancestors.push(selector);
                                    ancestors
                                } else {
                                    Vec::new()
                                },
                                subiterstack: Vec::new(),
                                recurse_annotation: self.recurse_annotation,
                                track_ancestors: self.track_ancestors,
                                store: self.store,
                                depth: self.depth + 1,
                            });
                        }
                    }
                    Selector::MultiSelector(v) | Selector::DirectionalSelector(v) => {
                        leaf = false;
                        for subselector in v.iter() {
                            self.subiterstack.push(SelectorIter {
                                selector: Some(subselector),
                                ancestors: if self.track_ancestors {
                                    let mut ancestors = self.ancestors.clone(); //MAYBE TODO: the clones are fairly expensive
                                    ancestors.push(subselector);
                                    ancestors
                                } else {
                                    Vec::new()
                                },
                                subiterstack: Vec::new(),
                                recurse_annotation: self.recurse_annotation,
                                track_ancestors: self.track_ancestors,
                                store: self.store,
                                depth: self.depth + 1,
                            });
                        }
                    }
                    _ => {}
                };
                self.selector = None; //this flags that we have processed the selector
                Some(SelectorIterItem {
                    ancestors: if self.track_ancestors {
                        self.ancestors.clone() //MAYBE TODO: the clones are fairly expensive
                    } else {
                        Vec::new()
                    },
                    selector,
                    depth: self.depth,
                    leaf,
                })
            } else {
                None
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

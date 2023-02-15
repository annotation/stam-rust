use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::ops::Deref;
use std::borrow::Cow;
use smallvec::{SmallVec, smallvec};

use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::error::*;
use crate::resources::{TextResource, TextResourceHandle};
use crate::textselection::{TextSelection, TextSelectionHandle};
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

    /// Shortcut constructor to create a constructor that selects everything of the target
    pub fn whole() -> Self {
        Offset {
            begin: Cursor::BeginAligned(0),
            end: Cursor::EndAligned(0),
        }
    }

    /// Returns true if this Offset only uses begin-aligned cursors 
    pub fn is_simple(&self) -> bool {
        match (self.begin, self.end) {
            (Cursor::BeginAligned(_), Cursor::BeginAligned(_)) => true,
            _ => false
        }
    }

    /// Returns true if this Offset only uses begin-aligned cursors, or if it selects the whole target
    pub fn is_simple_or_whole(&self) -> bool {
        match (self.begin, self.end) {
            (Cursor::BeginAligned(_), Cursor::BeginAligned(_)) => true,
            (Cursor::BeginAligned(0), Cursor::EndAligned(0)) => true,
            _ => false
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
    /// Refers to an [`Annotation`] (as owned by the AnnotationStore) and optionally a *relative* text selection offset in it
    AnnotationSelector(AnnotationHandle, Option<Offset>),
    /// Refers to the [`TextResource`] (as owned by the AnnotationStore) an an offset in it
    TextSelector(TextResourceHandle, Offset),
    /// Refers to an [`crate::AnnotationDataSet`] as owned by an [`AnnotationStore']
    /// Annotations using this selector can be considered metadata.
    DataSetSelector(AnnotationDataSetHandle),

    /// A selector that combines selectors, where the annotation applies to each target
    /// individually.  without any relation between the different targets. Leaving one out or
    /// adding one MUST NOT affect the interpretation of any of the others nor of the whole. This
    /// is a way to express multiple annotations as one, a more condensed representation. This
    /// selector SHOULD be used sparingly in your modelling, as it is generally RECOMMENDED to
    /// simply use multiple [`Annotation'] instances instead. In STAM, even with multiple annotations, you
    /// benefit from the fact that multiple annotations may share the same [`AnnotationData`], and can
    /// therefore easily retrieve all annotations that share particular data.
    MultiSelector(Vec<Selector>),

    /// A selector that consists of multiple other selectors, used to select more complex targets
    /// that transcend the idea of a single simple selection. This MUST be interpreted as the
    /// annotation applying equally to the conjunction as a whole, its parts being inter-dependent
    /// and for any of them it goes that they MUST NOT be omitted for the annotation to make sense.
    /// The interpretation of the whole relies on all its parts. Note that the order of the
    /// selectors is not significant (use a [`Self::DirectionalSelector`] instead if they are). When there is
    /// no dependency relation between the selectors, you MUST simply use multiple [`Annotation`] instances or a
    /// [`Self::MultiSelector`] instead. When grouping things into a set, do use this [`Self::CompositeSelector'], as the
    /// set as a whole is considered a composite entity.
    CompositeSelector(Vec<Selector>),

    /// Combines selectors and expresseds a direction between two or more selectors in the exact order specified (from -> to)
    DirectionalSelector(Vec<Selector>),

    /// Internal selector pointing directly to a TextSelection, exposed as TextSelector to the outside world
    InternalTextSelector {
        resource: TextResourceHandle,
        textselection: TextSelectionHandle,
    },
    /// Internal selector pointing directly to a TextSelection and an Annotation, exposed as AnnotationSelector to the outside world
    /// This can only be used for annotations that select the entire text of the underlying annotation (no subslices)
    InternalAnnotationTextSelector {
        annotation: AnnotationHandle,
        resource: TextResourceHandle,
        textselection: TextSelectionHandle,
    },

    /// Internal ranged selector, used as subselector for MultiSelector/CompositeSelector/DirectionalSelector
    /// Conserved memory by pointing to a internal ID range
    InternalRangedTextSelector {
        resource: TextResourceHandle,
        begin: TextSelectionHandle,
        end: TextSelectionHandle,
    },
    /// Internal ranged selector, used as subselector for MultiSelector/CompositeSelector/DirectionalSelector
    /// Conserved memory by pointing to a internal ID range
    InternalRangedAnnotationSelector {
        begin: AnnotationHandle,
        end: AnnotationHandle,
    },
    /// Internal ranged selector, used as subselector for MultiSelector/CompositeSelector/DirectionalSelector
    /// Conserved memory by pointing to a internal ID range
    InternalRangedResourceSelector {
        begin: TextResourceHandle,
        end: TextResourceHandle,
    },
    /// Internal ranged selector, used as subselector for MultiSelector/CompositeSelector/DirectionalSelector
    /// Conserved memory by pointing to a internal ID range
    InternalRangedDataSetSelector {
        begin: AnnotationDataSetHandle,
        end: AnnotationDataSetHandle,
    },
}

impl Selector {
    /// Returns a [`SelectorKind`]
    pub fn kind(&self) -> SelectorKind {
        self.into()
    }

    /// A complex selector targets multiple targets. Note the internal ranged selector is not counted as part of this category.
    pub fn is_complex(&self) -> bool {
        match self.kind() {
            SelectorKind::MultiSelector => true,
            SelectorKind::DirectionalSelector => true,
            SelectorKind::CompositeSelector => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// See [`Selector`], this is a simplified variant that carries only the type, not the target.
pub enum SelectorKind {
    ResourceSelector = 1,
    AnnotationSelector = 2,
    TextSelector = 3,
    DataSetSelector = 4,
    MultiSelector = 5,
    CompositeSelector = 6,
    DirectionalSelector = 7,
    InternalRangedSelector = 8,
}

impl From<&Selector> for SelectorKind {
    fn from(selector: &Selector) -> Self {
        match selector {
            Selector::ResourceSelector(_) => Self::ResourceSelector,
            Selector::AnnotationSelector(_, _) => Self::AnnotationSelector,
            Selector::TextSelector(_, _) => Self::TextSelector,
            Selector::DataSetSelector(_) => Self::DataSetSelector,
            Selector::MultiSelector(_) => Self::MultiSelector,
            Selector::CompositeSelector(_) => Self::CompositeSelector,
            Selector::DirectionalSelector(_) => Self::DirectionalSelector,
            Selector::InternalTextSelector {
                resource: _,
                textselection: _,
            } => Self::TextSelector,
            Selector::InternalAnnotationTextSelector {
                annotation: _,
                resource: _,
                textselection: _,
            } => Self::AnnotationSelector,
            Selector::InternalRangedTextSelector {
                resource: _,
                begin: _,
                end: _,
            }
            | Selector::InternalRangedAnnotationSelector { begin: _, end: _ }
            | Selector::InternalRangedResourceSelector { begin: _, end: _ }
            | Selector::InternalRangedDataSetSelector { begin: _, end: _ } => {
                Self::InternalRangedSelector
            }
        }
    }
}

impl From<&SelectorBuilder> for SelectorKind {
    fn from(selector: &SelectorBuilder) -> Self {
        match selector {
            SelectorBuilder::ResourceSelector(_) => Self::ResourceSelector,
            SelectorBuilder::AnnotationSelector(_, _) => Self::AnnotationSelector,
            SelectorBuilder::TextSelector(_, _) => Self::TextSelector,
            SelectorBuilder::DataSetSelector(_) => Self::DataSetSelector,
            SelectorBuilder::MultiSelector(_) => Self::MultiSelector,
            SelectorBuilder::CompositeSelector(_) => Self::CompositeSelector,
            SelectorBuilder::DirectionalSelector(_) => Self::DirectionalSelector,
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
    CompositeSelector(Vec<SelectorBuilder>),
    DirectionalSelector(Vec<SelectorBuilder>),
}

impl SelectorBuilder {
    /// Returns a [`SelectorKind`]
    pub fn kind(&self) -> SelectorKind {
        self.into()
    }

    /// A complex selector targets multiple targets. Note the internal ranged selector is not counted as part of this category.
    pub fn is_complex(&self) -> bool {
        match self.kind() {
            SelectorKind::MultiSelector => true,
            SelectorKind::DirectionalSelector => true,
            SelectorKind::CompositeSelector => true,
            _ => false,
        }
    }
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
    CompositeSelector(Vec<SelectorBuilder>),
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
            SelectorJson::CompositeSelector(v) => Self::CompositeSelector(v),
            SelectorJson::DirectionalSelector(v) => Self::DirectionalSelector(v),
        }
    }
}

/// This trait is implemented by types that can return a Selector to themselves
pub trait SelfSelector {
    /// Returns a selector that points to this resource
    fn selector(&self) -> Result<Selector, StamError>;
}


/// This is a smart pointer that encapsulates both a selector and the annotationstore in which it can be resolved.
/// We need the wrapped structure for serialization.
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

/// This structure is used for serializing subselectors
pub struct WrappedSelectors<'a> {
    selectors: &'a Vec<Selector>,
    store: &'a AnnotationStore,
}

impl<'a> Serialize for WrappedSelectors<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.selectors.len()))?;
        for subselector in self.selectors.iter() {
            if subselector.kind() != SelectorKind::InternalRangedSelector {
                //normal case
                let wrappedselector = WrappedSelector {
                    selector: subselector,
                    store: self.store,
                };
                seq.serialize_element(&wrappedselector)?;
            } else {
                //we have an internal ranged selector
                for subselector in subselector.iter(self.store, false,false) {
                    let wrappedselector = WrappedSelector {
                        selector: &subselector.selector,
                        store: self.store,
                    };
                    seq.serialize_element(&wrappedselector)?;
                }
            }
        }
        seq.end()
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
                let textresource = textresource.map_err(serde::ser::Error::custom)?;
                let mut state = serializer.serialize_struct("Selector", 2)?;
                state.serialize_field("@type", "ResourceSelector")?;
                if textresource.id().is_none() {
                    return Err(serde::ser::Error::custom(
                        "Selector target must have an ID if it is to be serialized",
                    ));
                }
                state.serialize_field("resource", &textresource.id())?;
                state.end()
            }
            Selector::TextSelector(res_handle, offset) => {
                let textresource: Result<&TextResource, _> = self.store().get(*res_handle);
                let textresource = textresource.map_err(serde::ser::Error::custom)?;
                let mut state = serializer.serialize_struct("Selector", 3)?;
                state.serialize_field("@type", "TextSelector")?;
                if textresource.id().is_none() {
                    return Err(serde::ser::Error::custom(
                        "Selector target must have an ID if it is to be serialized",
                    ));
                }
                state.serialize_field("resource", &textresource.id())?;
                state.serialize_field("offset", offset)?;
                state.end()
            }
            Selector::DataSetSelector(dataset_handle) => {
                let annotationset: Result<&AnnotationDataSet, _> =
                    self.store().get(*dataset_handle);
                let annotationset = annotationset.map_err(serde::ser::Error::custom)?;
                let mut state = serializer.serialize_struct("Selector", 2)?;
                state.serialize_field("@type", "DataSetSelector")?;
                if annotationset.id().is_none() {
                    return Err(serde::ser::Error::custom(
                        "Selector target must have an ID if it is to be serialized",
                    ));
                }
                state.serialize_field("resource", &annotationset.id())?;
                state.end()
            }
            Selector::AnnotationSelector(annotation_handle, offset) => {
                let annotation: Result<&Annotation, _> = self.store().get(*annotation_handle);
                let annotation = annotation.map_err(serde::ser::Error::custom)?;
                let mut state = serializer.serialize_struct("Selector", 3)?;
                state.serialize_field("@type", "AnnotationSelector")?;
                if annotation.id().is_none() {
                    return Err(serde::ser::Error::custom(
                        "Selector target must have an ID if it is to be serialized",
                    ));
                }
                state.serialize_field("annotation", &annotation.id())?;
                if let Some(offset) = offset {
                    state.serialize_field("offset", offset)?;
                }
                state.end()
            }
            Selector::InternalAnnotationTextSelector {
                annotation: annotation_handle,
                resource: _res_handle,
                textselection: _textselection_handle,
            } => {
                let annotation: Result<&Annotation, _> = self.store().get(*annotation_handle);
                let annotation = annotation.map_err(serde::ser::Error::custom)?;

                let mut state = serializer.serialize_struct("Selector", 3)?;
                state.serialize_field("@type", "AnnotationSelector")?;
                if annotation.id().is_none() {
                    return Err(serde::ser::Error::custom(
                        "Selector target must have an ID if it is to be serialized",
                    ));
                }
                state.serialize_field("annotation", &annotation.id())?;
                state.serialize_field("offset", &Offset::whole())?;
                state.end()
            }
            Selector::InternalTextSelector {
                resource: res_handle,
                textselection: textselection_handle,
            } => {
                let textresource: Result<&TextResource, _> = self.store().get(*res_handle);
                let textresource = textresource.map_err(serde::ser::Error::custom)?;
                let textselection: &TextSelection = textresource
                    .get(*textselection_handle)
                    .map_err(serde::ser::Error::custom)?;

                let mut state = serializer.serialize_struct("Selector", 3)?;
                state.serialize_field("@type", "TextSelector")?;
                if textresource.id().is_none() {
                    return Err(serde::ser::Error::custom(
                        "Selector target must have an ID if it is to be serialized",
                    ));
                }
                state.serialize_field("resource", &textresource.id())?;
                let offset: Offset = textselection.into();
                state.serialize_field("offset", &offset)?;
                state.end()
            }
            Selector::MultiSelector(subselectors) => {
                let mut state = serializer.serialize_struct("Selector", 2)?;
                state.serialize_field("@type", "MultiSelector")?;
                let subselectors = WrappedSelectors {
                    selectors: subselectors,
                    store: self.store,
                };
                state.serialize_field("selectors", &subselectors)?;
                state.end()
            }
            Selector::CompositeSelector(subselectors) => {
                let mut state = serializer.serialize_struct("Selector", 2)?;
                state.serialize_field("@type", "CompositeSelector")?;
                let subselectors = WrappedSelectors {
                    selectors: subselectors,
                    store: self.store,
                };
                state.serialize_field("selectors", &subselectors)?;
                state.end()
            }
            Selector::DirectionalSelector(subselectors) => {
                let mut state = serializer.serialize_struct("Selector", 2)?;
                state.serialize_field("@type", "DirectionalSelector")?;
                let subselectors = WrappedSelectors {
                    selectors: subselectors,
                    store: self.store,
                };
                state.serialize_field("selectors", &subselectors)?;
                state.end()
            }
            Selector::InternalRangedTextSelector {
                resource: _,
                begin: _,
                end: _,
            } 
            | Selector::InternalRangedAnnotationSelector { begin: _, end: _ }
            | Selector::InternalRangedDataSetSelector { begin: _, end: _ }
            | Selector::InternalRangedResourceSelector { begin: _, end: _ } => {
                Err(serde::ser::Error::custom(
                    "Internal Ranged selectors can not be serialized directly, they can be serialized only when under a complex selector",
                ))
            }
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
            selector: self,
            ancestors: SmallVec::new(),
            subiterstack: Vec::new(),
            cursor_in_range: 0,
            recurse_annotation,
            track_ancestors,
            store,
            done: false,
            depth: 0,
        }
    }
}

const ANCESTORSIZE: usize = 3;
pub type AncestorVec<'a> = SmallVec<[Cow<'a,Selector>;ANCESTORSIZE]>;

/// Iterator that returns the selector itself, plus all selectors under it (recursively)
pub struct SelectorIter<'a> {
    selector: &'a Selector, //we keep the root item out of subiterstack to save ourselves the Vec<> allocation
    done: bool,
    ancestors: AncestorVec<'a>,
    subiterstack: Vec<SelectorIter<'a>>,
    cursor_in_range: usize, //used to track iteration of InternalRangedSelectors
    recurse_annotation: bool,
    track_ancestors: bool,
    pub(crate) store: &'a AnnotationStore,
    pub(crate) depth: usize,
}

pub struct SelectorIterItem<'a> {
    ancestors: AncestorVec<'a>,
    selector: Cow<'a, Selector>, //we use Cow because we may return create new owned selectors on the fly (like with ranged internal selectors)
    depth: usize,
    leaf: bool,
}


impl<'a> SelectorIterItem<'a> {
    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn selector<'b>(&'b self) -> &'b Cow<'a,Selector> {
        &self.selector
    }
    pub fn ancestors<'b>(&'b self) -> &'b AncestorVec<'a> {
        &self.ancestors
    }
    pub fn is_leaf(&self) -> bool {
        self.leaf
    }
}

impl<'a> SelectorIter<'a> {
    fn get_internal_ranged_item(&self, selector: &'a Selector) -> SelectorIterItem<'a> {
        SelectorIterItem {
            ancestors: if self.track_ancestors {
                self.ancestors.clone() //MAYBE TODO: the clones are fairly expensive
            } else {
                SmallVec::new()
            },
            selector: match selector {
                Selector::InternalRangedResourceSelector {..} => {
                    Cow::Owned(Selector::ResourceSelector(TextResourceHandle::new(self.cursor_in_range)))
                }
                Selector::InternalRangedDataSetSelector {..} => {
                    Cow::Owned(Selector::DataSetSelector(AnnotationDataSetHandle::new(self.cursor_in_range)))
                }
                Selector::InternalRangedAnnotationSelector {..} => {
                    Cow::Owned(Selector::AnnotationSelector(AnnotationHandle::new(self.cursor_in_range), Some(Offset::whole())))
                }
                Selector::InternalRangedTextSelector { resource, .. } => {
                    Cow::Owned(Selector::InternalTextSelector {resource:*resource, textselection: TextSelectionHandle::new(self.cursor_in_range)} )
                }
                _ => {
                    unreachable!()
                }
            },
            depth: self.depth,
            leaf: true,
        }
    }
}

impl<'a> Iterator for SelectorIter<'a> {
    type Item = SelectorIterItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {

        if self.subiterstack.is_empty() {
            if !self.done {
                let mut leaf = true;
                match self.selector {
                    //  higher-order annotation, appends to the subiterstack
                    Selector::AnnotationSelector(a_handle, _) | Selector::InternalAnnotationTextSelector { annotation: a_handle, .. }=> {
                        if self.recurse_annotation {
                            leaf = false;
                            let annotation: &Annotation = self
                                .store
                                .get(*a_handle)
                                .expect("referenced annotation must exist");
                            self.subiterstack.push(SelectorIter {
                                selector: annotation.target(),
                                ancestors: if self.track_ancestors {
                                    let mut ancestors = self.ancestors.clone(); //MAYBE TODO: the clones are fairly expensive
                                    ancestors.push(Cow::Borrowed(&self.selector));
                                    ancestors
                                } else {
                                    SmallVec::new()
                                },
                                //note: Vec::new() should be cheap as Vec only allocates on push!
                                subiterstack: Vec::new(),
                                cursor_in_range: 0,
                                recurse_annotation: self.recurse_annotation,
                                track_ancestors: self.track_ancestors,
                                store: self.store,
                                done: false,
                                depth: self.depth + 1,
                            });
                        }
                    }
                    // complex iterators, these append to the subiterstack
                    Selector::MultiSelector(v)
                    | Selector::CompositeSelector(v)
                    | Selector::DirectionalSelector(v) => {
                        leaf = false;
                        for subselector in v.iter().rev() {
                            self.subiterstack.push(SelectorIter {
                                selector: subselector,
                                ancestors: if self.track_ancestors {
                                    let mut ancestors = self.ancestors.clone(); //MAYBE TODO: the clones are fairly expensive
                                    ancestors.push(Cow::Borrowed(subselector));
                                    ancestors
                                } else {
                                    SmallVec::new()
                                },
                                //note: Vec::new() should be cheap as Vec only allocates on push!
                                subiterstack: Vec::new(),
                                cursor_in_range: 0,
                                recurse_annotation: self.recurse_annotation,
                                track_ancestors: self.track_ancestors,
                                store: self.store,
                                done: false,
                                depth: self.depth + 1,
                            });
                        }
                    }
                    // internal ranged selector, these return a result immediately
                    // some duplication because each begin/end has different parameter types even though it looks the same
                    Selector::InternalRangedResourceSelector { begin , end } => {
                        if begin.unwrap() + self.cursor_in_range >= end.unwrap() {
                            //we're done with this iterator
                            self.done = true; //this flags that we have processed this selector
                            return None;
                        } else {
                            let result = self.get_internal_ranged_item(self.selector);
                            self.cursor_in_range += 1;
                            return Some(result);
                        }
                    }
                    Selector::InternalRangedDataSetSelector { begin, end } => {
                        if begin.unwrap() + self.cursor_in_range >= end.unwrap() {
                            //we're done with this iterator
                            self.done = true; //this flags that we have processed this selector
                            return None;
                        } else {
                            let result = self.get_internal_ranged_item(self.selector);
                            self.cursor_in_range += 1;
                            return Some(result);
                        }
                    }
                    Selector::InternalRangedAnnotationSelector { begin, end } => {
                        if begin.unwrap() + self.cursor_in_range >= end.unwrap() {
                            //we're done with this iterator
                            self.done = true; //this flags that we have processed this selector
                            return None;
                        } else {
                            let result = self.get_internal_ranged_item(self.selector);
                            self.cursor_in_range += 1;
                            return Some(result);
                        }
                    }
                    Selector::InternalRangedTextSelector { resource: _, begin, end } => {
                        if begin.unwrap() + self.cursor_in_range >= end.unwrap() {
                            //we're done with this iterator
                            self.done = true; //this flags that we have processed this selector
                            return None;
                        } else {
                            let result = self.get_internal_ranged_item(self.selector);
                            self.cursor_in_range += 1;
                            return Some(result);
                        }
                    },
                    // simple selectors fall back to the default behaviour after this match clause
                    Selector::TextSelector(_, _) => {},
                    Selector::InternalTextSelector {..} => {},
                    Selector::DataSetSelector(_) => {}
                    Selector::ResourceSelector(_) => {}
                };
                self.done = true; //this flags that we have processed the selector
                Some(SelectorIterItem {
                    ancestors: if self.track_ancestors {
                        self.ancestors.clone() //MAYBE TODO: the clones are fairly expensive
                    } else {
                        SmallVec::new()
                    },
                    selector: Cow::Borrowed(self.selector),
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

use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::borrow::Cow;
use datasize::{DataSize,data_size};
use minicbor::{Encode,Decode};

use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::error::*;
use crate::resources::{TextResource, TextResourceHandle};
use crate::textselection::{TextSelection, TextSelectionHandle};
use crate::types::*;
use crate::store::*;
use crate::text::Text;

/// Text selection offset. Specifies begin and end offsets to select a range of a text, via two [`Cursor`] instances.
/// The end-point is non-inclusive.
#[derive(Debug, Clone, Deserialize, PartialEq, DataSize, Encode, Decode)]
pub struct Offset {
    #[n(0)]  //these macros are field index numbers for cbor binary (de)serialisation
    pub begin: Cursor,

    #[n(1)] 
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

    /// Writes a datavalue to one STAM JSON string, with appropriate formatting
    pub fn to_json(&self) -> Result<String, StamError> {
        //note: this function is not called during normal serialisation
        serde_json::to_string_pretty(&self).map_err(|e| {
            StamError::SerializationError(format!("Writing textselection to string: {}", e))
        })
    }

    /// Writes a datavalue to one STAM JSON string, without any indentation
    pub fn to_json_compact(&self) -> Result<String, StamError> {
        //note: this function is not called during normal serialisation
        serde_json::to_string(&self).map_err(|e| {
            StamError::SerializationError(format!("Writing textselection to string: {}", e))
        })
    }

    pub fn mode(&self) -> OffsetMode {
        self.into()
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

#[derive(Debug, Clone, Copy, PartialEq, Encode, Decode,DataSize)]
pub enum OffsetMode {
    #[n(0)]
    BeginBegin,
    #[n(1)]
    BeginEnd,
    #[n(2)]
    EndEnd,
    #[n(3)]
    EndBegin,
}

impl Default for OffsetMode {
    fn default() -> Self {
        Self::BeginBegin
    }
}

impl From<&Offset> for OffsetMode {
    fn from(offset: &Offset) -> OffsetMode {
        match offset.begin {
            Cursor::BeginAligned(_) => {
                match offset.end {
                    Cursor::BeginAligned(_) => {
                        Self::BeginBegin
                    },
                    Cursor::EndAligned(_) => {
                        Self::BeginEnd
                    }
                }
            },
            Cursor::EndAligned(_) => {
                match offset.end {
                    Cursor::BeginAligned(_) => {
                        Self::EndBegin
                    },
                    Cursor::EndAligned(_) => {
                        Self::EndEnd
                    }
                }
            }
        }
    }
}

/// A `Selector` identifies the target of an annotation and the part of the
/// target that the annotation applies to. Selectors can be considered the labelled edges of the graph model, tying all nodes together.
/// There are multiple types of selectors, all captured in this enum.
///
/// You usually do not instantiate these directly but via [`SelectorBuilder`].
/// In searching, you also don't need direct access to this structure as
/// the various search methods on AnnotationStore will resolve the selectors
/// transparently.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum Selector {
    /// Refers to the [`TextResource`] and a [`TextSelection`] within
    #[n(0)] //these macros are field index numbers for cbor binary (de)serialisation
    TextSelector(
        #[n(0)]
        TextResourceHandle,
        #[n(1)]
        TextSelectionHandle,
        #[n(2)]
        OffsetMode, //this allows us to know what kind of offset to compute on serialisation
    ),

    /// Refers to an [`Annotation`] (as owned by the AnnotationStore) and optionally a *relative* text selection offset in it
    #[n(1)] //these macros are field index numbers for cbor binary (de)serialisation
    AnnotationSelector(
        #[n(0)]
        AnnotationHandle, 
        #[n(1)]
        Option<(TextResourceHandle, TextSelectionHandle, OffsetMode)>
    ),

    /// Refers to a [`TextResource`] as a whole (as opposed to a text fragment inside it), as owned by an AnnotationStore.
    /// Annotations using this selector can be considered metadata of a text
    #[n(2)]
    ResourceSelector(
        #[n(0)]
        TextResourceHandle
    ),

    /// Refers to an [`crate::AnnotationDataSet`] as owned by an [`AnnotationStore']
    /// Annotations using this selector can be considered metadata.
    #[n(3)]
    DataSetSelector(
        #[n(0)]
        AnnotationDataSetHandle
    ),

    /// A selector that combines selectors, where the annotation applies to each target
    /// individually.  without any relation between the different targets. Leaving one out or
    /// adding one MUST NOT affect the interpretation of any of the others nor of the whole. This
    /// is a way to express multiple annotations as one, a more condensed representation. This
    /// selector SHOULD be used sparingly in your modelling, as it is generally RECOMMENDED to
    /// simply use multiple [`Annotation'] instances instead. In STAM, even with multiple annotations, you
    /// benefit from the fact that multiple annotations may share the same [`AnnotationData`], and can
    /// therefore easily retrieve all annotations that share particular data.
    #[n(4)]
    MultiSelector(
        #[n(0)]
        Vec<Selector>
    ),

    /// A selector that consists of multiple other selectors, used to select more complex targets
    /// that transcend the idea of a single simple selection. This MUST be interpreted as the
    /// annotation applying equally to the conjunction as a whole, its parts being inter-dependent
    /// and for any of them it goes that they MUST NOT be omitted for the annotation to make sense.
    /// The interpretation of the whole relies on all its parts. Note that the order of the
    /// selectors is not significant (use a [`Self::DirectionalSelector`] instead if they are). When there is
    /// no dependency relation between the selectors, you MUST simply use multiple [`Annotation`] instances or a
    /// [`Self::MultiSelector`] instead. When grouping things into a set, do use this [`Self::CompositeSelector'], as the
    /// set as a whole is considered a composite entity.
    #[n(5)]
    CompositeSelector(
        #[n(0)]
        Vec<Selector>
    ),

    /// Combines selectors and expresseds a direction between two or more selectors in the exact order specified (from -> to)
    #[n(6)]
    DirectionalSelector(
        #[n(0)]
        Vec<Selector>
    ),


    /// Internal ranged selector, used as subselector for MultiSelector/CompositeSelector/DirectionalSelector
    // Conserves memory by pointing to a internal ID range, end is inclusive
    #[n(52)]
    RangedTextSelector {
        #[n(0)]
        resource: TextResourceHandle,
        #[n(1)]
        begin: TextSelectionHandle,
        #[n(2)]
        end: TextSelectionHandle,
    },

    /// Internal ranged selector, used as subselector for MultiSelector/CompositeSelector/DirectionalSelector
    /// Conserves memory by pointing to a internal ID range, end is inclusive
    #[n(53)]
    RangedAnnotationSelector {
        #[n(0)]
        begin: AnnotationHandle,
        #[n(1)]
        end: AnnotationHandle,
        #[n(2)]
        with_text: bool, //if set, each individual AnnotationSelector will get a Offset::whole(), otherwise it gets no text reference
    },
}

impl Selector {
    /// Returns a [`SelectorKind`] identifying the type of selector
    pub fn kind(&self) -> SelectorKind {
        self.into()
    }

    /// A complex selector targets multiple targets. Note the internal ranged selector is not counted as part of this category.
    pub fn is_complex(&self) -> bool {
        self.kind().is_complex()
    }

    /// Writes a Selector to a STAM JSON string, with appropriate formatting
    pub fn to_json(&self, store: &AnnotationStore) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        let wrapped = WrappedSelector::new(self, store);
        serde_json::to_string_pretty(&wrapped).map_err(|e| {
            StamError::SerializationError(format!("Writing selector to string: {}", e))
        })
    }

    /// Writes a Selector to a STAM JSON string, without indentation
    pub fn to_json_compact(&self, store: &AnnotationStore) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        let wrapped = WrappedSelector::new(self, store);
        serde_json::to_string(&wrapped).map_err(|e| {
            StamError::SerializationError(format!("Writing selector to string: {}", e))
        })
    }

    /// Returns all subselectors. Use ['iter()`] instead if you want an iterator
    /// with more functionality.
    pub fn subselectors(&self) -> Option<&[Selector]> {
        match self {
            Selector::MultiSelector(v) | Selector::CompositeSelector(v) | Selector::DirectionalSelector(v) => Some(v),
            _ => None,
        }
    }   

    /// Returns the textselection this selector points at, if any
    pub fn textselection<'store>(&self, store: &'store AnnotationStore) -> Option<&'store TextSelection> {
        match self {
            Selector::TextSelector(res_handle,tsel_handle,_) | Selector::AnnotationSelector(_, Some((res_handle,tsel_handle,_))) => {
                let resource: &TextResource = store.get(*res_handle).expect("handle must be valid");
                let textselection: &TextSelection = resource.get(*tsel_handle).expect("handle must be valid");
                Some(textselection)
            }
            _ => None
        }
    }

    /// Returns the textselection handle this selector points at, if any
    pub fn textselection_handle(&self) -> Option<TextSelectionHandle> {
        match self {
            Selector::TextSelector(_,tsel_handle,_) | Selector::AnnotationSelector(_, Some((_,tsel_handle,_))) => {
                Some(*tsel_handle)
            }
            _ => None
        }
    }

    /// Returns the handle of the resource this selector points at, if any
    pub fn resource_handle(&self) -> Option<TextResourceHandle> {
        match self {
            Selector::ResourceSelector(res_handle) | Selector::TextSelector(res_handle,_,_) | Selector::AnnotationSelector(_, Some((res_handle,_,_))) => {
                Some(*res_handle)
            }
            _ => None
        }
    }


    /// Returns the associated offset if the selector carries one
    pub fn offset(&self, store: &AnnotationStore) -> Option<Offset> {
        self.offset_with_mode(store, None)
}

    /// Returns the associated offset if the selector carries one
    /// You may set `override_mode` if you want to override the OffsetMode rather than take it from the selector
    pub fn offset_with_mode(&self, store: &AnnotationStore, override_mode: Option<OffsetMode>) -> Option<Offset> {
        match self {
            Selector::TextSelector(res_handle,tsel_handle,offsetmode) => {
                let offsetmode = if let Some(override_mode) = override_mode {
                    override_mode 
                } else {
                    *offsetmode
                };
                let resource: &TextResource = store.get(*res_handle).expect("handle must be valid");
                let textselection: &TextSelection = resource.get(*tsel_handle).expect("handle must be valid");
                match offsetmode {
                    OffsetMode::BeginBegin => //this is the easy one, the From<TextSelection> trait provides it:
                        Some(textselection.into()),
                    OffsetMode::BeginEnd => {
                        let begin = textselection.begin();
                        let end: isize = textselection.end() as isize - resource.textlen() as isize;
                        Some(Offset::new(Cursor::BeginAligned(begin), Cursor::EndAligned(end)))
                    }
                    OffsetMode::EndBegin => { 
                        let begin: isize = textselection.begin() as isize - resource.textlen() as isize;
                        let end = textselection.end();
                        Some(Offset::new(Cursor::EndAligned(begin), Cursor::BeginAligned(end)))
                    }
                    OffsetMode::EndEnd => {
                        let begin: isize = textselection.begin() as isize - resource.textlen() as isize;
                        let end: isize = textselection.end() as isize - resource.textlen() as isize;
                        Some(Offset::new(Cursor::EndAligned(begin), Cursor::EndAligned(end)))
                    }
                }
            }
            Selector::AnnotationSelector(annotation_handle, Some((res_handle, tsel_handle, offsetmode))) => {
                let offsetmode = if let Some(override_mode) = override_mode {
                    override_mode 
                } else {
                    *offsetmode
                };
                let resource: &TextResource = store.get(*res_handle).expect("handle must be valid");
                let textselection: &TextSelection = resource.get(*tsel_handle).expect("handle must be valid");
                let target: &Annotation = store.get(*annotation_handle).expect("handle must be valid");
                if let Some(parent_textselection) = target.target().textselection(store) {
                    textselection.relative_offset(parent_textselection, offsetmode)
                } else {
                    None
                }
            }
            _ => None
        }
    }
}

impl DataSize for Selector {
    // `MyType` contains a `Vec` and a `String`, so `IS_DYNAMIC` is set to true.
    const IS_DYNAMIC: bool = true;
    const STATIC_HEAP_SIZE: usize = 8; //the descriminator/tag of the enum (worst case estimate)

    #[inline]
    fn estimate_heap_size(&self) -> usize {
        match self {
            Self::TextSelector(handle, handle2 ,mode) => 8 + data_size(handle) + data_size(handle2) + data_size(mode),
            Self::AnnotationSelector(handle, handle2) => 8 + data_size(handle) + data_size(handle2),
            Self::ResourceSelector(handle) => 8 + data_size(handle),
            Self::DataSetSelector(handle) => 8 + data_size(handle),
            Self::MultiSelector(v) => 8 + data_size(v),
            Self::CompositeSelector(v) => 8 + data_size(v),
            Self::DirectionalSelector(v) => 8 + data_size(v),
            Self::RangedTextSelector { resource, begin, end } => 8 + data_size(resource) + data_size(begin) + data_size(end),
            Self::RangedAnnotationSelector { begin, end, with_text } => 8 + data_size(with_text) + data_size(begin) + data_size(end),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize,Deserialize)]
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

impl SelectorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ResourceSelector => "ResourceSelector",
            Self::AnnotationSelector => "AnnotationSelector",
            Self::TextSelector => "TextSelector",
            Self::DataSetSelector => "DataSetSelector",
            Self::MultiSelector => "MultiSelector",
            Self::CompositeSelector => "CompositeSelector",
            Self::DirectionalSelector => "DirectionalSelector",
            Self::InternalRangedSelector => "InternalRangedSelector",
        }
    }

    pub fn is_complex(&self) -> bool {
        match self {
            Self::MultiSelector => true,
            Self::DirectionalSelector => true,
            Self::CompositeSelector => true,
            _ => false,
        }
    }

}

impl TryFrom<&str> for SelectorKind {
    type Error = StamError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "ResourceSelector" | "resourceselector" | "resource" => Ok(Self::ResourceSelector),
            "AnnotationSelector" | "annotationselector" | "annotation"  => Ok(Self::AnnotationSelector),
            "TextSelector" | "textselector" | "text" => Ok(Self::TextSelector),
            "DataSetSelector" | "datasetselector" | "set" | "annotationset"  => Ok(Self::DataSetSelector),
            "MultiSelector" | "multiselector" | "multi"  => Ok(Self::MultiSelector),
            "CompositeSelector" | "compositeselector" | "composite"  => Ok(Self::CompositeSelector),
            "DirectionalSelector" | "directionalselector" | "directional"  => Ok(Self::DirectionalSelector),
            _ => Err(StamError::ValueError(value.to_string(), "Expected a valid SelectorKind"))
        }
    }

}

impl From<&Selector> for SelectorKind {
    fn from(selector: &Selector) -> Self {
        match selector {
            Selector::ResourceSelector(_) => Self::ResourceSelector,
            Selector::AnnotationSelector(_, _) => Self::AnnotationSelector,
            Selector::TextSelector(_, _,_) => Self::TextSelector,
            Selector::DataSetSelector(_) => Self::DataSetSelector,
            Selector::MultiSelector(_) => Self::MultiSelector,
            Selector::CompositeSelector(_) => Self::CompositeSelector,
            Selector::DirectionalSelector(_) => Self::DirectionalSelector,
            Selector::RangedTextSelector {
                ..
            }
            | Selector::RangedAnnotationSelector { .. } => {
                Self::InternalRangedSelector
            }
        }
    }
}

impl<'a> From<&SelectorBuilder<'a>> for SelectorKind {
    fn from(selector: &SelectorBuilder<'a>) -> Self {
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
#[derive(Debug, Deserialize)]
#[serde(from = "SelectorJson")]
pub enum SelectorBuilder<'a> {
    ResourceSelector(BuildItem<'a,TextResource>),
    AnnotationSelector(BuildItem<'a,Annotation>, Option<Offset>),
    TextSelector(BuildItem<'a,TextResource>, Offset),
    DataSetSelector(BuildItem<'a,AnnotationDataSet>),
    MultiSelector(Vec<SelectorBuilder<'a>>),
    CompositeSelector(Vec<SelectorBuilder<'a>>),
    DirectionalSelector(Vec<SelectorBuilder<'a>>),
}

impl<'a> SelectorBuilder<'a> {
    /// Returns a [`SelectorKind`]
    pub fn kind(&self) -> SelectorKind {
        self.into()
    }

    /// A complex selector targets multiple targets. Note the internal ranged selector is not counted as part of this category.
    pub fn is_complex(&self) -> bool {
        self.kind().is_complex()
    }

    // Creates a new ResourceSelector
    pub fn resourceselector(resource: impl Into<BuildItem<'a,TextResource>>) -> Self {
        Self::ResourceSelector(resource.into())
    }

    // Creates a new TextSelector
    pub fn textselector(resource: impl Into<BuildItem<'a,TextResource>>, offset: impl Into<Offset>) -> Self {
        Self::TextSelector(resource.into(), offset.into())
    }

    // Creates a new AnnotationSelector
    pub fn annotationselector(annotation: impl Into<BuildItem<'a,Annotation>>, offset: Option<Offset>) -> Self {
        Self::AnnotationSelector(annotation.into(), offset)
    }

    // Creates a new ResourceSelector
    pub fn datasetselector(dataset: impl Into<BuildItem<'a,AnnotationDataSet>>) -> Self {
        Self::DataSetSelector(dataset.into())
    }

    // Creates a new MultiSelector from an iterator
    pub fn multiselector<I>(iter: I) -> Self where I: IntoIterator<Item = SelectorBuilder<'a>>, {
        Self::MultiSelector(iter.into_iter().collect())
    }

    // Creates a new CompositeSelector from an iterator
    pub fn compositeselector<I>(iter: I) -> Self where I: IntoIterator<Item = SelectorBuilder<'a>>  {
        Self::CompositeSelector(iter.into_iter().collect())
    }

    // Creates a new DirectionalSelector from an iterator
    pub fn directionalselector<I>(iter: I) -> Self where I: IntoIterator<Item = SelectorBuilder<'a>> {
        Self::DirectionalSelector(iter.into_iter().collect())
    }
}




/// Helper structure for Json deserialisation, we need named fields for the serde tag macro to work
#[derive(Debug, Deserialize)]
#[serde(tag = "@type")]
enum SelectorJson where
    {
    ResourceSelector {
        resource: String,
    },
    AnnotationSelector {
        annotation: String,
        offset: Option<Offset>,
    },
    TextSelector {
        resource: String,
        offset: Offset,
    },
    DataSetSelector {
        dataset: String,
    },
    MultiSelector { selectors: Vec<SelectorJson> },
    CompositeSelector { selectors: Vec<SelectorJson>},
    DirectionalSelector{ selectors: Vec<SelectorJson>},
}

impl<'a> From<SelectorJson> for SelectorBuilder<'a> {
    fn from(helper: SelectorJson) -> Self {
        match helper {
            SelectorJson::ResourceSelector { resource: res } => Self::ResourceSelector(res.into()),
            SelectorJson::TextSelector {
                resource: res,
                offset: o,
            } => Self::TextSelector(res.into(), o),
            SelectorJson::AnnotationSelector {
                annotation: a,
                offset: o,
            } => Self::AnnotationSelector(a.into(), o),
            SelectorJson::DataSetSelector { dataset: s } => Self::DataSetSelector(s.into()),
            SelectorJson::MultiSelector { selectors: v } => Self::MultiSelector(v.into_iter().map(|j| j.into()).collect()),
            SelectorJson::CompositeSelector { selectors: v } => Self::CompositeSelector(v.into_iter().map(|j| j.into()).collect()),
            SelectorJson::DirectionalSelector { selectors: v } => Self::DirectionalSelector(v.into_iter().map(|j| j.into()).collect()),
        }
    }
}


/// This trait is implemented by types that can return a Selector to themselves
pub trait SelfSelector {
    /// Returns a selector that points to this resource
    fn to_selector(&self) -> Result<Selector, StamError>;
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
                for subselector in subselector.iter(self.store, false) {
                    let wrappedselector = WrappedSelector {
                        selector: &subselector,
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
                if let Some(id) = textresource.id() {
                    state.serialize_field("resource", &id)?;
                } else {
                    state.serialize_field("resource", &textresource.temp_id().map_err(serde::ser::Error::custom)?)?;
                }
                state.end()
            }
            Selector::TextSelector(res_handle, _, _) => {
                let textresource: Result<&TextResource, _> = self.store().get(*res_handle);
                let textresource = textresource.map_err(serde::ser::Error::custom)?;
                let mut state = serializer.serialize_struct("Selector", 3)?;
                state.serialize_field("@type", "TextSelector")?;
                if let Some(id) = textresource.id() {
                    state.serialize_field("resource", &id)?;
                } else {
                    state.serialize_field("resource", &textresource.temp_id().map_err(serde::ser::Error::custom)?)?;
                }
                let offset: Offset = self.selector.offset(self.store()).expect("must have offset");
                state.serialize_field("offset", &offset)?;
                state.end()
            }
            Selector::DataSetSelector(dataset_handle) => {
                let dataset: Result<&AnnotationDataSet, _> =
                    self.store().get(*dataset_handle);
                let dataset = dataset.map_err(serde::ser::Error::custom)?;
                let mut state = serializer.serialize_struct("Selector", 2)?;
                state.serialize_field("@type", "DataSetSelector")?;
                if let Some(id) = dataset.id() {
                    state.serialize_field("annotationset", &id)?;
                } else {
                    state.serialize_field("annotationset", &dataset.temp_id().map_err(serde::ser::Error::custom)?)?;
                }
                state.end()
            }
            Selector::AnnotationSelector(annotation_handle, _) => {
                let annotation: Result<&Annotation, _> = self.store().get(*annotation_handle);
                let annotation = annotation.map_err(serde::ser::Error::custom)?;
                let mut state = serializer.serialize_struct("Selector", 3)?;
                state.serialize_field("@type", "AnnotationSelector")?;
                if let Some(id) = annotation.id() {
                    state.serialize_field("annotation", &id)?;
                } else {
                    state.serialize_field("annotation", &annotation.temp_id().map_err(serde::ser::Error::custom)?)?;
                }
                if let Some(offset) = self.selector.offset(self.store()) {
                    state.serialize_field("offset", &offset)?;
                }
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
            Selector::RangedTextSelector { .. } 
            | Selector::RangedAnnotationSelector { .. }
            => {
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
    ) -> SelectorIter<'a> {
        SelectorIter {
            selector: self,
            subiterstack: Vec::new(),
            cursor_in_range: 0,
            recurse_annotation,
            store,
            done: false,
        }
    }
}

/// Iterator that returns the selector itself, plus all selectors under it (recursively)
pub struct SelectorIter<'a> {
    selector: &'a Selector, //we keep the root item out of subiterstack to save ourselves the Vec<> allocation
    done: bool,
    subiterstack: Vec<SelectorIter<'a>>,
    ///used to track iteration of InternalRangedSelectors, starts at 0 (not begin)
    cursor_in_range: usize,
    /// follow AnnotationSelectors recursively
    pub(crate) recurse_annotation: bool,
    pub(crate) store: &'a AnnotationStore,
}


impl<'a> SelectorIter<'a> {
    fn get_internal_ranged_item(&self, selector: &'a Selector) -> Cow<'a,Selector> {
        match selector {
            Selector::RangedAnnotationSelector { begin, with_text, .. } => {
                let handle = AnnotationHandle::new(begin.as_usize() + self.cursor_in_range);
                if *with_text {
                    let annotation: &Annotation = self.store.get(handle).expect("annotation handle must be valid");
                    if let (Some(textselection_handle), Some(resource_handle)) = (annotation.target().textselection_handle(), annotation.target().resource_handle()) {
                        Cow::Owned(Selector::AnnotationSelector(handle, Some((resource_handle, textselection_handle, OffsetMode::default()))))
                    } else {
                        Cow::Owned(Selector::AnnotationSelector(handle, None))
                    }
                } else {
                        Cow::Owned(Selector::AnnotationSelector(handle, None))
                }
            }
            Selector::RangedTextSelector { resource, begin, .. } => {
                Cow::Owned(Selector::TextSelector(*resource, TextSelectionHandle::new(begin.as_usize() + self.cursor_in_range), OffsetMode::default()) )
            }
            _ => {
                unreachable!()
            }
        }
    }
}

impl<'a> Iterator for SelectorIter<'a> {
    type Item = Cow<'a, Selector>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.subiterstack.is_empty() {
                if !self.done {
                    match self.selector {
                        //  higher-order annotation, appends to the subiterstack
                        Selector::AnnotationSelector(a_handle, _) => {
                            if self.recurse_annotation {
                                let annotation: &Annotation = self
                                    .store
                                    .get(*a_handle)
                                    .expect("referenced annotation must exist");
                                self.subiterstack.push(SelectorIter {
                                    selector: annotation.target(),
                                    //note: Vec::new() should be cheap as Vec only allocates on push!
                                    subiterstack: Vec::new(),
                                    cursor_in_range: 0,
                                    recurse_annotation: self.recurse_annotation,
                                    store: self.store,
                                    done: false,
                                });
                            }
                        }
                        // complex iterators, these append to the subiterstack
                        Selector::MultiSelector(v)
                        | Selector::CompositeSelector(v)
                        | Selector::DirectionalSelector(v) => {
                            for subselector in v.iter().rev() {
                                self.subiterstack.push(SelectorIter {
                                    selector: subselector,
                                    //note: Vec::new() should be cheap as Vec only allocates on push!
                                    subiterstack: Vec::new(),
                                    cursor_in_range: 0,
                                    recurse_annotation: self.recurse_annotation,
                                    store: self.store,
                                    done: false,
                                });
                            }
                        }
                        Selector::RangedAnnotationSelector { begin, end, .. } => {
                            if begin.as_usize() + self.cursor_in_range > end.as_usize() { //end is inclusive
                                //we're done with this iterator
                                self.done = true; //this flags that we have processed this selector
                                return None;
                            } else {
                                let result = self.get_internal_ranged_item(self.selector);
                                self.cursor_in_range += 1;
                                return Some(result);
                            }
                        }
                        Selector::RangedTextSelector { resource: _, begin, end } => {
                            if begin.as_usize() + self.cursor_in_range > end.as_usize() { //end is inclusive
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
                        Selector::TextSelector(_, _,_) => {},
                        Selector::DataSetSelector(_) => {}
                        Selector::ResourceSelector(_) => {}
                    };
                    self.done = true; //this flags that we have processed the selector
                    return Some(Cow::Borrowed(self.selector));
                } else {
                    return None;
                }
            } else {
                let result = self.subiterstack.last_mut().unwrap().next();
                if result.is_none() {
                    self.subiterstack.pop();
                    if self.subiterstack.is_empty() {
                        return None;
                    } else {
                        continue; //recursion
                    }
                } else {
                    return result;
                }
            }
        }
    }
}

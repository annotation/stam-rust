use std::collections::btree_map;
use std::collections::BTreeMap;
use std::io::prelude::*;
use std::ops::Bound::{Excluded, Included};
use std::slice::Iter;
use std::sync::{Arc, RwLock};

use datasize::{data_size, DataSize};
use minicbor::{Decode, Encode};
use regex::{Regex, RegexSet};
use sealed::sealed;
use serde::de::DeserializeSeed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;

use crate::annotation::Annotation;
use crate::annotationstore::AnnotationStore;
use crate::cbor::*;
use crate::config::{Config, Configurable, SerializeMode};
use crate::error::StamError;
use crate::file::*;
use crate::json::{FromJson, ToJson};
use crate::selector::{Offset, Selector, SelfSelector};
use crate::store::*;
use crate::text::*;
use crate::textselection::PositionIndexItem;
use crate::textselection::{
    PositionIndex, ResultTextSelection, TextSelection, TextSelectionHandle,
};
use crate::types::*;

/// This holds the textual resource to be annotated. It holds the full text in memory.
///
/// The text *SHOULD* be in
/// [Unicode Normalization Form C (NFC)](https://www.unicode.org/reports/tr15/) but
/// *MAY* be in another unicode normalization forms.
#[derive(Debug, Clone, DataSize, Decode, Encode)]
pub struct TextResource {
    /// The internal numeric identifier for the resource (may only be None upon creation when not bound yet)
    #[n(0)] //these macros are field index numbers for cbor binary (de)serialisation
    intid: Option<TextResourceHandle>,

    /// Public identifier for the text resource (often the filename/URL)
    #[n(1)]
    id: String,

    /// Is this resource stored stand-off in an external file via @include? This holds the filename.
    #[n(2)]
    filename: Option<String>,

    /// The complete textual content of the resource
    #[n(3)]
    text: String,

    /// Length of the text in unicode points
    #[n(4)]
    textlen: usize,

    /// Flags if the text contents have changed, if so, they need to be reserialised if stored via the include mechanism
    #[n(5)]
    #[cbor(
        encode_with = "cbor_encode_changed",
        decode_with = "cbor_decode_changed"
    )]
    changed: Arc<RwLock<bool>>, //this is modified via internal mutability

    /// A store of text selections (not in textual order)
    #[n(6)]
    textselections: Store<TextSelection>,

    /// Maps character positions to utf8 bytes and to text selections
    #[n(7)]
    positionindex: PositionIndex,

    /// Reverse position index, maps utf8 bytes to character positions (and nothing more)
    #[n(8)]
    byte2charmap: BTreeMap<usize, usize>,

    #[data_size(skip)]
    #[n(9)]
    config: Config,
}

#[derive(Deserialize, Debug, Default)]
pub struct TextResourceBuilder {
    /// Public identifier for the text resource (often the filename/URL)
    #[serde(rename = "@id")]
    id: Option<String>,
    text: Option<String>,

    /// Associates an external resource with the text resource.
    /// if we have a filename but no text, the include is still to be parsed.
    #[serde(rename = "@include")]
    filename: Option<String>,

    #[serde(skip)]
    config: Config,
}

impl TryFrom<TextResourceBuilder> for TextResource {
    type Error = StamError;

    fn try_from(builder: TextResourceBuilder) -> Result<Self, StamError> {
        debug(&builder.config, || {
            format!("TryFrom<TextResourceBuilder for TextResource>: Creation of TextResource from builder (done)")
        });

        //do we need to resolve an @include?
        let mut includebuilder: Option<TextResourceBuilder> = None;
        if builder.text.is_none() {
            if let Some(filename) = &builder.filename {
                // we have a filename but no text, that means the include has to be resolved still
                // we load the resource from the external file into a new builder and
                // merge it with this one at the end of this function
                includebuilder = Some(TextResourceBuilder::from_file(
                    filename.as_str(),
                    builder.config.clone(),
                )?);
            }
        }

        let textlen = if let Some(text) = &builder.text {
            text.chars().count()
        } else if let Some(includebuilder) = includebuilder.as_ref() {
            includebuilder
                .text
                .as_ref()
                .map(|s| s.chars().count())
                .unwrap_or(0)
        } else {
            0
        };
        Ok(Self {
            intid: None,
            id: if let Some(id) = builder.id {
                id
            } else if includebuilder.is_some() && includebuilder.as_ref().unwrap().id.is_some() {
                includebuilder.as_ref().unwrap().id.clone().unwrap()
            } else if let Some(filename) = builder.filename.as_ref() {
                filename.clone()
            } else {
                return Err(StamError::NoIdError("Expected an ID for resource"));
            },
            text: if let Some(text) = builder.text {
                text
            } else if let Some(includebuilder) = includebuilder {
                //this consumes the includebuilder
                includebuilder
                    .text
                    .ok_or_else(|| StamError::NoText("Included resource has no text"))?
            } else {
                String::new()
            },
            textlen,
            positionindex: PositionIndex::default(),
            byte2charmap: BTreeMap::default(),
            textselections: Store::default(),
            config: builder.config,
            filename: builder.filename,
            changed: Arc::new(RwLock::new(false)),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, DataSize, Encode, Decode)]
#[cbor(transparent)]
pub struct TextResourceHandle(#[n(0)] u32);
#[sealed]
impl Handle for TextResourceHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u32)
    }
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> Request<TextResource> for TextResourceHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<TextResourceHandle>
    where
        S: StoreFor<TextResource>,
    {
        Some(*self)
    }
}

impl<'a> From<&TextResourceHandle> for BuildItem<'a, TextResource> {
    fn from(handle: &TextResourceHandle) -> Self {
        BuildItem::Handle(*handle)
    }
}
impl<'a> From<Option<&TextResourceHandle>> for BuildItem<'a, TextResource> {
    fn from(handle: Option<&TextResourceHandle>) -> Self {
        if let Some(handle) = handle {
            BuildItem::Handle(*handle)
        } else {
            BuildItem::None
        }
    }
}
impl<'a> From<TextResourceHandle> for BuildItem<'a, TextResource> {
    fn from(handle: TextResourceHandle) -> Self {
        BuildItem::Handle(handle)
    }
}
impl<'a> From<Option<TextResourceHandle>> for BuildItem<'a, TextResource> {
    fn from(handle: Option<TextResourceHandle>) -> Self {
        if let Some(handle) = handle {
            BuildItem::Handle(handle)
        } else {
            BuildItem::None
        }
    }
}

#[sealed]
impl TypeInfo for TextResource {
    fn typeinfo() -> Type {
        Type::TextResource
    }
}

#[sealed]
impl TypeInfo for TextResourceBuilder {
    fn typeinfo() -> Type {
        Type::TextResource
    }
}

impl ToJson for TextResource {}

#[sealed]
impl Storable for TextResource {
    type HandleType = TextResourceHandle;
    type StoreType = AnnotationStore;

    fn id(&self) -> Option<&str> {
        Some(self.id.as_str())
    }
    fn handle(&self) -> Option<TextResourceHandle> {
        self.intid
    }
    fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }
    fn set_handle(&mut self, handle: TextResourceHandle) {
        self.intid = Some(handle);
    }

    fn carries_id() -> bool {
        true
    }
    fn set_id(&mut self, id: Option<String>) {
        self.id = id.expect("can't reset ID for resources, they must always have a public ID");
    }
}

impl Serialize for TextResource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TextResource", 2)?;
        state.serialize_field("@type", "TextResource")?;
        if self.filename.is_some() && self.config.serialize_mode() == SerializeMode::AllowInclude {
            let filename = self.filename.as_ref().unwrap();
            if self.id() != Some(&filename) {
                state.serialize_field("@id", &self.id())?;
            }
            state.serialize_field("@include", &filename)?;
            //if there are any changes, we write to the standoff file
            if self.changed() {
                if filename.ends_with(".json") {
                    let result = self.to_json_file(&filename, self.config()); //this reinvokes this function after setting config.standoff_include
                    result.map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
                } else {
                    //plain text
                    std::fs::write(filename, &self.text)
                        .map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
                }
                self.mark_unchanged();
            }
        } else {
            state.serialize_field("@id", &self.id())?;
            state.serialize_field("text", &self.text())?;
        }
        state.end()
    }
}

#[sealed]
impl ChangeMarker for TextResource {
    fn change_marker(&self) -> &Arc<RwLock<bool>> {
        &self.changed
    }
}

impl PartialEq<TextResource> for TextResource {
    fn eq(&self, other: &TextResource) -> bool {
        self.id == other.id && self.text == other.text
    }
}

impl FromJson for TextResourceBuilder {
    /// Loads a Text Resource from a STAM JSON  or plain text file file.
    /// If the file is JSON, it file must contain a single object which has "@type": "TextResource"
    /// If `include` is true, the file will be included via the `@include` mechanism, and is kept external upon serialization
    fn from_json_file(filename: &str, config: Config) -> Result<Self, StamError> {
        let reader = open_file_reader(filename, &config)?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);
        let mut result: Result<TextResourceBuilder, _> =
            serde_path_to_error::deserialize(deserializer);
        if result.is_ok() && config.use_include {
            let result = result.as_mut().unwrap();
            result.filename = Some(filename.to_string()); //always uses the original filename (not the found one)
            result.config = config;
        }
        result.map_err(|e| {
            StamError::JsonError(e, filename.to_string(), "Reading text resource from file")
        })
    }

    /// Loads a text resource from a STAM JSON string
    /// The string must contain a single object which has "@type": "TextResource"
    fn from_json_str(string: &str, config: Config) -> Result<Self, StamError> {
        let deserializer = &mut serde_json::Deserializer::from_str(string);
        let mut result: Result<TextResourceBuilder, _> =
            serde_path_to_error::deserialize(deserializer);
        if result.is_ok() {
            let result = result.as_mut().unwrap();
            result.config = config;
        }
        result.map_err(|e| {
            StamError::JsonError(e, string.to_string(), "Reading text resource from string")
        })
    }
}

impl TextResourceBuilder {
    pub fn new() -> Self {
        TextResourceBuilder::default()
    }

    fn text_from_file(filename: &str, config: &Config) -> Result<String, StamError> {
        let mut f = open_file(filename, config)?;
        let mut text: String = String::new();
        if let Err(err) = f.read_to_string(&mut text) {
            return Err(StamError::IOError(
                err,
                filename.to_owned(),
                "TextResourceBuilder::from_txt_file",
            ));
        }
        Ok(text)
    }

    /// Loads a resource from text file
    pub fn from_txt_file(filename: &str, config: Config) -> Result<Self, StamError> {
        //plain text
        debug(&config, || {
            format!("TextResourceBuilder::from_txt_file: filename={}", filename)
        });
        let text = Self::text_from_file(filename, &config)?;
        Ok(Self {
            id: Some(filename.to_string()),
            text: Some(text),
            filename: Some(filename.to_string()),
            config,
        })
    }

    /// Load a resource from file. The extension determines the type.
    pub fn from_file(filename: &str, config: Config) -> Result<Self, StamError> {
        if filename.ends_with(".json") {
            Self::from_json_file(filename, config)
        } else {
            Self::from_txt_file(filename, config)
        }
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn with_filename(mut self, filename: impl Into<String>) -> Self {
        self.filename = Some(filename.into());
        self
    }

    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    ///Builds a new [`TextResource`] from [`TextResourceBuilder'], consuming the latter
    pub fn build(self) -> Result<TextResource, StamError> {
        debug(&self.config, || format!("TextResourceBuilder::build"));
        let mut res: TextResource = self.try_into()?;
        res.textlen = res.text.chars().count();
        if res.config().milestone_interval > 0 {
            res.create_milestones(res.config().milestone_interval)
        }
        Ok(res)
    }
}

impl TextResource {
    /// Instantiates a new completely empty TextResource
    pub fn new(id: impl Into<String>, config: Config) -> Self {
        Self {
            id: id.into(),
            intid: None,
            text: String::new(),
            textlen: 0,
            filename: None,
            changed: Arc::new(RwLock::new(false)),
            positionindex: PositionIndex::default(),
            byte2charmap: BTreeMap::new(),
            textselections: Store::default(),
            config,
        }
    }

    pub fn builder() -> TextResourceBuilder {
        TextResourceBuilder::default()
    }

    /// Create a new TextResource from file, the text will be loaded into memory entirely
    pub fn from_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "TextResourceBuilder::from_file: filename={:?} config={:?}",
                filename, config
            )
        });
        TextResourceBuilder::from_file(filename, config)?.build()
    }

    /// Loads a text for the TextResource from file (STAM JSON or plain text), the text will be loaded into memory entirely
    /// The use of [`Self.from_file()`] is preferred instead. This method can be dangerous
    /// if it modifies any existing text of a resource.
    #[allow(unused_assignments)]
    pub fn with_file(mut self, filename: &str, config: Config) -> Result<Self, StamError> {
        self.check_mutation();
        self = TextResourceBuilder::from_file(filename, config)?.build()?;
        Ok(self)
    }

    /// Sets the filename for writing, will force a write to it when the underlying store is serialized.
    /// *CAUTION*: This method does not load a file so it will overwrite any existing file!
    // Use [`Self.from_file()`] or [`Self.with_file()`] instead to load from file.
    pub fn with_filename(mut self, filename: &str) -> Self {
        self.filename = Some(filename.to_string());
        self.mark_changed();
        self
    }

    /// Sets the text of the TextResource from string, kept in memory entirely
    /// The use of [`Self.from_string()`] is preferred instead. This method can be dangerous
    /// if it modifies any existing text of a resource.
    pub fn with_string(mut self, text: impl Into<String>) -> Self {
        self.check_mutation();
        self.text = text.into();
        self.textlen = self.text.chars().count();
        if self.config.milestone_interval > 0 {
            self.create_milestones(self.config.milestone_interval)
        }
        self
    }

    /// Writes a plain text file
    pub fn to_txt_file(&self, filename: &str) -> Result<(), StamError> {
        let mut f = create_file(filename, self.config())?;
        write!(f, "{}", self.text()).map_err(|err| {
            StamError::IOError(err, filename.to_owned(), "TextResource::to_txt_file")
        })?;
        if Some(filename) == self.filename() {
            self.mark_unchanged();
        }
        Ok(())
    }

    /// Check if there is already text associated, if so, this is a mutation and some indices will be invalidated
    fn check_mutation(&mut self) -> bool {
        if !self.text.is_empty() {
            // in case we change an existing text
            // 1. mark as changed
            self.mark_changed();
            // 2. invalidate all the reverse indices
            if !self.positionindex.0.is_empty() {
                self.positionindex = PositionIndex::default();
            }
            if !self.byte2charmap.is_empty() {
                self.byte2charmap = BTreeMap::new();
            }
            if !self.textselections.is_empty() {
                self.textselections = Vec::new();
            }
            true
        } else {
            false
        }
    }

    /// Create a new TextResource from string, kept in memory entirely
    pub fn from_string(id: impl Into<String>, text: impl Into<String>, config: Config) -> Self {
        let text = text.into();
        let textlen = text.chars().count();
        let mut resource = TextResource {
            id: id.into(),
            text: text.into(),
            intid: None,
            filename: None,
            changed: Arc::new(RwLock::new(false)),
            textlen,
            positionindex: PositionIndex::default(),
            byte2charmap: BTreeMap::new(),
            textselections: Store::default(),
            config,
        };
        if resource.config.milestone_interval > 0 {
            resource.create_milestones(resource.config.milestone_interval)
        }
        resource
    }

    /// Creates milestones (reverse index to facilitate character positions to utf8 byte position lookup and vice versa)
    /// Does initial population of the positionindex.
    fn create_milestones(&mut self, interval: usize) {
        for (charpos, (bytepos, _)) in self.text.char_indices().enumerate() {
            if charpos > 0 && charpos % interval == 0 {
                self.positionindex.0.insert(
                    charpos,
                    PositionIndexItem {
                        bytepos,
                        end2begin: smallvec!(),
                        begin2end: smallvec!(),
                    },
                );
                self.byte2charmap.insert(bytepos, charpos);
            }
        }
    }

    /// Finds a known text selection, as specified by the offset. Known textselections
    /// are associated with an annotation. Returns a handle.
    /// Use the higher-level method [`Self.textselection()`] instead if you want to
    /// return a textselection regardless of whether it's known or not.
    pub fn known_textselection(
        &self,
        offset: &Offset,
    ) -> Result<Option<TextSelectionHandle>, StamError> {
        let (begin, end) = (
            self.beginaligned_cursor(&offset.begin)?,
            self.beginaligned_cursor(&offset.end)?,
        );
        if let Some(beginitem) = self.positionindex.0.get(&begin) {
            for (end2, handle) in beginitem.begin2end.iter() {
                if *end2 == end {
                    return Ok(Some(*handle));
                }
            }
        }
        Ok(None)
    }

    /// Returns an unsorted iterator over all textselections in this resource
    /// Use this only if order doesn't matter for. For a sorted version, use [`Self::iter()`] or [`Self::range()`] instead.
    pub fn textselections(&self) -> impl Iterator<Item = ResultItem<TextSelection>> {
        self.store().iter().filter_map(|item| {
            item.as_ref().map(|textselection| {
                textselection
                    .as_resultitem(self)
                    .expect("Wrap must succeed")
            })
        })
    }

    pub fn textselections_len(&self) -> usize {
        self.store().len()
    }

    /// Returns a sorted double-ended iterator over a range of all textselections and returns all
    /// textselections that either start or end in this range (depending on the direction you're
    /// iterating in)
    pub fn range<'a>(&'a self, begin: usize, end: usize) -> TextSelectionIter<'a> {
        TextSelectionIter {
            iter: self
                .positionindex
                .0
                .range((Included(&begin), Excluded(&end))),
            begin2enditer: None,
            end2beginiter: None,
            resource: self,
        }
    }

    /// returns a sorted double-ended iterator over all textselections in this resource
    /// for unsorted (slightly more performant), use [`textresource::textselections()`] instead.
    pub fn iter<'a>(&'a self) -> TextSelectionIter<'a> {
        self.range(0, self.textlen())
    }

    /// Returns a sorted iterator over all absolute positions (begin aligned cursors) that are in use
    /// By passing a [`PositionMode`] parameter you can specify whether you want only positions where a textselection begins, ends or both.
    pub fn positions<'a>(&'a self, mode: PositionMode) -> Box<dyn Iterator<Item = &'a usize> + 'a> {
        match mode {
            PositionMode::Begin => {
                Box::new(self.positionindex.iter().filter_map(|(k, positem)| {
                    if !positem.begin2end.is_empty() {
                        Some(k)
                    } else {
                        None
                    }
                }))
            }
            PositionMode::End => Box::new(self.positionindex.iter().filter_map(|(k, positem)| {
                if !positem.end2begin.is_empty() {
                    Some(k)
                } else {
                    None
                }
            })),
            PositionMode::Both => Box::new(self.positionindex.keys()),
        }
    }

    /// Lookup a position (unicode point) in the PositionIndex. Low-level function.
    /// Only works for positions at which a TextSelection starts or ends (non-inclusive), returns None otherwise
    pub fn position(&self, index: usize) -> Option<&PositionIndexItem> {
        self.positionindex.0.get(&index)
    }

    /// Returns the number of positions in the positionindex
    pub fn positionindex_len(&self) -> usize {
        self.positionindex.0.len()
    }

    /// Returns a text selection by offset.
    /// This is a lower-level method that does not check if the text selection exists, use [`textselection()`]. instead.
    // this is deliberately NOT part of HasText if applied to e.g. TextSelection it would yield TextSelections with relative offsets
    pub(crate) fn textselection_by_offset(
        &self,
        offset: &Offset,
    ) -> Result<TextSelection, StamError> {
        let begin = self.beginaligned_cursor(&offset.begin)?;
        let end = self.beginaligned_cursor(&offset.end)?;
        if begin > self.textlen() {
            Err(StamError::CursorOutOfBounds(
                Cursor::BeginAligned(begin),
                "Begin cursor is out of bounds",
            ))
            //note: we do > instead of >=  because we allow a zero-size textselection just past the very end of the end (begin == end == self.textlen())
        } else if end > self.textlen() {
            Err(StamError::CursorOutOfBounds(
                Cursor::BeginAligned(end),
                "End cursor is out of bounds",
            ))
        } else if end >= begin {
            Ok(TextSelection {
                intid: None,
                begin,
                end,
            })
        } else {
            Err(StamError::InvalidOffset(
                offset.begin,
                offset.end,
                "End must be greater than begin",
            ))
        }
    }

    /// Returns a lower-bound estimate of memory usage in bytes
    pub fn meminfo(&self) -> usize {
        return data_size(self);
    }

    pub fn shrink_to_fit(&mut self) {
        self.textselections.shrink_to_fit();
    }
}

impl<'store, 'slf> ResultItem<'store, TextResource> {
    /// Returns all annotations that reference any text selection in the resource. Use
    /// [`Self.annotations_metadata()`] instead if you are looking for annotations that reference
    /// the resource as is via a ResourceSelector. These are **NOT** include here.
    pub fn annotations(
        &'slf self,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + '_> {
        if let Some(iter) = self.store().annotations_by_resource(self.handle()) {
            Some(iter.filter_map(|a_handle| self.store().annotation(a_handle)))
        } else {
            None
        }
    }

    /// This only returns annotations that directly point at the resource, i.e. are metadata for it. It does not include annotations that
    /// point at a text in the resource, use [`Self.annotations_by_resource()`] instead for those.
    pub fn annotations_metadata(
        &'slf self,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + '_> {
        if let Some(vec) = self.store().annotations_by_resource_metadata(self.handle()) {
            Some(
                vec.iter()
                    .filter_map(|a_handle| self.store().annotation(*a_handle)),
            )
        } else {
            None
        }
    }

    pub fn textselections(&'slf self) -> impl Iterator<Item = ResultItem<'store, TextSelection>> {
        self.as_ref().textselections()
    }

    pub fn textselections_len(&self) -> usize {
        self.as_ref().textselections_len()
    }

    /// Returns a sorted double-ended iterator over a range of all textselections and returns all
    /// textselections that either start or end in this range (depending on the direction you're
    /// iterating in)
    pub fn range<'a>(&'a self, begin: usize, end: usize) -> TextSelectionIter<'a> {
        self.as_ref().range(begin, end)
    }

    /// returns a sorted double-ended iterator over all textselections in this resource
    /// for unsorted (slightly more performant), use [`textresource::textselections()`] instead.
    pub fn iter<'a>(&'a self) -> TextSelectionIter<'a> {
        self.as_ref().iter()
    }

    /// Returns a sorted iterator over all absolute positions (begin aligned cursors) that are in use
    /// By passing a [`PositionMode`] parameter you can specify whether you want only positions where a textselection begins, ends or both.
    pub fn positions<'a>(&'a self, mode: PositionMode) -> Box<dyn Iterator<Item = &'a usize> + 'a> {
        self.as_ref().positions(mode)
    }

    /// Lookup a position (unicode point) in the PositionIndex. Low-level function.
    /// Only works for positions at which a TextSelection starts or ends (non-inclusive), returns None otherwise
    pub fn position(&self, index: usize) -> Option<&PositionIndexItem> {
        self.as_ref().position(index)
    }

    /// Returns the number of positions in the positionindex
    pub fn positionindex_len(&self) -> usize {
        self.as_ref().positionindex_len()
    }
}

// expose methods on higher-level for convenience:
impl<'store> Text<'store, 'store> for ResultItem<'store, TextResource> {
    /// Returns the length of the text in unicode points
    /// For bytes, use `self.text().len()` instead.
    fn textlen(&self) -> usize {
        self.as_ref().textlen()
    }

    /// Returns a reference to the full text of this resource
    fn text(&'store self) -> &'store str {
        self.as_ref().text()
    }

    /// Returns a string reference to a slice of text as specified by the offset
    fn text_by_offset(&'store self, offset: &Offset) -> Result<&'store str, StamError> {
        self.as_ref().text_by_offset(offset)
    }

    fn absolute_cursor(&self, cursor: usize) -> usize {
        cursor
    }

    /// Resolves a begin aligne cursor to UTF-8 byteposition
    /// If you have a Cursor instance, pass it through [`Self.beginaligned_cursor()`] first.
    fn utf8byte(&self, abscursor: usize) -> Result<usize, StamError> {
        self.as_ref().utf8byte(abscursor)
    }

    /// Convert utf8 byte to unicode point. O(n), not as efficient as the reverse operation in ['utf8byte()`]
    fn utf8byte_to_charpos(&self, bytecursor: usize) -> Result<usize, StamError> {
        self.as_ref().utf8byte_to_charpos(bytecursor)
    }

    /// Returns a [`TextSelection'] that corresponds to the offset. If the TextSelection
    /// exists, the existing one will be returned.
    /// If it doesn't exist yet, a new one will be returned, and it won't have a handle, nor will it be added to the store automatically.
    ///
    /// The [`TextSelection`] is returned in a fat pointer (`WrappedTextSelection`) that also contains reference to the underlying store.
    fn textselection(
        &'store self,
        offset: &Offset,
    ) -> Result<ResultTextSelection<'store>, StamError> {
        self.as_ref().textselection(offset)
    }

    /// Searches the text using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// is held by the [`FindRegexMatch'] struct.
    ///
    /// Passing multiple regular expressions at once is more efficient than calling this function anew for each one.
    /// If capture groups are used in the regular expression, only those parts will be returned (the rest is context). If none are used,
    /// the entire expression is returned.
    ///
    /// The `allow_overlap` parameter determines if the matching expressions are allowed to
    /// overlap. It you are doing some form of tokenisation, you also likely want this set to
    /// false. All of this only matters if you supply multiple regular expressions.
    ///
    /// Results are returned in the exact order they are found in the text
    fn find_text_regex<'regex>(
        &'store self,
        expressions: &'regex [Regex],
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<FindRegexIter<'store, 'regex>, StamError> {
        self.as_ref()
            .find_text_regex(expressions, precompiledset, allow_overlap)
    }

    /// Searches for the specified text fragment. Returns an iterator to iterate over all matches in the text.
    /// The iterator returns [`TextSelection`] items.
    ///
    /// This search is case sensitive, use [`Self.find_text_nocase()`] to search case insensitive.
    /// For more complex and powerful searching use [`Self.find_text_regex()`] instead
    ///
    /// If you want to search only a subpart of the text, extract a ['TextSelection`] first with
    /// [`Self.textselection()`] and then run `find_text()` on that instead.
    fn find_text<'fragment>(
        &'store self,
        fragment: &'fragment str,
    ) -> FindTextIter<'store, 'fragment> {
        self.as_ref().find_text(fragment)
    }

    /// Searches for the specified text fragment. Returns an iterator to iterate over all matches in the text.
    /// The iterator returns [`TextSelection`] items.
    ///
    /// This search is case insensitive, use [`Self.find_text()`] to search case sensitive. This variant is slightly less performant than the exact variant.
    /// For more complex and powerful searching use [`Self.find_text_regex()`] instead
    ///
    /// If you want to search only a subpart of the text, extract a ['TextSelection`] first with
    /// [`Self.textselection()`] and then run `find_text_nocase()` on that instead.
    fn find_text_nocase(&'store self, fragment: &str) -> FindNoCaseTextIter<'store> {
        self.as_ref().find_text_nocase(fragment)
    }

    fn split_text<'b>(&'store self, delimiter: &'b str) -> SplitTextIter<'store, 'b> {
        self.as_ref().split_text(delimiter)
    }

    /// Finds the utf-8 byte position where the specified text subslice begins
    fn subslice_utf8_offset(&self, subslice: &str) -> Option<usize> {
        self.as_ref().subslice_utf8_offset(subslice)
    }
}

impl<'store> Text<'store, 'store> for TextResource {
    /// Returns the length of the text in unicode points
    /// For bytes, use `self.text().len()` instead.
    fn textlen(&self) -> usize {
        self.textlen
    }

    /// Returns a reference to the full text of this resource
    fn text(&'store self) -> &'store str {
        self.text.as_str()
    }

    /// Returns a string reference to a slice of text as specified by the offset
    fn text_by_offset(&'store self, offset: &Offset) -> Result<&'store str, StamError> {
        let beginbyte = self.utf8byte(self.beginaligned_cursor(&offset.begin)?)?;
        let endbyte = self.utf8byte(self.beginaligned_cursor(&offset.end)?)?;
        if endbyte < beginbyte {
            Err(StamError::InvalidOffset(
                Cursor::BeginAligned(beginbyte),
                Cursor::BeginAligned(endbyte),
                "End must be greater than begin. (Cursor should be interpreted as UTF-8 bytes in this error context only)",
            ))
        } else {
            Ok(&self.text()[beginbyte..endbyte])
        }
    }

    fn absolute_cursor(&self, cursor: usize) -> usize {
        cursor
    }

    /// Resolves a begin aligne cursor to UTF-8 byteposition
    /// If you have a Cursor instance, pass it through [`Self.beginaligned_cursor()`] first.
    fn utf8byte(&self, abscursor: usize) -> Result<usize, StamError> {
        if let Some(posindexitem) = self.positionindex.0.get(&abscursor) {
            //exact position is in the position index, return the byte
            Ok(posindexitem.bytepos)
        } else {
            // Get the item previous to abscursor using a double ended range iterator
            if let Some((before_pos, posindexitem)) = self
                .positionindex
                .0
                .range((Included(&0), Excluded(&abscursor)))
                .next_back()
            {
                let before_bytepos = posindexitem.bytepos;
                let textslice = &self.text[before_bytepos..];
                if self.textlen == abscursor {
                    //non-inclusive end is also a valid point to return
                    return Ok(before_bytepos + textslice.len());
                }
                // now we just count characters and keep track of the bytes they take,
                // if everything went well, we have only a minimum amount to count
                // at most the O(n) where n is config.milestone_interval
                for (charpos, (bytepos, _)) in textslice.char_indices().enumerate() {
                    if before_pos + charpos == abscursor {
                        return Ok(before_bytepos + bytepos);
                    }
                }
            } else {
                //fallback, position index has no useful entries (config.mileston_interval < textlen?), search from 0
                if self.textlen == abscursor {
                    //non-inclusive end is also a valid point to return
                    return Ok(self.text().len());
                }
                for (charpos, (bytepos, _)) in self.text().char_indices().enumerate() {
                    if charpos == abscursor {
                        return Ok(bytepos);
                    }
                }
            }
            Err(StamError::CursorOutOfBounds(
                Cursor::BeginAligned(abscursor),
                "TextResource::utf8byte()",
            ))
        }
    }

    /// Convert utf8 byte to unicode point. O(n), not as efficient as the reverse operation in ['utf8byte()`]
    fn utf8byte_to_charpos(&self, bytecursor: usize) -> Result<usize, StamError> {
        if let Some(charpos) = self.byte2charmap.get(&bytecursor) {
            //exact byte position is in the index, return the position
            Ok(*charpos)
        } else {
            // Get the item previous to bytecursor using a double ended range iterator
            if let Some((before_bytepos, before_charpos)) = self
                .byte2charmap
                .range((Included(&0), Excluded(&bytecursor)))
                .next_back()
            {
                let textslice = &self.text[*before_bytepos..];
                if before_bytepos + textslice.len() == bytecursor {
                    //non-inclusive end is also a valid point to return
                    return Ok(self.textlen);
                }
                // now we just count characters and keep track of the bytes they take,
                // if everything went well, we have only a minimum amount to count
                // at most the O(n) where n is config.milestone_interval
                for (charpos, (bytepos, _)) in textslice.char_indices().enumerate() {
                    if before_bytepos + bytepos == bytecursor {
                        return Ok(before_charpos + charpos);
                    }
                }
            } else {
                //fallback, position index has no useful entries (config.mileston_interval < textlen?), search from 0
                if self.text().len() == bytecursor {
                    //non-inclusive end is also a valid point to return
                    return Ok(self.textlen);
                }
                for (charpos, (bytepos, _)) in self.text().char_indices().enumerate() {
                    if bytepos == bytecursor {
                        return Ok(charpos);
                    }
                }
            }
            Err(StamError::CursorOutOfBounds(
                Cursor::BeginAligned(bytecursor),
                "TextResource::utf8byte_to_charpos() (cursor value in this error is to be interpreted as a utf-8 byte position in this rare context!!). It is also possible that the UTF-8 byte is not out of bounds but ends up in the middle of a unicodepoint.",
            ))
        }
    }

    /// Returns a [`TextSelection'] that corresponds to the offset. If the TextSelection
    /// exists, the existing one will be returned.
    /// If it doesn't exist yet, a new one will be returned, and it won't have a handle, nor will it be added to the store automatically.
    ///
    /// The [`TextSelection`] is returned in a fat pointer (`WrappedTextSelection`) that also contains reference to the underlying store.
    fn textselection(
        &'store self,
        offset: &Offset,
    ) -> Result<ResultTextSelection<'store>, StamError> {
        match self.known_textselection(offset) {
            Ok(Some(handle)) => {
                //existing textselection
                let textselection: &TextSelection = self.get(handle)?; //shouldn't fail here anymore
                let wrapped = textselection.as_resultitem(self)?;
                Ok(ResultTextSelection::Bound(wrapped))
            }
            Ok(None) => {
                let textselection: TextSelection = self.textselection_by_offset(offset)?;
                Ok(ResultTextSelection::Unbound(self, textselection))
            }
            Err(err) => Err(err), //an error occured, propagate
        }
    }

    /// Searches the text using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// is held by the [`FindRegexMatch'] struct.
    ///
    /// Passing multiple regular expressions at once is more efficient than calling this function anew for each one.
    /// If capture groups are used in the regular expression, only those parts will be returned (the rest is context). If none are used,
    /// the entire expression is returned.
    ///
    /// The `allow_overlap` parameter determines if the matching expressions are allowed to
    /// overlap. It you are doing some form of tokenisation, you also likely want this set to
    /// false. All of this only matters if you supply multiple regular expressions.
    ///
    /// Results are returned in the exact order they are found in the text
    fn find_text_regex<'regex>(
        &'store self,
        expressions: &'regex [Regex],
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<FindRegexIter<'store, 'regex>, StamError> {
        debug(self.config(), || {
            format!("find_text_regex: expressions={:?}", expressions)
        });
        let selectexpressions =
            find_text_regex_select_expressions(self.text(), expressions, precompiledset)?;
        //Returns an iterator that does the remainder of the actual searching
        Ok(FindRegexIter {
            resource: self,
            expressions,
            selectexpressions,
            matchiters: Vec::new(),
            nextmatches: Vec::new(),
            text: self.text(),
            begincharpos: 0,
            beginbytepos: 0,
            allow_overlap,
        })
    }

    /// Searches for the specified text fragment. Returns an iterator to iterate over all matches in the text.
    /// The iterator returns [`TextSelection`] items.
    ///
    /// This search is case sensitive, use [`Self.find_text_nocase()`] to search case insensitive.
    /// For more complex and powerful searching use [`Self.find_text_regex()`] instead
    ///
    /// If you want to search only a subpart of the text, extract a ['TextSelection`] first with
    /// [`Self.textselection()`] and then run `find_text()` on that instead.
    fn find_text<'fragment>(
        &'store self,
        fragment: &'fragment str,
    ) -> FindTextIter<'store, 'fragment> {
        FindTextIter {
            resource: self,
            fragment,
            offset: Offset::whole(),
        }
    }

    /// Searches for the specified text fragment. Returns an iterator to iterate over all matches in the text.
    /// The iterator returns [`TextSelection`] items.
    ///
    /// This search is case insensitive, use [`Self.find_text()`] to search case sensitive. This variant is slightly less performant than the exact variant.
    /// For more complex and powerful searching use [`Self.find_text_regex()`] instead
    ///
    /// If you want to search only a subpart of the text, extract a ['TextSelection`] first with
    /// [`Self.textselection()`] and then run `find_text_nocase()` on that instead.
    fn find_text_nocase(&'store self, fragment: &str) -> FindNoCaseTextIter<'store> {
        FindNoCaseTextIter {
            resource: self,
            fragment: fragment.to_lowercase(),
            offset: Offset::whole(),
        }
    }

    fn split_text<'b>(&'store self, delimiter: &'b str) -> SplitTextIter<'store, 'b> {
        SplitTextIter {
            resource: self,
            iter: self.text().split(delimiter),
            byteoffset: 0,
        }
    }

    /// Finds the utf-8 byte position where the specified text subslice begins
    fn subslice_utf8_offset(&self, subslice: &str) -> Option<usize> {
        let self_begin = self.text().as_ptr() as usize;
        let sub_begin = subslice.as_ptr() as usize;
        if sub_begin < self_begin || sub_begin > self_begin.wrapping_add(self.text().len()) {
            None
        } else {
            Some(sub_begin.wrapping_sub(self_begin))
        }
    }
}

pub enum PositionMode {
    /// Select only positions where a text selection begins
    Begin,
    /// Select only positions where a text selection ends
    End,
    /// Select all positions where a text selection begins or ends
    Both,
}

impl Configurable for TextResource {
    fn config(&self) -> &Config {
        &self.config
    }

    fn config_mut(&mut self) -> &mut Config {
        &mut self.config
    }

    fn set_config(&mut self, config: Config) -> &mut Self {
        self.config = config;
        self
    }
}

#[sealed]
impl AssociatedFile for TextResource {
    /// Get the filename for stand-off file specified using @include (if any)
    fn filename(&self) -> Option<&str> {
        self.filename.as_ref().map(|x| x.as_str())
    }

    /// Get the filename for stand-off file specified using @include (if any)
    fn set_filename(&mut self, filename: &str) -> &mut Self {
        self.filename = Some(filename.into());
        self
    }
}

//An TextResource is a StoreFor TextSelection
#[sealed]
impl StoreFor<TextSelection> for TextResource {
    /// Get a reference to the entire store for the associated type
    fn store(&self) -> &Store<TextSelection> {
        &self.textselections
    }
    /// Get a mutable reference to the entire store for the associated type
    fn store_mut(&mut self) -> &mut Store<TextSelection> {
        &mut self.textselections
    }
    /// Get a reference to the id map for the associated type, mapping global ids to internal ids
    fn idmap(&self) -> Option<&IdMap<TextSelectionHandle>> {
        None
    }
    /// Get a mutable reference to the id map for the associated type, mapping global ids to internal ids
    fn idmap_mut(&mut self) -> Option<&mut IdMap<TextSelectionHandle>> {
        None
    }
    fn store_typeinfo() -> &'static str {
        "TextSelection in TextResource"
    }

    fn inserted(&mut self, handle: TextSelectionHandle) -> Result<(), StamError> {
        //called after a TextSelection gets inserted into this Store
        //update the PositionIndex
        let textselection: &TextSelection = self.get(handle)?;
        let begin = textselection.begin();
        let end = textselection.end();
        let beginbyte = self.utf8byte(begin)?; //MAYBE TODO: move this inside closure? (not done now because it violates borrow checker)
        let endbyte = self.utf8byte(end)?;

        self.positionindex
            .0
            .entry(begin)
            .and_modify(|positem| {
                if !positem.begin2end.contains(&(end, handle)) {
                    positem.begin2end.push((end, handle))
                }
            })
            .or_insert_with(|| PositionIndexItem {
                bytepos: beginbyte,
                begin2end: smallvec!((end, handle)),
                end2begin: smallvec!(),
            });
        self.positionindex
            .0
            .entry(end)
            .and_modify(|positem| {
                if !positem.end2begin.contains(&(begin, handle)) {
                    positem.end2begin.push((begin, handle))
                }
            })
            .or_insert_with(|| PositionIndexItem {
                bytepos: endbyte,
                end2begin: smallvec!((begin, handle)),
                begin2end: smallvec!(),
            });
        self.byte2charmap.entry(beginbyte).or_insert(begin);
        self.byte2charmap.entry(endbyte).or_insert(end);
        Ok(())
    }
}

impl SelfSelector for TextResource {
    /// Returns a selector to this resource
    fn selector(&self) -> Result<Selector, StamError> {
        if let Some(intid) = self.handle() {
            Ok(Selector::ResourceSelector(intid))
        } else {
            Err(StamError::Unbound("TextResource::self_selector()"))
        }
    }
}

/// This iterator is used for iterating over TextSelections in a resource in a sorted fashion
/// using the so-called position index.
pub struct TextSelectionIter<'a> {
    iter: btree_map::Range<'a, usize, PositionIndexItem>, //btree_map::Iter
    begin2enditer: Option<Iter<'a, (usize, TextSelectionHandle)>>,
    end2beginiter: Option<Iter<'a, (usize, TextSelectionHandle)>>,
    resource: &'a TextResource,
}

impl<'a> Iterator for TextSelectionIter<'a> {
    type Item = ResultItem<'a, TextSelection>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(begin2enditer) = &mut self.begin2enditer {
                if let Some((_end, handle)) = begin2enditer.next() {
                    let textselection: &TextSelection =
                        self.resource.get(*handle).expect("handle must exist");
                    return Some(
                        textselection
                            .as_resultitem(self.resource)
                            .expect("wrap must succeed"),
                    );
                }
                //fall back to final clause
            } else {
                if let Some((_begin, posindexitem)) = self.iter.next() {
                    self.begin2enditer = Some(posindexitem.begin2end.iter());
                    continue;
                } else {
                    return None;
                }
            }
            //final clause
            self.begin2enditer = None;
        }
    }
}

impl<'a> DoubleEndedIterator for TextSelectionIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(end2beginiter) = &mut self.end2beginiter {
                if let Some((_begin, handle)) = end2beginiter.next() {
                    let textselection: &TextSelection =
                        self.resource.get(*handle).expect("handle must exist");
                    return Some(
                        textselection
                            .as_resultitem(self.resource)
                            .expect("wrap must succeed"),
                    );
                }
                //fall back to final clause
            } else {
                if let Some((_, posindexitem)) = self.iter.next_back() {
                    self.end2beginiter = Some(posindexitem.end2begin.iter());
                    continue;
                } else {
                    return None;
                }
            }
            //final clause
            self.end2beginiter = None;
        }
    }
}

#[derive(Debug)]
pub(crate) struct DeserializeTextResource<'a> {
    config: &'a Config,
}

impl<'a> DeserializeTextResource<'a> {
    pub fn new(config: &'a Config) -> Self {
        Self { config }
    }
}

impl<'de> DeserializeSeed<'de> for DeserializeTextResource<'_> {
    type Value = TextResource;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut builder: TextResourceBuilder = Deserialize::deserialize(deserializer)?;
        //inject the config
        builder.config = self.config.clone();
        builder
            .build()
            .map_err(|e| -> D::Error { serde::de::Error::custom(e) })
    }
}

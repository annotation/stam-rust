use std::collections::btree_map;
use std::collections::BTreeMap;
use std::io::prelude::*;
use std::ops::Bound::{Excluded, Included};
use std::slice::Iter;
use std::sync::{Arc, RwLock};

use sealed::sealed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;

use crate::config::{get_global_config, Config, Configurable, SerializeMode};
use crate::error::StamError;
use crate::file::*;
use crate::json::{FromJson, ToJson};
use crate::selector::{Offset, Selector, SelfSelector};
use crate::store::*;
use crate::textselection::PositionIndexItem;
use crate::textselection::{PositionIndex, TextSelection, TextSelectionHandle};
use crate::types::*;

/// This holds the textual resource to be annotated. It holds the full text in memory.
///
/// The text *SHOULD* be in
/// [Unicode Normalization Form C (NFC)](https://www.unicode.org/reports/tr15/) but
/// *MAY* be in another unicode normalization forms.
#[derive(Deserialize, Debug)]
#[serde(try_from = "TextResourceBuilder")]
pub struct TextResource {
    /// Public identifier for the text resource (often the filename/URL)
    id: String,

    /// The complete textual content of the resource
    text: String,

    /// The internal numeric identifier for the resource (may only be None upon creation when not bound yet)
    intid: Option<TextResourceHandle>,

    /// Is this resource stored stand-off in an external file via @include? This holds the filename.
    #[serde(skip)]
    filename: Option<String>,

    /// Flags if the text contents have changed, if so, they need to be reserialised if stored via the include mechanism
    #[serde(skip)]
    changed: Arc<RwLock<bool>>, //this is modified via internal mutability

    /// Length of the text in unicode points
    #[serde(skip)]
    textlen: usize,

    #[serde(skip)]
    textselections: Store<TextSelection>,

    /// Maps character positions to utf8 bytes and to text selections
    #[serde(skip)]
    positionindex: PositionIndex,

    /// Reverse position index, maps utf8 bytes to character positions (and nothing more)
    #[serde(skip)]
    byte2charmap: BTreeMap<usize, usize>,

    #[serde(skip, default = "get_global_config")]
    config: Config,
}

#[derive(Deserialize, Debug)]
pub struct TextResourceBuilder {
    /// Public identifier for the text resource (often the filename/URL)
    #[serde(rename = "@id")]
    id: Option<String>,
    text: Option<String>,

    /// Associates an external resource with the text resource.
    /// `mode` determines whether it is still to be parsed.
    #[serde(rename = "@include")]
    filename: Option<String>,

    /// Sets mode for deserialisation (whether to follow @include statements)
    #[serde(skip)]
    mode: SerializeMode,

    #[serde(skip, default = "get_global_config")]
    config: Config,
}

impl TryFrom<TextResourceBuilder> for TextResource {
    type Error = StamError;

    fn try_from(builder: TextResourceBuilder) -> Result<Self, StamError> {
        debug(&builder.config, || {
            format!("TryFrom<TextResourceBuilder for TextResource>: Creation of TextResource from builder (done)")
        });
        let textlen = if let Some(text) = &builder.text {
            text.chars().count()
        } else {
            0
        };
        Ok(Self {
            intid: None,
            id: if let Some(id) = builder.id {
                id
            } else if let Some(filename) = builder.filename.as_ref() {
                filename.clone()
            } else {
                return Err(StamError::NoIdError("Expected an ID for resource"));
            },
            text: if let Some(text) = builder.text {
                text
            } else {
                String::new()
            },
            textlen,
            positionindex: PositionIndex::default(),
            byte2charmap: BTreeMap::default(),
            textselections: Store::default(),
            config: builder.config,
            //note: includes have to be resolved in a later stage via [`AnnotationStore.process_includes()`]
            //      we don't do it here as we don't have state information from the deserializer (believe me, I tried)
            filename: builder.filename.clone(),
            changed: Arc::new(RwLock::new(false)),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TextResourceHandle(u32);
#[sealed]
impl Handle for TextResourceHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u32)
    }
    fn unwrap(&self) -> usize {
        self.0 as usize
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

    fn id(&self) -> Option<&str> {
        Some(self.id.as_str())
    }
    fn handle(&self) -> Option<TextResourceHandle> {
        self.intid
    }
    fn with_id(mut self, id: String) -> Self {
        self.id = id;
        self
    }
    fn set_handle(&mut self, handle: TextResourceHandle) {
        self.intid = Some(handle);
    }

    fn carries_id() -> bool {
        true
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

impl<'a> FromJson<'a> for TextResourceBuilder {
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
            result.mode = SerializeMode::NoInclude;
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
    pub fn from_txt_file(filename: &str, config: Config) -> Result<Self, StamError> {
        //plain text
        debug(&config, || {
            format!("TextResourceBuilder::from_txt_file: filename={}", filename)
        });
        let mut f = open_file(filename, &config)?;
        let mut text: String = String::new();
        if let Err(err) = f.read_to_string(&mut text) {
            return Err(StamError::IOError(
                err,
                filename.to_owned(),
                "TextResourceBuilder::from_txt_file",
            ));
        }
        Ok(Self {
            id: Some(filename.to_string()),
            text: Some(text),
            filename: Some(filename.to_string()),
            mode: SerializeMode::NoInclude, //we just processed the include, this instructs the JSON deserialiser not to do it again
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

    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_filename(mut self, filename: &str) -> Self {
        self.filename = Some(filename.to_string());
        self
    }

    pub fn with_text(mut self, text: String) -> Self {
        self.text = Some(text);
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
    pub fn new(id: String, config: Config) -> Self {
        Self {
            id,
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
    /// The use of [`from_file()`] is preferred instead. This method can be dangerous
    /// if it modifies any existing text of a resource.
    #[allow(unused_assignments)]
    pub fn with_file(mut self, filename: &str, config: Config) -> Result<Self, StamError> {
        self.check_mutation();
        self = TextResourceBuilder::from_file(filename, config)?.build()?;
        Ok(self)
    }

    /// Sets the text of the TextResource from string, kept in memory entirely
    /// The use of [`from_string()`] is preferred instead. This method can be dangerous
    /// if it modifies any existing text of a resource.
    pub fn with_string(mut self, text: String) -> Self {
        self.check_mutation();
        self.text = text;
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

    /// Returns the length of the text in unicode points
    /// For bytes, use `self.text().len()` instead.
    pub fn textlen(&self) -> usize {
        self.textlen
    }

    /// Create a new TextResource from string, kept in memory entirely
    pub fn from_string(id: String, text: String, config: Config) -> Self {
        let textlen = text.chars().count();
        let mut resource = TextResource {
            id,
            text,
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

    /// Returns a reference to the full text of this resource
    pub fn text(&self) -> &str {
        self.text.as_str()
    }

    /// Returns a [`TextSelection'] that corresponds to the offset. If the TextSelection
    /// exists, the existing one will be returned (as a copy, but it will have a `TextSelection.handle()`).
    /// If it doesn't exist yet, a new one will be returned, and it won't have a handle, nor will it be added to the store automatically.
    ///
    /// Use [`find_textselection()`] instead if you want to limit to existing text selections only.
    pub fn textselection(&self, offset: &Offset) -> Result<TextSelection, StamError> {
        match self.find_textselection(offset) {
            Ok(Some(handle)) => {
                //existing textselection
                let textselection: &TextSelection = self.get(handle)?; //shouldn't fail here anymore
                Ok(textselection.clone()) //clone is relatively cheap
            }
            Ok(None) => {
                //create a new one
                let begin = self.absolute_cursor(&offset.begin)?; //this can't fail because it would have already in find_selection()
                let end = self.absolute_cursor(&offset.end)?;
                if end > begin {
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
            Err(err) => Err(err), //an error occured, propagate
        }
    }

    /// Finds an **existing** text selection**, as specified by the offset. Returns a handle.
    /// by the offset. Use the higher-level method [`Self.textselection()`] instead if you
    /// in most circumstances.
    pub fn find_textselection(
        &self,
        offset: &Offset,
    ) -> Result<Option<TextSelectionHandle>, StamError> {
        let (begin, end) = (
            self.absolute_cursor(&offset.begin)?,
            self.absolute_cursor(&offset.end)?,
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

    /// Returns a string reference to a slice of text as specified by the offset
    /// This is a higher-level variant of [`Self.text_by_textselection()`].
    pub fn text_slice(&self, offset: &Offset) -> Result<&str, StamError> {
        let textselection = self.textselection(offset)?;
        self.text_by_textselection(&textselection)
    }

    /// Returns the text for a given [`TextSelection`]. Make sure the [`TextSelection`] applies to this resource, there are no further checks here.
    /// Use [`Self.text_slice()`] for a higher-level method that takes an offset.
    pub fn text_by_textselection(&self, selection: &TextSelection) -> Result<&str, StamError> {
        let beginbyte = self.utf8byte(selection.begin)?;
        let endbyte = self.utf8byte(selection.end)?;
        Ok(&self.text()[beginbyte..endbyte])
    }

    /// Resolves a cursor to an absolute position (by definition begin aligned)
    pub fn absolute_cursor(&self, cursor: &Cursor) -> Result<usize, StamError> {
        match *cursor {
            Cursor::BeginAligned(cursor) => Ok(cursor),
            Cursor::EndAligned(cursor) => {
                if cursor.abs() as usize > self.textlen {
                    Err(StamError::CursorOutOfBounds(
                        Cursor::EndAligned(cursor),
                        "TextResource::absolute_cursor(): end aligned cursor ends up before the beginning",
                    ))
                } else {
                    Ok(self.textlen - cursor.abs() as usize)
                }
            }
        }
    }

    /// Resolves an absolute cursor (by definition begin aligned) to UTF-8 byteposition
    /// If you have a Cursor instance, pass it through [`Self.absolute_cursor()`] first.
    pub fn utf8byte(&self, abscursor: usize) -> Result<usize, StamError> {
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
    pub fn utf8byte_to_charpos(&self, bytecursor: usize) -> Result<usize, StamError> {
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

    /// Returns a text selector with the specified offset in this resource
    pub fn text_selector(&self, begin: Cursor, end: Cursor) -> Result<Selector, StamError> {
        if let Some(handle) = self.handle() {
            Ok(Selector::TextSelector(handle, Offset { begin, end }))
        } else {
            Err(StamError::Unbound("TextResource::select_text()"))
        }
    }

    /// Returns an unsorted iterator over all textselections in this resource
    /// Use this only if order doesn't matter for. For a sorted version, used [`iter()`] or [`range()`] instead.
    pub fn textselections(&self) -> Box<impl Iterator<Item = &TextSelection>> {
        Box::new(self.store().iter().filter_map(|item| item.as_ref()))
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

    /// Returns a sorted double-ended iterator over all textselections in this resource
    /// For unsorted (slightly more performant), use [`textselections()`] instead.
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
    type Item = &'a TextSelection;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(begin2enditer) = &mut self.begin2enditer {
            if let Some((_end, handle)) = begin2enditer.next() {
                let textselection: &TextSelection =
                    self.resource.get(*handle).expect("handle must exist");
                return Some(textselection);
            }
            //fall back to final clause
        } else {
            if let Some((_begin, posindexitem)) = self.iter.next() {
                self.begin2enditer = Some(posindexitem.begin2end.iter());
                return self.next();
            } else {
                return None;
            }
        }
        //final clause
        self.begin2enditer = None;
        self.next()
    }
}

impl<'a> DoubleEndedIterator for TextSelectionIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(end2beginiter) = &mut self.end2beginiter {
            if let Some((_begin, handle)) = end2beginiter.next() {
                let textselection: &TextSelection =
                    self.resource.get(*handle).expect("handle must exist");
                return Some(textselection);
            }
            //fall back to final clause
        } else {
            if let Some((_, posindexitem)) = self.iter.next_back() {
                self.end2beginiter = Some(posindexitem.end2begin.iter());
                return self.next_back();
            } else {
                return None;
            }
        }
        //final clause
        self.end2beginiter = None;
        self.next_back()
    }
}

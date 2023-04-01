use std::collections::btree_map;
use std::collections::BTreeMap;
use std::io::prelude::*;
use std::ops::Bound::{Excluded, Included};
use std::slice::Iter;
use std::sync::{Arc, RwLock};

use regex::{Regex, RegexSet};
use sealed::sealed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

use crate::config::{get_global_config, Config, SerializeMode};
use crate::error::StamError;
use crate::selector::{Offset, Selector, SelfSelector};
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
    include: Option<String>,

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
            format!("TryFrom<TextResourceBuilder for TextResource>: Creation of TextResource from builder")
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
            } else if let Some(filename) = builder.include.as_ref() {
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
            filename: builder.include.clone(),
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
            if let Ok(changed) = self.changed.read() {
                if *changed {
                    if filename.ends_with(".json") {
                        let result = self.to_json_file(&filename, self.config()); //this reinvokes this function after setting config.standoff_include
                        result.map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
                    } else {
                        //plain text
                        std::fs::write(filename, &self.text)
                            .map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
                    }
                }
            }
            if let Ok(mut changed) = self.changed.write() {
                //reset
                *changed = false;
            }
        } else {
            state.serialize_field("@id", &self.id())?;
            state.serialize_field("text", &self.text())?;
        }
        state.end()
    }
}

impl PartialEq<TextResource> for TextResource {
    fn eq(&self, other: &TextResource) -> bool {
        self.id == other.id && self.text == other.text
    }
}

impl TextResourceBuilder {
    /// Loads a Text Resource from a STAM JSON  or plain text file file.
    /// If the file is JSON, it file must contain a single object which has "@type": "TextResource"
    /// If `include` is true, the file will be included via the `@include` mechanism, and is kept external upon serialization
    pub fn from_file(filename: &str, config: Config) -> Result<Self, StamError> {
        if filename.ends_with(".json") {
            let reader = open_file_reader(filename, &config)?;
            let deserializer = &mut serde_json::Deserializer::from_reader(reader);
            let mut result: Result<TextResourceBuilder, _> =
                serde_path_to_error::deserialize(deserializer);
            if result.is_ok() && config.use_include {
                let result = result.as_mut().unwrap();
                result.include = Some(filename.to_string()); //always uses the original filename (not the found one)
                result.mode = SerializeMode::NoInclude;
                result.config = config;
            }
            result.map_err(|e| {
                StamError::JsonError(e, filename.to_string(), "Reading text resource from file")
            })
        } else {
            //plain text
            let mut f = open_file(filename, &config)?;
            let mut text: String = String::new();
            if let Err(err) = f.read_to_string(&mut text) {
                return Err(StamError::IOError(
                    err,
                    filename.to_owned(),
                    "TextResource::from_file",
                ));
            }
            Ok(Self {
                id: Some(filename.to_string()),
                text: Some(text),
                include: if config.use_include {
                    Some(filename.to_string())
                } else {
                    None
                }, //may be overridden in with_file
                mode: SerializeMode::NoInclude, //we just processed the include, this instructs the deserialiser not to do it again
                config,
            })
        }
    }

    /// Loads a text resource from a STAM JSON string
    /// The string must contain a single object which has "@type": "TextResource"
    pub fn from_json(string: &str, config: Config) -> Result<Self, StamError> {
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

    ///Builds a new text resource from [`TextResourceBuilder'].
    pub fn from_builder(builder: TextResourceBuilder) -> Result<Self, StamError> {
        let mut res: Self = builder.try_into()?;
        res.textlen = res.text.chars().count();
        if res.config().milestone_interval > 0 {
            res.create_milestones(res.config().milestone_interval)
        }
        Ok(res)
    }

    /// Create a new TextResource from file, the text will be loaded into memory entirely
    pub fn from_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "TextResourceBuilder::from_file: filename={:?} config={:?}",
                filename, config
            )
        });
        let builder = TextResourceBuilder::from_file(filename, config)?;
        Ok(Self::from_builder(builder)?)
    }

    /// Loads a text for the TextResource from file (STAM JSON or plain text), the text will be loaded into memory entirely
    /// The use of [`from_file()`] is preferred instead. This method can be dangerous
    /// if it modifies any existing text of a resource.
    #[allow(unused_assignments)]
    pub fn with_file(mut self, filename: &str, config: Config) -> Result<Self, StamError> {
        self.check_mutation();
        let builder = TextResourceBuilder::from_file(filename, config)?;
        self = Self::from_builder(builder)?;
        Ok(self)
    }

    /// Get the filename for stand-off file specified using @include (if any)
    pub fn filename(&self) -> Option<&str> {
        self.filename.as_ref().map(|x| x.as_str())
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

    /// Check if there is already text associated, if so, this is a mutation and some indices will be invalidated
    fn check_mutation(&mut self) -> bool {
        if !self.text.is_empty() {
            // in case we change an existing text
            // 1. mark has changed
            if let Ok(mut changed) = self.changed.write() {
                *changed = true;
            }
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

    /// Searches the text using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// is held by the [`SearchTextMatch'] struct.
    ///
    /// Passing multiple regular expressions at once is more efficient than calling this function anew for each one.
    /// If capture groups are used in the regular expression, only those parts will be returned (the rest is context). If none are used,
    /// the entire expression is returned.
    ///
    /// An `offset` can be specified to work on a sub-part rather than the entire text (like an existing TextSelection).
    ///
    /// The `allow_overlap` parameter determines if the matching expressions are allowed to
    /// overlap. It you are doing some form of tokenisation, you also likely want this set to
    /// false. All of this only matters if you supply multiple regular expressions.
    ///
    /// Results are returned in the exact order they are found in the text
    pub fn search_text<'a, 'b>(
        &'a self,
        expressions: &'b [Regex],
        offset: Option<&Offset>,
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<SearchTextIter<'a, 'b>, StamError> {
        debug(self.config(), || {
            format!("search_text: expressions={:?}", expressions)
        });
        let (text, begincharpos, beginbytepos) = if let Some(offset) = offset {
            let selection = self.textselection(&offset)?;
            (
                self.text_by_textselection(&selection)?,
                selection.begin(),
                self.utf8byte(selection.begin())?,
            )
        } else {
            (self.text(), 0, 0)
        };
        let selectexpressions = if expressions.len() > 2 {
            //we have multiple expressions, first we do a pass to see WHICH of the regular expression matche (taking them all into account in a single pass!).
            //then afterwards we find for each of the matching expressions WHERE they are found
            let foundexpressions: Vec<_> = if let Some(regexset) = precompiledset {
                regexset.matches(text).into_iter().collect()
            } else {
                RegexSet::new(expressions.iter().map(|x| x.as_str()))
                    .map_err(|e| {
                        StamError::RegexError(e, "Parsing regular expressions in search_text()")
                    })?
                    .matches(text)
                    .into_iter()
                    .collect()
            };
            foundexpressions
        } else {
            match expressions.len() {
                1 => vec![0],
                2 => vec![0, 1],
                _ => unreachable!("Expected 1 or 2 expressions"),
            }
        };
        //Returns an iterator that does the remainder of the actual searching
        Ok(SearchTextIter {
            resource: self,
            expressions,
            selectexpressions,
            matchiters: Vec::new(),
            nextmatches: Vec::new(),
            text,
            begincharpos,
            beginbytepos,
            allow_overlap,
        })
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

#[sealed]
impl Configurable for TextResource {
    fn config(&self) -> &Config {
        &self.config
    }

    fn set_config(&mut self, config: Config) -> &mut Self {
        self.config = config;
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

/// Wrapper over iterator regex Matches or CaptureMatches
enum Matches<'r, 't> {
    NoCapture(regex::Matches<'r, 't>),
    WithCapture(regex::CaptureMatches<'r, 't>),
}

/// Wrapper over regex Match or Captures (as returned by the iterator)
enum Match<'t> {
    NoCapture(regex::Match<'t>),
    WithCapture(regex::Captures<'t>),
}

impl<'t> Match<'t> {
    /// Return the begin offset of the match (in utf-8 bytes)
    fn begin(&self) -> usize {
        match self {
            Self::NoCapture(m) => m.start(),
            Self::WithCapture(m) => {
                let mut begin = None;
                for group in m.iter() {
                    if let Some(group) = group {
                        if begin.is_none() || begin.unwrap() < group.start() {
                            begin = Some(group.start());
                        }
                    }
                }
                begin.expect("there must be at least one capture group that was found")
            }
        }
    }

    /// Return the end offset of the match (in utf-8 bytes)
    fn end(&self) -> usize {
        match self {
            Self::NoCapture(m) => m.end(),
            Self::WithCapture(m) => {
                let mut end = None;
                for group in m.iter() {
                    if let Some(group) = group {
                        if end.is_none() || end.unwrap() < group.start() {
                            end = Some(group.start());
                        }
                    }
                }
                end.expect("there must be at least one capture group that was found")
            }
        }
    }
}

impl<'r, 't> Iterator for Matches<'r, 't> {
    type Item = Match<'t>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::NoCapture(iter) => {
                if let Some(m) = iter.next() {
                    Some(Match::NoCapture(m))
                } else {
                    None
                }
            }
            Self::WithCapture(iter) => {
                if let Some(m) = iter.next() {
                    Some(Match::WithCapture(m))
                } else {
                    None
                }
            }
        }
    }
}

pub struct SearchTextMatch<'t, 'r> {
    expression: &'r Regex,
    expression_index: usize,
    textselections: SmallVec<[TextSelection; 2]>,
    //Records the numbers of the capture that match (1-indexed)
    capturegroups: SmallVec<[usize; 2]>,
    resource: &'t TextResource,
}

impl<'t, 'r> SearchTextMatch<'t, 'r> {
    /// Does this match return multiple text selections?
    /// Multiple text selections are returned only when the expression contains multiple capture groups.
    pub fn multi(&self) -> bool {
        self.textselections.len() > 1
    }

    /// Returns the regular expression that matched
    pub fn expression(&self) -> &'r Regex {
        self.expression
    }

    /// Returns the index of regular expression that matched
    pub fn expression_index(&self) -> usize {
        self.expression_index
    }

    pub fn textselections(&self) -> &[TextSelection] {
        &self.textselections
    }

    pub fn resource(&self) -> &'t TextResource {
        self.resource
    }

    /// Records the number of the capture groups (1-indexed!) that match.
    /// This array has the same length as textselections and identifies precisely
    /// which textselection corresponds with which capture group.
    pub fn capturegroups(&self) -> &[usize] {
        &self.capturegroups
    }

    /// Return the text of the match, this only works
    /// if there the regular expression targets a single
    /// consecutive text, i.e. by not using multiple capture groups.
    pub fn as_str(&self) -> Option<&'t str> {
        if self.multi() {
            None
        } else {
            Some(
                self.resource
                    .text_by_textselection(
                        self.textselections
                            .first()
                            .expect("there must be a textselection"),
                    )
                    .expect("textselection should exist"),
            )
        }
    }

    /// This returns a vector of texts and is mainly useful in case multiple
    /// patterns were captured.
    /// Use [`as_str()`] instead if you expect only a single text item.
    pub fn text(&self) -> Vec<&str> {
        self.textselections
            .iter()
            .map(|textselection| {
                self.resource
                    .text_by_textselection(textselection)
                    .expect("textselection should exist")
            })
            .collect()
    }
}

pub struct SearchTextIter<'t, 'r> {
    resource: &'t TextResource,
    expressions: &'r [Regex], // allows keeping all of the regular expressions external and borrow it, even if only a subset is found (subset is detected in prior pass by search_by_text())
    selectexpressions: Vec<usize>, //points at an expression, not used directly but via selectionexpression() method
    matchiters: Vec<Matches<'r, 't>>, //each expression (from selectexpressions) has its own interator  (same length as above vec)
    nextmatches: Vec<Option<Match<'t>>>, //this buffers the next match for each expression (from selectexpressions, same length as above vec)
    text: &'t str,
    begincharpos: usize,
    beginbytepos: usize,
    allow_overlap: bool,
}

impl<'t, 'r> Iterator for SearchTextIter<'t, 'r> {
    type Item = SearchTextMatch<'t, 'r>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.matchiters.is_empty() {
            //instantiate the iterators for the expressions and retrieve the first item for each
            //this is only called once when the iterator first starts
            for i in self.selectexpressions.iter() {
                let re = &self.expressions[*i];
                let mut iter = if re.captures_len() > 1 {
                    Matches::WithCapture(re.captures_iter(self.text))
                } else {
                    Matches::NoCapture(re.find_iter(self.text))
                };
                self.nextmatches.push(iter.next());
                self.matchiters.push(iter);
            }
        }

        //find the best next match (the single one next in line amongst all the iterators)
        let mut bestnextmatch: Option<&Match<'t>> = None;
        let mut bestmatchindex = None;
        for (i, m) in self.nextmatches.iter().enumerate() {
            if let Some(m) = m {
                if bestnextmatch.is_none() || m.begin() < bestnextmatch.unwrap().begin() {
                    bestnextmatch = Some(m);
                    bestmatchindex = Some(i);
                }
            }
        }

        if let Some(i) = bestmatchindex {
            // this match will be the result, convert it to the proper structure
            let m = self.nextmatches[i].take().unwrap();

            // iterate any buffers than overlap with this result, discarding those matces in the process
            if !self.allow_overlap {
                for (j, m2) in self.nextmatches.iter_mut().enumerate() {
                    if j != i && m2.is_some() {
                        if m2.as_ref().unwrap().begin() >= m.begin()
                            && m2.as_ref().unwrap().begin() <= m.end()
                        {
                            //(note: no need to check whether m2.end in range m.begin-m.end)
                            *m2 = self.matchiters[j].next();
                        }
                    }
                }
            }

            let result = self.match_to_result(m, i);

            // iterate the iterator for this one and buffer the next match for next round
            self.nextmatches[i] = self.matchiters[i].next();

            Some(result)
        } else {
            //nothing found, we are all done
            None
        }
    }
}

impl<'t, 'r> SearchTextIter<'t, 'r> {
    /// Build the final match structure we return
    fn match_to_result(
        &self,
        m: Match<'t>,
        selectexpression_index: usize,
    ) -> SearchTextMatch<'t, 'r> {
        let expression_index = self.selectexpressions[selectexpression_index];
        match m {
            Match::NoCapture(m) => {
                let textselection = TextSelection {
                    intid: None,
                    begin: self.begincharpos
                        + self
                            .resource
                            .utf8byte_to_charpos(self.beginbytepos + m.start())
                            .expect("byte to pos conversion must succeed"),
                    end: self.begincharpos
                        + self
                            .resource
                            .utf8byte_to_charpos(self.beginbytepos + m.end())
                            .expect("byte to pos conversion must succeed"),
                };
                SearchTextMatch {
                    expression: &self.expressions[expression_index],
                    expression_index,
                    resource: self.resource,
                    textselections: smallvec!(textselection),
                    capturegroups: smallvec!(),
                }
            }
            Match::WithCapture(m) => {
                let mut groupiter = m.iter();
                groupiter.next(); //The first match always corresponds to the overall match of the regex, we can ignore it
                let mut textselections: SmallVec<[TextSelection; 2]> = SmallVec::new();
                let mut capturegroups: SmallVec<[usize; 2]> = SmallVec::new();
                for (i, group) in groupiter.enumerate() {
                    if let Some(group) = group {
                        capturegroups.push(i + 1); //1-indexed
                        textselections.push(TextSelection {
                            intid: None,
                            begin: self.begincharpos
                                + self
                                    .resource
                                    .utf8byte_to_charpos(self.beginbytepos + group.start())
                                    .expect("byte to pos conversion must succeed"),
                            end: self.begincharpos
                                + self
                                    .resource
                                    .utf8byte_to_charpos(self.beginbytepos + group.end())
                                    .expect("byte to pos conversion must succeed"),
                        });
                    }
                }
                SearchTextMatch {
                    expression: &self.expressions[expression_index],
                    expression_index,
                    resource: self.resource,
                    textselections,
                    capturegroups,
                }
            }
        }
    }
}

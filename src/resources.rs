use std::collections::btree_map;
use std::io::prelude::*;
use std::ops::Bound::{Excluded, Included};
use std::ops::Deref;
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

    #[serde(skip)]
    positionindex: PositionIndex,

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
                        let result = self.to_file(&filename); //this reinvokes this function after setting config.standoff_include
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
    pub fn from_file(filename: &str, config: &Config) -> Result<Self, StamError> {
        if filename.ends_with(".json") {
            let reader = open_file_reader(filename, config)?;
            let deserializer = &mut serde_json::Deserializer::from_reader(reader);
            let mut result: Result<TextResourceBuilder, _> =
                serde_path_to_error::deserialize(deserializer);
            if result.is_ok() && config.use_include {
                let result = result.as_mut().unwrap();
                result.include = Some(filename.to_string()); //always uses the original filename (not the found one)
                result.mode = SerializeMode::NoInclude;
            }
            result.map_err(|e| {
                StamError::JsonError(e, filename.to_string(), "Reading text resource from file")
            })
        } else {
            //plain text
            let mut f = open_file(filename, config)?;
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
                config: config.clone(),
            })
        }
    }

    /// Loads a text resource from a STAM JSON string
    /// The string must contain a single object which has "@type": "TextResource"
    pub fn from_str(string: &str) -> Result<Self, StamError> {
        let deserializer = &mut serde_json::Deserializer::from_str(string);
        let result: Result<TextResourceBuilder, _> = serde_path_to_error::deserialize(deserializer);
        result.map_err(|e| {
            StamError::JsonError(e, string.to_string(), "Reading text resource from string")
        })
    }
}

impl TextResource {
    /// Instantiates a new completely empty TextResource
    pub fn new(id: String) -> Self {
        Self {
            id,
            intid: None,
            text: String::new(),
            textlen: 0,
            filename: None,
            changed: Arc::new(RwLock::new(false)),
            positionindex: PositionIndex::default(),
            textselections: Store::default(),
            config: Config::default(),
        }
    }

    ///Builds a new text resource from [`TextResourceBuilder'].
    pub fn from_builder(builder: TextResourceBuilder) -> Result<Self, StamError> {
        let mut res: Self = builder.try_into()?;
        res.textlen = res.text.chars().count();
        Ok(res)
    }

    /// Create a new TextResource from file, the text will be loaded into memory entirely
    pub fn from_file(filename: &str, config: &Config) -> Result<Self, StamError> {
        debug(config, || {
            format!(
                "TextResourceBuilder::from_file: filename={:?} config={:?}",
                filename, config
            )
        });
        let builder = TextResourceBuilder::from_file(filename, config)?;
        Ok(Self::from_builder(builder)?)
    }

    /// Loads a text for the TextResource from file (STAM JSON or plain text), the text will be loaded into memory entirely
    pub fn with_file(mut self, filename: &str, config: &Config) -> Result<Self, StamError> {
        let builder = TextResourceBuilder::from_file(filename, config)?;
        self = Self::from_builder(builder)?;
        Ok(self)
    }

    /// Writes a resource to a STAM JSON file, with appropriate formatting
    pub fn to_file(&self, filename: &str) -> Result<(), StamError> {
        let writer = open_file_writer(filename, self.config())?;
        self.config.set_serialize_mode(SerializeMode::NoInclude); //set standoff mode, what we're about the write is the standoff file
        let result = serde_json::to_writer_pretty(writer, &self)
            .map_err(|e| StamError::SerializationError(format!("Writing dataset to file: {}", e)));
        self.config.set_serialize_mode(SerializeMode::AllowInclude); //reset
        result?;
        Ok(())
    }

    /// Writes a resource to a STAM JSON file, without any indentation
    pub fn to_file_compact(&self, filename: &str) -> Result<(), StamError> {
        let writer = open_file_writer(filename, self.config())?;
        self.config.set_serialize_mode(SerializeMode::NoInclude); //set standoff mode, what we're about the write is the standoff file
        let result = serde_json::to_writer(writer, &self)
            .map_err(|e| StamError::SerializationError(format!("Writing dataset to file: {}", e)));
        self.config.set_serialize_mode(SerializeMode::AllowInclude); //reset
        result?;
        Ok(())
    }

    /// Writes a resource to a STAM JSON string, with appropriate formatting
    pub fn to_json(&self) -> Result<String, StamError> {
        self.config.set_serialize_mode(SerializeMode::NoInclude); //set standoff mode
        let result = serde_json::to_string_pretty(&self).map_err(|e| {
            StamError::SerializationError(format!("Serializing resource to string: {}", e))
        });
        self.config.set_serialize_mode(SerializeMode::AllowInclude); //reset
        result
    }

    /// Writes a resource to a STAM JSON string, without any indentation
    pub fn to_json_compact(&self) -> Result<String, StamError> {
        self.config.set_serialize_mode(SerializeMode::NoInclude); //set standoff mode
        let result = serde_json::to_string(&self).map_err(|e| {
            StamError::SerializationError(format!("Serializing resource to string: {}", e))
        });
        self.config.set_serialize_mode(SerializeMode::AllowInclude); //reset
        result
    }

    /// Get the filename for stand-off file specified using @include (if any)
    pub fn filename(&self) -> Option<&str> {
        self.filename.as_ref().map(|x| x.as_str())
    }

    /// Sets the text of the TextResource from string, kept in memory entirely
    pub fn with_string(mut self, text: String) -> Self {
        if !self.text.is_empty() {
            if let Ok(mut changed) = self.changed.write() {
                *changed = true;
            }
        }
        self.text = text;
        self.textlen = self.text.chars().count();
        self
    }

    /// Returns the length of the text in unicode points
    /// For bytes, use `self.text().len()` instead.
    pub fn textlen(&self) -> usize {
        self.textlen
    }

    /// Create a new TextResource from string, kept in memory entirely
    pub fn from_string(id: String, text: String) -> Self {
        let textlen = text.chars().count();
        TextResource {
            id,
            text,
            intid: None,
            filename: None,
            changed: Arc::new(RwLock::new(false)),
            textlen,
            positionindex: PositionIndex::default(),
            textselections: Store::default(),
            config: Config::default(),
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
            // Get the item previous to abscursor usin a double ended range iterator
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
                for (charpos, (bytepos, _)) in textslice.char_indices().enumerate() {
                    if before_pos + charpos == abscursor {
                        return Ok(before_bytepos + bytepos);
                    }
                }
            } else {
                //fallback, position index has no useful entries, search from 0
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
        let mut beginpos = 0;
        let mut beginbyte = 0;
        for (pos, posindexitem) in self.positionindex.0.iter() {
            if bytecursor == posindexitem.bytepos {
                //lucky shot! an exact match! not likely to happen often
                return Ok(*pos);
            } else if posindexitem.bytepos > bytecursor {
                break;
            }
            beginpos = *pos;
            beginbyte = posindexitem.bytepos;
        }
        let textslice = &self.text[beginbyte..];
        let mut charcount = 0;
        for (charpos, (bytepos, _)) in textslice.char_indices().enumerate() {
            charcount = charpos;
            if beginbyte + bytepos == bytecursor {
                return Ok(beginpos + charpos);
            } else if beginbyte + bytepos > bytecursor {
                break;
            }
        }
        if bytecursor == textslice.len() {
            //non-inclusive end is also a valid point to return
            return Ok(beginpos + charcount + 1);
        }
        Err(StamError::CursorOutOfBounds(
            Cursor::BeginAligned(bytecursor),
            "TextResource::utf8byte_to_charpos() (value in error is to be interpreted as a byte position here)",
        ))
    }

    /// Searches the text using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// is held by the [`SearchTextMatch'] struct.
    /// Passing multiple regular expressions at once is more efficient than calling this function anew for each one.
    /// If capture groups are used in the regular expression, only those parts will be returned (the rest is context). If none are used,
    /// The entire experssion is returned.
    /// An offset can be specified to work on a sub-part rather than the entire text (like an existing TextSelection).
    pub fn search_text<'a>(
        &'a self,
        expressions: &'a [&'a Regex],
        offset: Option<&Offset>,
        precompiledset: Option<&RegexSet>,
    ) -> Result<SearchTextIter<'a>, StamError> {
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
            Some(foundexpressions)
        } else {
            None //means we select all
        };
        //Returns an iterator that does the remainder of the actual searching
        Ok(SearchTextIter {
            resource: self,
            expressions,
            selectexpressions,
            cursor: 0,
            matchiter: Matches::None,
            text,
            begincharpos,
            beginbytepos,
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
    fn introspect_type(&self) -> &'static str {
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

enum Matches<'a> {
    None,
    NoCapture(regex::Matches<'a, 'a>),
    WithCapture(regex::CaptureMatches<'a, 'a>),
}

pub struct SearchTextMatch<'a> {
    expression: &'a Regex,
    expression_index: usize,
    textselections: SmallVec<[TextSelection; 2]>,
    //Records the numbers of the capture that match (1-indexed)
    capturegroups: SmallVec<[usize; 2]>,
    resource: &'a TextResource,
}

impl<'a> SearchTextMatch<'a> {
    /// Does this match return multiple text selections?
    /// Multiple text selections are returned only when the expression contains multiple capture groups.
    pub fn multi(&self) -> bool {
        self.textselections.len() > 1
    }

    /// Returns the regular expression that matched
    pub fn expression(&self) -> &'a Regex {
        self.expression
    }

    /// Returns the index of regular expression that matched
    pub fn expression_index(&self) -> usize {
        self.expression_index
    }

    pub fn textselections(&self) -> &[TextSelection] {
        &self.textselections
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
    pub fn as_str(&self) -> Option<&'a str> {
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
}

pub struct SearchTextIter<'a> {
    resource: &'a TextResource,
    expressions: &'a [&'a Regex], // allows keeping all of the regular expressions external and borrow it, even if only a subset is found (subset is detected in prior pass by search_by_text())
    selectexpressions: Option<Vec<usize>>, //points at an expression
    cursor: usize,                //points at an expression in selectexpressions
    matchiter: Matches<'a>,
    text: &'a str,
    begincharpos: usize,
    beginbytepos: usize,
}

impl<'a> Iterator for SearchTextIter<'a> {
    type Item = SearchTextMatch<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Matches::None = self.matchiter {
            //a new iterator
            let selectexpressions = self.selectexpressions();
            if self.cursor >= selectexpressions.len() {
                return None;
            }
            let re = self.expressions[selectexpressions[self.cursor]];
            if re.captures_len() > 1 {
                self.matchiter = Matches::WithCapture(re.captures_iter(self.text))
            } else {
                self.matchiter = Matches::NoCapture(re.find_iter(self.text))
            }
        }
        match &mut self.matchiter {
            Matches::NoCapture(matchiter) => {
                if let Some(m) = matchiter.next() {
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
                    return Some(SearchTextMatch {
                        expression: self.expressions[self.selectexpressions()[self.cursor]],
                        expression_index: self.selectexpressions()[self.cursor],
                        resource: self.resource,
                        textselections: smallvec!(textselection),
                        capturegroups: smallvec!(),
                    });
                }
            }
            Matches::WithCapture(matchiter) => {
                if let Some(m) = matchiter.next() {
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
                    return Some(SearchTextMatch {
                        expression: self.expressions[self.selectexpressions()[self.cursor]],
                        expression_index: self.selectexpressions()[self.cursor],
                        resource: self.resource,
                        textselections,
                        capturegroups,
                    });
                }
            }
            _ => unreachable!("Matchiter must exist"),
        }
        //if we reach this point without returning we have no matches to process
        self.matchiter = Matches::None;
        //increase cursor for next round
        self.cursor += 1;
        //and recurse
        self.next()
    }
}

impl<'a> SearchTextIter<'a> {
    fn selectexpressions(&self) -> &[usize] {
        match self.selectexpressions.as_ref() {
            Some(v) => {
                if !v.is_empty() {
                    v
                } else {
                    unreachable!("Selectexpressions may not be empty")
                }
            }
            None => match self.expressions.len() {
                1 => &[0],
                2 => &[0, 1],
                _ => unreachable!("Expected 1 or 2 expressions"),
            },
        }
    }
}

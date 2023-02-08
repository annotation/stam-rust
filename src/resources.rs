use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::ops::Bound::{Excluded, Included};

use sealed::sealed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use smallvec::{smallvec, SmallVec};

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
#[serde_as]
#[derive(Deserialize, Debug)]
#[serde(try_from = "TextResourceBuilder")]
pub struct TextResource {
    /// Public identifier for the text resource (often the filename/URL)
    id: String,

    /// The complete textual content of the resource
    text: String,

    /// The internal numeric identifier for the resource (may only be None upon creation when not bound yet)
    intid: Option<TextResourceHandle>,

    /// Length of the text in unicode points
    #[serde(skip)]
    textlen: usize,

    #[serde(skip)]
    textselections: Store<TextSelection>,

    #[serde(skip)]
    positionindex: PositionIndex,

    #[serde(skip)]
    config: StoreConfig,
}

#[serde_as]
#[derive(Deserialize, Debug)]
pub struct TextResourceBuilder {
    /// Public identifier for the text resource (often the filename/URL)
    #[serde(rename = "@id")]
    id: Option<String>,
    text: Option<String>,
    #[serde(rename = "@include")]
    include: Option<String>,
}

impl TryFrom<TextResourceBuilder> for TextResource {
    type Error = StamError;

    fn try_from(builder: TextResourceBuilder) -> Result<Self, StamError> {
        let textlen = if let Some(text) = &builder.text {
            text.chars().count()
        } else {
            0
        };
        let mut text = Self {
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
            config: StoreConfig::default(),
        };
        if let Some(filename) = builder.include.as_ref() {
            text = text.with_file(filename)?;
        }
        Ok(text)
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
        state.serialize_field("@id", &self.id())?;
        state.serialize_field("text", &self.text())?;
        //TODO: implement @include
        state.end()
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
            positionindex: PositionIndex::default(),
            textselections: Store::default(),
            config: StoreConfig::default(),
        }
    }

    ///Builds a new text resource from [`TextResourceBuilder'].
    pub fn build_new(builder: TextResourceBuilder) -> Result<Self, StamError> {
        let store: Self = builder.try_into()?;
        Ok(store)
    }

    /// Create a new TextResource from file, the text will be loaded into memory entirely
    pub fn from_file(filename: &str) -> Result<Self, StamError> {
        if filename.ends_with(".json") {
            let f = File::open(filename).map_err(|e| {
                StamError::IOError(e, "Reading text resource from STAM JSON file, open failed")
            })?;
            let reader = BufReader::new(f);
            let builder: TextResourceBuilder = serde_json::from_reader(reader).map_err(|e| {
                StamError::JsonError(e, "Reading text resource from STAM JSON file")
            })?;
            Self::build_new(builder)
        } else {
            Ok(Self {
                id: filename.to_string(),
                text: String::new(),
                intid: None, //unbounded for now, will be assigned when added to a AnnotationStore
                textlen: 0,
                positionindex: PositionIndex::default(),
                textselections: Store::default(),
                config: StoreConfig::default(),
            }
            .with_file(filename)?)
        }
    }

    /// Loads a text for the TextResource from file, the text will be loaded into memory entirely
    pub fn with_file(mut self, filename: &str) -> Result<Self, StamError> {
        match File::open(filename) {
            Ok(mut f) => {
                if let Err(err) = f.read_to_string(&mut self.text) {
                    return Err(StamError::IOError(err, "TextResource::from_file"));
                }
            }
            Err(err) => {
                return Err(StamError::IOError(err, "TextResource::from_file"));
            }
        }
        self.textlen = self.text.chars().count();
        Ok(self)
    }

    /// Sets the text of the TextResource from string, kept in memory entirely
    pub fn with_string(mut self, text: String) -> Self {
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
            textlen,
            positionindex: PositionIndex::default(),
            textselections: Store::default(),
            config: StoreConfig::default(),
        }
    }

    /// Returns a reference to the full text of this resource
    pub fn text(&self) -> &str {
        self.text.as_str()
    }

    /// Returns a text selection to a slice of the text as specified by the offset
    pub fn textselection(&self, offset: &Offset) -> Result<TextSelection, StamError> {
        let begin = self.absolute_cursor(&offset.begin)?;
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

    /// Returns a reference to a slice of the text as specified by the offset
    pub fn text_slice(&self, offset: &Offset) -> Result<&str, StamError> {
        let textselection = self.textselection(offset)?;
        self.text_of(&textselection)
    }

    /// Returns the text for a give [`TextSelection`]. Make sure the [`TextSelection`] applies to this resource, there are no further checks here.
    /// Use [`Self.text_slice()`] for a safer method if you want to explicitly specify an offset.
    pub fn text_of(&self, selection: &TextSelection) -> Result<&str, StamError> {
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

    /// Returns a text selector with the specified offset in this resource
    pub fn text_selector(&self, begin: Cursor, end: Cursor) -> Result<Selector, StamError> {
        if let Some(handle) = self.handle() {
            Ok(Selector::TextSelector(handle, Offset { begin, end }))
        } else {
            Err(StamError::Unbound("TextResource::select_text()"))
        }
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

    fn config(&self) -> &StoreConfig {
        &self.config
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
            .and_modify(|positem| positem.end.push(handle))
            .or_insert_with(|| PositionIndexItem {
                bytepos: beginbyte,
                begin: smallvec!(handle),
                end: smallvec!(),
            });
        self.positionindex
            .0
            .entry(end)
            .and_modify(|positem| positem.begin.push(handle))
            .or_insert_with(|| PositionIndexItem {
                bytepos: endbyte,
                begin: smallvec!(),
                end: smallvec!(handle),
            });
        Ok(())
    }
}

/*
impl<'a> ApplySelector<'a, TextSelection> for TextResource {
    fn select(&'a self, selector: &Selector) -> Result<TextSelection, StamError> {
        match selector {
            Selector::TextSelector(resource_handle, offset) => {
                if self.handle() != Some(*resource_handle) {
                    Err(StamError::WrongSelectorTarget("TextResource:select() can not apply selector meant for another TextResource"))
                } else {
                    Ok(self.text_selection(offset)?)
                }
            }
            _ => Err(StamError::WrongSelectorType(
                "TextResource::select() expected a TextSelector, got another",
            )),
        }
    }
}

impl<'a> ApplySelector<'a, &'a str> for TextResource {
    fn select(&'a self, selector: &Selector) -> Result<&'a str, StamError> {
        match selector {
            Selector::TextSelector(resource_handle, offset) => {
                if self.handle() != Some(*resource_handle) {
                    Err(StamError::WrongSelectorTarget("TextResource:select() can not apply selector meant for another TextResource"))
                } else {
                    Ok(self.text_slice(offset)?)
                }
            }
            _ => Err(StamError::WrongSelectorType(
                "TextResource::select() expected a TextSelector, got another",
            )),
        }
    }
}
*/

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

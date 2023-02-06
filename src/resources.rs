use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

use sealed::sealed;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::error::StamError;
use crate::selector::{Offset, Selector, SelfSelector};
use crate::textselection::TextSelection;
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
        Ok(self)
    }

    /// Sets the text of the TextResource from string, kept in memory entirely
    pub fn with_string(mut self, text: String) -> Self {
        self.text = text;
        self
    }

    /// Create a new TextResource from string, kept in memory entirely
    pub fn from_string(id: String, text: String) -> Self {
        TextResource {
            id,
            text,
            intid: None,
        }
    }

    /// Returns a reference to the full text of this resource
    pub fn text(&self) -> &str {
        self.text.as_str()
    }

    /// Returns a text selection to a slice of the text as specified by the offset
    pub fn text_selection(&self, offset: &Offset) -> Result<TextSelection, StamError> {
        let beginbyte = self.resolve_cursor(&offset.begin)?;
        let endbyte = self.resolve_cursor(&offset.end)?;
        if endbyte > beginbyte {
            Ok(TextSelection { beginbyte, endbyte })
        } else {
            Err(StamError::InvalidOffset(offset.begin, offset.end, ""))
        }
    }

    /// Returns a reference to a slice of the text as specified by the offset
    pub fn text_slice(&self, offset: &Offset) -> Result<&str, StamError> {
        let textselection = self.text_selection(offset)?;
        Ok(self.text_of(&textselection))
    }

    /// Returns the text for a give [`TextSelection`]. Make sure the [`TextSelection`] applies to this resource, there are no further checks here.
    /// Use [`text_slice()`] for a safer method if you want to explicitly specify an offset.
    pub fn text_of(&self, selection: &TextSelection) -> &str {
        &self.text()[selection.beginbyte()..selection.endbyte()]
    }

    /// Resolves a cursor to a utf8 byte position on the text
    pub fn resolve_cursor(&self, cursor: &Cursor) -> Result<usize, StamError> {
        //TODO: implementation is not efficient enough (O(n) with n in the order of text size), implement a pre-computed 'milestone' mechanism
        match *cursor {
            Cursor::BeginAligned(cursor) => {
                let mut prevcharindex = 0;
                for (charindex, (byteindex, _)) in self.text().char_indices().enumerate() {
                    if cursor == charindex {
                        return Ok(byteindex);
                    } else if cursor < charindex {
                        break;
                    }
                    prevcharindex = charindex;
                }
                //is the cursor at the very end? (non-inclusive)
                if cursor == prevcharindex + 1 {
                    return Ok(self.text().len());
                }
            }
            Cursor::EndAligned(0) => return Ok(self.text().len()),
            Cursor::EndAligned(cursor) => {
                let mut iter = self.text().char_indices();
                let mut endcharindex: isize = 0;
                while let Some((byteindex, _)) = iter.next_back() {
                    endcharindex -= 1;
                    if cursor == endcharindex {
                        return Ok(byteindex);
                    } else if cursor > endcharindex {
                        break;
                    }
                }
            }
        };
        Err(StamError::CursorOutOfBounds(
            *cursor,
            "TextResource::resolve_cursor()",
        ))
    }

    /// Returns a text selector the the specified offsed in this resource
    pub fn text_selector(&self, begin: Cursor, end: Cursor) -> Result<Selector, StamError> {
        if let Some(handle) = self.handle() {
            Ok(Selector::TextSelector(handle, Offset { begin, end }))
        } else {
            Err(StamError::Unbound("TextResource::select_text()"))
        }
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

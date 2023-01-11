use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;

use serde::{Serialize,Deserialize};
use serde::ser::Serializer;
use serde_with::serde_as;

use crate::types::*;
use crate::selector::{Selector,Offset};
use crate::error::StamError;

/// This holds the textual resource to be annotated. It holds the full text in memory.
///
/// The text *SHOULD* be in
/// [Unicode Normalization Form C (NFC)](https://www.unicode.org/reports/tr15/) but
/// *MAY* be in another unicode normalization forms.
#[serde_as]
#[derive(Deserialize)]
#[serde(try_from="TextResourceBuilder")]
pub struct TextResource {
    /// Public identifier for the text resource (often the filename/URL)
    id: String,

    /// The complete textual content of the resource
    text: String,

    /// The internal numeric identifier for the resource (may only be None upon creation when not bound yet)
    intid: Option<TextResourcePointer>
}

#[serde_as]
#[derive(Deserialize)]
pub struct TextResourceBuilder {
    /// Public identifier for the text resource (often the filename/URL)
    #[serde(rename="@id")]
    id: Option<String>,
    text: Option<String>,
    #[serde(rename="@include")]
    include: Option<String>
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
                return Err(StamError::NoIdError("Expected an ID for resource"))
            },
            text: if let Some(text) = builder.text {
                text
            } else {
                String::new()
            }
        };
        if let Some(filename) = builder.include.as_ref() {
            text = text.with_file(filename)?;
        }
        Ok(text)
    }
}



#[derive(Clone,Copy,Debug,PartialEq,Eq,PartialOrd,Hash)]
pub struct TextResourcePointer(u32);
impl Pointer for TextResourcePointer {
    fn new(intid: usize) -> Self { Self(intid as u32) }
    fn unwrap(&self) -> usize { self.0 as usize }
}

impl Storable for TextResource {
    type PointerType = TextResourcePointer;

    fn id(&self) -> Option<&str> { 
        Some(self.id.as_str())
    }
    fn pointer(&self) -> Option<TextResourcePointer> { 
        self.intid
    }
    fn with_id(mut self, id: String) -> Self {
        self.id = id;
        self
    }
}

impl MutableStorable for TextResource {
    fn set_pointer(&mut self, pointer: TextResourcePointer) {
        self.intid = Some(pointer);
    }
}

impl TextResource {

    /// Instantiates a new completely empty TextResource
    pub fn new(id: String) -> Self {
        Self {
            id: id,
            intid: None,
            text: String::new(),
        }
    }

    ///Builds a new text resource from [`TextResourceBuilder'].
    pub fn build_new(builder: TextResourceBuilder) -> Result<Self,StamError> {
        let store: Self = builder.try_into()?;
        Ok(store)
    }

    /// Create a new TextResource from file, the text will be loaded into memory entirely
    pub fn from_file(filename: &str) -> Result<Self, StamError> {
        if filename.ends_with(".json") {
            let f = File::open(filename).map_err(|e| StamError::IOError(e, "Reading text resource from STAM JSON file, open failed"))?;
            let reader = BufReader::new(f);
            let builder: TextResourceBuilder = serde_json::from_reader(reader).map_err(|e| StamError::JsonError(e, "Reading text resource from STAM JSON file"))?;
            Self::build_new(builder)
        } else {
            Ok(Self {
                id: filename.to_string(),
                text: String::new(),
                intid: None, //unbounded for now, will be assigned when added to a AnnotationStore
            }.with_file(filename)?)
        }
    }

    /// Loads a text for the TextResource from file, the text will be loaded into memory entirely
    pub fn with_file(mut self, filename: &str) -> Result<Self,StamError> {
        match File::open(filename) {
            Ok(mut f) => {
                if let Err(err) = f.read_to_string(&mut self.text) {
                    return Err(StamError::IOError(err,"TextResource::from_file"));
                }
            },
            Err(err) => {
               return Err(StamError::IOError(err,"TextResource::from_file"));
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
            intid: None
        }
    }

    /// Returns a reference to the full text of this resource
    pub fn get_text(&self) -> &str {
        self.text.as_str()
    }

    /// Returns a reference to a slice of the text as specified by the offset
    pub fn get_text_slice(&self, offset: &Offset) -> Result<&str,StamError> {
        let begin = self.resolve_cursor(&offset.begin)?;
        let end = self.resolve_cursor(&offset.end)?;
        if end > begin {
            Ok(&self.get_text()[begin..end])
        } else {
            Err(StamError::InvalidOffset(offset.begin, offset.end,""))
        }
    }

    /// Resolves a cursor to a utf8 byte position on the text
    pub fn resolve_cursor(&self, cursor: &Cursor) -> Result<usize,StamError> {
        //TODO: implementation is not efficient enough (O(n) with n in the order of text size), implement a pre-computed 'milestone' mechanism
        match *cursor {
            Cursor::BeginAligned(cursor) => {
                let mut prevcharindex = 0;
                for (charindex, (byteindex, _)) in self.get_text().char_indices().enumerate() {
                    if cursor == charindex {
                        return Ok(byteindex);
                    } else if cursor < charindex {
                        break;
                    }
                    prevcharindex = charindex;
                }
                //is the cursor at the very end? (non-inclusive)
                if cursor == prevcharindex + 1 {
                    return Ok(self.get_text().len());
                }
            },
            Cursor::EndAligned(0) => {
                return Ok(self.get_text().len())
            },
            Cursor::EndAligned(cursor) => {
                let mut iter = self.get_text().char_indices();
                let mut endcharindex: isize = 0;
                while let Some((byteindex, _)) = iter.next_back() {
                    endcharindex -= 1;
                    if cursor == endcharindex {
                        return Ok(byteindex)
                    } else if cursor > endcharindex {
                        break;
                    }
                }
            }
        };
        Err(StamError::CursorOutOfBounds(*cursor,"TextResource::resolve_cursor()"))
    }


    pub fn select_text(&self, begin: Cursor, end: Cursor) -> Result<Selector,StamError> {
        if let Some(pointer) = self.pointer() {
            Ok(Selector::TextSelector(pointer, Offset { begin, end }))
        } else {
            Err(StamError::Unbound("TextResource::select_text()"))
        }
    }
}

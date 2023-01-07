use crate::types::*;
use crate::selector::{Selector,Offset};
use crate::error::StamError;

use std::io::prelude::*;
use std::fs::File;



/// This holds the textual resource to be annotated. It holds the full text in memory.
///
/// The text *SHOULD* be in
/// [Unicode Normalization Form C (NFC)](https://www.unicode.org/reports/tr15/) but
/// *MAY* be in another unicode normalization forms.
pub struct TextResource {
    /// Public identifier for the text resource (often the filename/URL)
    id: String,

    /// The complete textual content of the resource
    text: String,

    /// The internal numeric identifier for the resource (may only be None upon creation when not bound yet)
    intid: Option<IntId>

    //pub(crate) _index: Vec<TextSelection>; //TODO
}


impl MayHaveId for TextResource {
    fn get_id(&self) -> Option<&str> { 
        Some(self.id.as_str())
    }
    fn with_id(mut self, id: String) -> Self {
        self.id = id;
        self
    }
}

impl MayHaveIntId for TextResource {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
}
impl SetIntId for TextResource {
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}


impl TextResource {
    /// Create a new TextResource from file, the text will be loaded into memory entirely
    pub fn from_file(filename: &str) -> Result<Self,StamError> {
        match File::open(filename) {
            Ok(mut f) => {
                let mut text = String::new();
                if let Err(err) = f.read_to_string(&mut text) {
                    return Err(StamError::IOError(err,"TextResource::from_file"));
                }
                Ok(Self {
                    id: filename.to_string(),
                    text,
                    intid: None, //unbounded for now, will be assigned when passing this via AnnotationStore.add_resource()
                })
            },
            Err(err) => {
                Err(StamError::IOError(err,"TextResource::from_file"))
            }
        }
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
        Ok(&self.get_text()[begin..end])
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
        if let Some(intid) = self.get_intid() {
            Ok(Selector::TextSelector {
                resource: intid, 
                offset: Offset { begin, end }
            })
        } else {
            Err(StamError::Unbound("TextResource::select_text()"))
        }
    }
}

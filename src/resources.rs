use crate::types::*;
use crate::selector::{Selector,Offset};
use crate::error::StamError;

use std::io::prelude::*;
use std::fs::File;

pub struct TextResource {
    /// Public identifier for the text resource (often the filename/URL)
    id: String,

    /// The complete textual content of the resource
    text: String,

    /// The internal numeric identifier for the resource (may only be None upon creation when not bound yet)
    intid: Option<IntId>

    //pub(crate) _index: Vec<TextSelection>; //TODO
}


impl HasId for TextResource {
    fn get_id(&self) -> Option<&str> { 
        Some(self.id.as_str())
    }
    fn with_id(mut self, id: String) -> Self {
        self.id = id;
        self
    }
}

impl HasIntId for TextResource {
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
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
                    return Err(StamError::IOError(err,None));
                }
                Ok(Self {
                    id: filename.to_string(),
                    text,
                    intid: None, //unbounded for now, will be assigned when passing this via AnnotationStore.add_resource()
                })
            },
            Err(err) => {
                Err(StamError::IOError(err,None))
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

    pub fn select_resource(&self) -> Result<Selector,StamError> {
        if let Some(intid) = self.get_intid() {
            Ok(Selector::ResourceSelector(intid))
        } else {
            Err(StamError::Unbound(Some(format!("select_resource failed"))))
        }
    }

    pub fn select_text(&self, begin: Cursor, end: Cursor) -> Result<Selector,StamError> {
        if let Some(intid) = self.get_intid() {
            Ok(Selector::TextSelector {
                resource: intid, 
                offset: Offset { begin, end }
            })
        } else {
            Err(StamError::Unbound(Some(format!("select_text failed"))))
        }
    }
}

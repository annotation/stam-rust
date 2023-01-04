use crate::types::*;
use crate::annotationstore::AnnotationStore;
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
    pub fn from_file(filename: &str) -> Result<Self,StamError> {
        match File::open(filename) {
            Ok(mut f) => {
                let mut text = String::new();
                if let Err(err) = f.read_to_string(&mut text) {
                    return Err(StamError::IOError(err));
                }
                Ok(Self {
                    id: filename.to_string(),
                    text,
                    intid: None, //unbounded for now, will be assigned when passing this via AnnotationStore.add_resource()
                })
            },
            Err(err) => {
                Err(StamError::IOError(err))
            }
        }
    }

    pub fn get_text(&self) -> &str {
        self.text.as_str()
    }
}

use crate::types::*;
use crate::error::StamError;

use std::fs::File;

pub struct TextResource {
    /// Public identifier for the text resource
    pub id: String,

    /// The complete textual content of the resource
    pub text: String,

    /// The internal numeric identifier for the resource (may only be None upon creation when not bound yet)
    pub(crate) intid: Option<IntId>

    //pub(crate) _index: Vec<TextSelection>; //TODO
}


impl TextResource {
    pub fn from_file(filename: &str) -> Result<Self,StamError> {
        match File::open(filename) {
            Ok(f) => {
                let text = String::new();
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
                Err(StamError::IoError(err))
            }
        }
    }
}

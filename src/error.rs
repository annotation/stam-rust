use crate::types::*;

use std::error::Error;
use std::fmt;
use std::io;

// ------------------------------ ERROR DEFINITIONS & IMPLEMENTATIONS -------------------------------------------------------------

#[derive(Debug)]
pub enum StamError {
    IntIdError(IntId),
    IdError(String),
    DuplicateIdError(String),
    IoError(std::io::Error)
}

impl From<StamError> for String {
    fn from(error: StamError) -> String {
        match error {
            StamError::IntIdError(id) => format!("No such internal ID: {}",id),
            StamError::IdError(id) => format!("No such ID: {}",id),
            StamError::DuplicateIdError(id) => format!("ID already exists: {}",id),
            StamError::IoError(err) => format!("IO Error: {}",err)
        }
    }
}

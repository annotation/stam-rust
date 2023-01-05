use crate::types::*;

use std::error::Error;
use std::fmt;
use std::io;

// ------------------------------ ERROR DEFINITIONS & IMPLEMENTATIONS -------------------------------------------------------------

#[derive(Debug)]
pub enum StamError {
    IntIdError(IntId),
    IdError(String),
    NoIdError,
    Unbound,
    DuplicateIdError(String),
    IOError(std::io::Error)
}

impl From<StamError> for String {
    fn from(error: StamError) -> String {
        match error {
            StamError::IntIdError(id) => format!("No such internal ID: {}",id),
            StamError::IdError(id) => format!("No such ID: {}",id),
            StamError::Unbound => format!("Item is not bound yet, add it to a store first"),
            StamError::NoIdError => "Store does not map IDs".to_string(),
            StamError::DuplicateIdError(id) => format!("ID already exists: {}",id),
            StamError::IOError(err) => format!("IO Error: {}",err)
        }
    }
}

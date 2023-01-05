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
    BuildError(Box<StamError>),
    StoreError(Box<StamError>),
    IOError(std::io::Error)
}

impl From<&StamError> for String {
    /// Returns the error message as a String
    fn from(error: &StamError) -> String {
        match error {
            StamError::IntIdError(id) => format!("IntIdError: No such internal ID: {}",id),
            StamError::IdError(id) => format!("IdError: No such ID: {}",id),
            StamError::Unbound => format!("Unbound: Item is not bound yet, add it to a store first"),
            StamError::NoIdError => "NoIdError: Store does not map IDs".to_string(),
            StamError::DuplicateIdError(id) => format!("DuplicatIdError: ID already exists: {}",id),
            StamError::IOError(err) => format!("IOError: {}",err),
            StamError::BuildError(err) => format!("BuildError: Error during build: {}",err),
            StamError::StoreError(err) => format!("StoreError: Error during store: {}",err)
        }
    }
}

impl fmt::Display for StamError {
    /// Formats the error message for printing
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let errmsg: String = String::from(self);
        write!(f, "[StamError] {}", errmsg)
    }
}

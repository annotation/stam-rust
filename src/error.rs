use crate::types::*;

use std::error::Error;
use std::fmt;
use std::io;

// ------------------------------ ERROR DEFINITIONS & IMPLEMENTATIONS -------------------------------------------------------------

#[derive(Debug)]
pub enum StamError {
    IntIdError(IntId,Option<String>),
    IdError(String,Option<String>),
    NoIdError(Option<String>),
    Unbound(Option<String>),
    DuplicateIdError(String,Option<String>),
    BuildError(Box<StamError>,Option<String>),
    StoreError(Box<StamError>,Option<String>),
    IOError(std::io::Error,Option<String>)
}

impl From<&StamError> for String {
    /// Returns the error message as a String
    fn from(error: &StamError) -> String {
        match error {
            StamError::IntIdError(id, msg) => format!("IntIdError: No such internal ID: {} ({})",id, msg.as_ref().unwrap_or(&"".to_string())),
            StamError::IdError(id, msg) => format!("IdError: No such ID: {} ({})",id, msg.as_ref().unwrap_or(&"".to_string())),
            StamError::Unbound(msg) => format!("Unbound: Item is not bound yet, add it to a store first. ({})", msg.as_ref().unwrap_or(&"".to_string())),
            StamError::NoIdError(msg) => format!("NoIdError: Store does not map IDs. ({})", msg.as_ref().unwrap_or(&"".to_string())),
            StamError::DuplicateIdError(id, msg) => format!("DuplicatIdError: ID already exists: {} ({})",id, msg.as_ref().unwrap_or(&"".to_string())),
            StamError::IOError(err, msg) => format!("IOError: {} ({})",err,msg.as_ref().unwrap_or(&"".to_string())),
            StamError::BuildError(err, msg) => format!("BuildError: Error during build: {} ({})",err, msg.as_ref().unwrap_or(&"".to_string())),
            StamError::StoreError(err, msg) => format!("StoreError: Error during store: {} ({}) ",err, msg.as_ref().unwrap_or(&"".to_string()))
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

use crate::types::*;

use std::error::Error;
use std::fmt;
use std::io;

// ------------------------------ ERROR DEFINITIONS & IMPLEMENTATIONS -------------------------------------------------------------

/// This enum groups the different kind of errors that this STAM library can produce
#[derive(Debug)]
pub enum StamError {
    /// This error is raised when the specified internal ID does not exist.
    /// The first parameter is the requested internal ID
    IntIdError(IntId,Option<String>),

    /// This error is raised when the specified public ID does not exist
    /// The first parameter is the requested public ID
    IdError(String,Option<String>),

    /// This error is raised when an item has no public ID but one is expected 
    NoIdError(Option<String>),

    /// This error is raised when an item has no internal ID but one is expected.
    /// This happens when an item is instantiated but not yet added to a store.
    /// We such an item unbound.
    Unbound(Option<String>),

    /// This error is raised when you attempt to set a public ID that is already in use (within a particular scope)
    /// The first parameter is the requested public ID
    DuplicateIdError(String,Option<String>),

    /// This error indicates there was an error during the building of an item from its recipe. It wraps the deeper error that occured.
    BuildError(Box<StamError>,Option<String>),

    /// This error indicates there was an error during the storage of an item  It wraps the deeper error that occured.
    StoreError(Box<StamError>,Option<String>),

    /// This error indicates there was an Input/Output error. It wraps the deeper error that occured.
    IOError(std::io::Error,Option<String>),

    /// This error is raised when you ask a selector to do something it is not capable of because it is the wrong type of selector
    WrongSelectorType(Option<String>),

    /// This error is raised when you apply a selector on a target it is not intended for
    WrongSelectorTarget(Option<String>)
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
            StamError::StoreError(err, msg) => format!("StoreError: Error during store: {} ({}) ",err, msg.as_ref().unwrap_or(&"".to_string())),
            StamError::WrongSelectorType(msg) => format!("WrongSelectorType: Selector is not of the right type here ({})", msg.as_ref().unwrap_or(&"".to_string())),
            StamError::WrongSelectorTarget(msg) => format!("WrongSelectorTarget: Selector is not applied on the right target ({})", msg.as_ref().unwrap_or(&"".to_string())),
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

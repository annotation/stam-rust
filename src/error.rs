use crate::types::*;
use std::fmt;

// ------------------------------ ERROR DEFINITIONS & IMPLEMENTATIONS -------------------------------------------------------------

/// This enum groups the different kind of errors that this STAM library can produce
#[derive(Debug)]
pub enum StamError {
    /// This error is raised when the specified internal ID does not exist.
    /// The first parameter is the requested internal ID
    HandleError(&'static str),

    /// This error is raised when the specified public ID does not exist
    /// The first parameter is the requested public ID
    IdError(String, &'static str),

    /// This error is raised when an item has no public ID but one is expected
    NoIdError(&'static str),

    /// This error is raised when an item has no internal ID but one is expected.
    /// This happens when an item is instantiated but not yet added to a store.
    /// We such an item unbound.
    Unbound(&'static str),

    /// This error is raised when an item is already bound and you are trying it again
    AlreadyBound(&'static str),

    /// This error is raised when an item is already exists and you are adding it again
    AlreadyExists(usize, &'static str),

    /// This error is raised when you attempt to set a public ID that is already in use (within a particular scope)
    /// The first parameter is the requested public ID
    DuplicateIdError(String, &'static str),

    /// This error indicates there was an error during the building of an item from its recipe. It wraps the deeper error that occured.
    BuildError(Box<StamError>, &'static str),

    /// This error indicates there was an error during the storage of an item  It wraps the deeper error that occured.
    StoreError(Box<StamError>, &'static str),

    /// This error indicates there was an Input/Output error. It wraps the deeper error that occured.
    IOError(std::io::Error, &'static str),

    /// This error indicates there was an error during JSON parsing. It wraps the deeper error that occurred.
    JsonError(serde_json::error::Error, &'static str),

    SerializationError(String),

    /// This error is raised when you ask a selector to do something it is not capable of because it is the wrong type of selector
    WrongSelectorType(&'static str),

    /// This error is raised when you apply a selector on a target it is not intended for
    WrongSelectorTarget(&'static str),

    /// This error indicates the cursor is out of bounds when applied to the text.
    CursorOutOfBounds(Cursor, &'static str),

    /// This error indicates the offset is invalid, the end precedes the beginning. It wraps the begin and end cursors, respectively
    InvalidOffset(Cursor, Cursor, &'static str),

    /// Annotation has no target
    NoTarget(&'static str),

    /// Annotation has no text
    NoText(&'static str),

    /// Called when removal of an item is requested but it is still being referenced.
    InUse(&'static str),

    /// This error is raised when the information supplied during build is incomplete
    IncompleteError(&'static str),

    /// Category for other errors, try to use this sparingly
    OtherError(&'static str),
}

impl From<&StamError> for String {
    /// Returns the error message as a String
    fn from(error: &StamError) -> String {
        match error {
            StamError::HandleError(contextmsg) => {
                format!("IntIdError: No such internal ID: ({})", contextmsg)
            }
            StamError::IdError(id, contextmsg) => {
                format!("IdError: No such ID: {} ({})", id, contextmsg)
            }
            StamError::Unbound(contextmsg) => format!(
                "Unbound: Item is not bound yet, add it to a store first. ({})",
                contextmsg
            ),
            StamError::AlreadyBound(contextmsg) => {
                format!("AlreadyBound: Item is already bound. ({})", contextmsg)
            }
            StamError::AlreadyExists(intid, contextmsg) => format!(
                "AlreadyExists: Item already exists: {} ({})",
                intid, contextmsg
            ),
            StamError::NoIdError(contextmsg) => {
                format!("NoIdError: Store does not map IDs. ({})", contextmsg)
            }
            StamError::DuplicateIdError(id, contextmsg) => format!(
                "DuplicatIdError: ID already exists: {} ({})",
                id, contextmsg
            ),
            StamError::IOError(err, contextmsg) => format!("IOError: {} ({})", err, contextmsg),
            StamError::JsonError(err, contextmsg) => {
                format!("JsonError: Parsing failed: {} ({})", err, contextmsg)
            }
            StamError::SerializationError(err) => {
                format!("SerializationError: Serialization failed: {}", err)
            }
            StamError::BuildError(err, contextmsg) => {
                format!("BuildError: Error during build: {} ({})", err, contextmsg)
            }
            StamError::StoreError(err, contextmsg) => {
                format!("StoreError: Error during store: {} ({}) ", err, contextmsg)
            }
            StamError::WrongSelectorType(contextmsg) => format!(
                "WrongSelectorType: Selector is not of the right type here ({})",
                contextmsg
            ),
            StamError::WrongSelectorTarget(contextmsg) => format!(
                "WrongSelectorTarget: Selector is not applied on the right target ({})",
                contextmsg
            ),
            StamError::CursorOutOfBounds(cursor, contextmsg) => {
                format!("CursorOutOfBounds: {:?} ({}) ", cursor, contextmsg)
            }
            StamError::InvalidOffset(begincursor, endcursor, contextmsg) => format!(
                "InvalidOffset: begin cursor {:?} must be before end cursor {:?} ({}) ",
                begincursor, endcursor, contextmsg
            ),
            StamError::NoTarget(contextmsg) => {
                format!("NoTarget: Annotation has no target ({})", contextmsg)
            }
            StamError::NoText(contextmsg) => {
                format!("NoText: Annotation has no text ({})", contextmsg)
            }
            StamError::InUse(contextmsg) => format!(
                "InUse: Item can't be removed because it is being referenced ({})",
                contextmsg
            ),
            StamError::IncompleteError(contextmsg) => {
                format!("IncompleteError: Not enough data to build ({})", contextmsg)
            }
            StamError::OtherError(contextmsg) => {
                format!("OtherError: {}", contextmsg)
            }
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

impl std::error::Error for StamError {}

impl serde::ser::Error for StamError {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        StamError::SerializationError(format!("{}", msg))
    }
}

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
    IdNotFoundError(String, &'static str),

    /// A more generic NotFound error
    NotFoundError(Type, &'static str),

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
    IOError(std::io::Error, String, &'static str),

    /// This error indicates there was an error during JSON parsing. It wraps the deeper error that occurred.
    JsonError(
        serde_path_to_error::Error<serde_json::error::Error>,
        String, //filename or string of entire json
        &'static str,
    ),

    /// This error indicates there was an error during CSV parsing. It wraps the deeper error that occurred.
    #[cfg(feature = "csv")]
    CsvError(String, &'static str),

    /// This error is raised when there is an error in regular expressions
    RegexError(regex::Error, &'static str),

    SerializationError(String),
    DeserializationError(String),

    /// This error is raised when you ask a selector to do something it is not capable of because it is the wrong type of selector
    WrongSelectorType(&'static str),

    /// This error is raised when you apply a selector on a target it is not intended for
    WrongSelectorTarget(&'static str),

    /// This error indicates the cursor is out of bounds when applied to the text.
    CursorOutOfBounds(Cursor, &'static str),

    /// This error indicates the offset is invalid, the end precedes the beginning. It wraps the begin and end cursors, respectively
    InvalidOffset(Cursor, Cursor, &'static str),

    /// This error indicates the cursor is invalid
    InvalidCursor(String, &'static str),

    /// Annotation has no target
    NoTarget(&'static str),

    /// Annotation has no text
    NoText(&'static str),

    /// Called when removal of an item is requested but it is still being referenced.
    InUse(&'static str),

    /// This error is raised when the information supplied during build is incomplete
    IncompleteError(String, &'static str),

    /// Unexpected value error
    ValueError(String, &'static str),

    /// Undefined variable in query
    UndefinedVariable(String, &'static str),

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
            StamError::IdNotFoundError(id, contextmsg) => {
                format!("IdError: No such ID: {} ({})", id, contextmsg)
            }
            StamError::NotFoundError(tp, contextmsg) => {
                format!("NotFoundError: {} not found ({})", tp, contextmsg)
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
                "DuplicateIdError: ID already exists for a different item: {} ({})",
                id, contextmsg
            ),
            StamError::IOError(err, filename, contextmsg) => {
                format!("IOError for {}: {} ({})", filename, err, contextmsg)
            }
            StamError::JsonError(err, input, contextmsg) => {
                format!(
                    "JsonError: Parsing failed: {} ({}). Input: {}",
                    err, contextmsg, input
                )
            }
            #[cfg(feature = "csv")]
            StamError::CsvError(msg, contextmsg) => {
                format!("CsvError: Parsing failed: {} ({})", msg, contextmsg)
            }
            StamError::RegexError(err, contextmsg) => {
                format!("RegexError: {} ({})", err, contextmsg)
            }
            StamError::SerializationError(err) => {
                format!("SerializationError: Serialization failed: {}", err)
            }
            StamError::DeserializationError(err) => {
                format!("DeserializationError: Serialization failed: {}", err)
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
            StamError::InvalidCursor(s, contextmsg) => {
                format!("InvalidCursor: {:?} ({}) ", s, contextmsg)
            }
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
            StamError::IncompleteError(data, contextmsg) => {
                format!(
                    "IncompleteError: Not enough data to build: {} ({})",
                    data, contextmsg
                )
            }
            StamError::ValueError(value, contextmsg) => {
                format!("ValueError: Unexpected value: {} - ({})", value, contextmsg)
            }
            StamError::UndefinedVariable(varname, contextmsg) => {
                format!(
                    "ValueError: Undefined variable in search query: {} - ({})",
                    varname, contextmsg
                )
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

impl serde::de::Error for StamError {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        StamError::DeserializationError(format!("{}", msg))
    }
}

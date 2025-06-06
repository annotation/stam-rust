/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module implements the [`StamError`] type, which encapsulates all different kind of errors this STAM library can produce.

use crate::types::*;
use serde::ser::SerializeStruct;
use serde::Serialize;
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
    NotFoundError(Type, String),

    /// This error is raised when the specified variable (e.g. in a query) does not resolve
    /// The first parameter is the requested public variable
    VariableNotFoundError(String, Option<Type>, &'static str),

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

    /// A syntax error in query syntax
    QuerySyntaxError(String, &'static str),

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

    #[cfg(feature = "transpose")]
    TransposeError(String, &'static str),

    ValidationError(String, &'static str),

    /// Category for other errors, try to use this sparingly
    OtherError(&'static str),
}

impl StamError {
    pub fn name(&self) -> &'static str {
        match self {
            StamError::HandleError(..) => "HandleError",
            StamError::IdNotFoundError(..) => "IdNotFoundError",
            StamError::NotFoundError(..) => "NotFoundError",
            StamError::Unbound(..) => "Unbound",
            StamError::AlreadyBound(..) => "AlreadyBound",
            StamError::AlreadyExists(..) => "AlreadyExists",
            StamError::NoIdError(..) => "NoIdError",
            StamError::DuplicateIdError(..) => "DuplicateIdError",
            StamError::IOError(..) => "IoError",
            StamError::JsonError(..) => "JsonError",

            #[cfg(feature = "csv")]
            StamError::CsvError(..) => "CsvError",

            StamError::RegexError(..) => "RegexError",
            StamError::QuerySyntaxError(..) => "QuerySyntaxError",
            StamError::SerializationError(..) => "SerializationError",
            StamError::DeserializationError(..) => "DeserializationError",
            StamError::BuildError(..) => "BuildError",
            StamError::StoreError(..) => "StoreError",
            StamError::WrongSelectorType(..) => "WrongSelectorType",
            StamError::WrongSelectorTarget(..) => "WrongSelectorTarget",
            StamError::CursorOutOfBounds(..) => "CursorOutOfBounds",
            StamError::InvalidOffset(..) => "InvalidOffset",
            StamError::InvalidCursor(..) => "InvalidCursor",
            StamError::NoTarget(..) => "NoTarget",
            StamError::NoText(..) => "NoText",
            StamError::InUse(..) => "InUse",
            StamError::IncompleteError(..) => "IncompleteError",
            StamError::ValueError(..) => "ValueError",
            #[cfg(feature = "transpose")]
            StamError::TransposeError(..) => "TransposeError",
            StamError::ValidationError(..) => "ValidationError",
            StamError::UndefinedVariable(..) => "UndefinedVariable",
            StamError::VariableNotFoundError(..) => "VariableNotFoundError",
            StamError::OtherError(..) => "OtherError",
        }
    }
}

impl Serialize for StamError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("StamError", 3)?;
        let message: String = self.into();
        state.serialize_field("@type", "StamError")?;
        state.serialize_field("name", self.name())?;
        state.serialize_field("message", &message)?;
        state.end()
    }
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
            StamError::NotFoundError(tp, msg) => {
                format!("NotFoundError: {} not found: {}", tp, msg)
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
            StamError::QuerySyntaxError(err, contextmsg) => {
                format!(
                    "QuerySyntaxError: Malformed query: {} ({})",
                    err.as_str(),
                    contextmsg
                )
            }
            StamError::SerializationError(err) => {
                format!("SerializationError: Serialization failed: {}", err)
            }
            StamError::DeserializationError(err) => {
                format!("DeserializationError: Deserialization failed: {}", err)
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
            #[cfg(feature = "transpose")]
            StamError::TransposeError(msg, contextmsg) => {
                format!(
                    "TransposeError: Unable to transpose: {} - ({})",
                    msg, contextmsg
                )
            }
            StamError::ValidationError(msg, contextmsg) => {
                format!(
                    "ValidationError: Failed to validate: {} - ({})",
                    msg, contextmsg
                )
            }
            StamError::UndefinedVariable(varname, contextmsg) => {
                format!(
                    "UndefinedVariable: Undefined variable in search query: {} - ({})",
                    varname, contextmsg
                )
            }
            StamError::VariableNotFoundError(var, tp, contextmsg) => {
                if let Some(tp) = tp {
                    format!(
                        "VariableNotFoundError: variable ?{} of type {} not found ({})",
                        var, tp, contextmsg
                    )
                } else {
                    format!(
                        "VariableNotFoundError: variable ?{} not found ({})",
                        var, contextmsg
                    )
                }
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

impl From<std::io::Error> for StamError {
    fn from(value: std::io::Error) -> Self {
        StamError::IOError(value, String::new(), "")
    }
}

/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the [`ToJson`] and [`FromJson`] trait thare are used
//! in serialisation to/from STAM JSON. Most of the actual deserialisation/serialisation methods
//! are implemented alongside the low-level data structures themselves, not here.

use crate::config::{Config, SerializeMode};
use crate::error::StamError;
use crate::file::*;
use crate::types::*;

pub trait ToJson
where
    Self: TypeInfo + serde::Serialize,
{
    /// Writes a serialisation (choose a dataformat) to any writer
    /// Lower-level function
    fn to_json_writer<W>(&self, writer: W, compact: bool) -> Result<(), StamError>
    where
        W: std::io::Write,
    {
        match compact {
            false => serde_json::to_writer_pretty(writer, &self).map_err(|e| {
                StamError::SerializationError(format!(
                    "Writing {} to file: {}",
                    Self::typeinfo(),
                    e
                ))
            }),
            true => serde_json::to_writer(writer, &self).map_err(|e| {
                StamError::SerializationError(format!(
                    "Writing {} to file: {}",
                    Self::typeinfo(),
                    e
                ))
            }),
        }
    }

    /// Writes this structure to a file
    /// The actual dataformat can be set via `config`, the default is STAM JSON.
    fn to_json_file(&self, filename: &str, config: &Config) -> Result<(), StamError> {
        debug(config, || {
            format!("{}.to_file: filename={:?}", Self::typeinfo(), filename)
        });
        if let Type::TextResource | Type::AnnotationDataSet = Self::typeinfo() {
            //introspection to detect whether type can do @include
            config.set_serialize_mode(SerializeMode::NoInclude); //set standoff mode, what we're about the write is the standoff file
        }
        let compact = match config.dataformat {
            DataFormat::Json { compact } => compact,
            _ => {
                if let Type::AnnotationStore = Self::typeinfo() {
                    return Err(StamError::SerializationError(format!(
                        "Unable to serialize to JSON for {} (filename {}) when config dataformat is set to {}",
                        Self::typeinfo(),
                        filename,
                        config.dataformat
                    )));
                } else {
                    false
                }
            }
        };
        let writer = open_file_writer(filename, &config)?;
        let result = self.to_json_writer(writer, compact);
        if let Type::TextResource | Type::AnnotationDataSet = Self::typeinfo() {
            //introspection to detect whether type can do @include
            config.set_serialize_mode(SerializeMode::AllowInclude); //set standoff mode, what we're about the write is the standoff file
        }
        result
    }

    /// Serializes this structure to one string.
    /// The actual dataformat can be set via `config`, the default is STAM JSON.
    /// If `config` not not specified, an attempt to fetch the AnnotationStore's initial config is made
    fn to_json_string(&self, config: &Config) -> Result<String, StamError> {
        if let Type::TextResource | Type::AnnotationDataSet = Self::typeinfo() {
            //introspection to detect whether type can do @include
            config.set_serialize_mode(SerializeMode::NoInclude); //set standoff mode, what we're about the write is the standoff file
        }
        let result = match config.dataformat {
            DataFormat::Json { compact: false } => {
                serde_json::to_string_pretty(&self).map_err(|e| {
                    StamError::SerializationError(format!(
                        "Writing {} to string: {}",
                        Self::typeinfo(),
                        e
                    ))
                })
            }
            DataFormat::Json { compact: true } => serde_json::to_string(&self).map_err(|e| {
                StamError::SerializationError(format!(
                    "Writing {} to string: {}",
                    Self::typeinfo(),
                    e
                ))
            }),
            _ => Err(StamError::SerializationError(format!(
                "Unable to serialize to JSON for {} when config dataformat is set to {}",
                Self::typeinfo(),
                config.dataformat
            ))),
        };
        if let Type::TextResource | Type::AnnotationDataSet = Self::typeinfo() {
            //introspection to detect whether type can do @include
            config.set_serialize_mode(SerializeMode::AllowInclude); //set standoff mode, what we're about the write is the standoff file
        }
        result
    }
}

pub trait FromJson
where
    Self: TypeInfo + Sized,
{
    fn from_json_file(filename: &str, config: Config) -> Result<Self, StamError>;

    fn from_json_str(string: &str, config: Config) -> Result<Self, StamError>;

    fn merge_json_file(&mut self, _filename: &str) -> Result<(), StamError> {
        unimplemented!("merge_json_file not implemented")
    }

    fn merge_json_str(&mut self, _string: &str) -> Result<(), StamError> {
        unimplemented!("merge_json_str not implemented")
    }
}

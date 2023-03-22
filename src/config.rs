use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::error::StamError;
use crate::types::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SerializeMode {
    /// Allow serialisation of stand-off files (which means we allow @include)
    AllowInclude,

    ///We are in standoff mode to serialized stand-off files (which means we don't output @include again)
    NoInclude,
}

impl Default for SerializeMode {
    fn default() -> Self {
        Self::AllowInclude
    }
}

/// This holds the configuration for the annotationstore
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    /// Enable/disable the reverse index for text, it maps TextResource => TextSelection => Annotation
    pub textrelationmap: bool,
    /// Enable/disable reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`Selector::ResourceSelector`]), i.e. metadata
    pub resource_annotation_map: bool,
    /// Enable/disable reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`Selector::DataSetSelector`]), i.e. metadata
    pub dataset_annotation_map: bool,
    /// Enable/disable index for annotations that reference other annotations
    pub annotation_annotation_map: bool,

    ///generate ids when missing
    pub generate_ids: bool,

    /// The working directory
    pub workdir: Option<PathBuf>,

    /// This flag can be flagged on or off (using internal mutability) to indicate whether we are serializing for the standoff include mechanism
    #[serde(skip)]
    serialize_mode: Arc<RwLock<SerializeMode>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            textrelationmap: true,
            resource_annotation_map: true,
            dataset_annotation_map: true,
            annotation_annotation_map: true,
            generate_ids: true,
            workdir: None,
            serialize_mode: Arc::new(RwLock::new(SerializeMode::AllowInclude)),
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the mode for (de)serialization
    pub fn set_serialize_mode(&self, mode: SerializeMode) {
        if let Ok(mut serialize_mode) = self.serialize_mode.write() {
            *serialize_mode = mode;
        }
    }

    /// Gets the mdoe for (de)serialization
    pub fn serialize_mode(&self) -> SerializeMode {
        if let Ok(serialize_mode) = self.serialize_mode.read() {
            *serialize_mode
        } else {
            panic!("Unable to get lock for serialize mode");
        }
    }

    ///  Return the working directory
    pub fn workdir(&self) -> Option<&Path> {
        self.workdir.as_ref().map(|x| x.as_path())
    }

    /// Loads configuration a JSON file
    pub fn from_file(filename: &str, workdir: Option<&Path>) -> Result<Self, StamError> {
        let reader = open_file_reader(filename, workdir)?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);
        let result: Result<Self, _> = serde_path_to_error::deserialize(deserializer);
        result
            .map_err(|e| StamError::JsonError(e, filename.to_string(), "Reading config from file"))
    }

    /// Writes an AnnotationStore to one big STAM JSON string, with appropriate formatting
    pub fn to_json(&self) -> Result<String, StamError> {
        serde_json::to_string_pretty(&self)
            .map_err(|e| StamError::SerializationError(format!("Writing config to string: {}", e)))
    }
}

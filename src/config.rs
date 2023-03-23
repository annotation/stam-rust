use serde::{Deserialize, Serialize};
use std::cell::RefCell;
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

    ///use the @include mechanism to point to external files, if unset, all data will be kept in a single STAM JSON file
    pub use_include: bool,

    /// The working directory
    pub workdir: Option<PathBuf>,

    /// Debug mode
    pub debug: bool,

    /// This flag can be flagged on or off (using internal mutability) to indicate whether we are serializing for the standoff include mechanism
    /// TODO: move this out of the config?
    #[serde(skip)]
    pub serialize_mode: Arc<RwLock<SerializeMode>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            textrelationmap: true,
            resource_annotation_map: true,
            dataset_annotation_map: true,
            annotation_annotation_map: true,
            generate_ids: true,
            use_include: true,
            workdir: None,
            debug: false,
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
    pub fn from_file(filename: &str) -> Result<Self, StamError> {
        let reader = open_file_reader(filename, &Config::default())?;
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

// below is a fairly ugly solution needed to get the Config into Builder structure deserialised with Serde,
// which itself has no means to propagate state.. We abuse a global variable to accomplish
// We use the serde macro default=get_global_config() on the config field to inject this state (as it is not in the serialisation)

thread_local! {
    pub(crate) static GLOBAL_CONFIG: RefCell<Config> = RefCell::new(Config::default());
}

pub(crate) fn set_global_config(config: Config) {
    debug(&config, || format!("set_global_config: {:?}", &config));
    GLOBAL_CONFIG.with(|global_config| *global_config.borrow_mut() = config);
}

pub(crate) fn get_global_config() -> Config {
    let mut config = Config::default();
    GLOBAL_CONFIG.with(|global_config| config = global_config.borrow().clone());
    debug(&config, || format!("get_global_config: {:?}", &config));
    config
}

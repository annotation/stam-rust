use minicbor::{Decode, Encode};
use sealed::sealed;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::cbor::{cbor_decode_serialize_mode, cbor_encode_serialize_mode};
use crate::error::StamError;
use crate::file::*;
use crate::json::*;
use crate::types::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SerializeMode {
    /// Allow serialisation of stand-off files (which means we allow @include)
    AllowInclude,

    ///We are in standoff mode to serialized stand-off files (which means we don't output @include again)
    NoInclude,
}

impl Default for SerializeMode {
    fn default() -> Self {
        Self::NoInclude
    }
}

pub trait Configurable: Sized {
    //// Obtain the configuration
    fn config(&self) -> &Config;

    //// Obtain the configuration mutably
    fn config_mut(&mut self) -> &mut Config;

    ///Builder pattern to associate a configuration
    fn with_config(mut self, config: Config) -> Self {
        self.set_config(config);
        self
    }

    ///Setter to associate a configuration
    fn set_config(&mut self, config: Config) -> &mut Self;
}

/// This holds the configuration. It is not limited to configuring a single part of the model, but unifies all in a single configuration.
#[derive(Deserialize, Serialize, Debug, Clone, Encode, Decode)]
pub struct Config {
    /// Debug mode
    #[n(0)]
    pub(crate) debug: bool,

    /// The working directory
    #[n(1)]
    pub(crate) workdir: Option<PathBuf>,

    ///generate pseudo-random public identifiers when missing. Each will consist of 21 URL-friendly ASCII symbols after a prefix of A for Annotations, S for DataSets, D for AnnotationData, R for resources
    #[n(2)]
    pub(crate) generate_ids: bool,

    /// Strip temporary IDs during deserialisation. Temporary IDs start with an exclamation mark, a capital ASCII letter denoting the type, and a number
    #[n(3)]
    pub(crate) strip_temp_ids: bool,

    ///use the `@include` mechanism to point to external files, if unset, all data will be kept in a single STAM JSON file.
    #[n(4)]
    pub(crate) use_include: bool,

    /// shrink data structures to optimize memory (at the cost of longer deserialisation times)
    #[n(5)]
    pub(crate) shrink_to_fit: bool,

    /// Milestone placement interval (in unicode codepoints) in indexing text resources. A low number above zero increases search performance at the cost of memory and increased initialisation time.
    #[n(6)]
    pub(crate) milestone_interval: usize,

    /// The chosen dataformat for serialisation, defaults to STAM JSON.
    #[n(7)]
    pub(crate) dataformat: DataFormat,

    /// This flag can be flagged on or off (using internal mutability) to indicate whether we are serializing for the standoff include mechanism
    // TODO: move this out of the config?
    #[serde(skip)]
    #[n(8)]
    #[cbor(
        encode_with = "cbor_encode_serialize_mode",
        decode_with = "cbor_decode_serialize_mode"
    )]
    pub(crate) serialize_mode: Arc<RwLock<SerializeMode>>,

    /// Enable/disable the reverse index for text, it maps TextResource => TextSelection => Annotation
    #[n(100)]
    pub(crate) textrelationmap: bool,

    /// Enable/disable reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`crate::Selector::ResourceSelector`]), i.e. metadata
    #[n(101)]
    pub(crate) resource_annotation_metamap: bool,

    /// Enable/disable reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`crate::Selector::DataSetSelector`]), i.e. metadata
    #[n(102)]
    pub(crate) dataset_annotation_metamap: bool,

    /// Enable/disable index for annotations that reference other annotations
    #[n(103)]
    pub(crate) annotation_annotation_map: bool,

    /// Reserved for future usage, not in use yet
    #[n(104)]
    pub(crate) key_annotation_map: bool,

    /// Enable/disable reverse index for DataKey => Annotation. Holds only annotations that **directly** reference the DataKey (via [`crate::Selector::DataKeySelector`]), i.e. metadata
    #[n(105)]
    pub(crate) key_annotation_metamap: bool,

    /// Enable/disable reverse index for AnnotationData => Annotation. Holds only annotations that **directly** reference the DataKey (via [`crate::Selector::AnnotationDataSelector`]), i.e. metadata
    #[n(106)]
    pub(crate) data_annotation_metamap: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            textrelationmap: true,
            resource_annotation_metamap: true,
            dataset_annotation_metamap: true,
            annotation_annotation_map: true,
            key_annotation_map: true,
            key_annotation_metamap: true,
            data_annotation_metamap: true,
            generate_ids: false,
            strip_temp_ids: true,
            shrink_to_fit: true,
            use_include: true,
            dataformat: DataFormat::Json { compact: false },
            milestone_interval: 100,
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

    /// Enable/disable the reverse index for text, it maps TextResource => TextSelection => Annotation
    /// Do not change this on a configuration that is already in use!
    pub fn with_textrelationmap(mut self, value: bool) -> Self {
        self.textrelationmap = value;
        self
    }

    /// Is the reverse index for text enabled? It maps TextResource => TextSelection => Annotation
    pub fn textrelationmap(&self) -> bool {
        self.textrelationmap
    }

    /// Enable/disable reverse index for TextResource => Annotation. Holds only annotations that **directly** reference the TextResource (via [`crate::Selector::ResourceSelector`]), i.e. metadata
    /// Do not change this on a configuration that is already in use!
    pub fn with_resource_annotation_map(mut self, value: bool) -> Self {
        self.resource_annotation_metamap = value;
        self
    }

    /// Is the reverse index for TextResource => Annotation enabled? Holds only annotations that **directly** reference the TextResource (via [`crate::Selector::ResourceSelector`]), i.e. metadata
    pub fn resource_annotation_map(&self) -> bool {
        self.resource_annotation_metamap
    }

    /// Enable/disable reverse index for AnnotationDataSet => Annotation. Holds only annotations that **directly** reference the AnnotationDataSet (via [`crate::Selector::DataSetSelector`]), i.e. metadata
    /// Do not change this on a configuration that is already in use!
    pub fn with_dataset_annotation_map(mut self, value: bool) -> Self {
        self.dataset_annotation_metamap = value;
        self
    }

    /// Is the reverse index for AnnotationDataSet => Annotation enabled?. Holds only annotations that **directly** reference the AnnotationDataSet (via [`crate::Selector::DataSetSelector`]), i.e. metadata
    pub fn dataset_annotation_map(&self) -> bool {
        self.dataset_annotation_metamap
    }

    /// Is the reverse index for DataKey => Annotation enabled?. Holds only annotations that **directly** reference the DataKey (via [`crate::Selector::DataKeySelector`]), i.e. metadata
    pub fn key_annotation_metamap(&self) -> bool {
        self.key_annotation_metamap
    }

    /// Enable/disable reverse index for DataKey => Annotation. Holds only annotations that **directly** reference the DataKey (via [`crate::Selector::DataKeySelector`]), i.e. metadata
    /// Do not change this on a configuration that is already in use!
    pub fn with_key_annotation_metamap(mut self, value: bool) -> Self {
        self.key_annotation_metamap = value;
        self
    }

    /// Is the reverse index for AnnotationData => Annotation enabled?. Holds only annotations that **directly** reference the AnnotationData (via [`crate::Selector::AnnotationDataSelector`]), i.e. metadata
    pub fn data_annotation_metamap(&self) -> bool {
        self.data_annotation_metamap
    }

    /// Enable/disable reverse index for AnnotationData => Annotation. Holds only annotations that **directly** reference the AnnotationData (via [`crate::Selector::AnnotationDataSelector`]), i.e. metadata
    /// Do not change this on a configuration that is already in use!
    pub fn with_data_annotation_metamap(mut self, value: bool) -> Self {
        self.data_annotation_metamap = value;
        self
    }

    /// Enable/disable index for annotations that reference other annotations
    /// Do not change this on a configuration that is already in use!
    pub fn with_annotation_annotation_map(mut self, value: bool) -> Self {
        self.annotation_annotation_map = value;
        self
    }

    /// Is the index for annotations that reference other annotations enabled?
    pub fn annotation_annotation_map(&self) -> bool {
        self.annotation_annotation_map
    }

    /// Sets chosen dataformat for serialisation, defaults to STAM JSON.
    /// Do not change this on a configuration that is already in use! Use [`AnnotationStore.set_filename()`] instead.
    pub fn with_dataformat(mut self, value: DataFormat) -> Self {
        self.dataformat = value;
        self
    }

    pub fn with_shrink_to_fit(mut self, value: bool) -> Self {
        self.shrink_to_fit = value;
        self
    }

    /// Strip temporary public identifiers when deserialising?
    pub fn with_strip_temp_ids(mut self, value: bool) -> Self {
        self.strip_temp_ids = value;
        self
    }

    /// Returns the configured dataformat for serialisation.
    pub fn dataformat(&self) -> DataFormat {
        self.dataformat
    }

    /// Generate public IDs when missing.
    /// Each will consist of 21 URL-friendly ASCII symbols after a prefix of A for Annotations, S for DataSets, D for AnnotationData, R for resources
    pub fn with_generate_ids(mut self, value: bool) -> Self {
        self.generate_ids = value;
        self
    }

    /// Is generation of public IDs when missing enabled or not?
    pub fn generate_ids(&self) -> bool {
        self.generate_ids
    }

    /// Strip temporary public identifiers when deserialising?
    pub fn strip_temp_ids(&self) -> bool {
        self.strip_temp_ids
    }

    /// Shrink data structures for minimal memory footprint?
    pub fn shrink_to_fit(&self) -> bool {
        self.shrink_to_fit
    }

    /// Use @include mechanism for STAM JSON, or output all to a single file?
    pub fn with_use_include(mut self, value: bool) -> Self {
        self.use_include = value;
        self
    }

    /// Use @include mechanism for STAM JSON, or output all to a single file?
    pub fn use_include(&self) -> bool {
        self.use_include
    }

    /// Set the configured milestone interval
    /// The Milestone placement interval (in unicode codepoints) is used in indexing text resources. A low number above zero increases search performance at the cost of memory and increased initialisation time.
    pub fn with_milestone_interval(mut self, value: usize) -> Self {
        self.milestone_interval = value;
        self
    }

    /// Return the configured milestone interval
    /// The Milestone placement interval (in unicode codepoints) is used in indexing text resources. A low number above zero increases search performance at the cost of memory and increased initialisation time.
    pub fn milestone_interval(&self) -> usize {
        self.milestone_interval
    }

    /// Enable or disable debug mode. In debug mode, verbose output will be printed to standard error output
    pub fn with_debug(mut self, value: bool) -> Self {
        self.debug = value;
        self
    }

    /// Is debug mode enabled or not?
    pub fn debug(&self) -> bool {
        self.debug
    }

    /// Sets the mode for (de)serialization. This is a low-level method that you won't need directly.
    pub(crate) fn set_serialize_mode(&self, mode: SerializeMode) {
        if let Ok(mut serialize_mode) = self.serialize_mode.write() {
            *serialize_mode = mode;
        }
    }

    /// Gets the mode for (de)serialization. This is a low-level method that you won't need directly.
    pub(crate) fn serialize_mode(&self) -> SerializeMode {
        if let Ok(serialize_mode) = self.serialize_mode.read() {
            *serialize_mode
        } else {
            panic!("Unable to get lock for serialize mode");
        }
    }

    ///  Return the working directory, if set
    pub fn workdir(&self) -> Option<&Path> {
        self.workdir.as_ref().map(|x| x.as_path())
    }

    /// Loads configuration from a JSON file
    pub fn from_file(filename: &str) -> Result<Self, StamError> {
        let reader = open_file_reader(filename, &Config::default())?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);
        let result: Result<Self, _> = serde_path_to_error::deserialize(deserializer);
        result
            .map_err(|e| StamError::JsonError(e, filename.to_string(), "Reading config from file"))
    }
}

#[sealed]
impl TypeInfo for Config {
    fn typeinfo() -> Type {
        Type::Config
    }
}

impl ToJson for Config {}

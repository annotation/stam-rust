/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module implements text validation, as defined in the
//! [STAM Textvalidation Extension](https://github.com/annotation/stam/tree/master/extensions/stam-textvalidation).
//!
//! Being a STAM extension, this module is implemented as an extra feature and may be enabled/disabled
//! at compile time.

use crate::annotation::Annotation;
use crate::annotationstore::AnnotationStore;
use crate::api::*;
use crate::error::StamError;
use crate::store::*;
use crate::AnnotationDataSet;

use base16ct;
use sha1::{Digest, Sha1};

const TEXTVALIDATION_SET: &str = "https://w3id.org/stam/extensions/stam-textvalidation/";

#[derive(Copy, Clone, PartialEq)]
pub enum TextValidationMode {
    /// Use checksums
    Checksum,
    /// Use text
    Text,
    /// Use both
    Both,
    /// Automatically choose based on text length
    Auto,
}

impl Default for TextValidationMode {
    fn default() -> Self {
        Self::Auto
    }
}

pub struct TextValidationResult {
    valid: usize,
    invalid: usize,
    missing: usize,
}

impl TextValidationResult {
    /// Returns the number of annotations that do not have validation information
    pub fn missing(&self) -> usize {
        self.missing
    }
    /// Returns the number of annotations that are valid
    pub fn valid(&self) -> usize {
        self.valid
    }
    /// Returns the number of annotations that are invalid
    pub fn invalid(&self) -> usize {
        self.invalid
    }
    /// Returns a boolean representing the validation result
    pub fn is_ok(&self) -> bool {
        self.invalid == 0 && self.missing == 0
    }
    pub fn is_ok_maybe_incomplete(&self) -> bool {
        self.invalid == 0
    }
}

impl AnnotationStore {
    /// Mark all annotations in this store as valid, enabling text validation checks in the future.
    pub fn make_validation(&mut self, mode: TextValidationMode) -> Result<(), StamError> {
        let mut queue_checksums = Vec::new();
        let mut queue_texts = Vec::new();
        for annotation in self.annotations() {
            let (do_checksum, do_text) = match mode {
                TextValidationMode::Checksum => (true, false),
                TextValidationMode::Text => (false, true),
                TextValidationMode::Both => (true, true),
                TextValidationMode::Auto => {
                    let textlen = annotation
                        .textselections()
                        .fold(0 as usize, |a, x| a + (x.end() - x.begin()));
                    if textlen < 40 {
                        (false, true)
                    } else {
                        (true, false)
                    }
                }
            };

            if do_checksum && annotation.validation_checksum().is_none() {
                if let Some(checksum) =
                    annotation.text_checksum(annotation.text_validation_delimiter().unwrap_or(""))
                {
                    queue_checksums.push((annotation.handle(), checksum));
                }
            }
            if do_text && annotation.validation_text().is_none() {
                let text =
                    annotation.text_join(annotation.text_validation_delimiter().unwrap_or(""));
                if !text.is_empty() {
                    queue_texts.push((annotation.handle(), text));
                }
            }
        }

        // get or add the dataset
        let mut set_handle = self.dataset(TEXTVALIDATION_SET).map(|set| set.handle());
        if set_handle.is_none() {
            set_handle = self
                .insert(AnnotationDataSet::new(self.config.clone()).with_id(TEXTVALIDATION_SET))
                .ok();
        }

        // add validation information to existing annotations
        if let Some(set_handle) = set_handle {
            for (handle, checksum) in queue_checksums {
                let set: &mut AnnotationDataSet = self.get_mut(set_handle)?;
                let data_handle = set.insert_data(BuildItem::None, "checksum", checksum, true)?;
                let annotation: &mut Annotation = self.get_mut(handle)?;
                annotation.add_data(set_handle, data_handle);
                // we need to update the reverse index manually:
                self.dataset_data_annotation_map
                    .insert(set_handle, data_handle, handle);
            }
            for (handle, text) in queue_texts {
                let set: &mut AnnotationDataSet = self.get_mut(set_handle)?;
                let data_handle = set.insert_data(BuildItem::None, "text", text, true)?;
                let annotation: &mut Annotation = self.get_mut(handle)?;
                annotation.add_data(set_handle, data_handle);
                // we need to update the reverse index manually:
                self.dataset_data_annotation_map
                    .insert(set_handle, data_handle, handle);
            }
        } else {
            panic!("Set must exist");
        }
        Ok(())
    }

    /// Tests if the store has validation info associated
    /// This not guarantee that all annotations have validation information
    pub fn has_validation_info(&self) -> bool {
        self.key(TEXTVALIDATION_SET, "checksum").is_some()
            || self.key(TEXTVALIDATION_SET, "text").is_some()
    }

    /// Validate the text of all annotations
    /// If quiet is not set, it will output validation failures to stderr
    pub fn validate_text(&self, quiet: bool) -> TextValidationResult {
        let mut total = TextValidationResult {
            invalid: 0,
            valid: 0,
            missing: 0,
        };
        for annotation in self.annotations() {
            let result = annotation.validate_text();
            if result == Some(true) {
                total.valid += 1;
            } else {
                if result.is_none() {
                    total.missing += 1;
                } else {
                    total.invalid += 1;
                }
                if !quiet {
                    eprintln!(
                        "[STAM VALIDATION] Failed on {}",
                        annotation
                            .id()
                            .unwrap_or(annotation.as_ref().temp_id().as_deref().unwrap())
                    );
                }
            }
        }
        return total;
    }
}

impl<'store> ResultItem<'store, Annotation> {
    /// Returns (computes) the SHA-1 checksum of the text of this annotation (if any, empty vec otherwise)
    /// Note that this is cryptographically insecure! (but fast)
    pub fn text_checksum(&self, delimiter: &str) -> Option<String> {
        let text = self.text_join(delimiter);
        if text.is_empty() {
            None
        } else {
            let mut hasher = Sha1::new();
            hasher.update(text);
            Some(base16ct::lower::encode_string(&hasher.finalize()))
        }
    }

    /// Returns the text delimiter used for text validation (if any)
    pub fn text_validation_delimiter(&self) -> Option<&'store str> {
        if let Some(delimiter) = self.store().key(TEXTVALIDATION_SET, "delimiter") {
            self.data().filter_key(&delimiter).value_as_str()
        } else {
            None
        }
    }

    /// Get the text validation checksum associated with this annotation (if any)
    /// To compute the actual checksum, use [`text_checksum()`]
    pub fn validation_checksum(&self) -> Option<&'store str> {
        if let Some(key) = self.store().key(TEXTVALIDATION_SET, "checksum") {
            self.data().filter_key(&key).value_as_str()
        } else {
            None
        }
    }

    /// Returns the associated validation text, if any
    pub fn validation_text(&self) -> Option<&'store str> {
        if let Some(key) = self.store().key(TEXTVALIDATION_SET, "text") {
            self.data().filter_key(&key).value_as_str()
        } else {
            None
        }
    }

    /// Tests if the annotation's target text is still valid
    /// Returns None if there is no validation information in the annotation at all!
    pub fn validate_text(&self) -> Option<bool> {
        let delimiter = self.text_validation_delimiter();
        let mut found = false;
        if let Some(refchecksum) = self.validation_checksum() {
            let checksum = self.text_checksum(delimiter.unwrap_or(""));
            found = true;
            if checksum.as_deref() != Some(refchecksum) {
                return Some(false);
            }
        }
        if let Some(reftext) = self.validation_text() {
            let text = self.text_join(delimiter.unwrap_or(""));
            found = true;
            if reftext != text {
                return Some(false);
            }
        }
        if found {
            Some(true)
        } else {
            None
        }
    }
}

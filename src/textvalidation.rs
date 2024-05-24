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

impl AnnotationStore {
    /// Mark all annotations in this store as valid, enabling text validation checks in the future.
    pub fn make_validation_checksums(&mut self) -> Result<(), StamError> {
        let mut queue = Vec::new();
        for annotation in self.annotations() {
            if annotation.validation_checksum().is_none() {
                if let Some(checksum) =
                    annotation.text_checksum(annotation.text_validation_delimiter().unwrap_or(""))
                {
                    queue.push((annotation.handle(), checksum));
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
            for (handle, checksum) in queue {
                let set: &mut AnnotationDataSet = self.get_mut(set_handle)?;
                let data_handle = set.insert_data(BuildItem::None, "checksum", checksum, true)?;
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

    /// Mark all annotations in this store as valid, enabling text validation checks in the future.
    /// Note, this may have considerable space overhead, [`make_validation_checksums()`] is preferred!
    pub fn make_validation_texts(&mut self) -> Result<(), StamError> {
        let mut queue = Vec::new();
        for annotation in self.annotations() {
            if annotation.validation_text().is_none() {
                let text =
                    annotation.text_join(annotation.text_validation_delimiter().unwrap_or(""));
                if !text.is_empty() {
                    queue.push((annotation.handle(), text));
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
            for (handle, text) in queue {
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
    pub fn validate_text(&self, warn_for_all: bool) -> Result<(), StamError> {
        let mut failures = 0;
        for annotation in self.annotations() {
            if !annotation.validate_text() {
                let msg = format!(
                    "Failed on {}",
                    annotation
                        .id()
                        .unwrap_or(annotation.as_ref().temp_id().as_deref().unwrap())
                );
                failures += 1;
                if warn_for_all {
                    eprintln!("[STAM VALIDATION] {}", msg);
                } else {
                    return Err(StamError::ValidationError(msg, ""));
                }
            }
        }
        if failures == 0 {
            Ok(())
        } else {
            return Err(StamError::ValidationError(
                format!("{} annotations failed to validate their text!", failures),
                "",
            ));
        }
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
    /// Returns false if there is no validation information in the annotation at all!
    pub fn validate_text(&self) -> bool {
        let delimiter = self.text_validation_delimiter();
        let mut found = false;
        if let Some(refchecksum) = self.validation_checksum() {
            let checksum = self.text_checksum(delimiter.unwrap_or(""));
            found = true;
            if checksum.as_deref() != Some(refchecksum) {
                return false;
            }
        }
        if let Some(reftext) = self.validation_text() {
            let text = self.text_join(delimiter.unwrap_or(""));
            found = true;
            if reftext != text {
                return false;
            }
        }
        found
    }
}

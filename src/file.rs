/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains some common helper functions for dealing with file I/O

use sealed::sealed;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::config::{Config, Configurable};
use crate::error::StamError;
use crate::types::*;

const KNOWN_EXTENSIONS: &[&str; 14] = &[
    ".store.stam.json",
    ".annotationset.stam.json",
    ".stam.json",
    ".store.stam.cbor",
    ".stam.cbor",
    ".store.stam.csv",
    ".annotationset.stam.csv",
    ".annotations.stam.csv",
    ".stam.csv",
    ".json",
    ".cbor",
    ".csv",
    ".txt",
    ".md",
];

/// Get a file for reading or writing, this resolves relative files more intelligently
pub(crate) fn get_filepath(filename: &str, workdir: Option<&Path>) -> Result<PathBuf, StamError> {
    if filename == "-" {
        //designates stdin or stdout
        return Ok(filename.into());
    }
    if filename.starts_with("https://") || filename.starts_with("http://") {
        //TODO: implement downloading of remote URLs and storing them locally
        return Err(StamError::OtherError("Loading URLs is not supported yet"));
    }
    let path = if filename.starts_with("file://") {
        //strip the file:// prefix
        PathBuf::from(&filename[7..])
    } else {
        PathBuf::from(filename)
    };
    if path.is_absolute() {
        Ok(path)
    } else {
        //check whether we can find one in our workdir first
        if let Some(workdir) = workdir {
            let path = workdir.join(&path);
            if path.is_file() {
                //should also work with symlinks
                return Ok(path);
            }
        }

        //final fallback is simply relative to the current working directly
        // we don't test for existance here
        Ok(path)
    }
}

/// Auxiliary function to help open files
pub(crate) fn open_file(filename: &str, config: &Config) -> Result<File, StamError> {
    let found_filename = get_filepath(filename, config.workdir())?;
    debug(config, || format!("open_file: {:?}", found_filename));
    File::open(found_filename.as_path()).map_err(|e| {
        StamError::IOError(
            e,
            found_filename
                .as_path()
                .to_str()
                .expect("path must be valid unicode")
                .to_owned(),
            "Opening file for reading failed",
        )
    })
}

/// Auxiliary function to help open files
pub(crate) fn create_file(filename: &str, config: &Config) -> Result<File, StamError> {
    let found_filename = get_filepath(filename, config.workdir())?;
    debug(config, || format!("create_file: {:?}", found_filename));
    File::create(found_filename.as_path()).map_err(|e| {
        StamError::IOError(
            e,
            found_filename
                .as_path()
                .to_str()
                .expect("path must be valid unicode")
                .to_owned(),
            "Opening file for reading failed",
        )
    })
}

/// Auxiliary function to help open files
pub(crate) fn open_file_reader(
    filename: &str,
    config: &Config,
) -> Result<Box<dyn BufRead>, StamError> {
    if filename == "-" {
        //read from stdin
        Ok(Box::new(std::io::stdin().lock()))
    } else {
        Ok(Box::new(BufReader::new(open_file(filename, config)?)))
    }
}

/// Auxiliary function to help open files
pub(crate) fn open_file_writer(
    filename: &str,
    config: &Config,
) -> Result<Box<dyn Write>, StamError> {
    if filename == "-" {
        Ok(Box::new(std::io::stdout()))
    } else {
        Ok(Box::new(BufWriter::new(create_file(filename, config)?)))
    }
}

/// Returns the filename without (known!) extension. The extension must be a known extension used by STAM for this to work.
pub(crate) fn strip_known_extension(s: &str) -> &str {
    for extension in KNOWN_EXTENSIONS.iter() {
        if s.ends_with(extension) {
            return &s[0..s.len() - extension.len()];
        }
    }
    s
}

/// Helper function to replace some symbols that may not be valid in a filename
/// Only the actual file name part, without any directories, should be passed here.
/// It is mainly useful in converting public IDs to filenames
pub(crate) fn sanitize_id_to_filename(id: &str) -> String {
    let mut id = id.replace("://", ".").replace(&['/', '\\', ':', '?'], ".");
    for extension in KNOWN_EXTENSIONS.iter() {
        if id.ends_with(extension) {
            id.truncate(id.len() - extension.len());
        }
    }
    id
}

pub(crate) fn filename_without_workdir<'a>(filename: &'a str, config: &Config) -> &'a str {
    if let Some(workdir) = config.workdir().map(|x| x.to_str().expect("valid utf-8")) {
        let filename = &filename[workdir.len()..];
        if filename.starts_with(&['/', '\\']) {
            return &filename[1..];
        } else {
            return filename;
        }
    }
    filename
}

#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait AssociatedFile: Configurable {
    fn filename(&self) -> Option<&str>;

    //Set the associated filename for this structure.
    fn set_filename(&mut self, filename: &str) -> &mut Self;

    //Set the associated filename for this annotation store. Also sets the working directory. Builder pattern.
    fn with_filename(mut self, filename: &str) -> Self
    where
        Self: Sized,
    {
        self.set_filename(filename);
        self
    }

    /// Returns the filename without (known!) extension. The extension must be a known extension used by STAM for this to work.
    fn filename_without_extension(&self) -> Option<&str> {
        if let Some(filename) = self.filename() {
            Some(strip_known_extension(filename))
        } else {
            None
        }
    }

    /// Serializes the filename ready for use with STAM JSON's @include or STAM CSV.
    /// It basically only strips the workdir component, if any.
    fn filename_without_workdir(&self) -> Option<&str> {
        if let Some(filename) = self.filename() {
            Some(filename_without_workdir(filename, self.config()))
        } else {
            None
        }
    }
}

#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub(crate) trait ChangeMarker {
    fn change_marker(&self) -> &Arc<RwLock<bool>>;

    fn changed(&self) -> bool {
        let mut result = true;
        if let Ok(changed) = self.change_marker().read() {
            result = *changed;
        }
        result
    }

    fn mark_changed(&self) {
        if let Ok(mut changed) = self.change_marker().write() {
            *changed = true;
        }
    }

    fn mark_unchanged(&self) {
        if let Ok(mut changed) = self.change_marker().write() {
            *changed = false;
        }
    }
}

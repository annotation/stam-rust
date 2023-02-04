extern crate stam as libstam;

use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::*;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::error::PyStamError;
use libstam::*;

#[pyclass(dict, name = "TextResource")]
pub(crate) struct PyTextResource {
    pub(crate) handle: TextResourceHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
}

#[pymethods]
impl PyTextResource {
    #[getter]
    /// Returns the public ID (by value, aka a copy)
    /// Don't use this for ID comparisons, use has_id() instead
    fn id(&self) -> PyResult<Option<String>> {
        self.map(|res| Ok(res.id().map(|x| x.to_owned())))
    }

    /// Tests the ID of the dataset
    fn has_id(&self, other: &str) -> PyResult<bool> {
        self.map(|res| Ok(res.id() == Some(other)))
    }

    /// Tests whether two datasets are equal
    fn __eq__(&self, other: &PyTextResource) -> PyResult<bool> {
        Ok(self.handle == other.handle)
    }

    /// Returns the full text of the resource (by value, aka a copy)
    fn __str__<'py>(&self, py: Python<'py>) -> PyResult<&'py PyString> {
        self.map(|resource| Ok(PyString::new(py, resource.text())))
    }

    /// Returns the text slice at the specified offset, or of the text as a whole if omitted
    fn text<'py>(&self, offset: Option<&PyOffset>, py: Python<'py>) -> PyResult<&'py PyString> {
        if let Some(offset) = offset {
            self.map(|res| Ok(PyString::new(py, res.text_slice(&offset.offset)?)))
        } else {
            self.__str__(py)
        }
    }

    /// Returns a TextSelection instance referring to the specified offset
    fn text_selection(&self, offset: &PyOffset) -> PyResult<PyTextSelection> {
        self.map(|res| Ok(self.wrap_textselection(res.text_selection(offset.into())?)))
    }
}

impl PyTextResource {
    /// Map function to act on the actual underlying store, helps reduce boilerplate
    fn map<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&TextResource) -> Result<T, StamError>,
    {
        if let Ok(store) = self.store.read() {
            let resource: &TextResource = store
                .resource(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolve textresource"))?;
            f(resource).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }

    fn wrap_textselection(&self, textselection: TextSelection) -> PyTextSelection {
        PyTextSelection {
            textselection,
            handle: self.handle,
            store: self.store.clone(),
        }
    }
}

#[pyclass(dict, name = "Cursor", frozen, freelist = 64)]
#[derive(Clone)]
pub(crate) struct PyCursor {
    cursor: Cursor,
}

#[pymethods]
impl PyCursor {
    #[new]
    fn new(index: isize, endaligned: Option<bool>) -> PyResult<Self> {
        if endaligned.unwrap_or(false) {
            if index <= 0 {
                Ok(Self {
                    cursor: Cursor::EndAligned(index),
                })
            } else {
                Err(PyValueError::new_err(
                    "End aligned cursor should be 0 or negative",
                ))
            }
        } else {
            if index >= 0 {
                Ok(Self {
                    cursor: Cursor::BeginAligned(index as usize),
                })
            } else {
                Err(PyValueError::new_err(
                    "Begin aligned cursor should be 0 or positive",
                ))
            }
        }
    }

    /// Tests if this is a begin-aligned cursor
    fn is_beginaligned(&self) -> bool {
        match self.cursor {
            Cursor::BeginAligned(_) => true,
            _ => false,
        }
    }

    /// Tests if this is an end-aligned cursor
    fn is_endaligned(&self) -> bool {
        match self.cursor {
            Cursor::EndAligned(_) => true,
            _ => false,
        }
    }

    /// Returns the actual cursor value
    fn value(&self) -> isize {
        match self.cursor {
            Cursor::BeginAligned(v) => v as isize,
            Cursor::EndAligned(v) => v,
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.cursor == other.cursor
    }
}

#[pyclass(dict, name = "Offset", frozen, freelist = 64)]
pub(crate) struct PyOffset {
    offset: Offset,
}

#[pymethods]
impl PyOffset {
    #[new]
    fn new(begin: PyCursor, end: PyCursor) -> Self {
        Self {
            offset: Offset {
                begin: begin.cursor,
                end: end.cursor,
            },
        }
    }

    #[staticmethod]
    /// Creates a simple offset with begin aligned cursors
    /// This is typically faster than using the normal constructor
    fn simple(begin: usize, end: usize) -> Self {
        Self {
            offset: Offset::simple(begin, end),
        }
    }

    /// Return the begin cursor
    fn begin(&self) -> PyCursor {
        PyCursor {
            cursor: self.offset.begin,
        }
    }

    /// Return the end cursor
    fn end(&self) -> PyCursor {
        PyCursor {
            cursor: self.offset.end,
        }
    }
}

#[pyclass(dict, name = "TextSelection", frozen, freelist = 64)]
#[derive(Clone)]
pub(crate) struct PyTextSelection {
    pub(crate) textselection: TextSelection,
    pub(crate) handle: TextResourceHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
}

#[pymethods]
impl PyTextSelection {
    /// Resolves a text selection to the actual underlying text
    fn __str__<'py>(&self, py: Python<'py>) -> PyResult<&'py PyString> {
        self.map(|res| Ok(PyString::new(py, res.text_of(&(self.textselection.into())))))
    }

    fn __eq__(&self, other: &PyTextSelection) -> bool {
        self.handle == other.handle && self.textselection == other.textselection
    }

    fn __gt__(&self, other: &PyTextSelection) -> bool {
        self.textselection > other.textselection
    }

    fn __gte__(&self, other: &PyTextSelection) -> bool {
        self.textselection >= other.textselection
    }

    fn __lt__(&self, other: &PyTextSelection) -> bool {
        self.textselection < other.textselection
    }

    fn __lte__(&self, other: &PyTextSelection) -> bool {
        self.textselection <= other.textselection
    }
}

impl PyTextSelection {
    fn map<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&TextResource) -> Result<T, StamError>,
    {
        if let Ok(store) = self.store.read() {
            let resource: &TextResource = store
                .resource(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolve textresource"))?;
            f(resource).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }
}

impl From<PyTextSelection> for TextSelection {
    fn from(other: PyTextSelection) -> Self {
        other.textselection
    }
}

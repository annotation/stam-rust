extern crate stam as libstam;

use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::*;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::annotationdata::PyAnnotationData;
use crate::error::PyStamError;
use libstam::*;

#[pyclass(dict, name = "Annotation")]
pub(crate) struct PyAnnotation {
    pub(crate) handle: AnnotationHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
}

#[pymethods]
impl PyAnnotation {
    #[getter]
    /// Returns the public ID (by value, aka a copy)
    /// Don't use this for ID comparisons, use has_id() instead
    fn id(&self) -> PyResult<Option<String>> {
        self.map(|annotation| Ok(annotation.id().map(|x| x.to_owned())))
    }

    /// Tests the ID of the dataset
    fn has_id(&self, other: &str) -> PyResult<bool> {
        self.map(|annotation| Ok(annotation.id() == Some(other)))
    }

    /// Tests whether two datasets are equal
    fn __eq__(&self, other: &PyAnnotation) -> PyResult<bool> {
        Ok(self.handle == other.handle)
    }

    /// Returns a generator over all data in this annotation
    fn __iter__(&self) -> PyResult<PyDataIter> {
        Ok(PyDataIter {
            handle: self.handle,
            store: self.store.clone(),
            index: 0,
        })
    }
}

#[pyclass(name = "DataIter")]
struct PyDataIter {
    pub(crate) handle: AnnotationHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
    pub(crate) index: usize,
}

#[pymethods]
impl PyDataIter {
    fn __iter__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __next__(mut pyself: PyRefMut<'_, Self>) -> Option<PyAnnotationData> {
        pyself.index += 1; //increment first (prevent exclusive mutability issues)
        pyself.map(|annotation| {
            if let Some((set, handle)) = annotation.data_by_index(pyself.index - 1) {
                //index is one ahead, prevents exclusive lock issues
                Some(PyAnnotationData {
                    set: *set,
                    handle: *handle,
                    store: pyself.store.clone(),
                })
            } else {
                None
            }
        })
    }
}

impl PyDataIter {
    fn map<T, F>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&Annotation) -> Option<T>,
    {
        if let Ok(store) = self.store.read() {
            if let Some(annotation) = store.annotation(&self.handle.into()) {
                f(annotation)
            } else {
                None
            }
        } else {
            None //should never happen here
        }
    }
}

impl PyAnnotation {
    /// Map function to act on the actual underlying store, helps reduce boilerplate
    fn map<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&Annotation) -> Result<T, StamError>,
    {
        if let Ok(store) = self.store.read() {
            let annotation: &Annotation = store
                .annotation(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolve textresource"))?;
            f(annotation).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }

    /// Map function to act on the actual underlying store, helps reduce boilerplate
    fn map_mut<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&mut Annotation) -> Result<T, StamError>,
    {
        if let Ok(mut store) = self.store.write() {
            let annotation: &mut Annotation = store
                .annotation_mut(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolve textresource"))?;
            f(annotation).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }
}

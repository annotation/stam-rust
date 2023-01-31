extern crate stam as libstam;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::ffi::{PyLong_GetInfo, PyLong_Type};
use pyo3::prelude::*;
use pyo3::types::*;
use std::ops::FnOnce;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use libstam::*;

create_exception!(stam, PyStamError, pyo3::exceptions::PyException);

#[pymodule]
fn stam(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // ... other elements added to module ...
    m.add("StamError", py.get_type::<PyStamError>())?;
    m.add_class::<PyAnnotationStore>()?;
    Ok(())
}

#[pyclass(dict, name = "AnnotationStore")]
/// The AnnotationStore
pub struct PyAnnotationStore {
    store: Arc<RwLock<AnnotationStore>>,
}

#[pymethods]
impl PyAnnotationStore {
    #[new]
    #[args(kwargs = "**")]
    fn new(kwargs: Option<&PyDict>) -> PyResult<Self> {
        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs {
                if let Some(key) = key.extract().unwrap() {
                    match key {
                        "file" => {
                            if let Ok(Some(value)) = value.extract() {
                                return match AnnotationStore::from_file(value) {
                                    Ok(store) => Ok(PyAnnotationStore {
                                        store: Arc::new(RwLock::new(store)),
                                    }),
                                    Err(err) => Err(PyStamError::new_err(format!("{}", err))),
                                };
                            }
                        }
                        "id" => {
                            if let Ok(Some(value)) = value.extract() {
                                return Ok(PyAnnotationStore {
                                    store: Arc::new(RwLock::new(
                                        AnnotationStore::default().with_id(value),
                                    )),
                                });
                            }
                        }
                        _ => eprintln!("Ignored unknown kwargs option {}", key),
                    }
                }
            }
        }
        Ok(PyAnnotationStore {
            store: Arc::new(RwLock::new(AnnotationStore::default())),
        })
    }

    #[getter]
    /// Returns the public ID (by value, aka a copy)
    fn id(&self) -> PyResult<Option<String>> {
        self.map(|store| Ok(store.id().map(|x| x.to_owned())))
    }

    /// Saves the annotation store to file using STAM JSON
    fn to_file(&self, filename: &str) -> PyResult<()> {
        self.map(|store| store.to_file(filename))
    }

    fn annotationset(&self, key: &PyAny) -> PyResult<PyAnnotationDataSet> {
        if let Ok(key) = key.extract() {
            let handle = AnnotationDataSetHandle::new(key);
            if <AnnotationStore as StoreFor<AnnotationDataSet>>::has(
                &self.store.read().unwrap(),
                handle,
            ) {
                Ok(PyAnnotationDataSet {
                    handle,
                    store: self.store.clone(),
                })
            } else {
                Err(PyIndexError::new_err(
                    "Annotation set with specified handle does not exist",
                ))
            }
            // } else if let Ok(key) = key.extract() {
        } else {
            Err(PyValueError::new_err(
                "Key must be a string (public id) or integer (internal handle)",
            ))
        }
    }
}

impl PyAnnotationStore {
    /// Map function to act on the actual unlderyling store, helps reduce boilerplate
    fn map<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&AnnotationStore) -> Result<T, StamError>,
    {
        if let Ok(store) = self.store.read() {
            f(&store).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }
}

#[pyclass(dict, name = "AnnotationDataSet")]
pub struct PyAnnotationDataSet {
    handle: AnnotationDataSetHandle,
    store: Arc<RwLock<AnnotationStore>>,
}

#[pymethods]
impl PyAnnotationDataSet {
    #[getter]
    /// Returns the public ID (by value, aka a copy)
    fn id(&self) -> PyResult<Option<String>> {
        self.map(|annotationset| Ok(annotationset.id().map(|x| x.to_owned())))
    }

    fn to_file(&self, filename: &str) -> PyResult<()> {
        self.map(|annotationset| annotationset.to_file(filename))
    }
}

impl PyAnnotationDataSet {
    /// Map function to act on the actual unlderyling store, helps reduce boilerplate
    fn map<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&AnnotationDataSet) -> Result<T, StamError>,
    {
        if let Ok(store) = self.store.read() {
            let annotationset: &AnnotationDataSet = store
                .annotationset(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolved annotationset"))?;
            f(annotationset).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }
}

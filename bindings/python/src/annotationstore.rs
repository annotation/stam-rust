extern crate stam as libstam;

use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::*;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::annotation::PyAnnotation;
use crate::annotationdataset::PyAnnotationDataSet;
use crate::error::PyStamError;
use crate::resources::PyTextResource;
use libstam::*;

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

    /// Returns an AnnotationDataSet by ID
    fn annotationset(&self, id: &str) -> PyResult<PyAnnotationDataSet> {
        self.map(|store| {
            store
                .resolve_dataset_id(id)
                .map(|handle| PyAnnotationDataSet {
                    handle,
                    store: self.store.clone(),
                })
        })
    }

    /// Returns an Annotation by ID
    fn annotation(&self, id: &str) -> PyResult<PyAnnotation> {
        self.map(|store| {
            store.resolve_annotation_id(id).map(|handle| PyAnnotation {
                handle,
                store: self.store.clone(),
            })
        })
    }

    /// Returns a TextResource by ID
    fn resource(&self, id: &str) -> PyResult<PyTextResource> {
        self.map(|store| {
            store.resolve_resource_id(id).map(|handle| PyTextResource {
                handle,
                store: self.store.clone(),
            })
        })
    }

    fn add_resource(&self) -> PyResult<PyTextResource> {
        //TODO: implement
        panic!("not implemented yet");
    }

    fn add_dataset(&self) -> PyResult<PyAnnotationDataSet> {
        //TODO: implement
        panic!("not implemented yet");
    }

    fn add_annotation(&self) -> PyResult<PyAnnotation> {
        //TODO: implement
        panic!("not implemented yet");
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

    fn map_mut<T, F>(&mut self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&mut AnnotationStore) -> Result<T, StamError>,
    {
        if let Ok(mut store) = self.store.write() {
            f(&mut store).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "unable to obtain exclusive lock for writing to store",
            ))
        }
    }
}

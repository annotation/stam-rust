extern crate stam as libstam;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::ffi::{PyLong_GetInfo, PyLong_Type, Py_None};
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

    /// Returns an AnnotationDataSet
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
    /// Don't use this for ID comparisons, use has_id() instead
    fn id(&self) -> PyResult<Option<String>> {
        self.map(|annotationset| Ok(annotationset.id().map(|x| x.to_owned())))
    }

    /// Tests the ID of the dataset
    fn has_id(&self, other: &str) -> PyResult<bool> {
        self.map(|annotationset| Ok(annotationset.id() == Some(other)))
    }

    fn __eq__(&self, other: &PyAnnotationDataSet) -> PyResult<bool> {
        Ok(self.handle == other.handle)
    }

    /// Save the annotation dataset to a STAM JSON file
    fn to_file(&self, filename: &str) -> PyResult<()> {
        self.map(|annotationset| annotationset.to_file(filename))
    }

    /// Get a DataKey instance by ID, raises an exception if not found
    fn key(&self, key: &str) -> PyResult<PyDataKey> {
        self.map(|annotationset| {
            annotationset
                .resolve_key_id(key)
                .map(|keyhandle| PyDataKey {
                    set: self.handle,
                    handle: keyhandle,
                    store: self.store.clone(),
                })
        })
    }

    /// Get a AnnotationData instance by id, raises an exception if not found
    fn annotationdata(&self, data_id: &str) -> PyResult<PyAnnotationData> {
        self.map(|annotationset| {
            annotationset
                .resolve_data_id(data_id)
                .map(|datahandle| PyAnnotationData {
                    set: self.handle,
                    handle: datahandle,
                    store: self.store.clone(),
                })
        })
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

#[pyclass(dict, name = "DataKey")]
pub struct PyDataKey {
    set: AnnotationDataSetHandle,
    handle: DataKeyHandle,
    store: Arc<RwLock<AnnotationStore>>,
}

#[pymethods]
impl PyDataKey {
    #[getter]
    /// Returns the public ID (by value, aka a copy)
    /// Don't use this for ID comparisons, use has_id() instead
    fn id(&self) -> PyResult<Option<String>> {
        self.map(|datakey| Ok(datakey.id().map(|x| x.to_owned())))
    }

    /// Returns the public ID (by value, aka a copy)
    /// Use this sparingly
    fn __str__(&self) -> PyResult<Option<String>> {
        self.map(|datakey| Ok(datakey.id().map(|x| x.to_owned())))
    }

    /// Tests the ID of the dataset
    fn has_id(&self, other: &str) -> PyResult<bool> {
        self.map(|datakey| Ok(datakey.id() == Some(other)))
    }

    fn __eq__(&self, other: &PyDataKey) -> PyResult<bool> {
        Ok(self.handle == other.handle)
    }
}

impl PyDataKey {
    fn map<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&DataKey) -> Result<T, StamError>,
    {
        if let Ok(store) = self.store.read() {
            let annotationset: &AnnotationDataSet = store
                .annotationset(&self.set.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolved annotationset"))?;
            let datakey: &DataKey = annotationset
                .key(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolved annotationset"))?;
            f(datakey).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }
}

#[pyclass(dict, name = "AnnotationData")]
pub struct PyAnnotationData {
    set: AnnotationDataSetHandle,
    handle: AnnotationDataHandle,
    store: Arc<RwLock<AnnotationStore>>,
}

fn datavalue_into_py<'py>(datavalue: &DataValue, py: Python<'py>) -> Result<&'py PyAny, StamError> {
    match datavalue {
        DataValue::String(s) => Ok(s.into_py(py).into_ref(py)),
        DataValue::Float(f) => Ok(f.into_py(py).into_ref(py)),
        DataValue::Int(v) => Ok(v.into_py(py).into_ref(py)),
        DataValue::Bool(v) => Ok(v.into_py(py).into_ref(py)),
        DataValue::Null => {
            //feels a bit hacky, but I can't find a PyNone to return as PyAny
            let x: Option<bool> = None;
            Ok(x.into_py(py).into_ref(py))
        }
        DataValue::List(v) => {
            let pylist = PyList::empty(py);
            for item in v.iter() {
                let pyvalue = datavalue_into_py(item, py)?;
                pylist.append(pyvalue);
            }
            Ok(pylist)
        }
    }
}

#[pymethods]
impl PyAnnotationData {
    /// Returns a DataKey instance
    fn key(&self) -> PyResult<PyDataKey> {
        self.map(|annotationdata| {
            Ok(PyDataKey {
                set: self.set,
                handle: annotationdata.key(),
                store: self.store.clone(),
            })
        })
    }

    /// Returns the value (makes a copy)
    /// In comparisons, use test_value() instead
    fn value<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        self.map(|annotationdata| datavalue_into_py(annotationdata.value(), py))
    }
}

impl PyAnnotationData {
    fn map<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&AnnotationData) -> Result<T, StamError>,
    {
        if let Ok(store) = self.store.read() {
            let annotationset: &AnnotationDataSet = store
                .annotationset(&self.set.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolved annotationset"))?;
            let data: &AnnotationData = annotationset
                .annotationdata(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolved annotationset"))?;
            f(data).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }
}

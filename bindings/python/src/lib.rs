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
    m.add_class::<PyAnnotationDataSet>()?;
    m.add_class::<PyAnnotationData>()?;
    m.add_class::<PyDataKey>()?;
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

    /// Tests whether two datasets are equal
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

    /// Create a new DataKey and adds it to the dataset
    fn add_key(&self, key: &str) -> PyResult<PyDataKey> {
        self.map_mut(|annotationset| {
            let datakey = DataKey::new(key.to_string());
            let handle = annotationset.insert(datakey)?;
            Ok(PyDataKey {
                set: self.handle,
                handle,
                store: self.store.clone(),
            })
        })
    }

    /// Create a new AnnotationData instance and adds it to the dataset
    fn add_data<'py>(
        &self,
        key: &str,
        value: &'py PyAny,
        id: Option<&str>,
    ) -> PyResult<PyAnnotationData> {
        let datakey = if let Ok(datakey) = self.key(key) {
            datakey
        } else {
            self.add_key(key)?
        };
        self.map_mut(|annotationset| {
            let value = py_into_datavalue(value)?;
            let datakey = AnnotationData::new(id.map(|x| x.to_string()), datakey.handle, value);
            let handle = annotationset.insert(datakey)?;
            Ok(PyAnnotationData {
                set: self.handle,
                handle,
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
    /// Map function to act on the actual underlyingtore, helps reduce boilerplate
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

    /// Map function to act on the actual underlying store mutably, helps reduce boilerplate
    fn map_mut<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&mut AnnotationDataSet) -> Result<T, StamError>,
    {
        if let Ok(mut store) = self.store.write() {
            let annotationset: &mut AnnotationDataSet = store
                .annotationset_mut(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolved annotationset"))?;
            f(annotationset).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Can't get exclusive lock to write to store",
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

fn py_into_datavalue<'py>(value: &'py PyAny) -> Result<DataValue, StamError> {
    if let Ok(value) = value.extract() {
        Ok(DataValue::String(value))
    } else if let Ok(value) = value.extract() {
        Ok(DataValue::Int(value))
    } else if let Ok(value) = value.extract() {
        Ok(DataValue::Float(value))
    } else if let Ok(value) = value.extract() {
        Ok(DataValue::Bool(value))
    } else if let Ok(None) = value.extract::<Option<bool>>() {
        Ok(DataValue::Null)
    } else {
        if let Ok(true) = value.is_instance_of::<PyList>() {
            let value: &PyList = value.downcast().unwrap();
            let mut list: Vec<DataValue> = Vec::new();
            for item in value {
                let pyitem = py_into_datavalue(item)?;
                list.push(pyitem);
            }
            return Ok(DataValue::List(list));
        }
        Err(StamError::OtherError(
            "Can't convert supplied Python object to a DataValue",
        ))
    }
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
                pylist.append(pyvalue).expect("adding value to list");
            }
            Ok(pylist)
        }
    }
}

#[pyclass(dict, name = "DataValue")]
pub struct PyDataValue {
    value: DataValue,
}

#[pymethods]
impl PyDataValue {
    // Get the actual value
    fn get<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        datavalue_into_py(&self.value, py).map_err(|err| PyStamError::new_err(format!("{}", err)))
    }

    #[new]
    fn new<'py>(value: &PyAny) -> PyResult<Self> {
        Ok(PyDataValue {
            value: py_into_datavalue(value)
                .map_err(|err| PyStamError::new_err(format!("{}", err)))?,
        })
    }

    fn __eq__(&self, other: &PyDataValue) -> bool {
        self.value == other.value
    }
}

impl PyDataValue {
    fn new_cloned(value: &DataValue) -> Result<Self, StamError> {
        Ok(PyDataValue {
            value: value.clone(),
        })
    }

    fn test(&self, other: &DataValue) -> bool {
        self.value == *other
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
    fn value(&self) -> PyResult<PyDataValue> {
        self.map(|annotationdata| PyDataValue::new_cloned(annotationdata.value()))
    }

    /// Tests whether the value equals another
    /// This is more efficient than calling [`value()`] and doing the comparison yourself
    fn test_value<'py>(&self, reference: &'py PyDataValue) -> PyResult<bool> {
        self.map(|annotationdata| Ok(reference.test(&annotationdata.value())))
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

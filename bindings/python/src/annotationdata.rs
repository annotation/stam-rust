extern crate stam as libstam;

use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::*;
use std::fmt::Display;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::annotationdataset::PyAnnotationDataSet;
use crate::annotationstore::MapStore;
use crate::error::PyStamError;
use libstam::*;

#[pyclass(name = "DataKey")]
pub(crate) struct PyDataKey {
    pub(crate) set: AnnotationDataSetHandle,
    pub(crate) handle: DataKeyHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
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

    fn __richcmp__(&self, other: PyRef<Self>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.handle == other.handle).into_py(py),
            CompareOp::Ne => (self.handle != other.handle).into_py(py),
            _ => py.NotImplemented(),
        }
    }

    /// Returns the AnnotationDataSet this key is part of
    fn annotationset(&self) -> PyResult<PyAnnotationDataSet> {
        Ok(PyAnnotationDataSet {
            handle: self.set,
            store: self.store.clone(),
        })
    }

    /// Returns the AnnotationData instances this key refers to.
    /// This is a lookup in the reverse index.
    /// The results will be returned in a tuple.
    fn annotationdata<'py>(&self, py: Python<'py>) -> PyResult<&'py PyTuple> {
        self.map_store(|store| {
            let annotationset: &AnnotationDataSet = store.get(self.set)?;
            let elements: Vec<Py<PyAnnotationData>> = annotationset
                .data_by_key(self.handle)
                .unwrap_or(&Vec::new())
                .iter()
                .map(|handle| {
                    Py::new(
                        py,
                        PyAnnotationData {
                            handle: *handle,
                            set: self.set,
                            store: self.store.clone(),
                        },
                    )
                    .expect("Annotation.annotations() wrapping PyAnnotation")
                })
                .collect();
            Ok(PyTuple::new(py, elements))
        })
    }
}

impl MapStore for PyDataKey {
    fn get_store(&self) -> &Arc<RwLock<AnnotationStore>> {
        &self.store
    }
    fn get_store_mut(&mut self) -> &mut Arc<RwLock<AnnotationStore>> {
        &mut self.store
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

#[pyclass(name = "AnnotationData")]
pub(crate) struct PyAnnotationData {
    pub(crate) set: AnnotationDataSetHandle,
    pub(crate) handle: AnnotationDataHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
}

pub(crate) fn py_into_datavalue<'py>(value: &'py PyAny) -> Result<DataValue, StamError> {
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

pub(crate) fn datavalue_into_py<'py>(
    datavalue: &DataValue,
    py: Python<'py>,
) -> Result<&'py PyAny, StamError> {
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

#[pyclass(name = "DataValue")]
#[derive(Clone, Debug)]
/// Encapsulates a value and its type. Held by `AnnotationData`. This type is not a reference but holds the actual value.
pub(crate) struct PyDataValue {
    pub(crate) value: DataValue,
}

impl std::fmt::Display for PyDataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
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

    fn __richcmp__(&self, other: PyRef<Self>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.value == other.value).into_py(py),
            CompareOp::Ne => (self.value != other.value).into_py(py),
            _ => py.NotImplemented(),
        }
    }

    fn __str__(&self) -> String {
        self.to_string()
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

//not sure if we really need these from implementations here

impl From<&str> for PyDataValue {
    fn from(other: &str) -> Self {
        PyDataValue {
            value: other.into(),
        }
    }
}

impl From<String> for PyDataValue {
    fn from(other: String) -> Self {
        PyDataValue {
            value: other.into(),
        }
    }
}

impl From<usize> for PyDataValue {
    fn from(other: usize) -> Self {
        PyDataValue {
            value: other.into(),
        }
    }
}

impl From<isize> for PyDataValue {
    fn from(other: isize) -> Self {
        PyDataValue {
            value: other.into(),
        }
    }
}

impl From<f64> for PyDataValue {
    fn from(other: f64) -> Self {
        PyDataValue {
            value: other.into(),
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
    fn value(&self) -> PyResult<PyDataValue> {
        self.map(|annotationdata| PyDataValue::new_cloned(annotationdata.value()))
    }

    /// Tests whether the value equals another
    /// This is more efficient than calling [`value()`] and doing the comparison yourself
    fn test_value<'py>(&self, reference: &'py PyDataValue) -> PyResult<bool> {
        self.map(|annotationdata| Ok(reference.test(&annotationdata.value())))
    }

    /// Returns the public ID (by value, aka a copy)
    /// Don't use this for ID comparisons, use has_id() instead
    fn id(&self) -> PyResult<Option<String>> {
        self.map(|annotationdata| Ok(annotationdata.id().map(|x| x.to_owned())))
    }

    /// Returns the public ID (by value, aka a copy)
    /// Use this sparingly
    fn __str__(&self) -> PyResult<Option<String>> {
        self.map(|annotationdata| Ok(annotationdata.id().map(|x| x.to_owned())))
    }

    /// Tests the ID of the dataset
    fn has_id(&self, other: &str) -> PyResult<bool> {
        self.map(|annotationdata| Ok(annotationdata.id() == Some(other)))
    }

    fn __richcmp__(&self, other: PyRef<Self>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.handle == other.handle).into_py(py),
            CompareOp::Ne => (self.handle != other.handle).into_py(py),
            _ => py.NotImplemented(),
        }
    }

    /// Returns the AnnotationDataSet this data is part of
    fn annotationset(&self) -> PyResult<PyAnnotationDataSet> {
        Ok(PyAnnotationDataSet {
            handle: self.set,
            store: self.store.clone(),
        })
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
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolve annotationset"))?;
            let data: &AnnotationData = annotationset
                .annotationdata(&self.handle.into())
                .ok_or_else(|| PyRuntimeError::new_err("Failed to resolve annotationset"))?;
            f(data).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }
}

#[pyclass(name = "AnnotationDataBuilder")]
pub(crate) struct PyAnnotationDataBuilder {
    pub(crate) builder: AnnotationDataBuilder,
}

impl From<PyAnnotationDataBuilder> for AnnotationDataBuilder {
    fn from(other: PyAnnotationDataBuilder) -> Self {
        other.builder
    }
}

#[pymethods]
impl PyAnnotationDataBuilder {
    #[new]
    /// Holds a build recipe to build AnnotationData.
    /// It is typically passed to the annotate() function of the AnnotationStore.
    ///
    /// If you already have existing AnnotationData or DataKey objects, then consider
    /// using the `link()` respectively `link_key()` static methods instead, as those will be quicker.
    fn new(
        annotationset: String,
        key: String,
        value: &PyAny,
        id: Option<String>,
    ) -> PyResult<Self> {
        let mut builder = AnnotationDataBuilder::default();
        if let Some(id) = id {
            builder.id = AnyId::Id(id);
        }
        builder.annotationset = AnyId::Id(annotationset);
        builder.key = AnyId::Id(key);
        builder.value =
            py_into_datavalue(value).map_err(|err| PyStamError::new_err(format!("{}", err)))?;
        Ok(PyAnnotationDataBuilder { builder })
    }

    #[staticmethod]
    /// If you already have an existing AnnotationData you want to use, then
    /// using this method is much quicker than using the normal constructor
    fn link(reference: &PyAnnotationData) -> PyResult<Self> {
        let mut builder = AnnotationDataBuilder::default();
        builder.id = AnyId::Handle(reference.handle);
        builder.annotationset = AnyId::Handle(reference.set);
        Ok(PyAnnotationDataBuilder { builder })
    }

    #[staticmethod]
    /// If you already have an existing PyDataKey you want to use, then
    /// using this method is much quicker than using the normal constructor
    fn link_key(key: &PyDataKey, value: &PyAny, id: Option<String>) -> PyResult<Self> {
        let mut builder = AnnotationDataBuilder::default();
        builder.annotationset = AnyId::Handle(key.set);
        builder.key = AnyId::Handle(key.handle);
        if let Some(id) = id {
            builder.id = AnyId::Id(id);
        }
        builder.value =
            py_into_datavalue(value).map_err(|err| PyStamError::new_err(format!("{}", err)))?;
        Ok(PyAnnotationDataBuilder { builder })
    }
}

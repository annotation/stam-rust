extern crate stam as libstam;

use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::*;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::annotationdata::{py_into_datavalue, PyAnnotationData, PyDataKey, PyDataValue};
use crate::error::PyStamError;
use crate::selector::PySelector;
use libstam::*;

#[pyclass(dict, name = "AnnotationDataSet")]
pub(crate) struct PyAnnotationDataSet {
    pub(crate) handle: AnnotationDataSetHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
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

    /// Returns the number of keys in the set (not substracting deletions)
    fn keys_len(&self) -> PyResult<usize> {
        self.map(|store| Ok(store.keys_len()))
    }

    /// Returns the number of annotations in the set (not substracting deletions)
    fn data_len(&self) -> PyResult<usize> {
        self.map(|store| Ok(store.data_len()))
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

    /// Returns a generator over all keys in this store
    fn keys(&self) -> PyResult<PyDataKeyIter> {
        Ok(PyDataKeyIter {
            handle: self.handle,
            store: self.store.clone(),
            index: 0,
        })
    }

    /// Returns a generator over all data in this store
    fn __iter__(&self) -> PyResult<PyAnnotationDataIter> {
        Ok(PyAnnotationDataIter {
            handle: self.handle,
            store: self.store.clone(),
            index: 0,
        })
    }

    /// Returns a Selector (DataSetSelector) pointing to this AnnotationDataSet
    fn selector(&self) -> PyResult<PySelector> {
        self.map(|set| set.selector().map(|sel| sel.into()))
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

#[pyclass(name = "DataKeyIter")]
struct PyDataKeyIter {
    pub(crate) handle: AnnotationDataSetHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
    pub(crate) index: usize,
}

#[pymethods]
impl PyDataKeyIter {
    fn __iter__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __next__(mut pyself: PyRefMut<'_, Self>) -> Option<PyDataKey> {
        pyself.index += 1; //increment first (prevent exclusive mutability issues)
        let result = pyself.map(|dataset| {
            let datakey_handle = DataKeyHandle::new(pyself.index - 1);
            if <AnnotationDataSet as StoreFor<libstam::DataKey>>::has(dataset, datakey_handle) {
                //index is one ahead, prevents exclusive lock issues
                Some(PyDataKey {
                    set: pyself.handle,
                    handle: datakey_handle,
                    store: pyself.store.clone(),
                })
            } else {
                None
            }
        });
        if result.is_some() {
            result
        } else {
            if pyself.index >= pyself.map(|dataset| Some(dataset.keys_len())).unwrap() {
                None
            } else {
                Self::__next__(pyself)
            }
        }
    }
}

impl PyDataKeyIter {
    /// Map function to act on the actual underlyingtore, helps reduce boilerplate
    fn map<T, F>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&AnnotationDataSet) -> Option<T>,
    {
        if let Ok(store) = self.store.read() {
            if let Some(annotationset) = store.annotationset(&self.handle.into()) {
                f(annotationset)
            } else {
                None
            }
        } else {
            None //should never happen
        }
    }
}

#[pyclass(name = "AnnotationDataIter")]
struct PyAnnotationDataIter {
    pub(crate) handle: AnnotationDataSetHandle,
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
    pub(crate) index: usize,
}

#[pymethods]
impl PyAnnotationDataIter {
    fn __iter__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __next__(mut pyself: PyRefMut<'_, Self>) -> Option<PyAnnotationData> {
        pyself.index += 1; //increment first (prevent exclusive mutability issues)
        let result = pyself.map(|dataset| {
            let data_handle = AnnotationDataHandle::new(pyself.index - 1);
            if <AnnotationDataSet as StoreFor<libstam::AnnotationData>>::has(dataset, data_handle) {
                //index is one ahead, prevents exclusive lock issues
                Some(PyAnnotationData {
                    set: pyself.handle,
                    handle: data_handle,
                    store: pyself.store.clone(),
                })
            } else {
                None
            }
        });
        if result.is_some() {
            result
        } else {
            if pyself.index >= pyself.map(|dataset| Some(dataset.keys_len())).unwrap() {
                None
            } else {
                Self::__next__(pyself)
            }
        }
    }
}

impl PyAnnotationDataIter {
    /// Map function to act on the actual underlyingtore, helps reduce boilerplate
    fn map<T, F>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&AnnotationDataSet) -> Option<T>,
    {
        if let Ok(store) = self.store.read() {
            if let Some(annotationset) = store.annotationset(&self.handle.into()) {
                f(annotationset)
            } else {
                None
            }
        } else {
            None //should never happen
        }
    }
}

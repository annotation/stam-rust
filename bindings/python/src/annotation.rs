use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::*;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::annotationdata::PyAnnotationData;
use crate::annotationdataset::PyAnnotationDataSet;
use crate::annotationstore::MapStore;
use crate::error::PyStamError;
use crate::resources::{PyTextResource, PyTextSelection};
use crate::selector::PySelector;
use stam::*;

#[pyclass(name = "Annotation")]
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

    fn __richcmp__(&self, other: PyRef<Self>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.handle == other.handle).into_py(py),
            CompareOp::Ne => (self.handle != other.handle).into_py(py),
            _ => py.NotImplemented(),
        }
    }

    /// Returns a generator over all data in this annotation
    fn __iter__(&self) -> PyResult<PyDataIter> {
        Ok(PyDataIter {
            handle: self.handle,
            store: self.store.clone(),
            index: 0,
        })
    }

    /// Returns a Selector (AnnotationSelector) pointing to this Annotation
    /// If the annotation references any text, so will this
    fn selector(&self) -> PyResult<PySelector> {
        self.map(|annotation| annotation.selector().map(|sel| sel.into()))
    }

    /// Returns the text of the annotation.
    /// Note that this will always return a tuple (even it if only contains a single element),
    /// as an annotation may reference multiple texts.
    ///
    /// If you are sure an annotation only reference a single contingent text slice or are okay with slices being concatenated, then you can use `str()` instead.
    fn text<'py>(&self, py: Python<'py>) -> PyResult<&'py PyTuple> {
        self.map_store(|store| {
            let annotation: &Annotation = store.get(self.handle)?;
            let elements: Vec<&str> = store.text_by_annotation(annotation).collect();
            Ok(PyTuple::new(py, elements))
        })
    }

    /// Returns the text of the annotation.
    /// If the annotation references multiple text slices, they will be concatenated with a space as a delimiter,
    /// but note that in reality the different parts may be non-contingent!
    ///
    /// Use `text()` instead to retrieve a tuple
    fn __str__(&self) -> PyResult<String> {
        self.map_store(|store| {
            let annotation: &Annotation = store.get(self.handle)?;
            let elements: Vec<&str> = store.text_by_annotation(annotation).collect();
            let result: String = elements.join(" ");
            Ok(result)
        })
    }

    /// Returns the textselections of the annotation.
    /// Note that this will always return a tuple (even it if only contains a single element),
    /// as an annotation may reference multiple text selections.
    fn textselections<'py>(&self, py: Python<'py>) -> PyResult<&'py PyTuple> {
        self.map_store(|store| {
            let annotation: &Annotation = store.get(self.handle)?;
            let elements: Vec<Py<PyTextSelection>> = store
                .textselections_by_annotation(annotation)
                .map(|(reshandle, textselection)| {
                    Py::new(
                        py,
                        PyTextSelection {
                            textselection,
                            handle: reshandle,
                            store: self.store.clone(),
                        },
                    )
                    .expect("textselection to pytextselection")
                })
                .collect();
            Ok(PyTuple::new(py, elements))
        })
    }

    /// Returns the annotations this annotation refers to (i.e. using an AnnotationSelector)
    /// They will be returned in a tuple.
    #[pyo3(signature = (recursive=false))]
    fn annotations<'py>(&self, recursive: bool, py: Python<'py>) -> PyResult<&'py PyTuple> {
        self.map_store(|store| {
            let annotation: &Annotation = store.get(self.handle)?;
            let elements: Vec<Py<PyAnnotation>> = store
                .annotations_by_annotation(annotation, recursive, false)
                .map(|targetitem| {
                    Py::new(
                        py,
                        PyAnnotation {
                            handle: targetitem.handle().expect("must have handle"),
                            store: self.store.clone(),
                        },
                    )
                    .expect("Annotation.annotations() wrapping PyAnnotation")
                })
                .collect();
            Ok(PyTuple::new(py, elements))
        })
    }

    /// Returns the resources this annotation refers to
    /// They will be returned in a tuple.
    fn resources<'py>(&self, py: Python<'py>) -> PyResult<&'py PyTuple> {
        self.map_store(|store| {
            let annotation: &Annotation = store.get(self.handle)?;
            let elements: Vec<Py<PyTextResource>> = store
                .resources_by_annotation(annotation)
                .map(|targetitem| {
                    Py::new(
                        py,
                        PyTextResource {
                            handle: targetitem.handle().expect("must have handle"),
                            store: self.store.clone(),
                        },
                    )
                    .expect("Annotation.annotations() wrapping PyAnnotation")
                })
                .collect();
            Ok(PyTuple::new(py, elements))
        })
    }

    /// Returns the resources this annotation refers to
    /// They will be returned in a tuple.
    fn annotationsets<'py>(&self, py: Python<'py>) -> PyResult<&'py PyTuple> {
        self.map_store(|store| {
            let annotation: &Annotation = store.get(self.handle)?;
            let elements: Vec<Py<PyAnnotationDataSet>> = store
                .annotationsets_by_annotation(annotation)
                .map(|targetitem| {
                    Py::new(
                        py,
                        PyAnnotationDataSet {
                            handle: targetitem.handle().expect("must have handle"),
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

impl MapStore for PyAnnotation {
    fn get_store(&self) -> &Arc<RwLock<AnnotationStore>> {
        &self.store
    }
    fn get_store_mut(&mut self) -> &mut Arc<RwLock<AnnotationStore>> {
        &mut self.store
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

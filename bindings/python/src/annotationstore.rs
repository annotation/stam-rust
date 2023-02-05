extern crate stam as libstam;

use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::*;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::annotation::PyAnnotation;
use crate::annotationdata::PyAnnotationDataBuilder;
use crate::annotationdataset::PyAnnotationDataSet;
use crate::error::PyStamError;
use crate::resources::{PyTextResource, PyTextSelection};
use crate::selector::PySelector;
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
                    store: self.store.clone(), //just a smart pointer clone, not the whole store
                })
        })
    }

    /// Returns an Annotation by ID
    fn annotation(&self, id: &str) -> PyResult<PyAnnotation> {
        self.map(|store| {
            store.resolve_annotation_id(id).map(|handle| PyAnnotation {
                handle,
                store: self.store.clone(), //just a smart pointer clone, not the whole store
            })
        })
    }

    /// Returns a TextResource by ID
    fn resource(&self, id: &str) -> PyResult<PyTextResource> {
        self.map(|store| {
            store.resolve_resource_id(id).map(|handle| PyTextResource {
                handle,
                store: self.store.clone(), //just a smart pointer clone, not the whole store
            })
        })
    }

    /// Create a new TextResource and adds it to the store
    fn add_resource(
        &mut self,
        filename: Option<&str>,
        text: Option<String>,
        id: Option<&str>,
    ) -> PyResult<PyTextResource> {
        if id.is_none() && filename.is_none() {
            return Err(PyRuntimeError::new_err(
                "Incomplete, set either id or filename",
            ));
        }
        if filename.is_some() && text.is_some() {
            return Err(PyRuntimeError::new_err(
                "Set either filename or text keyword arguments, but not both",
            ));
        }
        let store_clone = self.store.clone(); //just a smart pointer clone, not the whole store
        self.map_mut(|store| {
            let mut resource = TextResource::new(
                id.unwrap_or_else(|| filename.expect("filename"))
                    .to_string(),
            );
            if let Some(text) = text {
                resource = resource.with_string(text);
            }
            let handle = store.insert(resource)?;
            Ok(PyTextResource {
                handle,
                store: store_clone,
            })
        })
    }

    /// Create a new AnnotationDataSet and adds it to the store
    fn add_annotationset(&mut self, id: String) -> PyResult<PyAnnotationDataSet> {
        let store_clone = self.store.clone();
        self.map_mut(|store| {
            let annotationset = AnnotationDataSet::new().with_id(id);
            let handle = store.insert(annotationset)?;
            Ok(PyAnnotationDataSet {
                handle,
                store: store_clone,
            })
        })
    }

    /// Adds an annotation. Returns an Annotation instance pointing to the added annotation.
    fn annotate(
        &mut self,
        target: PySelector,
        data: Vec<PyRef<PyAnnotationDataBuilder>>,
        id: Option<String>,
    ) -> PyResult<PyAnnotation> {
        let mut builder = AnnotationBuilder::new();
        if let Some(id) = id {
            builder = builder.with_id(id);
        }
        builder = builder.with_selector(target.selector);
        for databuilder in data.iter() {
            builder = builder.with_data_builder(databuilder.builder.clone()); //MAYBE TODO: I don't like needing an extra clone here, but it can't move out of the PyRef
        }
        let store_clone = self.store.clone(); //just a smart pointer clone, not the whole store
        self.map_mut(|store| {
            Ok(PyAnnotation {
                handle: store.annotate(builder)?,
                store: store_clone,
            })
        })
    }

    /// Returns a generator over all annotations in this store
    fn annotations(&self) -> PyResult<PyAnnotationIter> {
        Ok(PyAnnotationIter {
            store: self.store.clone(),
            index: 0,
        })
    }

    /// Returns a generator over all annotations in this store
    fn annotationsets(&self) -> PyResult<PyAnnotationDataSetIter> {
        Ok(PyAnnotationDataSetIter {
            store: self.store.clone(),
            index: 0,
        })
    }

    /// Returns a generator over all resources in this store
    fn resources(&self) -> PyResult<PyResourceIter> {
        Ok(PyResourceIter {
            store: self.store.clone(),
            index: 0,
        })
    }

    /// Returns the number of annotations in the store (not substracting deletions)
    fn annotations_len(&self) -> PyResult<usize> {
        self.map(|store| Ok(store.annotations_len()))
    }

    /// Returns the number of resources in the store (not substracting deletions)
    fn resources_len(&self) -> PyResult<usize> {
        self.map(|store| Ok(store.resources_len()))
    }

    /// Returns the number of annotation data sets in the store (not substracting deletions)
    fn annotationsets_len(&self) -> PyResult<usize> {
        self.map(|store| Ok(store.annotationsets_len()))
    }

    /// Applies a selector to the annotation store and returns the target(s)
    /// May return a multitude of types depending on the selector, returns
    /// a list if multiple targets were found (internally consumes an iterator).
    fn select<'py>(&self, selector: &PySelector, py: Python<'py>) -> PyResult<&'py PyAny> {
        match &selector.selector {
            Selector::ResourceSelector(handle) => Ok(Py::new(
                py,
                PyTextResource {
                    handle: *handle,
                    store: self.store.clone(),
                },
            )?
            .into_ref(py)),
            Selector::DataSetSelector(handle) => Ok(Py::new(
                py,
                PyAnnotationDataSet {
                    handle: *handle,
                    store: self.store.clone(),
                },
            )?
            .into_ref(py)),
            Selector::TextSelector(handle, offset) => self.map(|store| {
                let resource: &TextResource = store.get(*handle)?;
                let textselection = resource.text_selection(offset)?;
                let pytextselection: &'py PyAny = Py::new(
                    py,
                    PyTextSelection {
                        handle: *handle,
                        textselection,
                        store: self.store.clone(),
                    },
                )
                .expect("creating PyTextSelection")
                .into_ref(py);
                Ok(pytextselection)
            }),
            Selector::AnnotationSelector(handle, None) => Ok(Py::new(
                py,
                PyAnnotation {
                    handle: *handle,
                    store: self.store.clone(),
                },
            )?
            .into_ref(py)),
            Selector::AnnotationSelector(handle, _offset) => {
                self.map(|store| {
                    let annotation: &Annotation = store.get(*handle)?;
                    let result = PyList::empty(py); //TODO: I want a tuple rather than a list but didn't succeed
                    let textselections = PyList::empty(py);
                    for (resource, textselection) in store.textselections_by_annotation(annotation)
                    {
                        textselections
                            .append(
                                Py::new(
                                    py,
                                    PyTextSelection {
                                        handle: resource,
                                        textselection,
                                        store: self.store.clone(),
                                    },
                                )
                                .unwrap()
                                .into_ref(py),
                            )
                            .expect("adding textselection");
                    }
                    let pyannotation: &PyAny = Py::new(
                        py,
                        PyAnnotation {
                            handle: *handle,
                            store: self.store.clone(),
                        },
                    )
                    .unwrap()
                    .into_ref(py);
                    let textselections: &PyAny = textselections.into();
                    result.append(pyannotation).expect("adding to result"); //TODO: should go into tuple rather than list
                    result.append(textselections).expect("adding to result");
                    let result: &PyAny = result.into();
                    Ok(result)
                })
            }
            Selector::MultiSelector(v)
            | Selector::CompositeSelector(v)
            | Selector::DirectionalSelector(v) => {
                let result = PyList::empty(py);
                for subselector in v.iter() {
                    let subresult: &PyAny = self.select(
                        &PySelector {
                            selector: subselector.clone(),
                        },
                        py,
                    )?;
                    result.append(subresult)?;
                }
                let result: &PyAny = result.into();
                Ok(result)
            }
        }
    }
}

pub(crate) trait MapStore {
    fn get_store(&self) -> &Arc<RwLock<AnnotationStore>>;
    fn get_store_mut(&mut self) -> &mut Arc<RwLock<AnnotationStore>>;

    /// Map function only on the store
    fn map_store<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&AnnotationStore) -> Result<T, StamError>,
    {
        if let Ok(store) = self.get_store().read() {
            f(&store).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "Unable to obtain store (should never happen)",
            ))
        }
    }

    fn map_store_mut<T, F>(&mut self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&mut AnnotationStore) -> Result<T, StamError>,
    {
        if let Ok(mut store) = self.get_store_mut().write() {
            f(&mut store).map_err(|err| PyStamError::new_err(format!("{}", err)))
        } else {
            Err(PyRuntimeError::new_err(
                "unable to obtain exclusive lock for writing to store",
            ))
        }
    }
}

impl MapStore for PyAnnotationStore {
    fn get_store(&self) -> &Arc<RwLock<AnnotationStore>> {
        &self.store
    }
    fn get_store_mut(&mut self) -> &mut Arc<RwLock<AnnotationStore>> {
        &mut self.store
    }
}

impl PyAnnotationStore {
    /// Map function to act on the actual unlderyling store, helps reduce boilerplate
    fn map<T, F>(&self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&AnnotationStore) -> Result<T, StamError>,
    {
        self.map_store(f)
    }

    fn map_mut<T, F>(&mut self, f: F) -> Result<T, PyErr>
    where
        F: FnOnce(&mut AnnotationStore) -> Result<T, StamError>,
    {
        self.map_store_mut(f)
    }
}

#[pyclass(name = "AnnotationIter")]
struct PyAnnotationIter {
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
    pub(crate) index: usize,
}

#[pymethods]
impl PyAnnotationIter {
    fn __iter__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __next__(mut pyself: PyRefMut<'_, Self>) -> Option<PyAnnotation> {
        pyself.index += 1; //increment first (prevent exclusive mutability issues)
        let result = pyself.map(|store| {
            if let Some(annotation) = store.annotation_by_index(pyself.index - 1) {
                //index is one ahead, prevents exclusive lock issues
                let handle = annotation.handle().expect("annotation must have an ID");
                Some(PyAnnotation {
                    handle,
                    store: pyself.store.clone(),
                })
            } else {
                None
            }
        });
        if result.is_some() {
            result
        } else {
            if pyself.index >= pyself.map(|store| Some(store.annotations_len())).unwrap() {
                None
            } else {
                Self::__next__(pyself)
            }
        }
    }
}

impl PyAnnotationIter {
    fn map<T, F>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&AnnotationStore) -> Option<T>,
    {
        if let Ok(store) = self.store.read() {
            f(&store)
        } else {
            None //should never happen here
        }
    }
}

#[pyclass(name = "AnnotationDataSetIter")]
struct PyAnnotationDataSetIter {
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
    pub(crate) index: usize,
}

#[pymethods]
impl PyAnnotationDataSetIter {
    fn __iter__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __next__(mut pyself: PyRefMut<'_, Self>) -> Option<PyAnnotationDataSet> {
        pyself.index += 1; //increment first (prevent exclusive mutability issues)
        let result = pyself.map(|store| {
            if let Some(annotationset) = store.annotationset_by_index(pyself.index - 1) {
                //index is one ahead, prevents exclusive lock issues
                let handle = annotationset.handle().expect("annotation must have an ID");
                Some(PyAnnotationDataSet {
                    handle,
                    store: pyself.store.clone(),
                })
            } else {
                None
            }
        });
        if result.is_some() {
            result
        } else {
            if pyself.index
                >= pyself
                    .map(|store| Some(store.annotationsets_len()))
                    .unwrap()
            {
                None
            } else {
                Self::__next__(pyself)
            }
        }
    }
}

impl PyAnnotationDataSetIter {
    fn map<T, F>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&AnnotationStore) -> Option<T>,
    {
        if let Ok(store) = self.store.read() {
            f(&store)
        } else {
            None //should never happen here
        }
    }
}

#[pyclass(name = "ResourceIter")]
struct PyResourceIter {
    pub(crate) store: Arc<RwLock<AnnotationStore>>,
    pub(crate) index: usize,
}

#[pymethods]
impl PyResourceIter {
    fn __iter__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __next__(mut pyself: PyRefMut<'_, Self>) -> Option<PyTextResource> {
        pyself.index += 1; //increment first (prevent exclusive mutability issues)
        let result = pyself.map(|store| {
            if let Some(res) = store.resource_by_index(pyself.index - 1) {
                //index is one ahead, prevents exclusive lock issues
                let handle = res.handle().expect("annotation must have an ID");
                Some(PyTextResource {
                    handle,
                    store: pyself.store.clone(),
                })
            } else {
                None
            }
        });
        if result.is_some() {
            result
        } else {
            if pyself.index >= pyself.map(|store| Some(store.annotations_len())).unwrap() {
                None
            } else {
                Self::__next__(pyself)
            }
        }
    }
}

impl PyResourceIter {
    fn map<T, F>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&AnnotationStore) -> Option<T>,
    {
        if let Ok(store) = self.store.read() {
            f(&store)
        } else {
            None //should never happen here
        }
    }
}

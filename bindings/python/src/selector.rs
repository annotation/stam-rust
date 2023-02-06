use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::*;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::annotation::PyAnnotation;
use crate::annotationdataset::PyAnnotationDataSet;
use crate::error::PyStamError;
use crate::resources::{PyOffset, PyTextResource};
use stam::*;

#[pyclass(name = "SelectorKind")]
#[derive(Clone)]
pub struct PySelectorKind {
    kind: SelectorKind,
}

#[pymethods]
impl PySelectorKind {
    #[classattr]
    const RESOURCESELECTOR: PySelectorKind = PySelectorKind {
        kind: SelectorKind::ResourceSelector,
    };
    #[classattr]
    const ANNOTATIONSELECTOR: PySelectorKind = PySelectorKind {
        kind: SelectorKind::AnnotationSelector,
    };
    #[classattr]
    const TEXTSELECTOR: PySelectorKind = PySelectorKind {
        kind: SelectorKind::TextSelector,
    };
    #[classattr]
    const DATASETSELECTOR: PySelectorKind = PySelectorKind {
        kind: SelectorKind::DataSetSelector,
    };
    #[classattr]
    const MULTISELECTOR: PySelectorKind = PySelectorKind {
        kind: SelectorKind::MultiSelector,
    };
    #[classattr]
    const COMPOSITESELECTOR: PySelectorKind = PySelectorKind {
        kind: SelectorKind::CompositeSelector,
    };
    #[classattr]
    const DIRECTIONALSELECTOR: PySelectorKind = PySelectorKind {
        kind: SelectorKind::DirectionalSelector,
    };

    fn __richcmp__(&self, other: PyRef<Self>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.kind == other.kind).into_py(py),
            CompareOp::Ne => (self.kind != other.kind).into_py(py),
            _ => py.NotImplemented(),
        }
    }
}

#[pyclass(name = "Selector")]
#[derive(Clone)]
pub(crate) struct PySelector {
    pub(crate) selector: Selector,
}

impl From<Selector> for PySelector {
    fn from(selector: Selector) -> Self {
        Self { selector }
    }
}

#[pymethods]
impl PySelector {
    #[new]
    #[pyo3(signature = (kind, resource=None, annotation=None, dataset=None, offset=None, subselectors=Vec::new()))]
    fn new(
        kind: &PySelectorKind,
        resource: Option<PyRef<PyTextResource>>,
        annotation: Option<PyRef<PyAnnotation>>,
        dataset: Option<PyRef<PyAnnotationDataSet>>,
        offset: Option<PyRef<PyOffset>>,
        subselectors: Vec<PyRef<PySelector>>,
    ) -> PyResult<Self> {
        match kind.kind {
            SelectorKind::ResourceSelector => {
                if let Some(resource) = resource {
                    Ok(PySelector {
                        selector: Selector::ResourceSelector(resource.handle),
                    })
                } else {
                    Err(PyValueError::new_err("'resource' keyword argument must be specified for ResourceSelector and point to a TextResource instance"))
                }
            }
            SelectorKind::AnnotationSelector => {
                if let Some(annotation) = annotation {
                    if let Some(offset) = offset {
                        Ok(PySelector {
                            selector: Selector::AnnotationSelector(
                                annotation.handle,
                                Some(offset.offset.clone()),
                            ),
                        })
                    } else {
                        Ok(PySelector {
                            selector: Selector::AnnotationSelector(annotation.handle, None),
                        })
                    }
                } else {
                    Err(PyValueError::new_err("'annotation' keyword argument must be specified for AnnotationSelector and point to a annotation instance"))
                }
            }
            SelectorKind::TextSelector => {
                if let Some(resource) = resource {
                    if let Some(offset) = offset {
                        Ok(PySelector {
                            selector: Selector::TextSelector(
                                resource.handle,
                                offset.offset.clone(),
                            ),
                        })
                    } else {
                        Err(PyValueError::new_err("'offset' keyword argument must be specified for TextSelector and point to a Offset instance"))
                    }
                } else {
                    Err(PyValueError::new_err("'resource' keyword argument must be specified for TextSelector and point to a TextResource instance"))
                }
            }
            SelectorKind::DataSetSelector => {
                if let Some(dataset) = dataset {
                    Ok(PySelector {
                        selector: Selector::DataSetSelector(dataset.handle),
                    })
                } else {
                    Err(PyValueError::new_err("'dataset' keyword argument must be specified for DataSetSelector and point to an AnnotationDataSet instance"))
                }
            }
            SelectorKind::MultiSelector => {
                if subselectors.is_empty() {
                    Err(PyValueError::new_err("'subselectors' keyword argument must be specified for MultiSelector and point to a list of Selector instances"))
                } else {
                    Ok(PySelector {
                        selector: Selector::MultiSelector(
                            subselectors
                                .into_iter()
                                .map(|sel| sel.selector.clone())
                                .collect(),
                        ),
                    })
                }
            }
            SelectorKind::CompositeSelector => {
                if subselectors.is_empty() {
                    Err(PyValueError::new_err("'subselectors' keyword argument must be specified for CompositeSelector and point to a list of Selector instances"))
                } else {
                    Ok(PySelector {
                        selector: Selector::CompositeSelector(
                            subselectors
                                .into_iter()
                                .map(|sel| sel.selector.clone())
                                .collect(),
                        ),
                    })
                }
            }
            SelectorKind::DirectionalSelector => {
                if subselectors.is_empty() {
                    Err(PyValueError::new_err("'subselectors' keyword argument must be specified for DirectionalSelector and point to a list of Selector instances"))
                } else {
                    Ok(PySelector {
                        selector: Selector::DirectionalSelector(
                            subselectors
                                .into_iter()
                                .map(|sel| sel.selector.clone())
                                .collect(),
                        ),
                    })
                }
            }
        }
    }

    #[staticmethod]
    /// Shortcut static method to construct a TextSelector
    fn text(resource: PyRef<PyTextResource>, offset: PyRef<PyOffset>) -> PyResult<Self> {
        PySelector::new(
            &PySelectorKind::TEXTSELECTOR,
            Some(resource),
            None,
            None,
            Some(offset),
            Vec::new(),
        )
    }

    #[staticmethod]
    /// Shortcut static method to construct a AnnotationSelector
    fn annotation(annotation: PyRef<PyAnnotation>, offset: PyRef<PyOffset>) -> PyResult<Self> {
        PySelector::new(
            &PySelectorKind::ANNOTATIONSELECTOR,
            None,
            Some(annotation),
            None,
            Some(offset),
            Vec::new(),
        )
    }

    #[staticmethod]
    /// Shortcut static method to construct a ResourceSelector
    fn resource(resource: PyRef<PyTextResource>) -> PyResult<Self> {
        PySelector::new(
            &PySelectorKind::RESOURCESELECTOR,
            Some(resource),
            None,
            None,
            None,
            Vec::new(),
        )
    }

    #[staticmethod]
    /// Shortcut static method to construct a DataSetSelector
    fn dataset(annotationset: PyRef<PyAnnotationDataSet>) -> PyResult<Self> {
        PySelector::new(
            &PySelectorKind::DATASETSELECTOR,
            None,
            None,
            Some(annotationset),
            None,
            Vec::new(),
        )
    }

    #[staticmethod]
    /// Shortcut static method to construct a MultiSelector
    fn multi(subselectors: Vec<PyRef<PySelector>>) -> PyResult<Self> {
        PySelector::new(
            &PySelectorKind::MULTISELECTOR,
            None,
            None,
            None,
            None,
            subselectors,
        )
    }

    #[staticmethod]
    /// Shortcut static method to construct a CompositeSelector
    fn composite(subselectors: Vec<PyRef<PySelector>>) -> PyResult<Self> {
        PySelector::new(
            &PySelectorKind::COMPOSITESELECTOR,
            None,
            None,
            None,
            None,
            subselectors,
        )
    }

    #[staticmethod]
    /// Shortcut static method to construct a DirectionalSelector
    fn directional(subselectors: Vec<PyRef<PySelector>>) -> PyResult<Self> {
        PySelector::new(
            &PySelectorKind::DIRECTIONALSELECTOR,
            None,
            None,
            None,
            None,
            subselectors,
        )
    }

    /// Returns the selector kind, use is_kind() instead if you want to test
    fn kind(&self) -> PySelectorKind {
        PySelectorKind {
            kind: self.selector.kind(),
        }
    }

    fn is_kind(&self, kind: &PySelectorKind) -> bool {
        self.selector.kind() == kind.kind
    }

    /// Quicker test for specified selector kind
    fn is_resourceselector(&self) -> bool {
        match self.selector {
            Selector::ResourceSelector(_) => true,
            _ => false,
        }
    }

    /// Quicker test for specified selector kind
    fn is_textselector(&self) -> bool {
        match self.selector {
            Selector::TextSelector(_, _) => true,
            _ => false,
        }
    }

    /// Quicker test for specified selector kind
    fn is_annotationselector(&self) -> bool {
        match self.selector {
            Selector::AnnotationSelector(_, _) => true,
            _ => false,
        }
    }

    /// Quicker test for specified selector kind
    fn is_datasetselector(&self) -> bool {
        match self.selector {
            Selector::DataSetSelector(_) => true,
            _ => false,
        }
    }

    /// Quicker test for specified selector kind
    fn is_multiselector(&self) -> bool {
        match self.selector {
            Selector::MultiSelector(_) => true,
            _ => false,
        }
    }

    /// Quicker test for specified selector kind
    fn is_directionalselector(&self) -> bool {
        match self.selector {
            Selector::DirectionalSelector(_) => true,
            _ => false,
        }
    }

    /// Quicker test for specified selector kind
    fn is_compositeselector(&self) -> bool {
        match self.selector {
            Selector::CompositeSelector(_) => true,
            _ => false,
        }
    }

    fn __richcmp__(&self, other: PyRef<Self>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.selector == other.selector).into_py(py),
            CompareOp::Ne => (self.selector != other.selector).into_py(py),
            _ => py.NotImplemented(),
        }
    }
}

extern crate stam as libstam;

use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::*;
use std::ops::FnOnce;
use std::sync::{Arc, RwLock};

use crate::annotationdataset::PyAnnotationDataSet;
use crate::error::PyStamError;
use crate::resources::PyTextResource;
use libstam::*;

#[pyclass(dict, name = "SelectorKind")]
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

    fn __eq__(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

#[pyclass(dict, name = "Selector")]
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
    fn new(
        kind: &PySelectorKind,
        resource: Option<&PyTextResource>,
        dataset: Option<&PyAnnotationDataSet>,
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
                panic!("Not implemented yet");
            }
            SelectorKind::TextSelector => {
                panic!("Not implemented yet");
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
                panic!("Not implemented yet");
            }
            SelectorKind::CompositeSelector => {
                panic!("Not implemented yet");
            }
            SelectorKind::DirectionalSelector => {
                panic!("Not implemented yet");
            }
        }
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
}

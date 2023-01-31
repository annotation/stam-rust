extern crate stam as libstam;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::*;
use std::str::FromStr;

use libstam::{AnnotationStore, StamError};

create_exception!(stam, PyStamError, pyo3::exceptions::PyException);

#[pymodule]
fn stam(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // ... other elements added to module ...
    m.add("StamError", py.get_type::<PyStamError>())?;
    m.add_class::<PyAnnotationStore>()?;
    Ok(())
}

#[pyclass(dict, name = "AnnotationStore")]
pub struct PyAnnotationStore {
    store: libstam::AnnotationStore,
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
                                    Ok(store) => Ok(PyAnnotationStore { store }),
                                    Err(err) => Err(PyStamError::new_err(format!("{}", err))),
                                };
                            }
                        }
                        "id" => {
                            if let Ok(Some(value)) = value.extract() {
                                return Ok(PyAnnotationStore {
                                    store: AnnotationStore::default().with_id(value),
                                });
                            }
                        }
                        _ => eprintln!("Ignored unknown kwargs option {}", key),
                    }
                }
            }
        }
        Ok(PyAnnotationStore {
            store: AnnotationStore::default(),
        })
    }

    #[getter]
    fn id(&self) -> Option<&str> {
        self.store.id()
    }
}

use pyo3::create_exception;

create_exception!(stam, PyStamError, pyo3::exceptions::PyException);

use pyo3::prelude::*;

mod factors;

/// The Rust computation core, exposed to Python as `jerry_trader._rust`.
#[pymodule]
mod _rust {
    use super::*;

    /// Formats the sum of two numbers as string.
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }

    // ── Factor compute functions ────────────────────────────────────
    #[pyfunction]
    #[pyo3(name = "z_score")]
    fn py_z_score(value: f64, history: Vec<f64>) -> Option<f64> {
        factors::z_score(value, history)
    }

    #[pyfunction]
    #[pyo3(name = "price_accel")]
    fn py_price_accel<'py>(
        recent: &Bound<'py, pyo3::types::PyList>,
        older: &Bound<'py, pyo3::types::PyList>,
    ) -> PyResult<f64> {
        factors::price_accel(recent, older)
    }
}

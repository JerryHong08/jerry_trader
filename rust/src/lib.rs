use pyo3::prelude::*;

/// The Rust computation core, exposed to Python as `jerry_trader._rust`.
#[pymodule]
mod _rust {
    use pyo3::prelude::*;

    /// Formats the sum of two numbers as string.
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }
}

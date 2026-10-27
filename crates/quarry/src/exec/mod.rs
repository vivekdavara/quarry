//! Execution: running a logical plan over Arrow record batches (DESIGN.md, "Execution").

mod expr;

pub use expr::evaluate;

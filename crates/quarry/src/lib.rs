//! Quarry: a SQL query engine over Parquet and CSV files, built on Apache Arrow.
//!
//! The pipeline is SQL text → `sqlparser` AST → a bound [`plan::LogicalPlan`] (names resolved, types checked) →
//! execution over Arrow record batches. DESIGN.md describes each stage.

pub mod catalog;
pub mod datasource;
pub mod error;
pub mod plan;
pub mod sql;
pub mod types;

pub use catalog::Catalog;
pub use error::{QuarryError, Result};

/// The engine's version, as `quarry --version` prints it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

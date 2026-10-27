//! Quarry: a SQL query engine over Parquet and CSV files, built on Apache Arrow.
//!
//! The pipeline is SQL text → `sqlparser` AST → a bound [`plan::LogicalPlan`] (names resolved, types checked) →
//! execution over Arrow record batches. DESIGN.md describes each stage.

pub mod catalog;
pub mod datasource;
pub mod error;
pub mod exec;
pub mod plan;
pub mod sql;
pub mod types;

/// The Arrow and Parquet crates Quarry is built on, at the versions it uses.
pub use {arrow, parquet};

pub use catalog::Catalog;
pub use error::{QuarryError, Result};
pub use exec::QueryResult;

/// Parses, binds and runs one query against the catalog.
pub fn query(catalog: &Catalog, sql: &str) -> Result<QueryResult> {
    exec::collect(&sql::plan(catalog, sql)?)
}

/// The engine's version, as `quarry --version` prints it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

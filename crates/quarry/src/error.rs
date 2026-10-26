//! The one error type every part of the engine returns.

use thiserror::Error;

/// Everything that can go wrong between a SQL string and its result.
///
/// The variants say whose fault it is: `Sql` and `Plan` are the query's (a syntax error, an unknown column, a type
/// mismatch), `Unsupported` is the engine's (valid SQL it doesn't implement yet), and `Io`, `Arrow` and `Parquet`
/// are the data's or the machine's.
#[derive(Debug, Error)]
pub enum QuarryError {
    /// The SQL text doesn't parse.
    #[error("SQL error: {0}")]
    Sql(#[from] sqlparser::parser::ParserError),

    /// The SQL parses but doesn't make sense against the catalog: an unknown table or column, an ambiguous name, a
    /// type mismatch, an aggregate where none is allowed.
    #[error("plan error: {0}")]
    Plan(String),

    /// Valid SQL the engine doesn't implement.
    #[error("not supported: {0}")]
    Unsupported(String),

    /// A table can't be read or registered.
    #[error("table error: {0}")]
    Table(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

pub type Result<T, E = QuarryError> = std::result::Result<T, E>;

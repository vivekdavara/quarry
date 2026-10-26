//! From SQL text to a logical plan: parsing (by `sqlparser`) and binding (DESIGN.md, "Binding a SELECT").

mod binder;
mod expr;

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::Catalog;
use crate::error::{QuarryError, Result, unsupported};
use crate::plan::LogicalPlan;

pub use binder::Binder;

/// A bound statement.
#[derive(Debug, Clone)]
pub enum BoundStatement {
    /// A query to run.
    Query(LogicalPlan),
    /// `EXPLAIN` a query: print its plan without running it.
    Explain(LogicalPlan),
}

/// Parses SQL text holding exactly one statement.
pub fn parse(sql: &str) -> Result<Statement> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql)?;
    match statements.len() {
        0 => Err(QuarryError::Plan("no SQL statement given".into())),
        1 => Ok(statements.remove(0)),
        n => Err(unsupported!("{n} statements at once; give one")),
    }
}

/// Parses and binds one statement against the catalog.
pub fn bind(catalog: &Catalog, sql: &str) -> Result<BoundStatement> {
    Binder::new(catalog).bind_statement(&parse(sql)?)
}

/// Parses and binds one query (an `EXPLAIN` is an error here) against the catalog.
pub fn plan(catalog: &Catalog, sql: &str) -> Result<LogicalPlan> {
    match bind(catalog, sql)? {
        BoundStatement::Query(plan) => Ok(plan),
        BoundStatement::Explain(_) => {
            Err(QuarryError::Plan("expected a query, got EXPLAIN".into()))
        }
    }
}

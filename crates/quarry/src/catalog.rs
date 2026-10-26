//! The catalog: the tables a query can name.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::datasource::{CsvTable, ParquetTable, TableProvider};
use crate::error::{QuarryError, Result};

/// Table names mapped to tables. Names match without regard to case (DESIGN.md, "Data sources and the catalog").
#[derive(Debug, Default, Clone)]
pub struct Catalog {
    // Keyed by the lowercase name; the value keeps the name as registered.
    tables: BTreeMap<String, (String, Arc<dyn TableProvider>)>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog::default()
    }

    /// Adds a table under a name; a name already taken (in any case) is an error.
    pub fn register(&mut self, name: &str, table: Arc<dyn TableProvider>) -> Result<()> {
        if name.is_empty() {
            return Err(QuarryError::Table("a table name can't be empty".into()));
        }
        let key = name.to_lowercase();
        if let Some((existing, _)) = self.tables.get(&key) {
            return Err(QuarryError::Table(format!(
                "a table named {existing} is already registered"
            )));
        }
        self.tables.insert(key, (name.to_string(), table));
        Ok(())
    }

    /// Adds a Parquet or CSV file as a table, choosing the reader by the file's extension (`.parquet`, `.csv`).
    pub fn register_file(&mut self, name: &str, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_lowercase);
        let table: Arc<dyn TableProvider> = match extension.as_deref() {
            Some("parquet") => Arc::new(ParquetTable::try_new(path)?),
            Some("csv") => Arc::new(CsvTable::try_new(path)?),
            _ => {
                return Err(QuarryError::Table(format!(
                    "{}: unknown file type (expected .parquet or .csv)",
                    path.display()
                )));
            }
        };
        self.register(name, table)
    }

    /// The table with this name, in any case.
    pub fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables
            .get(&name.to_lowercase())
            .map(|(_, t)| t.clone())
    }

    /// The registered names, as registered, in alphabetical order.
    pub fn table_names(&self) -> Vec<String> {
        self.tables.values().map(|(name, _)| name.clone()).collect()
    }
}

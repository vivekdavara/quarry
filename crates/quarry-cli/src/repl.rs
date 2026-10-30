//! `quarry repl`: an interactive shell over registered files (DESIGN.md, "The REPL").
//!
//! Lines are read one at a time. A line starting with `.` when no statement is pending is a command (`.help`
//! lists them); everything else is SQL, collected until a `;` ends a statement, where `quarry::sql::split_statements`
//! decides what a `;` ends. Each statement runs as `quarry query` would run it. An error is printed and the session
//! goes on; the exit status says whether any statement or command failed.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::{self, BufRead, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::time::Instant;

use quarry::sql::{Rest, first_token_offset, split_statements_after};
use quarry::{Catalog, QuarryError, Result};

const HELP: &str = "\
SQL statements end with ';' and may span lines. Commands:
  .help                this list
  .tables              the tables and their files
  .schema [TABLE]      the columns and types of one table, or of all
  .load [NAME=]PATH    add a Parquet or CSV file as a table (named after the file without NAME=)
  .drop TABLE          remove a table
  .timer on|off        print how long each statement took (parse, plan, run and format)
  .quit, .exit         leave (so does the end of the input: Ctrl-D)
";

/// The tables and settings a session has.
#[derive(Debug)]
pub struct Session {
    catalog: Catalog,
    /// Each table's file, keyed by its name in lowercase (names match without regard to case).
    paths: BTreeMap<String, PathBuf>,
    batch_size: usize,
    timer: bool,
}

/// What to do after a command.
#[derive(Debug, PartialEq, Eq)]
enum Next {
    Continue,
    Quit,
}

impl Session {
    pub fn new(batch_size: usize) -> Self {
        Session {
            catalog: Catalog::new(),
            paths: BTreeMap::new(),
            batch_size,
            timer: false,
        }
    }

    /// Adds a table from a `--table` or `.load` argument (`name=path` or `path`); returns its name.
    pub fn load(&mut self, spec: &str) -> Result<String> {
        let (name, path) = crate::parse_table(spec)?;
        self.catalog.register_file(&name, &path)?;
        self.paths.insert(name.to_lowercase(), path);
        Ok(name)
    }

    /// Runs one SQL statement (no `;`), returning what to print.
    fn statement(&mut self, sql: &str) -> Result<String> {
        let started = Instant::now();
        let mut out = crate::run_sql(&self.catalog, sql, self.batch_size)?;
        if self.timer {
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            out.push_str(&format!("time: {ms:.3} ms\n"));
        }
        Ok(out)
    }

    /// Runs one command line (starting with `.`), returning what to print.
    fn command(&mut self, line: &str) -> Result<(String, Next)> {
        let (name, arg) = match line.split_once(char::is_whitespace) {
            Some((name, arg)) => (name, arg.trim()),
            None => (line, ""),
        };
        let usage = |text: &str| Err(QuarryError::Plan(format!("usage: {text}")));
        let out = match (name, arg) {
            (".help", "") => HELP.to_string(),
            (".quit" | ".exit", "") => return Ok((String::new(), Next::Quit)),
            (".tables", "") => self.tables(),
            (".schema", "") => {
                let names = self.catalog.table_names();
                if names.is_empty() {
                    return Ok((no_tables(), Next::Continue));
                }
                let described: Result<Vec<String>> =
                    names.iter().map(|name| self.describe(name)).collect();
                described?.concat()
            }
            (".schema", table) => self.describe(table)?,
            (".load", spec) if !spec.is_empty() => {
                let name = self.load(spec)?;
                format!("loaded {name}\n")
            }
            (".drop", table) if !table.is_empty() => match self.catalog.deregister(table) {
                Some(name) => {
                    self.paths.remove(&name.to_lowercase());
                    format!("dropped {name}\n")
                }
                None => return Err(QuarryError::Table(format!("no table named {table}"))),
            },
            (".timer", "on") => {
                self.timer = true;
                String::new()
            }
            (".timer", "off") => {
                self.timer = false;
                String::new()
            }
            (".help" | ".quit" | ".exit" | ".tables", _) => return usage(name),
            (".load", _) => return usage(".load [NAME=]PATH"),
            (".drop", _) => return usage(".drop TABLE"),
            (".timer", _) => return usage(".timer on|off"),
            _ => {
                return Err(QuarryError::Plan(format!(
                    "unknown command {name} (.help lists them)"
                )));
            }
        };
        Ok((out, Next::Continue))
    }

    fn tables(&self) -> String {
        let names = self.catalog.table_names();
        if names.is_empty() {
            return no_tables();
        }
        let width = names.iter().map(|n| n.chars().count()).max().unwrap_or(0);
        names
            .iter()
            .map(|name| {
                let path = &self.paths[&name.to_lowercase()];
                format!("{name:width$}  {}\n", path.display())
            })
            .collect()
    }

    /// One table's name, file, and columns (as `quarry schema` prints them), indented.
    fn describe(&self, table: &str) -> Result<String> {
        let provider = self
            .catalog
            .table(table)
            .ok_or_else(|| QuarryError::Table(format!("no table named {table}")))?;
        let name = self
            .catalog
            .table_names()
            .into_iter()
            .find(|n| n.to_lowercase() == table.to_lowercase())
            .unwrap_or_else(|| table.to_string());
        let path = &self.paths[&name.to_lowercase()];
        let mut out = format!("{name} ({})\n", path.display());
        for line in crate::describe(provider.as_ref()).lines() {
            out.push_str("  ");
            out.push_str(line);
            out.push('\n');
        }
        Ok(out)
    }
}

fn no_tables() -> String {
    "no tables (.load [NAME=]PATH adds one)\n".to_string()
}

/// Reads lines from `input` until its end or `.quit`, printing results to `out` and errors to `err`. With
/// `interactive`, it prints prompts (and errors without line numbers); otherwise each error names the line its
/// statement started on. Returns whether every statement and command succeeded.
pub fn run(
    session: &mut Session,
    mut input: impl BufRead,
    out: &mut dyn Write,
    err: &mut dyn Write,
    interactive: bool,
) -> io::Result<bool> {
    let mut errors = Errors {
        interactive,
        count: 0,
    };
    let mut pending = Pending::default();
    let mut line_no = 0;
    let mut bytes = Vec::new();
    loop {
        if interactive {
            write!(
                out,
                "{}",
                if pending.is_empty() {
                    "quarry> "
                } else {
                    "   ...> "
                }
            )?;
            out.flush()?;
        }
        bytes.clear();
        if input.read_until(b'\n', &mut bytes)? == 0 {
            break;
        }
        line_no += 1;
        // A line that isn't UTF-8 is kept (with U+FFFD for its bad bytes), so the statement it's in still ends
        // where it should, but that statement isn't run.
        let decoded = String::from_utf8_lossy(&bytes);
        let utf8 = matches!(decoded, Cow::Borrowed(_));
        let mut line: &str = &decoded;
        if line_no == 1 {
            line = line.strip_prefix('\u{feff}').unwrap_or(line);
        }
        if pending.is_empty() && line.trim_start().starts_with('.') {
            if !utf8 {
                errors.report(err, line_no, &"the line isn't valid UTF-8; skipped")?;
                continue;
            }
            match session.command(line.trim()) {
                Ok((text, next)) => {
                    out.write_all(text.as_bytes())?;
                    if next == Next::Quit {
                        return Ok(errors.count == 0);
                    }
                }
                Err(e) => errors.report(err, line_no, &e)?,
            }
            continue;
        }
        // The semicolons pending before this line have been tokenized and end nothing.
        let checked = pending.text.len();
        pending.push(line, line_no, utf8);
        if pending.invalid {
            if ends_with_semicolon(line) {
                run_rest(session, &pending, out, err, &mut errors)?;
                pending = Pending::default();
            }
            continue;
        }
        if pending.unended && !line.contains(';') {
            continue;
        }
        let split = split_statements_after(&pending.text, checked);
        for range in split.statements {
            run_statement(session, &pending, range, out, err, &mut errors)?;
        }
        match split.rest {
            Rest::Blank => pending = Pending::default(),
            Rest::Unended | Rest::Open => {
                pending.drain(split.rest_start);
                pending.unended = split.rest == Rest::Unended;
            }
            Rest::Invalid => {
                pending.drain(split.rest_start);
                if ends_with_semicolon(line) {
                    run_rest(session, &pending, out, err, &mut errors)?;
                    pending = Pending::default();
                } else {
                    pending.invalid = true;
                }
            }
        }
    }
    // The end of the input ends a statement still open: one with no `;` yet runs, and an open string or comment
    // is the parser's error.
    if !pending.is_empty() {
        run_rest(session, &pending, out, err, &mut errors)?;
    }
    if interactive {
        writeln!(out)?;
    }
    Ok(errors.count == 0)
}

/// Where an invalid statement ends: no one can tell from its tokens, so at the end of the first line (its own or a
/// later one) whose last character is `;`, as a statement typed or written in a script ends.
fn ends_with_semicolon(line: &str) -> bool {
    line.trim_end().ends_with(';')
}

/// The SQL read but not yet run.
#[derive(Debug, Default)]
struct Pending {
    text: String,
    /// The input line `text` starts on.
    line: usize,
    /// Byte ranges of `text` from input lines that weren't UTF-8, with their line numbers.
    bad: Vec<(Range<usize>, usize)>,
    /// The last split left a statement that tokenizes and has no `;` yet: a line without one can't end it.
    unended: bool,
    /// The last split left text the tokenizer rejects: it runs (and fails) at the end of a line ending in `;`.
    invalid: bool,
}

impl Pending {
    fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    fn push(&mut self, line: &str, line_no: usize, utf8: bool) {
        if self.text.is_empty() {
            self.line = line_no;
        }
        let start = self.text.len();
        self.text.push_str(line);
        if !utf8 {
            self.bad.push((start..self.text.len(), line_no));
        }
    }

    /// The input line that byte `offset` of `text` is on.
    fn line_of(&self, offset: usize) -> usize {
        self.line + self.text[..offset].matches('\n').count()
    }

    /// Drops `text[..upto]`, which has run.
    fn drain(&mut self, upto: usize) {
        self.line = self.line_of(upto);
        self.text.drain(..upto);
        self.bad.retain(|(r, _)| r.end > upto);
        for (r, _) in &mut self.bad {
            *r = r.start.saturating_sub(upto)..r.end - upto;
        }
    }

    /// The first line among `range`'s that wasn't UTF-8.
    fn bad_line(&self, range: &Range<usize>) -> Option<usize> {
        self.bad
            .iter()
            .find(|(r, _)| r.start < range.end && range.start < r.end)
            .map(|&(_, line)| line)
    }
}

/// Runs the statement at `range` of the pending text (its `;` just after it, if it has one).
fn run_statement(
    session: &mut Session,
    pending: &Pending,
    range: Range<usize>,
    out: &mut dyn Write,
    err: &mut dyn Write,
    errors: &mut Errors,
) -> io::Result<()> {
    let line = pending.line_of(range.start);
    let through = range.start..(range.end + 1).min(pending.text.len());
    if let Some(bad) = pending.bad_line(&through) {
        let e = format!("line {bad} isn't valid UTF-8, so its statement doesn't run");
        return errors.report(err, line, &e);
    }
    match session.statement(&pending.text[range]) {
        Ok(text) => out.write_all(text.as_bytes()),
        Err(e) => errors.report(err, line, &e),
    }
}

/// Runs everything pending as one statement: one the end of the input ended, or an invalid one.
fn run_rest(
    session: &mut Session,
    pending: &Pending,
    out: &mut dyn Write,
    err: &mut dyn Write,
    errors: &mut Errors,
) -> io::Result<()> {
    let first = first_token_offset(&pending.text).unwrap_or(0);
    run_statement(
        session,
        pending,
        first..pending.text.len(),
        out,
        err,
        errors,
    )
}

/// Prints errors, counting them.
struct Errors {
    interactive: bool,
    count: usize,
}

impl Errors {
    fn report(
        &mut self,
        err: &mut dyn Write,
        line: usize,
        e: &dyn std::fmt::Display,
    ) -> io::Result<()> {
        self.count += 1;
        if self.interactive {
            writeln!(err, "error: {e}")
        } else {
            writeln!(err, "error: line {line}: {e}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDERS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/orders.csv");
    const CUSTOMERS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/customers.csv");

    /// Runs a session over `script` with the example tables; returns (ok, stdout, stderr).
    fn session(script: &str, interactive: bool) -> (bool, String, String) {
        let mut session = Session::new(8192);
        session.load(ORDERS).unwrap();
        session.load(CUSTOMERS).unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let ok = run(
            &mut session,
            script.as_bytes(),
            &mut out,
            &mut err,
            interactive,
        )
        .unwrap();
        (
            ok,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }

    #[test]
    fn statements_run_when_their_semicolon_arrives() {
        let (ok, out, err) = session(
            "SELECT count(*) AS n\nFROM orders\n;SELECT 'a;b' AS s; SELECT 2 AS two;\n",
            false,
        );
        assert!(ok, "{err}");
        assert_eq!(
            out,
            "+---+\n| n |\n+---+\n| 8 |\n+---+\n1 row\n\
             +-----+\n| s   |\n+-----+\n| a;b |\n+-----+\n1 row\n\
             +-----+\n| two |\n+-----+\n| 2   |\n+-----+\n1 row\n"
        );
    }

    #[test]
    fn a_string_or_comment_spanning_lines_holds_its_semicolons() {
        let (ok, out, err) = session(
            "SELECT 'one;\ntwo' AS s /* a;\nb */;\nSELECT 1 AS x -- no; end here\n;\n",
            false,
        );
        assert!(ok, "{err}");
        assert!(out.contains("| one; |\n| two  |"), "{out}");
        assert!(out.contains("| x |\n+---+\n| 1 |"), "{out}");
    }

    #[test]
    fn errors_name_the_line_their_statement_starts_on_and_the_session_goes_on() {
        let (ok, out, err) = session(
            "SELECT 1 AS a;\n\n  SELECT nope\n  FROM orders;\nSELECT 2 AS b;\n",
            false,
        );
        assert!(!ok);
        assert!(out.contains("| a |") && out.contains("| b |"), "{out}");
        assert_eq!(
            err,
            "error: line 3: plan error: column nope not found; the columns are: orders.order_id, orders.customer, \
             orders.status, orders.total, orders.placed\n"
        );
    }

    #[test]
    fn the_end_of_the_input_ends_an_open_statement() {
        let (ok, out, err) = session("SELECT 3 AS c", false);
        assert!(ok, "{err}");
        assert!(out.contains("| 3 |"), "{out}");
        // An unterminated string at the end is the parser's error.
        let (ok, _, err) = session("SELECT 1 AS a;\nSELECT 'open;\n", false);
        assert!(!ok);
        assert!(
            err.starts_with("error: line 2: ") && err.contains("Unterminated string literal"),
            "{err}"
        );
        // A comment alone is nothing to run.
        let (ok, out, err) = session("-- just a comment\n/* and */", false);
        assert!(ok && out.is_empty() && err.is_empty(), "{out}{err}");
    }

    #[test]
    fn text_the_tokenizer_rejects_is_reported_at_once() {
        let (ok, out, err) = session("SELECT U&'\\zz';\nSELECT 4 AS d;\n", false);
        assert!(!ok);
        assert!(err.starts_with("error: line 1: "), "{err}");
        assert!(out.contains("| 4 |"), "{out}");
    }

    #[test]
    fn commands() {
        let (ok, out, err) = session(".tables\n.schema customers\n.drop ORDERS\n.tables\n", false);
        assert!(ok, "{err}");
        let customers = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/customers.csv");
        let orders = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/orders.csv");
        assert_eq!(
            out,
            format!(
                "customers  {customers}\norders     {orders}\n\
                 customers ({customers})\n  customer: Utf8\n  city: Utf8\n  tier: Utf8\n\
                 dropped orders\n\
                 customers  {customers}\n"
            )
        );
        // `.load` names a table after its file, or as told.
        let (ok, out, err) = session(
            &format!(
                ".drop orders\n.load o={orders}\nSELECT count(*) AS n FROM o;\n.load {orders}\n"
            ),
            false,
        );
        assert!(ok, "{err}");
        assert!(
            out.contains("loaded o\n") && out.contains("| 8 |") && out.contains("loaded orders\n"),
            "{out}"
        );
    }

    #[test]
    fn command_mistakes_are_errors() {
        for (line, message) in [
            (
                ".frobnicate",
                "unknown command .frobnicate (.help lists them)",
            ),
            (".timer maybe", "usage: .timer on|off"),
            (".load", "usage: .load [NAME=]PATH"),
            (".drop", "usage: .drop TABLE"),
            (".drop nope", "no table named nope"),
            (".schema nope", "no table named nope"),
            (".tables x", "usage: .tables"),
            (".load orders=/no/such/file.csv", "/no/such/file.csv"),
            // The REPL's own words, not the command line's `--table`.
            (".load =x.csv", "table error: =x.csv: expected NAME=PATH"),
        ] {
            let (ok, _, err) = session(&format!("{line}\nSELECT 1 AS one;\n"), false);
            assert!(!ok, "{line}");
            assert!(
                err.starts_with("error: line 1: ") && err.contains(message),
                "{line}: {err}"
            );
        }
        // A name already taken is refused, and the table stays.
        let (ok, out, err) = session(&format!(".load {ORDERS}\n.tables\n"), false);
        assert!(!ok && err.contains("already registered"), "{err}");
        assert!(out.contains("orders "), "{out}");
    }

    #[test]
    fn a_dot_inside_a_statement_is_sql() {
        // `.customer` here continues the statement (a qualified name's dot); it isn't a command.
        let (ok, out, err) = session("SELECT o\n.customer FROM orders o LIMIT 1;\n", false);
        assert!(ok, "{err}");
        assert!(out.contains("| customer |"), "{out}");
        // And so is a line inside an open comment.
        let (ok, out, err) = session("/* open\n.quit\n*/ SELECT 1 AS one;\n", false);
        assert!(ok, "{err}");
        assert!(out.contains("| one |"), "{out}");
    }

    #[test]
    fn quit_stops_reading() {
        let (ok, out, _) = session(".quit\nSELECT 1 AS one;\n", false);
        assert!(ok);
        assert!(out.is_empty(), "{out}");
    }

    #[test]
    fn the_timer_reports_each_statement() {
        let (ok, out, err) = session(
            ".timer on\nSELECT 1 AS one;\n.timer off\nSELECT 2 AS two;\n",
            false,
        );
        assert!(ok, "{err}");
        let times: Vec<&str> = out.lines().filter(|l| l.starts_with("time: ")).collect();
        assert_eq!(times.len(), 1, "{out}");
        assert!(times[0].ends_with(" ms"), "{out}");
    }

    #[test]
    fn interactive_sessions_prompt_and_continue() {
        let (ok, out, err) = session("SELECT 1\nAS one;\nSELECT oops;\n", true);
        assert!(!ok);
        assert!(out.starts_with("quarry>    ...> +-----+"), "{out}");
        assert!(out.ends_with("quarry> quarry> \n"), "{out}");
        assert_eq!(err.lines().count(), 1);
        assert!(err.starts_with("error: plan error: "), "{err}");
    }

    #[test]
    fn a_statement_with_a_line_that_is_not_utf8_does_not_run() {
        let mut session = Session::new(8192);
        session.load(ORDERS).unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let input: &[u8] = b"SELECT count(*) AS n\nFROM orders\nWHERE status = 'caf\xe9'\n;\nSELECT 1 AS a\n\xff;\nSELECT 2 AS b;\n";
        let ok = run(&mut session, input, &mut out, &mut err, false).unwrap();
        let (out, err) = (
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        );
        assert!(!ok);
        // Neither statement touching a bad line runs (not even with the bad bytes replaced), and both end where
        // their `;` is: the one after them runs.
        assert!(!out.contains("| n ") && !out.contains("| a "), "{out}");
        assert!(out.contains("| b |"), "{out}");
        assert_eq!(
            err,
            "error: line 1: line 3 isn't valid UTF-8, so its statement doesn't run\n\
             error: line 5: line 6 isn't valid UTF-8, so its statement doesn't run\n"
        );
    }

    #[test]
    fn a_byte_order_mark_before_the_first_line_is_ignored() {
        let (ok, out, err) = session("\u{feff}.tables\n\u{feff}SELECT 1 AS one;\n", false);
        // Only the first line's: a second one is text the tokenizer doesn't know.
        assert!(!ok, "{err}");
        assert!(out.starts_with("customers  "), "{out}");
        assert!(err.starts_with("error: line 2: "), "{err}");
    }

    #[test]
    fn a_line_that_is_not_utf8_is_skipped() {
        let mut session = Session::new(8192);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let input: &[u8] = b"SELECT \xff;\nSELECT 5 AS e;\n";
        let ok = run(&mut session, input, &mut out, &mut err, false).unwrap();
        assert!(!ok);
        assert!(String::from_utf8(out).unwrap().contains("| 5 |"));
        assert!(
            String::from_utf8(err)
                .unwrap()
                .starts_with("error: line 1: ")
        );
    }
}

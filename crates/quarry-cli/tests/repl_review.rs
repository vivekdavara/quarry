//! The day-5 review's cases for `quarry repl` (README, "Review of day 5"): the six that failed at 4e3fc75, each with
//! the behaviour now expected, and the tests that kill the mutants its suite let through.

use std::io::Write;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const EXAMPLES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples");

fn repl(input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_quarry"))
        .args(["repl", "-t", &format!("{EXAMPLES}/orders.csv")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let input = input.to_vec();
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(&input);
    });
    let out = child.wait_with_output().unwrap();
    writer.join().unwrap();
    out
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A line of a statement that isn't UTF-8 is dropped and the statement runs without it: here the WHERE clause
/// goes, and the count of every row is printed. Expected: the statement doesn't run (it is reported, and the
/// lines up to its `;` are discarded), or the line is kept (lossily) so the statement is the one written.
#[test]
fn a_non_utf8_line_inside_a_statement_does_not_run_what_is_left() {
    let out = repl(b"SELECT count(*) AS n\nFROM orders\nWHERE status = 'caf\xe9'\n;\n");
    assert!(
        !text(&out.stdout).contains("| 8 |"),
        "the statement ran without its WHERE line:\n{}{}",
        text(&out.stdout),
        text(&out.stderr)
    );
}

/// An E'' string with an escape the tokenizer rejects (`\x80`, `\0`, ...) fails with "Unterminated encoded string
/// literal", which `is_unterminated` takes for an open string: every later line, `.quit` included, joins the
/// statement, and nothing runs until the end of the input. Expected: the statement is reported when its line
/// ends (Rest::Invalid), and the session goes on.
#[test]
fn an_invalid_escape_in_an_e_string_does_not_swallow_the_rest_of_the_script() {
    let out = repl(b"SELECT E'\\x80' AS a;\nSELECT 1 AS b;\n.quit\nSELECT 2 AS c;\n");
    let stdout = text(&out.stdout);
    assert!(
        stdout.contains("| b |"),
        "SELECT 1 AS b never ran:\nstdout: {stdout}\nstderr: {}",
        text(&out.stderr)
    );
    assert!(
        !stdout.contains("| c |"),
        ".quit didn't stop the session: {stdout}"
    );
}

/// After a line the tokenizer rejects, the next line starts a new statement, so text inside the rejected
/// statement's still-open string runs as SQL. Expected: `SELECT 42 AS boom` (string content) never runs.
#[test]
fn text_inside_a_string_of_a_rejected_statement_does_not_run() {
    let out = repl(b"SELECT U&'\\zz' AS a, 'it\n; SELECT 42 AS boom; '\n;\n");
    assert!(
        !text(&out.stdout).contains("boom"),
        "string content ran as a statement:\n{}{}",
        text(&out.stdout),
        text(&out.stderr)
    );
}

/// A statement that starts after a comment closing on its line is reported on the line the comment opened on,
/// when its text doesn't tokenize (open at the end of the input, or rejected): `first_token_offset` gives up on
/// text that doesn't tokenize and the offset falls back to 0. Expected: line 2 (the parser's own message says
/// Line: 2 of the pending text).
#[test]
fn errors_in_untokenizable_statements_name_the_line_they_start_on() {
    for script in [
        &b"SELECT 1 AS a; /* note\n*/ SELECT 'open\n"[..],
        &b"SELECT 1 AS a; /* note\n*/ SELECT U&'\\zz';\n"[..],
    ] {
        let err = text(&repl(script).stderr);
        assert!(
            err.starts_with("error: line 2: "),
            "{:?}: {err}",
            text(script)
        );
    }
}

/// Each line re-splits everything pending, and each `;` inside an open comment or string re-tokenizes from the
/// statement's start: cubic in the lines of a comment holding semicolons (800 lines, 2.4 KB: 4.4 s in a debug
/// build; 1600 lines: 33 s), quadratic in the lines of any statement (5000 lines: 5.6 s; 20000: over 60 s).
#[test]
fn a_block_comment_of_lines_with_semicolons_is_quick() {
    let script = format!("/*\n{}*/ SELECT 1 AS one;\n", "x;\n".repeat(1000));
    let started = Instant::now();
    let out = repl(script.as_bytes());
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "took {:?}",
        started.elapsed()
    );
}

#[test]
fn a_statement_of_many_lines_is_quick() {
    let script = format!("SELECT 1 AS one\n{};\n", "-- more\n".repeat(10_000));
    let started = Instant::now();
    let out = repl(script.as_bytes());
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "took {:?}",
        started.elapsed()
    );
}

/// Both example tables.
fn repl2(input: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_quarry"))
        .args([
            "repl",
            "-t",
            &format!("{EXAMPLES}/orders.csv"),
            "-t",
            &format!("{EXAMPLES}/customers.csv"),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn err(o: &Output) -> String {
    String::from_utf8(o.stderr.clone()).unwrap()
}
fn out(o: &Output) -> String {
    String::from_utf8(o.stdout.clone()).unwrap()
}

/// R4, R5, R6, R8: statements that don't start on the first line of what is pending.
#[test]
fn error_lines_count_the_lines_before_the_statement() {
    for (script, line) in [
        ("/* c\n*/ SELECT nope;\n", 2), // R4: offset within pending
        ("SELECT 1 AS b\n; SELECT nope\nFROM orders;\n", 2), // R5: pending_line after an Incomplete drain
        ("SELECT 1 AS a\n; SELECT U&'\\zz';\n", 2), // R6: Invalid after a statement on an earlier line
        ("/* a\n*/ SELECT nope", 2),                // R8: open statement at EOF after a comment
    ] {
        let e = err(&repl2(script));
        assert!(
            e.starts_with(&format!("error: line {line}: ")),
            "{script:?}: {e}"
        );
    }
}

/// R1: a command may be indented.
#[test]
fn an_indented_command_is_a_command() {
    let o = repl2("   .tables\n");
    assert!(o.status.success(), "{}", err(&o));
    assert!(out(&o).starts_with("customers  "), "{}", out(&o));
}

/// R12: `.quit` after a failure still exits 1.
#[test]
fn quit_after_an_error_exits_1() {
    assert_eq!(repl2("SELECT nope;\n.quit\n").status.code(), Some(1));
}

/// R13, R14, R15: `.schema` of every table, of none, and a name in another case.
#[test]
fn schema_forms() {
    let all = out(&repl2(".schema\n"));
    assert!(
        all.contains("customers (")
            && all.contains("orders (")
            && all.contains("  placed: Date32\n"),
        "{all}"
    );
    let none = out(&repl2(".drop orders\n.drop customers\n.schema\n"));
    assert!(
        none.ends_with("no tables (.load [NAME=]PATH adds one)\n"),
        "{none}"
    );
    let upper = out(&repl2(".schema CUSTOMERS\n"));
    assert!(upper.starts_with("customers ("), "{upper}");
}

/// R17, R18: `.help` prints the list and `.exit` leaves.
#[test]
fn help_and_exit() {
    assert!(out(&repl2(".help\n")).contains(".load [NAME=]PATH"));
    let o = repl2(".exit\nSELECT 1 AS one;\n");
    assert!(o.status.success() && out(&o).is_empty(), "{}", out(&o));
}

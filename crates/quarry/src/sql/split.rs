//! Splitting SQL text into statements at its semicolons, for the REPL (DESIGN.md, "The REPL").
//!
//! A `;` ends a statement only where the parser would read it as a semicolon, not inside a string, a quoted name or
//! a comment. Rather than a second lexer that could disagree with the parser's, this asks the parser's own
//! tokenizer: the `;` at byte `q` ends a statement exactly when the text from the statement's start through `q`
//! tokenizes and its last token is that semicolon. (The tokenizer reads left to right and a `;` is a token of its
//! own, so the tokens up to it don't depend on what follows; a `;` inside a string or comment leaves the prefix
//! unterminated, and one inside a `--` comment ends a comment token instead.)

use std::ops::Range;

use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Location, Token, Tokenizer, TokenizerError};

/// What follows the last complete statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rest {
    /// Nothing but whitespace and comments.
    Blank,
    /// The start of a statement: no `;` yet, or one inside a string, quoted name or comment still open.
    Incomplete,
    /// Text the tokenizer rejects whatever follows (an invalid escape, say): a statement that is already wrong,
    /// running to the end of the text. Running it reports the tokenizer's error.
    Invalid,
}

/// SQL text cut at its semicolons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    /// The complete statements, as byte ranges of the text: each from its first token to just before its `;`.
    /// Statements with no tokens but whitespace and comments (`;;`) are left out.
    pub statements: Vec<Range<usize>>,
    /// Where the text after the last statement's `;` starts.
    pub rest_start: usize,
    /// What that text is.
    pub rest: Rest,
}

/// Cuts `text` into the statements its semicolons end and what is left after them.
pub fn split_statements(text: &str) -> Split {
    let mut statements = Vec::new();
    let mut start = 0;
    for (q, _) in text.match_indices(';') {
        if q < start {
            continue;
        }
        if let Ok(tokens) = tokenize(&text[start..=q])
            && tokens.last() == Some(&Token::SemiColon)
        {
            if let Some(first) = first_token_offset(&text[start..q]) {
                statements.push(start + first..q);
            }
            start = q + 1;
        }
    }
    let rest = match tokenize(&text[start..]) {
        Ok(tokens) if tokens.iter().all(is_blank) => Rest::Blank,
        Ok(_) => Rest::Incomplete,
        Err(e) if is_unterminated(&e) => Rest::Incomplete,
        Err(_) => Rest::Invalid,
    };
    Split {
        statements,
        rest_start: start,
        rest,
    }
}

/// The byte offset of the first token in `text` that isn't whitespace or a comment, if there is one.
///
/// Used on text that ends just before a semicolon token, so it holds whole tokens and tokenizes.
pub fn first_token_offset(text: &str) -> Option<usize> {
    let tokens = Tokenizer::new(&GenericDialect {}, text)
        .tokenize_with_location()
        .ok()?;
    let first = tokens.iter().find(|t| !is_blank(&t.token))?;
    Some(byte_offset(text, first.span.start))
}

/// The byte offset of a tokenizer location: a 1-based line, and a 1-based column counted in characters.
fn byte_offset(text: &str, location: Location) -> usize {
    let (mut line, mut column) = (1, 1);
    for (i, ch) in text.char_indices() {
        if (line, column) == (location.line, location.column) {
            return i;
        }
        if ch == '\n' {
            (line, column) = (line + 1, 1);
        } else {
            column += 1;
        }
    }
    text.len()
}

fn tokenize(text: &str) -> Result<Vec<Token>, TokenizerError> {
    Tokenizer::new(&GenericDialect {}, text).tokenize()
}

fn is_blank(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

/// The tokenizer's errors that more text could cure: each says the text ended inside a string, a quoted name, a
/// dollar-quoted string or a comment. `tests::each_unterminated_error_is_recognized` produces every one, so a
/// sqlparser upgrade that rewords one fails there.
fn is_unterminated(e: &TokenizerError) -> bool {
    e.message.starts_with("Unterminated")
        || e.message.ends_with("before EOF.")
        || e.message.starts_with("Unexpected EOF")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statements(text: &str) -> Vec<&str> {
        split_statements(text)
            .statements
            .into_iter()
            .map(|r| &text[r])
            .collect()
    }

    #[test]
    fn semicolons_end_statements() {
        let text = "SELECT 1; SELECT 2;\nSELECT 3";
        let split = split_statements(text);
        assert_eq!(statements(text), ["SELECT 1", "SELECT 2"]);
        assert_eq!(&text[split.rest_start..], "\nSELECT 3");
        assert_eq!(split.rest, Rest::Incomplete);
    }

    #[test]
    fn semicolons_inside_strings_names_and_comments_do_not() {
        for text in [
            "SELECT 'a;b';",
            "SELECT 'it''s; fine';",
            "SELECT E'\\';' ;",
            "SELECT \"a;b\" FROM t;",
            "SELECT `a;b` FROM t;",
            "SELECT 1 -- a; comment\n;",
            "SELECT /* a; /* nested; */ comment; */ 1;",
            "SELECT $$a;b$$;",
            "SELECT $tag$a;b$tag$;",
        ] {
            let split = split_statements(text);
            assert_eq!(split.statements.len(), 1, "{text}");
            assert_eq!(split.rest, Rest::Blank, "{text}");
            assert_eq!(split.rest_start, text.len(), "{text}");
        }
    }

    #[test]
    fn open_strings_and_comments_are_incomplete() {
        for text in [
            "SELECT 'a;",
            "SELECT E'\\';",
            "SELECT \"a;",
            "SELECT `a;",
            "SELECT /* a; /* b */ still open;",
            "SELECT $$a;",
            "SELECT $t$a;",
            "SELECT 1",
        ] {
            let split = split_statements(text);
            assert!(split.statements.is_empty(), "{text}");
            assert_eq!(split.rest, Rest::Incomplete, "{text}");
            assert_eq!(split.rest_start, 0, "{text}");
        }
    }

    #[test]
    fn statements_before_an_open_string_are_complete() {
        let text = "SELECT 1; SELECT 'a;";
        assert_eq!(statements(text), ["SELECT 1"]);
        let split = split_statements(text);
        assert_eq!(&text[split.rest_start..], " SELECT 'a;");
        assert_eq!(split.rest, Rest::Incomplete);
    }

    #[test]
    fn blank_statements_and_comments_are_skipped() {
        let text = ";; -- just a comment\n /* and this */ ;\n";
        let split = split_statements(text);
        assert!(split.statements.is_empty());
        assert_eq!(split.rest, Rest::Blank);
        assert_eq!(split_statements("").rest, Rest::Blank);
        assert_eq!(split_statements("  -- c").rest, Rest::Blank);
    }

    #[test]
    fn a_statement_starts_at_its_first_token() {
        let text = "  -- note\n  /* x */ SELECT 1 ;";
        assert_eq!(statements(text), ["SELECT 1 "]);
    }

    #[test]
    fn text_the_tokenizer_rejects_is_invalid() {
        // An invalid escape in a Unicode string: no text that follows could fix it.
        let split = split_statements("SELECT U&'\\zz'; SELECT 2;");
        assert!(split.statements.is_empty());
        assert_eq!(split.rest, Rest::Invalid);
    }

    #[test]
    fn each_unterminated_error_is_recognized() {
        for (text, message) in [
            ("'a", "Unterminated string literal"),
            ("E'a", "Unterminated encoded string literal"),
            ("\"a", "Expected close delimiter '\"' before EOF."),
            ("`a", "Expected close delimiter '`' before EOF."),
            ("$$a", "Unterminated dollar-quoted string"),
            ("$t$a", "Unterminated dollar-quoted, expected $"),
            ("/* a", "Unexpected EOF while in a multi-line comment"),
            ("U&'a", "Unterminated unicode encoded string literal"),
        ] {
            let e = tokenize(text).expect_err(text);
            assert!(e.message.starts_with(message), "{text}: {}", e.message);
            assert!(is_unterminated(&e), "{text}: {}", e.message);
        }
        let e = tokenize("U&'\\zz'").unwrap_err();
        assert!(!is_unterminated(&e), "{}", e.message);
    }
}

//! Splitting SQL text into statements at its semicolons, for the REPL (DESIGN.md, "The REPL").
//!
//! A `;` ends a statement only where the parser would read it as a semicolon, not inside a string, a quoted name or
//! a comment. Rather than a second lexer that could disagree with the parser's, this asks the parser's own
//! tokenizer: the `;` at byte `q` ends a statement exactly when the text from the statement's start through `q`
//! tokenizes and its last token is that semicolon. (The tokenizer reads left to right and a `;` is a token of its
//! own, so the tokens up to it don't depend on what follows; a `;` inside a string or comment leaves the prefix
//! unterminated, and one inside a `--` comment ends a comment token instead.)
//!
//! Text after the last statement that doesn't tokenize is either open (a string, name or comment the next line may
//! close) or invalid (wrong whatever follows). The tokenizer's messages don't tell them apart (sqlparser 0.63 says
//! "Unterminated encoded string literal" both for `E'abc` and for `E'\x80'`), so the text is open exactly when one
//! of the closing suffixes in `CLOSERS` (or a `*/` per `/*`, or a dollar tag it contains) makes it tokenize.

use std::ops::Range;

use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Location, Token, Tokenizer, TokenizerError};

/// What follows the last complete statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rest {
    /// Nothing but whitespace and comments.
    Blank,
    /// The start of a statement that tokenizes but has no `;` yet. A line without a `;` added to it can't end it.
    Unended,
    /// The start of a statement inside a string, quoted name or comment still open.
    Open,
    /// Text the tokenizer rejects whatever follows (an invalid escape, say): a statement that is already wrong,
    /// with no knowing where it ends. Running it reports the tokenizer's error.
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
    split_statements_after(text, 0)
}

/// As `split_statements`, for text whose semicolons before byte `checked` are known not to end a statement (a split
/// of `text[..checked]` found none). The REPL adds a line to what it has pending and splits again: only the new
/// line's semicolons need tokenizing, since a prefix's tokens don't change when text is added after it.
pub fn split_statements_after(text: &str, checked: usize) -> Split {
    let mut statements = Vec::new();
    let mut start = 0;
    for (q, _) in text.match_indices(';') {
        if q < checked {
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
        Ok(_) => Rest::Unended,
        Err(_) if can_close(&text[start..]) => Rest::Open,
        Err(_) => Rest::Invalid,
    };
    Split {
        statements,
        rest_start: start,
        rest,
    }
}

/// The byte offset of the first token in `text` that isn't whitespace or a comment, if there is one. For text that
/// doesn't tokenize (an open string, say), where whitespace and comments end, by `skip_blank`.
pub fn first_token_offset(text: &str) -> Option<usize> {
    match Tokenizer::new(&GenericDialect {}, text).tokenize_with_location() {
        Ok(tokens) => {
            let first = tokens.iter().find(|t| !is_blank(&t.token))?;
            Some(byte_offset(text, first.span.start))
        }
        Err(_) => Some(skip_blank(text)),
    }
}

/// The offset after the whitespace (what the tokenizer takes for it: `char::is_whitespace`), `--` comments and
/// nested `/* */` comments `text` starts with; at an open `/*`, its offset.
fn skip_blank(text: &str) -> usize {
    let b = text.as_bytes();
    let mut i = 0;
    loop {
        if let Some(c) = text[i..].chars().next().filter(|c| c.is_whitespace()) {
            i += c.len_utf8();
        } else if b[i..].starts_with(b"--") {
            i = b[i..]
                .iter()
                .position(|&c| c == b'\n')
                .map_or(b.len(), |n| i + n + 1);
        } else if b[i..].starts_with(b"/*") {
            let (mut depth, mut j) = (0, i);
            while j < b.len() {
                if b[j..].starts_with(b"/*") {
                    (depth, j) = (depth + 1, j + 2);
                } else if b[j..].starts_with(b"*/") {
                    (depth, j) = (depth - 1, j + 2);
                    if depth == 0 {
                        break;
                    }
                } else {
                    j += 1;
                }
            }
            if depth > 0 {
                return i;
            }
            i = j;
        } else {
            return i;
        }
    }
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

/// Suffixes that end what a text left open: a string (`''` after a trailing backslash in `E'...'`), a quoted name,
/// a raw triple-quoted string (`R'''...'''`), a `$$` string.
const CLOSERS: &[&str] = &["'", "\"", "`", "''", "'''", "\"\"\"", "$$"];

/// Whether some suffix makes `text`, which doesn't tokenize, tokenize: one of `CLOSERS`, a ` */` for each `/*` it
/// has (the spare ones close nothing and tokenize as `*` and `/`; the space keeps a trailing `/` from making a new
/// `/*`), a `$tag$` it contains, or the end of a quote-delimited string it opens. Only one thing can be open at the
/// end (the tokenizer stops at it), so one suffix is enough.
fn can_close(text: &str) -> bool {
    let comments = std::iter::once(" */".repeat(text.matches("/*").count()));
    let closers = CLOSERS
        .iter()
        .map(|c| c.to_string())
        .chain(comments)
        .chain(dollar_tags(text))
        .chain(quote_delimited_ends(text));
    closers
        .into_iter()
        .any(|c| tokenize(&format!("{text}{c}")).is_ok())
}

/// The ends of the quote-delimited strings `text` may open (`q'[...]'`, `Q'#...#'`): the delimiter after each `q'`,
/// its bracket's mirror for `[`, `{`, `(` and `<`, then a quote.
fn quote_delimited_ends(text: &str) -> Vec<String> {
    let mut ends = Vec::new();
    for (i, _) in text.match_indices(['q', 'Q']) {
        let mut chars = text[i + 1..].chars();
        if chars.next() == Some('\'')
            && let Some(delimiter) = chars.next()
        {
            let close = match delimiter {
                '[' => ']',
                '{' => '}',
                '(' => ')',
                '<' => '>',
                c => c,
            };
            ends.push(format!("{close}'"));
        }
    }
    ends
}

/// The `$tag$` openers of dollar-quoted strings in `text` (a tag is letters, digits and `_`).
fn dollar_tags(text: &str) -> Vec<String> {
    let mut tags = Vec::new();
    for (i, _) in text.match_indices('$') {
        let tag: String = text[i + 1..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !tag.is_empty() && text[i + 1 + tag.len()..].starts_with('$') {
            tags.push(format!("${tag}$"));
        }
    }
    tags.sort();
    tags.dedup();
    tags
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
        assert_eq!(split.rest, Rest::Unended);
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
    fn open_strings_and_comments_are_open() {
        for text in [
            "SELECT 'a;",
            "SELECT E'\\';",
            "SELECT E'a\\",
            "SELECT \"a;",
            "SELECT `a;",
            "SELECT /* a; /* b */ still open;",
            "SELECT /* /* /* three deep;",
            "SELECT $$a;",
            "SELECT $t$a;",
            "SELECT $t$a $u$ b;",
            "SELECT U&'a;",
            "SELECT q'[a;",
            "SELECT Q'{a;",
            "SELECT q'#a;",
            "SELECT q'aba;",
        ] {
            let split = split_statements(text);
            assert!(split.statements.is_empty(), "{text}");
            assert_eq!(split.rest, Rest::Open, "{text}");
            assert_eq!(split.rest_start, 0, "{text}");
        }
        assert_eq!(split_statements("SELECT 1").rest, Rest::Unended);
    }

    #[test]
    fn statements_before_an_open_string_are_complete() {
        let text = "SELECT 1; SELECT 'a;";
        assert_eq!(statements(text), ["SELECT 1"]);
        let split = split_statements(text);
        assert_eq!(&text[split.rest_start..], " SELECT 'a;");
        assert_eq!(split.rest, Rest::Open);
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
        // Invalid escapes: no text that follows could fix them. sqlparser calls the E'' ones "Unterminated encoded
        // string literal" (the review found the REPL waiting for more input after them, `.quit` included).
        for text in [
            "SELECT U&'\\zz'; SELECT 2;",
            "SELECT E'\\x80';",
            "SELECT E'\\0';",
            "SELECT E'\\200';",
            "SELECT E'\\uzzzz';",
            "SELECT E'\\U&';",
        ] {
            let split = split_statements(text);
            assert!(split.statements.is_empty(), "{text}");
            assert_eq!(split.rest, Rest::Invalid, "{text}");
        }
    }

    #[test]
    fn semicolons_before_checked_are_not_tokenized_again() {
        // A split that starts after the first `;` doesn't see it end a statement: the caller vouched it doesn't.
        let text = "SELECT 1; SELECT 2;";
        assert_eq!(
            split_statements_after(text, 9).statements,
            vec![Range { start: 0, end: 18 }]
        );
        assert_eq!(split_statements_after(text, 0).statements, [0..8, 10..18]);
    }

    #[test]
    fn the_first_token_of_text_that_does_not_tokenize() {
        assert_eq!(
            first_token_offset(" /* a\n */ -- b\n  SELECT 'open"),
            Some(17)
        );
        assert_eq!(
            first_token_offset("\n/* /* nested */ still */ SELECT U&'\\zz'"),
            Some(26)
        );
        // An open comment is where the text starts.
        assert_eq!(first_token_offset("  /* open"), Some(2));
        assert_eq!(first_token_offset("  -- c\n"), None);
    }
}

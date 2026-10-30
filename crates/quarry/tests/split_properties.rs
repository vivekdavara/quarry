//! The REPL's statement splitter against the parser's own tokenizer, over random text full of quotes, comment
//! markers, dollar quotes, backslashes and semicolons.
//!
//! Wherever the whole text tokenizes, the statements must end exactly at its semicolon tokens; wherever it doesn't,
//! everything before the rest must still tokenize and the rest must fail the way the split says.

use proptest::prelude::*;
use quarry::sql::{Rest, split_statements};
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

/// Pieces of SQL's lexical syntax, single characters and the multi-character markers that start or end something.
const PIECES: &[&str] = &[
    ";", ";", ";", "'", "'", "\"", "`", "-", "--", "/", "/*", "*/", "*", "\\", "$", "$$", "$t$",
    "E'", "U&'", "q'[", "]'", "\n", "\r\n", " ", "a", "1", "é", "😀", "SELECT 1", "x", "_", "#",
    "@", "&",
];

/// Suffixes that close what an unterminated text left open.
const CLOSERS: &[&str] = &["'", "\"", "`", "*/", "$$", "$t$", "]'"];

fn text() -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::sample::select(PIECES), 0..24).prop_map(|p| p.concat())
}

fn tokenize(text: &str) -> Result<Vec<TokenWithSpan>, String> {
    Tokenizer::new(&GenericDialect {}, text)
        .tokenize_with_location()
        .map_err(|e| e.message)
}

/// A location's byte offset, counted here independently of the splitter.
fn offset(text: &str, at: Location) -> usize {
    let mut lines = text.split_inclusive('\n');
    let before: usize = lines
        .by_ref()
        .take(at.line as usize - 1)
        .map(str::len)
        .sum();
    let line = lines.next().unwrap_or("");
    before
        + line
            .char_indices()
            .nth(at.column as usize - 1)
            .map_or(line.len(), |(i, _)| i)
}

fn is_blank(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(2_000)
    ))]

    #[test]
    fn statements_end_at_the_tokenizers_semicolons(text in text()) {
        let split = split_statements(&text);
        prop_assert!(split.rest_start <= text.len());
        prop_assert!(text.is_char_boundary(split.rest_start));
        match tokenize(&text) {
            Ok(tokens) => {
                // Cut the token list at its semicolons, as the parser would.
                let mut expected = Vec::new();
                let mut statement: Vec<&TokenWithSpan> = Vec::new();
                let mut rest_start = 0;
                for token in &tokens {
                    if token.token == Token::SemiColon {
                        let semicolon = offset(&text, token.span.start);
                        if let Some(first) = statement.iter().find(|t| !is_blank(&t.token)) {
                            expected.push(offset(&text, first.span.start)..semicolon);
                        }
                        statement.clear();
                        rest_start = semicolon + 1;
                    } else {
                        statement.push(token);
                    }
                }
                prop_assert_eq!(&split.statements, &expected, "{:?}", text);
                prop_assert_eq!(split.rest_start, rest_start, "{:?}", text);
                let rest = if statement.iter().all(|t| is_blank(&t.token)) { Rest::Blank } else { Rest::Incomplete };
                prop_assert_eq!(split.rest, rest, "{:?}", text);
            }
            Err(_) => {
                // The statements found still each end at a semicolon token of the text before the rest.
                let before = &text[..split.rest_start];
                let tokens = tokenize(before);
                prop_assert!(tokens.is_ok(), "{:?}: the text before the rest doesn't tokenize", text);
                for range in &split.statements {
                    prop_assert_eq!(&text[range.end..range.end + 1], ";");
                    prop_assert!(tokenize(&text[range.clone()]).is_ok(), "{:?}", text);
                }
                prop_assert_ne!(split.rest, Rest::Blank, "{:?}", text);
                // Invalid means no text that follows could make the rest tokenize; so if a closing quote, comment
                // end or dollar tag, repeated, does, the rest is incomplete.
                let rest = &text[split.rest_start..];
                let curable = CLOSERS.iter().any(|closer| {
                    (1..=12).any(|n| tokenize(&format!("{rest}{}", closer.repeat(n))).is_ok())
                });
                if curable {
                    prop_assert_eq!(split.rest, Rest::Incomplete, "{:?}", text);
                }
            }
        }
    }

    /// The REPL feeds the splitter one line at a time, running the statements each line completes and keeping
    /// the rest: that must find the same statements as splitting the whole text at once, whatever the line breaks.
    #[test]
    fn feeding_lines_one_at_a_time_finds_the_same_statements(text in text()) {
        let whole = split_statements(&text);
        let whole: Vec<&str> = whole.statements.iter().map(|r| &text[r.clone()]).collect();
        let mut pending = String::new();
        let mut found: Vec<String> = Vec::new();
        for line in text.split_inclusive('\n') {
            pending.push_str(line);
            let split = split_statements(&pending);
            found.extend(split.statements.iter().map(|r| pending[r.clone()].to_string()));
            pending.drain(..split.rest_start);
        }
        // A whole-text split stops at the first rejected stretch, and so does a feed: compare up to there.
        prop_assert_eq!(&found[..], &whole[..], "{:?}", text);
    }
}

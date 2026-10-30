//! The day-5 review's second round on the splitter (README, "Review of day 5"): open strings its closers missed, a
//! comment ending in `/`, and whitespace the tokenizer knows and `skip_blank` didn't (each failed at a491dde^); and
//! round 3's tests for mutants its suite let through.

use std::time::Instant;

use quarry::sql::{Rest, first_token_offset, split_statements};

/// Open strings the closers can't close are called Invalid, though one more line closes them: raw triple-quoted
/// strings (R'''...''', R"""...""", which GenericDialect tokenizes) and quote-delimited strings with any delimiter
/// but [ { ( < (q'#...#', q'!...!', q'"..."', nq'#...#'). Each text below ends in a newline, as the REPL's do.
#[test]
fn open_strings_the_closers_miss_are_open() {
    for (text, closer) in [
        ("SELECT R'''a\n", "'''"),
        ("SELECT R\"\"\"a\n", "\"\"\""),
        ("SELECT q'#a\n", "#'"),
        ("SELECT q'!a\n", "!'"),
        ("SELECT q'\"a\n", "\"'"),
        ("SELECT nq'#a\n", "#'"),
    ] {
        // The text is open: this closer makes it a complete statement.
        assert!(
            split_statements(&format!("{text}{closer};"))
                .statements
                .len()
                == 1,
            "{text:?}"
        );
        assert_eq!(split_statements(text).rest, Rest::Open, "{text:?}");
    }
}

/// At the very end of a text (no newline), `*/` glued to a trailing `/` makes a new `/*`, so no number of `*/`
/// closes `/* a /`, though ` */` does. (The REPL can't see this: its lines end in a newline.)
#[test]
fn a_comment_ending_in_a_slash_is_open() {
    assert_eq!(split_statements("SELECT /* a / */ 1;").statements.len(), 1);
    assert_eq!(split_statements("SELECT /* a /").rest, Rest::Open);
}

/// skip_blank knows four whitespace bytes; the tokenizer takes any `char::is_whitespace` (NBSP, U+3000, U+2028,
/// U+0085, VT, FF) for whitespace. So first_token_offset disagrees with itself on whether the text tokenizes.
#[test]
fn first_token_offset_skips_the_tokenizers_whitespace() {
    for prefix in [
        "\u{a0}\n",
        "\u{c}\n",
        "\u{b}\n",
        "\u{3000}\n",
        "\u{2028}\n",
        "\u{85}\n",
        "/* a */\u{a0}\n",
    ] {
        let tokenizes = first_token_offset(&format!("{prefix}SELECT 1"));
        assert_eq!(
            first_token_offset(&format!("{prefix}SELECT 'open")),
            tokenizes,
            "{prefix:?}"
        );
        assert_eq!(
            first_token_offset(&format!("{prefix}SELECT E'\\x80'")),
            tokenizes,
            "{prefix:?}"
        );
    }
}

// Round 3 (on a491dde, c1e63cb and 88b69a7): tests for the mutants of its run that weren't equivalent.

/// S14 / S15: quote_delimited_ends without its `(` -> `)` or `<` -> `>` mirror passes the whole suite (only `[` and
/// `{` are tested), and then a q-string with that delimiter open across lines is Invalid: a script stops on valid
/// text. (review3_line_prefixes.rs kills both too.)
#[test]
fn quote_delimited_strings_with_every_bracket_are_open() {
    for text in [
        "SELECT q'(a;\n",
        "SELECT q'<a;\n",
        "SELECT Q'(a\n",
        "SELECT nq'<a\n",
    ] {
        assert_eq!(split_statements(text).rest, Rest::Open, "{text:?}");
    }
}

/// S10: dollar_tags without its sort + dedup passes the suite (c1e63cb measured 7.6 s for 400 lines but kept no
/// test). An open `$z$` string whose text repeats an earlier tag: with the dedupe each line tries one copy.
#[test]
fn open_text_with_a_repeated_dollar_tag_is_quick() {
    // The tag copies come before the opener, so every copy is tried before `$z$` is.
    let mut text = format!("SELECT {} $z$\n", "$a$ x $a$ AS c, ".repeat(200));
    let started = Instant::now();
    for _ in 0..100 {
        text.push_str("line;\n");
        assert_eq!(split_statements(&text).rest, Rest::Open);
    }
    let elapsed = started.elapsed();
    // Debug build: 3.5 s with the dedupe, 26.7 s without it (measured in round 3).
    assert!(elapsed.as_secs_f64() < 10.0, "{elapsed:?}");
}

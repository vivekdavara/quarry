//! The day-5 review's second round on the splitter (README, "Review of day 5"): open strings its closers missed, a
//! comment ending in `/`, and whitespace the tokenizer knows and `skip_blank` didn't. Each failed at a491dde^.

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

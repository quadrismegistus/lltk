"""
Unit tests for `ch_quote` — the canonical ClickHouse/SQL string-literal escaper.

ClickHouse honors C-style backslash escapes in string literals, so naive
quote-doubling (`s.replace("'", "''")`) is insufficient: an input containing
`\\` or `\\'` can escape the closing quote and inject. `ch_quote` must double
backslashes *before* doubling quotes so that neither character can be used
to break out of the surrounding `'...'` literal.

No ClickHouse server or optional deps required — pure string-function tests.
"""

from lltk.db.adapter import ch_quote


class TestChQuote:
    def test_apostrophe_doubled(self):
        assert ch_quote("o'er") == "o''er"

    def test_backslash_doubled(self):
        assert ch_quote("a\\b") == "a\\\\b"

    def test_injection_attempt_cannot_break_out(self):
        # A classic escape-the-quote-via-backslash injection attempt.
        raw = "x\\' OR 1=1--"
        escaped = ch_quote(raw)
        # Backslash doubled AND quote doubled — the lone backslash no longer
        # escapes the quote that follows it.
        assert escaped == "x\\\\'' OR 1=1--"
        # Wrapped as a literal, this must remain a single, inert string value —
        # not a closed-then-reopened literal that lets `OR 1=1--` execute as SQL.
        wrapped = f"'{escaped}'"
        assert wrapped == "'x\\\\'' OR 1=1--'"

    def test_empty_string(self):
        assert ch_quote("") == ""

    def test_plain_string_unchanged(self):
        assert ch_quote("plain") == "plain"

    def test_non_string_input_stringified(self):
        # Callers sometimes pass non-str values (e.g. ints); ch_quote coerces.
        assert ch_quote(123) == "123"

    def test_order_matters_backslash_before_quote(self):
        # If quote-doubling ran before backslash-doubling, a trailing
        # backslash-quote pair would be mis-escaped. Verify the actual order:
        # backslash first, then quote.
        raw = "\\'"
        assert ch_quote(raw) == "\\\\''"

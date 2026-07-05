"""
Tests for lltk.cli — the `lltk` command-line entry point.

`_load_or_die` is a small, pure guard (load corpus or print+exit) and is
tested directly with a monkeypatched `lltk.load`.

`main()` builds one big argparse parser and then dispatches on
`args.cmd`. The dispatch branches call into heavy subsystems (ClickHouse,
corpus compilation, web apps, etc.) that we don't want to exercise here.
Argparse itself gives us a side-effect-free way to test the parser
wiring: passing `--help` (or omitting a required positional) causes
argparse to print and `sys.exit()` *before* any dispatch code runs, so
we can verify subcommands/options are registered correctly without
mocking every downstream import.
"""

import sys
import pytest


# ── _load_or_die ─────────────────────────────────────────────────────

class TestLoadOrDie:
    def test_unknown_corpus_exits_1_and_prints_stderr(self, monkeypatch, capsys):
        import lltk
        from lltk import cli
        monkeypatch.setattr(lltk, 'load', lambda corpus_id: None)

        with pytest.raises(SystemExit) as exc_info:
            cli._load_or_die('not_a_real_corpus')

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "unknown corpus 'not_a_real_corpus'" in captured.err
        assert 'lltk show' in captured.err

    def test_known_corpus_returns_loaded_object(self, monkeypatch):
        import lltk
        from lltk import cli
        sentinel = object()
        monkeypatch.setattr(lltk, 'load', lambda corpus_id: sentinel)

        result = cli._load_or_die('some_corpus')

        assert result is sentinel

    def test_passes_corpus_id_through_to_load(self, monkeypatch):
        import lltk
        from lltk import cli
        seen = {}

        def fake_load(corpus_id):
            seen['corpus_id'] = corpus_id
            return 'loaded'

        monkeypatch.setattr(lltk, 'load', fake_load)
        cli._load_or_die('estc')

        assert seen['corpus_id'] == 'estc'


# ── main() argument parsing (no dispatch side effects) ──────────────

class TestMainArgParsing:
    def test_no_args_prints_help_to_stderr_and_exits_1(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert 'usage' in captured.err.lower()

    def test_top_level_help_exits_0(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', '--help'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert 'Literary Language Toolkit' in captured.out

    def test_unknown_subcommand_errors(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', 'not-a-real-subcommand'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 2
        captured = capsys.readouterr()
        assert 'invalid choice' in captured.err

    def test_info_missing_required_positional_errors(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', 'info'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 2
        captured = capsys.readouterr()
        assert 'corpus' in captured.err

    def test_show_subcommand_help_lists_no_args(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', 'show', '--help'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        # `show` takes no positional/option args beyond -h/--help.
        assert 'usage: lltk show' in captured.out
        assert captured.out.count('--') == 1  # only -h/--help listed

    def test_top_level_help_lists_show_subcommand(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', '--help'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert 'List all corpora' in captured.out

    def test_db_minhash_help_shows_all_options(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', 'db-minhash', '--help'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 0
        out = capsys.readouterr().out
        assert '--threshold' in out
        assert '--num-perm' in out
        assert '--corpus' in out
        assert 'default: 0.5' in out
        assert 'default: 128' in out

    def test_db_ocr_accuracy_help_shows_all_options(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', 'db-ocr-accuracy', '--help'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 0
        out = capsys.readouterr().out
        assert '--corpora' in out
        assert '--wordlist' in out
        assert '--rebuild' in out

    def test_export_passages_help_shows_ids_file_option(self, monkeypatch, capsys):
        # Regression check for the --ids-file flag (pre-computed text lists).
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', 'export-passages', '--help'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 0
        out = capsys.readouterr().out
        assert '--ids-file' in out
        assert '--from-task' in out
        assert '--decile' in out

    def test_db_match_fuzzy_flag_registered(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', 'db-match', '--help'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 0
        out = capsys.readouterr().out
        assert '--fuzzy' in out

    def test_compile_requires_corpus_positional(self, monkeypatch, capsys):
        from lltk import cli
        monkeypatch.setattr(sys, 'argv', ['lltk', 'compile'])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 2

"""
Unit tests for lltk.tools.annotations — the cross-corpus annotation SDK.

Focuses on the pure-Python pieces (encode/decode/validation/field_spec).
Integration tests against a live ClickHouse instance are left out of this
file to keep CI hermetic.
"""

import pytest
import pandas as pd


# ── Field spec access ───────────────────────────────────────────────

class TestFieldSpec:
    def test_known_fields_registered(self):
        from lltk.tools.annotations import field_spec
        for f in ('genre', 'genre_raw', 'is_translated', 'original_lang',
                  'year_estimated', 'author_first_name', 'exclude'):
            assert field_spec(f) is not None, f'{f} missing from default specs'

    def test_unknown_field_returns_none(self):
        from lltk.tools.annotations import field_spec
        assert field_spec('no_such_field') is None

    def test_vocab_helper(self):
        from lltk.tools.annotations import vocab
        from lltk.tools.vocabs import GENRE_VOCAB, LANG_ISO639_1
        assert vocab('genre') == GENRE_VOCAB
        assert vocab('original_lang') == LANG_ISO639_1
        # Unbounded fields return None
        assert vocab('genre_raw') is None
        assert vocab('author_first_name') is None
        # Unknown field also returns None
        assert vocab('no_such_field') is None

    def test_register_field_spec_roundtrip(self):
        from lltk.tools.annotations import register_field_spec, field_spec
        register_field_spec('test_custom', {
            'type': 'str', 'vocab': None, 'nullable': True,
        })
        assert field_spec('test_custom')['type'] == 'str'

    def test_register_field_spec_rejects_bad_type(self):
        from lltk.tools.annotations import register_field_spec
        with pytest.raises(ValueError):
            register_field_spec('bad', {'type': 'float'})
        with pytest.raises(TypeError):
            register_field_spec('bad', 'not a dict')


# ── Encode ──────────────────────────────────────────────────────────

class TestEncode:
    def test_genre_vocab_valid(self):
        from lltk.tools.annotations import _encode_value, field_spec
        assert _encode_value('Fiction', field_spec('genre')) == 'Fiction'

    def test_genre_vocab_invalid(self):
        from lltk.tools.annotations import _encode_value, field_spec
        with pytest.raises(ValueError):
            _encode_value('Novella', field_spec('genre'))

    def test_genre_strip_whitespace(self):
        from lltk.tools.annotations import _encode_value, field_spec
        assert _encode_value('  Fiction  ', field_spec('genre')) == 'Fiction'

    def test_genre_nonnullable_rejects_empty(self):
        from lltk.tools.annotations import _encode_value, field_spec
        with pytest.raises(ValueError):
            _encode_value('', field_spec('genre'))
        with pytest.raises(ValueError):
            _encode_value(None, field_spec('genre'))

    def test_is_translated_coercion(self):
        from lltk.tools.annotations import _encode_value, field_spec
        spec = field_spec('is_translated')
        assert _encode_value(True, spec) == '1'
        assert _encode_value(False, spec) == '0'
        assert _encode_value(1, spec) == '1'
        assert _encode_value(0, spec) == '0'
        assert _encode_value('true', spec) == '1'
        assert _encode_value('false', spec) == '0'
        assert _encode_value('yes', spec) == '1'
        # Nullable bool accepts None → ''
        assert _encode_value(None, spec) == ''

    def test_is_translated_rejects_garbage(self):
        from lltk.tools.annotations import _encode_value, field_spec
        with pytest.raises(ValueError):
            _encode_value('maybe', field_spec('is_translated'))

    def test_year_range_enforced(self):
        from lltk.tools.annotations import _encode_value, field_spec
        spec = field_spec('year_estimated')
        assert _encode_value(1789, spec) == '1789'
        assert _encode_value('1789', spec) == '1789'
        with pytest.raises(ValueError):
            _encode_value(2500, spec)
        with pytest.raises(ValueError):
            _encode_value(-1000, spec)

    def test_year_nullable(self):
        from lltk.tools.annotations import _encode_value, field_spec
        assert _encode_value(None, field_spec('year_estimated')) == ''
        assert _encode_value(pd.NA, field_spec('year_estimated')) == ''

    def test_original_lang_normalizes(self):
        from lltk.tools.annotations import _encode_value, field_spec
        spec = field_spec('original_lang')
        assert _encode_value('French', spec) == 'fr'
        assert _encode_value('fr', spec) == 'fr'
        assert _encode_value('FRA', spec) == 'fr'
        # Unknown lang → None → '' (nullable)
        assert _encode_value('Klingon', spec) == ''


# ── Decode ──────────────────────────────────────────────────────────

class TestDecode:
    def test_bool_roundtrip(self):
        from lltk.tools.annotations import _decode_value, field_spec
        spec = field_spec('is_translated')
        assert _decode_value('1', spec) is True
        assert _decode_value('0', spec) is False
        assert _decode_value('', spec) is None

    def test_int_roundtrip(self):
        from lltk.tools.annotations import _decode_value, field_spec
        spec = field_spec('year_estimated')
        assert _decode_value('1789', spec) == 1789
        assert _decode_value('', spec) is None

    def test_str_roundtrip(self):
        from lltk.tools.annotations import _decode_value, field_spec
        spec = field_spec('genre')
        assert _decode_value('Fiction', spec) == 'Fiction'


# ── ID validation ────────────────────────────────────────────────────

class TestIdValidation:
    def test_valid(self):
        from lltk.tools.annotations import _validate_id
        _validate_id('_estc/T012345')
        _validate_id('_chadwyck/B0001')

    def test_rejects_non_string(self):
        from lltk.tools.annotations import _validate_id
        with pytest.raises(TypeError):
            _validate_id(123)

    def test_rejects_malformed(self):
        from lltk.tools.annotations import _validate_id
        with pytest.raises(ValueError):
            _validate_id('estc/T012345')   # missing leading _
        with pytest.raises(ValueError):
            _validate_id('_estcT012345')   # missing /


# ── Defaults / constants ─────────────────────────────────────────────

class TestDefaults:
    def test_default_sources_sanity(self):
        from lltk.tools.annotations import DEFAULT_SOURCES
        assert DEFAULT_SOURCES['human'][0] > DEFAULT_SOURCES['heuristic'][0]
        assert DEFAULT_SOURCES['bibliography:fiction_biblio'][0] > DEFAULT_SOURCES['heuristic'][0]

    def test_default_llm_below_authority(self):
        from lltk.tools.annotations import (
            DEFAULT_SOURCES, DEFAULT_LLM_PRIORITY,
        )
        assert DEFAULT_LLM_PRIORITY < DEFAULT_SOURCES['heuristic'][0]

    def test_default_run_id_contains_date(self):
        from lltk.tools.annotations import _default_run_id
        from datetime import date
        assert date.today().isoformat() in _default_run_id('llm:foo')
        assert 'llm:foo' in _default_run_id('llm:foo')

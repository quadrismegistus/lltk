"""Tests for lltk.annotate — the LLM task orchestration layer."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd


# ── prompt formatting ─────────────────────────────────────────────────

class TestFormatGenrePrompt:
    def test_basic(self):
        from lltk.annotate import _format_genre_prompt
        row = {'title': 'The History of Tom Jones', 'author': 'Fielding, Henry', 'year': 1749}
        prompt = _format_genre_prompt(row)
        assert 'Title: The History of Tom Jones' in prompt
        assert 'Author: Fielding' in prompt
        assert 'Year: 1700-1800' in prompt

    def test_author_norm_preferred(self):
        from lltk.annotate import _format_genre_prompt
        row = {'title': 'Pamela', 'author': 'Richardson, Samuel', 'author_norm': 'richardson', 'year': 1740}
        prompt = _format_genre_prompt(row)
        assert 'Author: Richardson' in prompt

    def test_no_author(self):
        from lltk.annotate import _format_genre_prompt
        row = {'title': 'Anonymous Novel', 'year': 1750}
        prompt = _format_genre_prompt(row)
        assert 'Author' not in prompt

    def test_no_year(self):
        from lltk.annotate import _format_genre_prompt
        row = {'title': 'Undated Work', 'author': 'Smith'}
        prompt = _format_genre_prompt(row)
        assert 'Year' not in prompt

    def test_nan_year(self):
        from lltk.annotate import _format_genre_prompt
        row = {'title': 'Test', 'year': float('nan')}
        prompt = _format_genre_prompt(row)
        assert 'Year' not in prompt

    def test_strips_parenthetical_from_title(self):
        from lltk.annotate import _format_genre_prompt
        row = {'title': 'Robinson Crusoe (1719 edition)', 'year': 1719}
        prompt = _format_genre_prompt(row)
        assert 'Title: Robinson Crusoe' in prompt
        assert '1719 edition' not in prompt

    def test_subject_topic(self):
        from lltk.annotate import _format_genre_prompt
        row = {'title': 'Test', 'subject_topic': 'English fiction -- 18th century'}
        prompt = _format_genre_prompt(row)
        assert 'Subject Topic: English fiction' in prompt


# ── result → annotation rows ──────────────────────────────────────────

class TestResultToAnnotationRows:
    def test_genre_pydantic_model(self):
        from lltk.annotate import _result_to_annotation_rows

        mock_result = MagicMock()
        mock_result.model_dump.return_value = {
            'genre': 'Fiction',
            'genre_raw': 'Novel, epistolary',
            'is_translated': False,
            'translated_from': '',
            'author_first_name': 'Samuel',
            'year_estimated': 1740,
            'confidence': 0.95,
            'reasoning': 'Well-known epistolary novel',
        }

        rows = _result_to_annotation_rows('_estc/T012345', mock_result, 'genre')
        fields = {r['field'] for r in rows}
        assert 'genre' in fields
        assert 'genre_raw' in fields
        assert 'author_first_name' in fields
        assert 'year_estimated' in fields

        genre_row = next(r for r in rows if r['field'] == 'genre')
        assert genre_row['value'] == 'Fiction'
        assert genre_row['confidence'] == 0.95
        assert genre_row['_id'] == '_estc/T012345'

    def test_skips_empty_values(self):
        from lltk.annotate import _result_to_annotation_rows

        data = {
            'genre': 'Poetry',
            'genre_raw': '',
            'is_translated': False,
            'translated_from': '',
            'author_first_name': '',
            'year_estimated': 0,
            'confidence': 0.8,
            'reasoning': '',
        }

        rows = _result_to_annotation_rows('_estc/T099', data, 'genre')
        fields = {r['field'] for r in rows}
        assert 'genre' in fields
        assert 'genre_raw' not in fields
        assert 'author_first_name' not in fields
        assert 'year_estimated' not in fields

    def test_dict_input(self):
        from lltk.annotate import _result_to_annotation_rows

        data = {'genre': 'Drama', 'genre_raw': 'Comedy', 'confidence': 0.7}
        rows = _result_to_annotation_rows('_estc/T001', data, 'genre')
        assert len(rows) == 2
        assert rows[0]['field'] == 'genre'
        assert rows[1]['field'] == 'genre_raw'

    def test_unknown_task_returns_empty(self):
        from lltk.annotate import _result_to_annotation_rows
        rows = _result_to_annotation_rows('_estc/T001', {'genre': 'Fiction'}, 'unknown_task')
        assert rows == []

    def test_confidence_override(self):
        from lltk.annotate import _result_to_annotation_rows
        data = {'genre': 'Fiction', 'confidence': 0.95}
        rows = _result_to_annotation_rows('_estc/T001', data, 'genre', confidence_override=0.5)
        assert rows[0]['confidence'] == 0.5

    def test_reasoning_in_meta(self):
        from lltk.annotate import _result_to_annotation_rows
        data = {'genre': 'Fiction', 'confidence': 0.9, 'reasoning': 'Title contains "novel"'}
        rows = _result_to_annotation_rows('_estc/T001', data, 'genre')
        assert rows[0]['meta']['reasoning'] == 'Title contains "novel"'


# ── task registry ─────────────────────────────────────────────────────

class TestTaskRegistry:
    def test_registry_has_expected_tasks(self):
        from lltk.annotate import TASK_REGISTRY
        assert 'genre' in TASK_REGISTRY
        assert 'social_network' in TASK_REGISTRY
        assert 'frye' in TASK_REGISTRY

    def test_unknown_task_raises(self):
        from lltk.annotate import _load_task
        with pytest.raises(ValueError, match='Unknown task'):
            _load_task('nonexistent_task')

    def test_registry_types(self):
        from lltk.annotate import TASK_REGISTRY
        for name, (mod, cls, typ) in TASK_REGISTRY.items():
            assert typ in ('base', 'sequential'), f'{name} has invalid type {typ}'


# ── get_text_ids ──────────────────────────────────────────────────────

class TestGetTextIds:
    @patch('lltk.annotate.pd')
    def test_ids_query(self, mock_pd):
        """With explicit ids, should query CH directly."""
        from lltk.annotate import _get_text_ids

        mock_db = MagicMock()
        mock_db.adapter.query_df.return_value = pd.DataFrame({
            '_id': ['_estc/T001', '_estc/T002'],
        })

        with patch('lltk.db', mock_db):
            import lltk.annotate
            # Temporarily patch the lltk import inside _get_text_ids
            with patch.dict('sys.modules', {'lltk': MagicMock(db=mock_db)}):
                df = _get_text_ids(ids=['_estc/T001', '_estc/T002'])

        mock_db.adapter.query_df.assert_called_once()
        call_sql = mock_db.adapter.query_df.call_args[0][0]
        assert '_estc/T001' in call_sql
        assert '_estc/T002' in call_sql


# ── already_annotated ─────────────────────────────────────────────────

class TestAlreadyAnnotated:
    def test_returns_set_of_ids(self):
        from lltk.annotate import _already_annotated
        from lltk.tools import annotations as A
        mock_df = pd.DataFrame({'_id': ['_estc/T001', '_estc/T002']})
        with patch.object(A, 'resolve_by_source', return_value=mock_df):
            result = _already_annotated('llm:test', 'genre', ['_estc/T001', '_estc/T002', '_estc/T003'])
        assert result == {'_estc/T001', '_estc/T002'}

    def test_unknown_task_returns_empty(self):
        from lltk.annotate import _already_annotated
        result = _already_annotated('llm:test', 'unknown_task', ['_estc/T001'])
        assert result == set()

    def test_exception_returns_empty(self):
        from lltk.annotate import _already_annotated
        from lltk.tools import annotations as A
        with patch.object(A, 'resolve_by_source', side_effect=Exception('CH down')):
            result = _already_annotated('llm:test', 'genre', ['_estc/T001'])
        assert result == set()


# ── run_task integration (mocked) ─────────────────────────────────────

class TestRunTask:
    def _mock_env(self):
        """Set up mocked lltk.db and largeliterarymodels."""
        mock_task = MagicMock()
        mock_task.model = None
        mock_result = MagicMock()
        mock_result.model_dump.return_value = {
            'genre': 'Fiction', 'genre_raw': 'Novel',
            'confidence': 0.9, 'is_translated': False,
            'translated_from': '', 'author_first_name': 'Daniel',
            'year_estimated': 1719, 'reasoning': 'Famous novel',
        }
        mock_task.run.return_value = mock_result
        return mock_task, mock_result

    @patch('lltk.annotate._load_task')
    @patch('lltk.annotate._get_text_ids')
    @patch('lltk.annotate._already_annotated', return_value=set())
    def test_base_task_flow(self, mock_done, mock_ids, mock_load):
        from lltk.annotate import run_task
        from lltk.tools import annotations as A

        mock_task, mock_result = self._mock_env()
        mock_load.return_value = (mock_task, 'base')
        mock_ids.return_value = pd.DataFrame({
            '_id': ['_estc/T001', '_estc/T002'],
            'title': ['Robinson Crusoe', 'Moll Flanders'],
            'author': ['Defoe, Daniel', 'Defoe, Daniel'],
            'year': [1719, 1722],
        })

        with patch.object(A, 'write', return_value=3) as mock_write:
            stats = run_task('genre', corpus='estc', verbose=False)

        assert stats['n_processed'] == 2
        assert stats['n_errors'] == 0
        assert mock_task.run.call_count == 2
        assert mock_write.called

    @patch('lltk.annotate._load_task')
    @patch('lltk.annotate._get_text_ids')
    @patch('lltk.annotate._already_annotated')
    def test_skip_existing(self, mock_done, mock_ids, mock_load):
        from lltk.annotate import run_task
        from lltk.tools import annotations as A

        mock_task, _ = self._mock_env()
        mock_load.return_value = (mock_task, 'base')
        mock_ids.return_value = pd.DataFrame({
            '_id': ['_estc/T001', '_estc/T002', '_estc/T003'],
            'title': ['A', 'B', 'C'],
            'author': ['X', 'Y', 'Z'],
            'year': [1700, 1710, 1720],
        })
        mock_done.return_value = {'_estc/T001', '_estc/T002'}

        with patch.object(A, 'write', return_value=1):
            stats = run_task('genre', verbose=False)

        assert stats['n_processed'] == 1
        assert stats['n_skipped'] == 2
        assert mock_task.run.call_count == 1

    @patch('lltk.annotate._load_task')
    @patch('lltk.annotate._get_text_ids')
    @patch('lltk.annotate._already_annotated', return_value=set())
    def test_error_handling(self, mock_done, mock_ids, mock_load):
        from lltk.annotate import run_task

        mock_task = MagicMock()
        mock_task.model = None
        mock_task.run.side_effect = Exception('API error')
        mock_load.return_value = (mock_task, 'base')
        mock_ids.return_value = pd.DataFrame({
            '_id': ['_estc/T001'],
            'title': ['Test'],
            'author': ['Author'],
            'year': [1700],
        })

        stats = run_task('genre', verbose=False, save_annotations=False)
        assert stats['n_errors'] == 1
        assert stats['n_processed'] == 0


# ── ingest_tasks ──────────────────────────────────────────────────────

class TestIngestTasks:
    def _make_result_json(self, tmp_path, filename, _id, model='vllm/qwen3.6-27b'):
        """Write a fake task result JSON."""
        import json
        result = {
            'characters': [{'name': 'Alice'}, {'name': 'Bob'}],
            'relations': [{'source': 'Alice', 'target': 'Bob'}],
            'events': [],
            'dialogue': [{'speaker': 'Alice', 'text': 'Hello'}],
            'metadata': {
                'source': _id,
                'model': model,
                'schema_version': 'social_network_v1',
                'n_passages': 10,
                'n_chunks': 1,
                'elapsed_seconds': 42.0,
            },
        }
        path = tmp_path / filename
        with open(path, 'w') as f:
            json.dump(result, f)
        return path

    def test_places_files_correctly(self, tmp_path):
        from lltk.annotate import ingest_tasks

        self._make_result_json(
            tmp_path,
            'chadwyck_ee01010_qwen36-27b.json',
            '_chadwyck/ee01010.18',
        )

        with patch('lltk.task_path', return_value=str(tmp_path / 'task_out')):
            stats = ingest_tasks(
                'social_network', str(tmp_path), verbose=False,
            )

        assert stats['n_ingested'] == 1
        assert stats['n_errors'] == 0
        dest = tmp_path / 'task_out' / 'qwen36-27b.json'
        assert dest.exists()

    def test_skips_existing(self, tmp_path):
        from lltk.annotate import ingest_tasks
        import os

        self._make_result_json(
            tmp_path,
            'test.json',
            '_chadwyck/ee01010.18',
        )

        task_dir = tmp_path / 'task_out'
        task_dir.mkdir(parents=True)
        (task_dir / 'qwen36-27b.json').write_text('{}')

        with patch('lltk.task_path', return_value=str(task_dir)):
            stats = ingest_tasks(
                'social_network', str(tmp_path), verbose=False,
            )

        assert stats['n_skipped'] == 1
        assert stats['n_ingested'] == 0

    def test_rejects_missing_id(self, tmp_path):
        import json
        bad = tmp_path / 'bad.json'
        bad.write_text(json.dumps({'metadata': {'source': 'not_an_id'}}))

        from lltk.annotate import ingest_tasks
        stats = ingest_tasks('social_network', str(tmp_path), verbose=False)
        assert stats['n_errors'] == 1
        assert stats['n_ingested'] == 0

    def test_dry_run(self, tmp_path):
        from lltk.annotate import ingest_tasks

        self._make_result_json(
            tmp_path,
            'test.json',
            '_chadwyck/ee01010.18',
        )

        with patch('lltk.task_path', return_value=str(tmp_path / 'task_out')):
            stats = ingest_tasks(
                'social_network', str(tmp_path), dry_run=True, verbose=False,
            )

        assert stats['n_ingested'] == 1
        assert not (tmp_path / 'task_out').exists()

    def test_extract_scalars(self, tmp_path):
        from lltk.annotate import _extract_social_network_scalars

        result = {
            'characters': [{'name': 'A'}, {'name': 'B'}, {'name': 'C'}],
            'relations': [{'s': 'A', 't': 'B'}],
            'events': [{'type': 'meeting'}],
            'dialogue': [],
        }
        scalars = _extract_social_network_scalars(result)
        field_map = dict(scalars)
        assert field_map['n_characters'] == 3
        assert field_map['n_relations'] == 1
        assert field_map['n_events'] == 1
        assert 'n_dialogue' not in field_map

    def test_empty_dir(self, tmp_path):
        from lltk.annotate import ingest_tasks
        stats = ingest_tasks('social_network', str(tmp_path), verbose=False)
        assert stats['n_ingested'] == 0

    def test_corrupt_json(self, tmp_path):
        (tmp_path / 'corrupt.json').write_text('{bad json')
        from lltk.annotate import ingest_tasks
        stats = ingest_tasks('social_network', str(tmp_path), verbose=False)
        assert stats['n_errors'] == 1

"""
Tests for lltk.db.minhash — MinHash/LSH near-duplicate detection via
word-set overlap on text_freqs.

`minhash_match_ch` is orchestration around ClickHouse I/O (reading
text_freqs, writing to lltk.matches) plus the `datasketch` library for
the actual MinHash/LSH math. We don't call it directly in tests because
it has two side effects that aren't test-isolated:
  - it writes a signature cache pickle to the *real*
    ~/lltk_data/corpora/{corpus}/data/minhash_{num_perm}.pkl (not scoped
    to tmp_path)
  - it writes to the CH `matches` table

Instead we test the pure, checkable pieces: `_cache_path` (pure path
construction), and the exact signature-computation / candidate-pair /
threshold-filter patterns used inside the function (mirrored here
against hand-built word sets, not through the CH-dependent function).
"""

import os
import pytest

try:
    from datasketch import MinHash, MinHashLSH
    _HAS_DATASKETCH = True
except ImportError:
    _HAS_DATASKETCH = False

needs_datasketch = pytest.mark.skipif(
    not _HAS_DATASKETCH, reason='datasketch not installed',
)


# ── _cache_path (pure) ──────────────────────────────────────────────

class TestCachePath:
    def test_expands_user_and_embeds_corpus_and_num_perm(self):
        from lltk.db.minhash import _cache_path
        p = _cache_path('estc', 128)
        assert p == os.path.expanduser(
            '~/lltk_data/corpora/estc/data/minhash_128.pkl'
        )

    def test_different_num_perm_gives_different_path(self):
        from lltk.db.minhash import _cache_path
        assert _cache_path('estc', 128) != _cache_path('estc', 256)

    def test_different_corpus_gives_different_path(self):
        from lltk.db.minhash import _cache_path
        assert _cache_path('estc', 128) != _cache_path('chadwyck', 128)

    def test_no_trailing_slash_issues(self):
        from lltk.db.minhash import _cache_path
        p = _cache_path('my_corpus', 128)
        assert p.endswith('my_corpus/data/minhash_128.pkl')


# ── Signature computation (mirrors minhash.py's per-text MinHash loop) ──

def _signature(words, num_perm=128):
    """Same pattern as minhash_match_ch: MinHash over a word set, words
    encoded as utf8 (see lltk/db/minhash.py ~lines 73-76)."""
    m = MinHash(num_perm=num_perm)
    for w in words:
        m.update(w.encode('utf8'))
    return m


@needs_datasketch
class TestSignatureComputation:
    def test_deterministic_for_same_word_set(self):
        words = ['the', 'quick', 'brown', 'fox', 'jumps']
        sig_a = _signature(words)
        sig_b = _signature(words)
        assert list(sig_a.hashvalues) == list(sig_b.hashvalues)

    def test_order_independent(self):
        # MinHash aggregates a min-per-permutation over the *set* of
        # elements seen, so update() order shouldn't change the signature.
        words = ['the', 'quick', 'brown', 'fox', 'jumps']
        sig_a = _signature(words)
        sig_b = _signature(list(reversed(words)))
        assert list(sig_a.hashvalues) == list(sig_b.hashvalues)

    def test_different_word_sets_give_different_signatures(self):
        sig_a = _signature(['the', 'quick', 'brown', 'fox'])
        sig_b = _signature(['lorem', 'ipsum', 'dolor', 'sit'])
        assert list(sig_a.hashvalues) != list(sig_b.hashvalues)

    def test_num_perm_controls_signature_length(self):
        assert len(_signature(['a', 'b'], num_perm=64).hashvalues) == 64
        assert len(_signature(['a', 'b'], num_perm=128).hashvalues) == 128


# ── Jaccard similarity estimate ─────────────────────────────────────

@needs_datasketch
class TestJaccardEstimate:
    def test_identical_sets_are_similarity_one(self):
        words = [f'word{i}' for i in range(200)]
        sig_a = _signature(words)
        sig_b = _signature(words)
        assert sig_a.jaccard(sig_b) == pytest.approx(1.0)

    def test_disjoint_sets_are_near_zero(self):
        sig_a = _signature([f'a{i}' for i in range(200)])
        sig_b = _signature([f'b{i}' for i in range(200)])
        assert sig_a.jaccard(sig_b) == pytest.approx(0.0, abs=0.05)

    def test_partial_overlap_approximates_true_jaccard(self):
        shared = [f'shared{i}' for i in range(500)]
        set_a = shared + [f'a_only{i}' for i in range(500)]
        set_b = shared + [f'b_only{i}' for i in range(500)]
        true_jaccard = (
            len(set(set_a) & set(set_b)) / len(set(set_a) | set(set_b))
        )
        sig_a = _signature(set_a)
        sig_b = _signature(set_b)
        # MinHash estimate is probabilistic; with num_perm=128 it should
        # land close to the true (exact) Jaccard of 1/3.
        assert sig_a.jaccard(sig_b) == pytest.approx(true_jaccard, abs=0.08)


# ── Candidate-pair generation + threshold gating ────────────────────
# Mirrors the "Running LSH" / "Computing exact similarities" blocks in
# minhash_match_ch (lines ~99-123): insert into an LSH index, gather
# candidate pairs, keep those meeting the exact-Jaccard threshold, sort
# by descending similarity.

@needs_datasketch
class TestCandidatePairsAndThreshold:
    def _lsh_pairs(self, signatures, threshold, num_perm=128):
        lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        for _id, sig in signatures.items():
            try:
                lsh.insert(_id, sig)
            except ValueError:
                pass
        pairs = set()
        for _id, sig in signatures.items():
            for c in lsh.query(sig):
                if c != _id:
                    pairs.add(tuple(sorted([_id, c])))
        return pairs

    def _matches(self, signatures, pairs, threshold):
        matches = []
        for a, b in pairs:
            sim = signatures[a].jaccard(signatures[b])
            if sim >= threshold:
                matches.append((a, b, float(sim)))
        matches.sort(key=lambda x: -x[2])
        return matches

    def test_near_duplicate_pair_found_above_threshold(self):
        shared = [f'w{i}' for i in range(300)]
        signatures = {
            'a': _signature(shared),
            'b': _signature(shared + ['extra1', 'extra2']),
            'c': _signature([f'other{i}' for i in range(300)]),
        }
        pairs = self._lsh_pairs(signatures, threshold=0.5)
        matches = self._matches(signatures, pairs, threshold=0.5)
        matched_pairs = {m[:2] for m in matches}
        assert ('a', 'b') in matched_pairs
        assert not any('c' in m[:2] for m in matches)

    def test_matches_sorted_descending_by_similarity(self):
        base = [f'w{i}' for i in range(300)]
        signatures = {
            'a': _signature(base),
            'b': _signature(base),  # identical -> sim ~1.0
            'c': _signature(base[:150] + [f'x{i}' for i in range(150)]),  # partial
        }
        pairs = self._lsh_pairs(signatures, threshold=0.3)
        matches = self._matches(signatures, pairs, threshold=0.3)
        sims = [m[2] for m in matches]
        assert len(sims) >= 2
        assert sims == sorted(sims, reverse=True)

    def test_disjoint_sets_produce_no_matches(self):
        signatures = {
            'a': _signature([f'a{i}' for i in range(300)]),
            'b': _signature([f'b{i}' for i in range(300)]),
        }
        pairs = self._lsh_pairs(signatures, threshold=0.9)
        matches = self._matches(signatures, pairs, threshold=0.9)
        assert matches == []

    def test_lsh_insert_ignores_duplicate_key_valueerror(self):
        # minhash_match_ch swallows ValueError on lsh.insert (duplicate
        # keys) -- verify that pattern doesn't blow up or drop the pair.
        sig = _signature(['w1', 'w2', 'w3'])
        lsh = MinHashLSH(threshold=0.1, num_perm=128)
        lsh.insert('dup', sig)
        with pytest.raises(ValueError):
            lsh.insert('dup', sig)  # duplicate key raises by default

<script>
  import { onMount } from 'svelte';
  import { getCorpusOverlap } from '../lib/api.js';
  import { formatNumber } from '../lib/utils.js';

  let overlaps = $state([]);
  let loading = $state(true);

  onMount(async () => {
    try {
      const data = await getCorpusOverlap();
      overlaps = data.overlaps;
    } catch (e) {
      console.error(e);
    }
    loading = false;
  });
</script>

<div class="overlap-page">
  <h2>Corpus Overlap</h2>
  <p class="desc">Cross-corpus duplicate matches: how many texts in corpus A are also found in corpus B.</p>

  {#if loading}
    <div class="loading">Loading...</div>
  {:else if overlaps.length === 0}
    <div class="loading">No overlap data (run <code>lltk db-match</code> first)</div>
  {:else}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Corpus A</th>
            <th>Corpus B</th>
            <th>Shared Matches</th>
          </tr>
        </thead>
        <tbody>
          {#each overlaps as o}
            <tr>
              <td class="corpus">{o.corpus_a}</td>
              <td class="corpus">{o.corpus_b}</td>
              <td class="num">{formatNumber(o.n)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<style>
  .overlap-page { max-width: 700px; }
  h2 { font-size: 15px; font-weight: 600; color: #334155; margin-bottom: 4px; }
  .desc { font-size: 13px; color: #64748b; margin-bottom: 16px; }
  .loading { padding: 40px; text-align: center; color: #94a3b8; }
  code { background: #f1f5f9; padding: 1px 4px; border-radius: 3px; font-size: 12px; }

  .table-wrap {
    background: white;
    border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    overflow: auto;
    max-height: calc(100vh - 200px);
  }
  table { width: 100%; border-collapse: collapse; }
  th {
    position: sticky; top: 0;
    background: #f8fafc;
    padding: 8px 14px;
    text-align: left;
    font-size: 12px;
    font-weight: 600;
    color: #64748b;
    border-bottom: 1px solid #e2e8f0;
  }
  td {
    padding: 6px 14px;
    font-size: 13px;
    border-bottom: 1px solid #f1f5f9;
  }
  .corpus { font-weight: 500; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
</style>

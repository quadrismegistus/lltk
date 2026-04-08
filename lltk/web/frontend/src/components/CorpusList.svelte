<script>
  import { onMount } from 'svelte';
  import { getCorpora, getCorpus } from '../lib/api.js';
  import { formatNumber, yearRange } from '../lib/utils.js';
  import { switchTab, filters } from '../stores.js';

  let corpora = $state([]);
  let selectedCorpus = $state(null);
  let detail = $state(null);
  let loading = $state(true);

  function browseCorpus(corpus) {
    filters.update(f => ({ ...f, corpus, page: 1 }));
    switchTab('texts');
  }

  async function selectCorpus(id) {
    selectedCorpus = id;
    detail = null;
    try {
      detail = await getCorpus(id);
    } catch (e) {
      console.error(e);
    }
  }

  onMount(async () => {
    const data = await getCorpora();
    corpora = data.corpora;
    loading = false;
  });
</script>

<div class="corpus-page">
  <div class="corpus-table-wrap">
    {#if loading}
      <div class="loading">Loading...</div>
    {:else}
      <table>
        <thead>
          <tr>
            <th>Corpus</th>
            <th>Texts</th>
            <th>Years</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each corpora as c}
            <tr
              class:selected={selectedCorpus === c.corpus}
              onclick={() => selectCorpus(c.corpus)}
            >
              <td class="corpus-name">{c.corpus}</td>
              <td class="num">{formatNumber(c.n_texts)}</td>
              <td>{yearRange(c.year_min, c.year_max)}</td>
              <td>
                <button class="browse-btn" onclick={(e) => { e.stopPropagation(); browseCorpus(c.corpus); }}>
                  Browse
                </button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>

  {#if detail}
    <div class="corpus-detail">
      <h3>{detail.corpus}</h3>
      <div class="detail-stats">
        <span>{formatNumber(detail.n_texts)} texts</span>
        <span>{yearRange(detail.year_min, detail.year_max)}</span>
        <span>{formatNumber(detail.n_freqs)} with freqs</span>
      </div>

      {#if detail.genres.length}
        <h4>Genres</h4>
        <div class="bar-chart">
          {#each detail.genres as g}
            {@const pct = (g.n / detail.n_texts * 100)}
            <div class="bar-row">
              <span class="bar-label">{g.genre}</span>
              <div class="bar-track">
                <div class="bar-fill" style="width: {Math.max(pct, 0.5)}%"></div>
              </div>
              <span class="bar-value">{formatNumber(g.n)}</span>
            </div>
          {/each}
        </div>
      {/if}

      {#if detail.years.length}
        <h4>Decades</h4>
        <div class="bar-chart">
          {#each detail.years as y}
            {@const maxY = Math.max(...detail.years.map(d => d.n))}
            <div class="bar-row">
              <span class="bar-label">{y.decade}s</span>
              <div class="bar-track">
                <div class="bar-fill" style="width: {(y.n / maxY * 100)}%"></div>
              </div>
              <span class="bar-value">{formatNumber(y.n)}</span>
            </div>
          {/each}
        </div>
      {/if}

      {#if detail.authors.length}
        <h4>Top Authors</h4>
        <div class="author-list">
          {#each detail.authors as a}
            <div class="author-row">
              <span>{a.author}</span>
              <span class="author-count">{a.n}</span>
            </div>
          {/each}
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .corpus-page { display: flex; gap: 20px; height: 100%; }

  .corpus-table-wrap {
    flex: 1;
    overflow: auto;
    background: white;
    border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  .loading { padding: 40px; text-align: center; color: #94a3b8; }
  table { width: 100%; border-collapse: collapse; }
  th {
    position: sticky; top: 0;
    background: #f8fafc;
    padding: 8px 12px;
    text-align: left;
    font-size: 12px;
    font-weight: 600;
    color: #64748b;
    border-bottom: 1px solid #e2e8f0;
  }
  td {
    padding: 6px 12px;
    font-size: 13px;
    border-bottom: 1px solid #f1f5f9;
  }
  tr { cursor: pointer; transition: background 0.1s; }
  tbody tr:hover { background: #f8fafc; }
  tr.selected { background: #eff6ff; }
  .corpus-name { font-weight: 500; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .browse-btn {
    padding: 3px 10px;
    border: 1px solid #d1d5db;
    border-radius: 4px;
    background: white;
    font-size: 12px;
    cursor: pointer;
  }
  .browse-btn:hover { background: #f3f4f6; }

  .corpus-detail {
    width: 360px;
    background: white;
    border-radius: 6px;
    padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    overflow-y: auto;
    flex-shrink: 0;
  }
  h3 { font-size: 16px; font-weight: 600; margin-bottom: 8px; }
  .detail-stats {
    display: flex;
    gap: 12px;
    font-size: 13px;
    color: #64748b;
    margin-bottom: 16px;
  }
  h4 {
    font-size: 13px;
    font-weight: 600;
    color: #475569;
    margin: 16px 0 8px;
    padding-top: 12px;
    border-top: 1px solid #e2e8f0;
  }

  .bar-chart { display: flex; flex-direction: column; gap: 4px; }
  .bar-row { display: flex; align-items: center; gap: 8px; font-size: 12px; }
  .bar-label { min-width: 70px; color: #475569; text-align: right; }
  .bar-track { flex: 1; height: 14px; background: #f1f5f9; border-radius: 2px; overflow: hidden; }
  .bar-fill { height: 100%; background: #3b82f6; border-radius: 2px; min-width: 1px; }
  .bar-value { min-width: 40px; text-align: right; color: #94a3b8; font-variant-numeric: tabular-nums; }

  .author-list { display: flex; flex-direction: column; gap: 2px; }
  .author-row {
    display: flex;
    justify-content: space-between;
    font-size: 12px;
    padding: 2px 0;
  }
  .author-count { color: #94a3b8; }
</style>

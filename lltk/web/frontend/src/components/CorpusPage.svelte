<script>
  import { onMount } from 'svelte';
  import { getCorpus } from '../lib/api.js';
  import { formatNumber, yearRange } from '../lib/utils.js';
  import { filters, switchTab, corpusPageId } from '../stores.js';
  import GenreTimeline from './GenreTimeline.svelte';

  let detail = $state(null);
  let loading = $state(true);
  let corpusId = $state('');

  corpusPageId.subscribe(v => {
    if (v && v !== corpusId) {
      corpusId = v;
      load(v);
    }
  });

  async function load(id) {
    loading = true;
    detail = null;
    try {
      detail = await getCorpus(id);
    } catch (e) {
      console.error(e);
    }
    loading = false;
  }

  function browseTexts() {
    filters.set({
      search: '', corpus: corpusId, genre: '', year_min: null, year_max: null,
      dedup: false, dedup_by: 'rank', has_freqs: false,
      sort_by: 'year', sort_dir: 'asc', page: 1, per_page: 100,
    });
    switchTab('texts');
  }

  function browseGenre(genre) {
    filters.set({
      search: '', corpus: corpusId, genre, year_min: null, year_max: null,
      dedup: false, dedup_by: 'rank', has_freqs: false,
      sort_by: 'year', sort_dir: 'asc', page: 1, per_page: 100,
    });
    switchTab('texts');
  }

  onMount(() => {
    if (corpusId) load(corpusId);
  });
</script>

<div class="corpus-page">
  {#if loading}
    <div class="loading">Loading...</div>
  {:else if !detail}
    <div class="loading">Corpus not found</div>
  {:else}
    <div class="hero">
      <div class="hero-main">
        <h2>{detail.corpus}</h2>
        <p class="desc">{detail.desc || ''}</p>
        <div class="stat-row">
          <div class="stat">
            <span class="stat-value">{formatNumber(detail.n_texts)}</span>
            <span class="stat-label">texts</span>
          </div>
          <div class="stat">
            <span class="stat-value">{yearRange(detail.year_min, detail.year_max)}</span>
            <span class="stat-label">years</span>
          </div>
          <div class="stat">
            <span class="stat-value">{formatNumber(detail.n_freqs)}</span>
            <span class="stat-label">with freqs</span>
          </div>
        </div>
        <div class="actions">
          <button class="btn btn-primary" onclick={browseTexts}>Browse texts</button>
          <a class="btn btn-secondary" href="/api/corpus/{detail.corpus}/download" download>Download CSV</a>
          {#if detail.link}
            <a class="btn btn-secondary" href={detail.link} target="_blank" rel="noopener">Source</a>
          {/if}
        </div>
      </div>
    </div>

    <div class="panels">
      {#if detail.genres.length}
        <div class="panel">
          <h3>Genres</h3>
          <div class="bar-chart">
            {#each detail.genres as g}
              {@const pct = (g.n / detail.n_texts * 100)}
              <div class="bar-row clickable" onclick={() => browseGenre(g.genre)}>
                <span class="bar-label">{g.genre}</span>
                <div class="bar-track">
                  <div class="bar-fill" style="width: {Math.max(pct, 0.5)}%"></div>
                </div>
                <span class="bar-value">{formatNumber(g.n)}</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if detail.authors.length}
        <div class="panel">
          <h3>Top Authors</h3>
          <div class="author-list">
            {#each detail.authors as a}
              <div class="author-row">
                <span>{a.author}</span>
                <span class="author-count">{formatNumber(a.n)}</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}
    </div>

    <div class="timeline-section">
      <h3>Genre Timeline</h3>
      <GenreTimeline corpus={corpusId} />
    </div>
  {/if}
</div>

<style>
  .corpus-page { max-width: 900px; margin: 0 auto; }
  .loading { padding: 40px; text-align: center; color: #94a3b8; }

  .hero {
    background: white;
    border-radius: 8px;
    padding: 28px 32px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    margin-bottom: 20px;
  }
  h2 { font-size: 22px; font-weight: 700; margin-bottom: 6px; }
  .desc { font-size: 14px; color: #64748b; margin-bottom: 16px; line-height: 1.5; }

  .stat-row { display: flex; gap: 32px; margin-bottom: 20px; }
  .stat { display: flex; flex-direction: column; }
  .stat-value { font-size: 20px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .stat-label { font-size: 12px; color: #94a3b8; }

  .actions { display: flex; gap: 8px; }
  .btn {
    padding: 7px 16px;
    border-radius: 5px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    text-decoration: none;
    border: none;
  }
  .btn-primary { background: #2563eb; color: white; }
  .btn-primary:hover { background: #1d4ed8; }
  .btn-secondary { background: white; color: #374151; border: 1px solid #d1d5db; }
  .btn-secondary:hover { background: #f3f4f6; }

  .timeline-section {
    background: white;
    border-radius: 8px;
    padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    margin-bottom: 16px;
  }
  .timeline-section h3 { font-size: 14px; font-weight: 600; color: #475569; margin-bottom: 12px; }

  .panels { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; }
  .panel {
    background: white;
    border-radius: 8px;
    padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  h3 { font-size: 14px; font-weight: 600; color: #475569; margin-bottom: 12px; }

  .bar-chart { display: flex; flex-direction: column; gap: 4px; }
  .bar-row { display: flex; align-items: center; gap: 8px; font-size: 12px; }
  .bar-row.clickable { cursor: pointer; border-radius: 3px; padding: 2px 4px; margin: -2px -4px; }
  .bar-row.clickable:hover { background: #f1f5f9; }
  .bar-label { min-width: 70px; color: #475569; text-align: right; }
  .bar-track { flex: 1; height: 14px; background: #f1f5f9; border-radius: 2px; overflow: hidden; }
  .bar-fill { height: 100%; background: #3b82f6; border-radius: 2px; min-width: 1px; }
  .bar-value { min-width: 40px; text-align: right; color: #94a3b8; font-variant-numeric: tabular-nums; }

  .author-list { display: flex; flex-direction: column; gap: 2px; }
  .author-row { display: flex; justify-content: space-between; font-size: 12px; padding: 2px 0; }
  .author-count { color: #94a3b8; }
</style>

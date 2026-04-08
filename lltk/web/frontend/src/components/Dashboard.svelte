<script>
  import { onMount } from 'svelte';
  import { getOverview, getHeatmap, getStats } from '../lib/api.js';
  import { formatNumber, yearRange } from '../lib/utils.js';
  import { switchTab, filters } from '../stores.js';

  let stats = $state(null);
  let corpora = $state([]);
  let heatmap = $state({ cells: [], genres: [], decades: [] });
  let loading = $state(true);

  function browseCorpus(corpus) {
    filters.update(f => ({ ...f, corpus, page: 1 }));
    switchTab('texts');
  }

  onMount(async () => {
    const [statsData, overviewData, heatmapData] = await Promise.all([
      getStats(), getOverview(), getHeatmap()
    ]);
    stats = statsData;
    corpora = overviewData.corpora;

    // Process heatmap (genre x century)
    const cells = heatmapData.cells;
    const genres = [...new Set(cells.map(c => c.genre))].sort();
    const centuries = [...new Set(cells.map(c => c.century))].sort((a, b) => a - b);
    const lookup = {};
    let maxVal = 0;
    for (const c of cells) {
      const key = `${c.genre}|${c.century}`;
      lookup[key] = c.n;
      if (c.n > maxVal) maxVal = c.n;
    }
    heatmap = { cells: lookup, genres, centuries, max: maxVal };
    loading = false;
  });

  function heatColor(val) {
    if (!val) return 'transparent';
    const intensity = Math.log(val + 1) / Math.log(heatmap.max + 1);
    const alpha = 0.1 + intensity * 0.9;
    return `rgba(37, 99, 235, ${alpha.toFixed(2)})`;
  }
</script>

{#if loading}
  <div class="loading">Loading...</div>
{:else}

<div class="stats-bar">
  <div class="stat-card">
    <div class="stat-value">{formatNumber(stats?.total_texts)}</div>
    <div class="stat-label">Texts</div>
  </div>
  <div class="stat-card">
    <div class="stat-value">{formatNumber(stats?.total_corpora)}</div>
    <div class="stat-label">Corpora</div>
  </div>
  <div class="stat-card">
    <div class="stat-value">{formatNumber(stats?.total_match_groups)}</div>
    <div class="stat-label">Match Groups</div>
  </div>
  <div class="stat-card">
    <div class="stat-value">{stats?.year_min}–{stats?.year_max}</div>
    <div class="stat-label">Year Range</div>
  </div>
</div>

<h2>Corpora</h2>
<div class="corpus-grid">
  {#each corpora as c}
    <button class="corpus-card" onclick={() => browseCorpus(c.corpus)}>
      <div class="corpus-name">{c.name || c.corpus}</div>
      {#if c.desc}<div class="corpus-desc">{c.desc}</div>{/if}
      <div class="corpus-count">{formatNumber(c.n_texts)} texts &middot; {yearRange(c.year_min, c.year_max)}</div>
      {#if c.genres && Object.keys(c.genres).length}
        <div class="corpus-genres">
          {#each Object.entries(c.genres).sort((a,b) => b[1] - a[1]).slice(0, 3) as [genre, n]}
            <span class="genre-tag">{genre} <small>{n}</small></span>
          {/each}
        </div>
      {/if}
      <div class="corpus-data">
        {#if c.n_freqs > 0}<span class="data-dot has" title="{c.n_freqs} with freqs">F</span>{/if}
      </div>
    </button>
  {/each}
</div>

{#if heatmap.genres.length}
<h2>Genre x Century</h2>
<div class="heatmap-wrap">
  <table class="heatmap">
    <thead>
      <tr>
        <th></th>
        {#each heatmap.centuries as c}
          <th class="century-label">{c}s</th>
        {/each}
      </tr>
    </thead>
    <tbody>
      {#each heatmap.genres as genre}
        <tr>
          <td class="genre-label">{genre}</td>
          {#each heatmap.centuries as c}
            {@const val = heatmap.cells[`${genre}|${c}`] || 0}
            <td
              class="heatmap-cell"
              style="background: {heatColor(val)}"
              title="{genre} {c}s: {formatNumber(val)}"
            >
              {#if val > 0}<span class="cell-val">{val > 999 ? Math.round(val/1000) + 'k' : val}</span>{/if}
            </td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
</div>
{/if}

{/if}

<style>
  .loading { padding: 40px; text-align: center; color: #64748b; }

  .stats-bar {
    display: flex;
    gap: 16px;
    margin-bottom: 24px;
  }
  .stat-card {
    background: white;
    border-radius: 8px;
    padding: 16px 24px;
    flex: 1;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
  }
  .stat-value { font-size: 24px; font-weight: 700; color: #1e293b; }
  .stat-label { font-size: 12px; color: #64748b; margin-top: 2px; }

  h2 {
    font-size: 15px;
    font-weight: 600;
    margin: 24px 0 12px;
    color: #334155;
  }

  .corpus-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    gap: 12px;
    margin-bottom: 24px;
  }
  .corpus-card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 14px;
    text-align: left;
    cursor: pointer;
    transition: border-color 0.15s, box-shadow 0.15s;
    display: block;
    width: 100%;
    font: inherit;
    color: inherit;
  }
  .corpus-card:hover { border-color: #3b82f6; box-shadow: 0 2px 8px rgba(59,130,246,0.1); }
  .corpus-name { font-weight: 600; font-size: 14px; margin-bottom: 2px; }
  .corpus-desc { font-size: 12px; color: #94a3b8; margin-bottom: 4px; line-height: 1.3; }
  .corpus-count { font-size: 12px; color: #64748b; }
  .corpus-genres { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 4px; }
  .genre-tag {
    background: #f1f5f9;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 11px;
    color: #475569;
  }
  .genre-tag small { color: #94a3b8; }
  .corpus-data { margin-top: 6px; }
  .data-dot {
    display: inline-block;
    font-size: 10px;
    font-weight: 600;
    padding: 1px 5px;
    border-radius: 3px;
    background: #dbeafe;
    color: #2563eb;
  }

  .heatmap-wrap { overflow-x: auto; margin-bottom: 24px; }
  .heatmap {
    border-collapse: collapse;
    font-size: 11px;
    width: 100%;
  }
  .heatmap th, .heatmap td {
    padding: 3px 4px;
    text-align: center;
    white-space: nowrap;
  }
  .century-label {
    font-weight: 500;
    color: #64748b;
  }
  .genre-label {
    text-align: right;
    padding-right: 8px;
    font-weight: 500;
    color: #475569;
  }
  .heatmap-cell {
    min-width: 32px;
    height: 24px;
    border: 1px solid #f1f5f9;
    border-radius: 2px;
  }
  .cell-val { color: white; font-size: 9px; font-weight: 600; text-shadow: 0 0 2px rgba(0,0,0,0.3); }
</style>

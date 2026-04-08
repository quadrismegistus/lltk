<script>
  import { onMount } from 'svelte';
  import { getGenreTimeline } from '../lib/api.js';
  import { switchTab, filters } from '../stores.js';
  import { formatNumber } from '../lib/utils.js';

  let data = $state([]);
  let genres = $state([]);
  let loading = $state(true);
  let mode = $state('absolute'); // 'absolute' or 'proportion'
  let hoveredCell = $state(null);

  // Curated color palette for genres
  const COLORS = {
    Fiction:    '#3b82f6',
    Poetry:     '#8b5cf6',
    Drama:      '#ec4899',
    Periodical: '#f59e0b',
    Essay:      '#10b981',
    Treatise:   '#6366f1',
    Letters:    '#14b8a6',
    Sermon:     '#f97316',
    Biography:  '#06b6d4',
    Nonfiction: '#84cc16',
    Legal:      '#78716c',
    Speech:     '#ef4444',
    Spoken:     '#d946ef',
    History:    '#0ea5e9',
    Criticism:  '#a855f7',
    Academic:   '#64748b',
    Almanac:    '#eab308',
    Reference:  '#94a3b8',
  };
  const DEFAULT_COLOR = '#cbd5e1';

  function color(genre) { return COLORS[genre] || DEFAULT_COLOR; }

  onMount(async () => {
    try {
      const res = await getGenreTimeline();
      data = res.data;
      genres = res.genres;
    } catch (e) {
      console.error(e);
    }
    loading = false;
  });

  function maxTotal() {
    let m = 0;
    for (const row of data) {
      let t = 0;
      for (const g of genres) t += row[g] || 0;
      if (t > m) m = t;
    }
    return m;
  }

  function rowTotal(row) {
    let t = 0;
    for (const g of genres) t += row[g] || 0;
    return t;
  }

  function barSegments(row) {
    const total = rowTotal(row);
    const max = mode === 'proportion' ? total : maxTotal();
    if (!max) return [];
    return genres
      .filter(g => (row[g] || 0) > 0)
      .map(g => ({
        genre: g,
        value: row[g],
        pct: ((row[g] || 0) / max * 100),
      }));
  }

  function drillDown(decade, genre) {
    filters.set({
      search: '', corpus: '', genre, year_min: decade, year_max: decade + 9,
      dedup: false, dedup_by: 'rank', has_freqs: false,
      sort_by: 'year', sort_dir: 'asc', page: 1, per_page: 100,
    });
    switchTab('texts');
  }
</script>

{#if loading}
  <div class="loading">Loading...</div>
{:else}

<div class="timeline-header">
  <div class="toggle">
    <button class:active={mode === 'absolute'} onclick={() => mode = 'absolute'}>Count</button>
    <button class:active={mode === 'proportion'} onclick={() => mode = 'proportion'}>Proportion</button>
  </div>
</div>

<div class="legend">
  {#each genres as g}
    <span class="legend-item">
      <span class="legend-dot" style="background: {color(g)}"></span>
      {g}
    </span>
  {/each}
</div>

<div class="timeline">
  {#each data as row}
    <div class="row">
      <span class="decade-label">{row.decade}</span>
      <div class="bar-track">
        {#each barSegments(row) as seg}
          <div
            class="bar-segment"
            style="width: {seg.pct}%; background: {color(seg.genre)}"
            title="{seg.genre} {row.decade}s: {formatNumber(seg.value)}"
            onclick={() => drillDown(row.decade, seg.genre)}
            onmouseenter={() => hoveredCell = { decade: row.decade, genre: seg.genre, value: seg.value }}
            onmouseleave={() => hoveredCell = null}
          ></div>
        {/each}
      </div>
      <span class="row-total">{formatNumber(rowTotal(row))}</span>
    </div>
  {/each}
</div>

{#if hoveredCell}
  <div class="tooltip">
    {hoveredCell.genre} &middot; {hoveredCell.decade}s &middot; {formatNumber(hoveredCell.value)} texts
  </div>
{/if}

{/if}

<style>
  .loading { padding: 20px; text-align: center; color: #94a3b8; }

  .timeline-header {
    display: flex;
    justify-content: flex-end;
    margin-bottom: 8px;
  }
  .toggle { display: flex; gap: 2px; }
  .toggle button {
    padding: 4px 12px;
    border: 1px solid #d1d5db;
    background: white;
    font-size: 12px;
    cursor: pointer;
    color: #64748b;
  }
  .toggle button:first-child { border-radius: 4px 0 0 4px; }
  .toggle button:last-child { border-radius: 0 4px 4px 0; }
  .toggle button.active { background: #1e293b; color: white; border-color: #1e293b; }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 14px;
    margin-bottom: 12px;
    font-size: 12px;
    color: #475569;
  }
  .legend-item { display: flex; align-items: center; gap: 4px; }
  .legend-dot { width: 10px; height: 10px; border-radius: 2px; flex-shrink: 0; }

  .timeline { display: flex; flex-direction: column; gap: 2px; }
  .row { display: flex; align-items: center; gap: 6px; height: 18px; }
  .decade-label {
    min-width: 40px;
    font-size: 11px;
    color: #64748b;
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .bar-track {
    flex: 1;
    display: flex;
    height: 14px;
    background: #f8fafc;
    border-radius: 2px;
    overflow: hidden;
  }
  .bar-segment {
    height: 100%;
    cursor: pointer;
    transition: opacity 0.1s;
    min-width: 1px;
  }
  .bar-segment:hover { opacity: 0.75; }
  .row-total {
    min-width: 45px;
    font-size: 11px;
    color: #94a3b8;
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .tooltip {
    position: fixed;
    bottom: 20px;
    left: 50%;
    transform: translateX(-50%);
    background: #1e293b;
    color: white;
    padding: 6px 14px;
    border-radius: 6px;
    font-size: 13px;
    pointer-events: none;
    z-index: 100;
  }
</style>

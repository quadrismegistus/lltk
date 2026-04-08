<script>
  import { onMount } from 'svelte';
  import { getNgram, getNgramExamples, getNgramCollocates, getGenres, getCorpora } from '../lib/api.js';
  import { formatNumber } from '../lib/utils.js';
  import { openDetail } from '../stores.js';

  let words = $state('');
  let genre = $state('');
  let corpus = $state('');
  let yearMin = $state(1500);
  let yearMax = $state(2020);
  let normalize = $state('per_million');

  let dedup = $state(false);
  let byCorpus = $state(false);

  let data = $state([]);
  let wordList = $state([]);
  let seriesList = $state([]);
  let loading = $state(false);
  let error = $state('');

  let genresList = $state([]);
  let corporaList = $state([]);

  // Examples panel
  let examplesWord = $state('');
  let examplesPeriod = $state(null);
  let examples = $state([]);
  let loadingExamples = $state(false);

  // Collocates panel
  let collocates = $state([]);
  let loadingCollocates = $state(false);

  const COLORS = [
    '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316',
    '#84cc16', '#14b8a6', '#6366f1', '#d946ef', '#0ea5e9', '#a855f7', '#64748b', '#eab308',
    '#78716c', '#e11d48', '#059669', '#7c3aed', '#0891b2', '#c026d3', '#ca8a04', '#dc2626',
  ];

  let debounceTimer;

  async function search() {
    if (!words.trim()) { data = []; wordList = []; seriesList = []; return; }
    loading = true;
    error = '';
    try {
      const res = await getNgram({
        words, genre, corpus, year_min: yearMin, year_max: yearMax, normalize,
        dedup: dedup ? true : undefined,
        by_corpus: byCorpus ? true : undefined,
      });
      if (res.error) { error = res.error; data = []; }
      else { data = res.data; wordList = res.words; seriesList = res.series || res.words; }
    } catch (e) {
      error = e.message;
    }
    loading = false;

    // Auto-load collocates for single word
    if (wordList.length === 1 && !byCorpus) loadCollocates(wordList[0]);
  }

  function onInput() {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(search, 500);
  }

  async function showExamples(word, period) {
    examplesWord = word;
    examplesPeriod = period;
    loadingExamples = true;
    try {
      const res = await getNgramExamples(word, {
        genre, corpus, year_min: period, year_max: period + 9, limit: 20,
      });
      examples = res.examples || [];
    } catch (e) {
      examples = [];
    }
    loadingExamples = false;
  }

  async function loadCollocates(word) {
    loadingCollocates = true;
    try {
      const res = await getNgramCollocates(word, { genre, corpus, year_min: yearMin, year_max: yearMax });
      collocates = res.collocates || [];
    } catch (e) {
      collocates = [];
    }
    loadingCollocates = false;
  }

  // SVG chart dimensions
  const W = 800, H = 300, PAD = { top: 20, right: 20, bottom: 30, left: 60 };
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;

  function chartPaths() {
    if (!data.length || !seriesList.length) return [];
    const periods = data.map(d => d.period);
    const minP = Math.min(...periods), maxP = Math.max(...periods);
    const rangeP = maxP - minP || 1;

    // Find max value across all series
    let maxVal = 0;
    for (const row of data) {
      for (const s of seriesList) {
        if (row[s] > maxVal) maxVal = row[s];
      }
    }
    if (!maxVal) maxVal = 1;

    return seriesList.map((s, i) => {
      const points = data
        .filter(d => d[s] != null)
        .map(d => {
          const x = PAD.left + ((d.period - minP) / rangeP) * plotW;
          const y = PAD.top + plotH - (d[s] / maxVal) * plotH;
          return { x, y, period: d.period, value: d[s], count: d[`${s}_count`], texts: d[`${s}_texts`] };
        });
      const pathD = points.map((p, j) => `${j === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
      // Display label: for "virtue:chadwyck" show "chadwyck", for "virtue" show "virtue"
      const label = s.includes(':') ? s : s;
      return { word: s, label, color: COLORS[i % COLORS.length], pathD, points };
    });
  }

  function yTicks() {
    if (!data.length || !seriesList.length) return [];
    let maxVal = 0;
    for (const row of data) {
      for (const s of seriesList) { if (row[s] > maxVal) maxVal = row[s]; }
    }
    if (!maxVal) return [];
    const ticks = [];
    const step = maxVal / 4;
    for (let i = 0; i <= 4; i++) {
      const val = step * i;
      const y = PAD.top + plotH - (val / maxVal) * plotH;
      ticks.push({ y, label: val < 10 ? val.toFixed(1) : Math.round(val) });
    }
    return ticks;
  }

  function xTicks() {
    if (!data.length) return [];
    const periods = data.map(d => d.period);
    const minP = Math.min(...periods), maxP = Math.max(...periods);
    const rangeP = maxP - minP || 1;
    // Show ~8 ticks
    const step = Math.max(10, Math.round(rangeP / 8 / 10) * 10);
    const ticks = [];
    for (let p = Math.ceil(minP / step) * step; p <= maxP; p += step) {
      const x = PAD.left + ((p - minP) / rangeP) * plotW;
      ticks.push({ x, label: p });
    }
    return ticks;
  }

  onMount(async () => {
    const [g, c] = await Promise.all([getGenres(), getCorpora()]);
    genresList = g.genres;
    corporaList = c.corpora;
  });
</script>

<div class="ngram-explorer">
  <div class="search-row">
    <input type="text" placeholder="Words (comma-separated): virtue, honor, sentiment"
      bind:value={words} oninput={onInput} class="word-input" />
    <select bind:value={genre} onchange={search}>
      <option value="">All genres</option>
      {#each genresList as g}<option value={g}>{g}</option>{/each}
    </select>
    <select bind:value={corpus} onchange={search}>
      <option value="">All corpora</option>
      {#each corporaList as c}<option value={c.corpus}>{c.corpus}</option>{/each}
    </select>
    <input type="number" bind:value={yearMin} onchange={search} style="width:70px" />
    <span>–</span>
    <input type="number" bind:value={yearMax} onchange={search} style="width:70px" />
    <select bind:value={normalize} onchange={search}>
      <option value="per_million">Per million</option>
      <option value="raw">Raw count</option>
    </select>
    <label class="checkbox-label">
      <input type="checkbox" bind:checked={dedup} onchange={search} />
      Dedup
    </label>
    <label class="checkbox-label">
      <input type="checkbox" bind:checked={byCorpus} onchange={search} />
      By corpus
    </label>
  </div>

  {#if error}
    <div class="error">{error}</div>
  {/if}

  {#if loading}
    <div class="loading">Searching...</div>
  {:else if data.length > 0}
    <div class="chart-area">
      <svg viewBox="0 0 {W} {H}" class="chart">
        <!-- Y axis -->
        {#each yTicks() as tick}
          <line x1={PAD.left} x2={W - PAD.right} y1={tick.y} y2={tick.y} stroke="#e2e8f0" />
          <text x={PAD.left - 8} y={tick.y + 4} text-anchor="end" class="tick-label">{tick.label}</text>
        {/each}
        <!-- X axis -->
        {#each xTicks() as tick}
          <text x={tick.x} y={H - 5} text-anchor="middle" class="tick-label">{tick.label}</text>
        {/each}
        <!-- Lines -->
        {#each chartPaths() as path}
          <path d={path.pathD} fill="none" stroke={path.color} stroke-width="2" />
          {#each path.points as pt}
            <circle cx={pt.x} cy={pt.y} r="4" fill={path.color} class="data-point"
              onclick={() => showExamples(path.word, pt.period)}
            >
              <title>{path.label} {pt.period}s: {pt.value?.toFixed?.(1)} ({formatNumber(pt.count)} in {pt.texts} texts)</title>
            </circle>
          {/each}
        {/each}
      </svg>
      <div class="legend">
        {#each chartPaths() as path}
          <span class="legend-item">
            <span class="legend-dot" style="background: {path.color}"></span>
            {path.label}
          </span>
        {/each}
      </div>
    </div>

    <div class="panels">
      {#if examplesWord}
        <div class="panel">
          <h3>"{examplesWord}" in {examplesPeriod}s</h3>
          {#if loadingExamples}
            <div class="loading-sm">Loading...</div>
          {:else}
            <table class="examples-table">
              <thead><tr><th>Title</th><th>Author</th><th>Year</th><th>Per M</th></tr></thead>
              <tbody>
                {#each examples as ex}
                  <tr class="clickable" onclick={() => openDetail(ex._id)}>
                    <td class="title-cell">{ex.title || '—'}</td>
                    <td>{ex.author || ''}</td>
                    <td class="num">{ex.year || ''}</td>
                    <td class="num">{ex.per_million?.toFixed?.(0) || ''}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          {/if}
        </div>
      {/if}

      {#if collocates.length > 0}
        <div class="panel">
          <h3>Collocates of "{wordList[0]}"</h3>
          <div class="collocate-list">
            {#each collocates.slice(0, 30) as col}
              <span class="collocate" onclick={() => { words = col.word; search(); }}
                title="{col.n_texts} texts"
              >{col.word} <small>{col.n_texts}</small></span>
            {/each}
          </div>
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .ngram-explorer { display: flex; flex-direction: column; gap: 16px; }

  .search-row {
    display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
    padding: 12px; background: white; border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  .word-input { flex: 1; min-width: 250px; }
  .search-row input[type="text"], .search-row input[type="number"], .search-row select {
    padding: 6px 10px; border: 1px solid #d1d5db; border-radius: 4px; font-size: 13px;
  }
  .checkbox-label {
    font-size: 13px; display: flex; align-items: center; gap: 4px; color: #475569;
  }

  .error {
    background: #fef2f2; color: #dc2626; padding: 10px 14px; border-radius: 6px; font-size: 13px;
  }
  .loading { padding: 40px; text-align: center; color: #94a3b8; }
  .loading-sm { padding: 12px; text-align: center; color: #94a3b8; font-size: 13px; }

  .chart-area {
    background: white; border-radius: 6px; padding: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  .chart { width: 100%; height: auto; }
  .tick-label { font-size: 11px; fill: #94a3b8; }
  .data-point { cursor: pointer; opacity: 0.8; }
  .data-point:hover { opacity: 1; r: 6; }

  .legend { display: flex; gap: 16px; margin-top: 8px; justify-content: center; }
  .legend-item { display: flex; align-items: center; gap: 4px; font-size: 13px; }
  .legend-dot { width: 12px; height: 3px; border-radius: 1px; }

  .panels { display: flex; gap: 16px; flex-wrap: wrap; }
  .panel {
    flex: 1; min-width: 300px; background: white; border-radius: 6px;
    padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  .panel h3 { font-size: 14px; font-weight: 600; margin-bottom: 10px; color: #334155; }

  .examples-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .examples-table th {
    text-align: left; padding: 4px 8px; font-size: 11px; font-weight: 600;
    color: #64748b; border-bottom: 1px solid #e2e8f0;
  }
  .examples-table td { padding: 4px 8px; border-bottom: 1px solid #f1f5f9; }
  .clickable { cursor: pointer; }
  .clickable:hover { background: #f8fafc; }
  .title-cell { font-weight: 500; max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }

  .collocate-list { display: flex; flex-wrap: wrap; gap: 6px; }
  .collocate {
    background: #f1f5f9; padding: 3px 8px; border-radius: 4px; font-size: 13px;
    cursor: pointer; transition: background 0.1s;
  }
  .collocate:hover { background: #e2e8f0; }
  .collocate small { color: #94a3b8; font-size: 11px; }
</style>

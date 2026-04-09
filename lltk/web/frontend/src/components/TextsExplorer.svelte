<script>
  import { onMount } from 'svelte';
  import { getTexts, getCorpora, getGenres } from '../lib/api.js';
  import { formatNumber, truncate } from '../lib/utils.js';
  import { filters, openDetail, replaceState } from '../stores.js';
  import Pagination from './Pagination.svelte';

  let texts = $state([]);
  let total = $state(0);
  let totalPages = $state(1);
  let loading = $state(false);
  let corporaList = $state([]);
  let genresList = $state([]);

  let currentFilters = $state({});
  filters.subscribe(v => { currentFilters = v; });

  let debounceTimer;

  async function loadTexts() {
    loading = true;
    try {
      const data = await getTexts(currentFilters);
      texts = data.texts;
      total = data.total;
      totalPages = data.total_pages;
    } catch (e) {
      console.error('Failed to load texts:', e);
    }
    loading = false;
  }

  function updateFilter(key, value) {
    filters.update(f => ({ ...f, [key]: value, page: key === 'page' ? value : 1 }));
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => { loadTexts(); replaceState(); }, key === 'search' ? 300 : 0);
  }

  function sort(col) {
    filters.update(f => ({
      ...f,
      sort_by: col,
      sort_dir: f.sort_by === col && f.sort_dir === 'asc' ? 'desc' : 'asc',
      page: 1,
    }));
    loadTexts();
  }

  function sortIcon(col) {
    if (currentFilters.sort_by !== col) return '';
    return currentFilters.sort_dir === 'asc' ? ' \u25B2' : ' \u25BC';
  }

  onMount(async () => {
    const [c, g] = await Promise.all([getCorpora(), getGenres()]);
    corporaList = c.corpora;
    genresList = g.genres;
    loadTexts();
  });
</script>

<div class="explorer">
  <div class="filter-bar">
    <input
      type="text"
      placeholder="Search title / author..."
      value={currentFilters.search}
      oninput={(e) => updateFilter('search', e.target.value)}
    />
    <select value={currentFilters.corpus} onchange={(e) => updateFilter('corpus', e.target.value)}>
      <option value="">All corpora</option>
      {#each corporaList as c}
        <option value={c.corpus}>{c.corpus} ({formatNumber(c.n_texts)})</option>
      {/each}
    </select>
    <select value={currentFilters.genre} onchange={(e) => updateFilter('genre', e.target.value)}>
      <option value="">All genres</option>
      {#each genresList as g}
        <option value={g}>{g}</option>
      {/each}
    </select>
    <input type="number" placeholder="Year min" value={currentFilters.year_min}
      onchange={(e) => updateFilter('year_min', e.target.value || null)} style="width:80px" />
    <input type="number" placeholder="Year max" value={currentFilters.year_max}
      onchange={(e) => updateFilter('year_max', e.target.value || null)} style="width:80px" />
    <label class="checkbox-label">
      <input type="checkbox" checked={currentFilters.dedup}
        onchange={(e) => { updateFilter('dedup', e.target.checked); loadTexts(); }} />
      Dedup
    </label>
    <span class="result-count">{formatNumber(total)} results</span>
    {#if currentFilters.corpus || currentFilters.genre || currentFilters.search}
      <a class="download-btn"
         href="/api/texts/download?{currentFilters.corpus ? 'corpus=' + currentFilters.corpus + '&' : ''}{currentFilters.genre ? 'genre=' + currentFilters.genre + '&' : ''}{currentFilters.year_min ? 'year_min=' + currentFilters.year_min + '&' : ''}{currentFilters.year_max ? 'year_max=' + currentFilters.year_max + '&' : ''}{currentFilters.dedup ? 'dedup=true&' : ''}{currentFilters.search ? 'search=' + encodeURIComponent(currentFilters.search) : ''}"
         download>
        CSV
      </a>
    {/if}
  </div>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th class="sortable" onclick={() => sort('corpus')}>Corpus{sortIcon('corpus')}</th>
          <th class="sortable" onclick={() => sort('title')}>Title{sortIcon('title')}</th>
          <th class="sortable" onclick={() => sort('author')}>Author{sortIcon('author')}</th>
          <th class="sortable" onclick={() => sort('year')}>Year{sortIcon('year')}</th>
          <th class="sortable" onclick={() => sort('genre')}>Genre{sortIcon('genre')}</th>
          <th class="sortable" onclick={() => sort('n_words')}>Words{sortIcon('n_words')}</th>
        </tr>
      </thead>
      <tbody>
        {#if loading}
          <tr><td colspan="6" class="loading-cell">Loading...</td></tr>
        {:else if texts.length === 0}
          <tr><td colspan="6" class="loading-cell">No results</td></tr>
        {:else}
          {#each texts as t}
            <tr class="text-row" onclick={() => openDetail(t._id)}>
              <td class="corpus-cell">{t.corpus}</td>
              <td class="title-cell" title={t.title}>{truncate(t.title, 60)}</td>
              <td>{truncate(t.author, 30)}</td>
              <td class="num-cell">{t.year || ''}</td>
              <td>{t.genre || ''}</td>
              <td class="num-cell">{t.n_words ? formatNumber(t.n_words) : ''}</td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>

  <Pagination
    page={currentFilters.page}
    {totalPages}
    onPageChange={(p) => { updateFilter('page', p); loadTexts(); }}
  />
</div>

<style>
  .explorer { display: flex; flex-direction: column; height: 100%; }

  .filter-bar {
    display: flex;
    gap: 8px;
    align-items: center;
    flex-wrap: wrap;
    margin-bottom: 12px;
    padding: 12px;
    background: white;
    border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  .filter-bar input[type="text"],
  .filter-bar input[type="number"],
  .filter-bar select {
    padding: 6px 10px;
    border: 1px solid #d1d5db;
    border-radius: 4px;
    font-size: 13px;
    background: white;
  }
  .filter-bar input[type="text"] { flex: 1; min-width: 180px; }
  .checkbox-label {
    font-size: 13px;
    display: flex;
    align-items: center;
    gap: 4px;
    color: #475569;
  }
  .download-btn {
    padding: 4px 10px;
    border: 1px solid #d1d5db;
    border-radius: 4px;
    background: white;
    font-size: 12px;
    cursor: pointer;
    text-decoration: none;
    color: #374151;
  }
  .download-btn:hover { background: #f3f4f6; }
  .result-count {
    font-size: 12px;
    color: #64748b;
    margin-left: auto;
  }

  .table-wrap {
    flex: 1;
    overflow: auto;
    background: white;
    border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  table { width: 100%; border-collapse: collapse; }
  th {
    position: sticky;
    top: 0;
    background: #f8fafc;
    padding: 8px 12px;
    text-align: left;
    font-size: 12px;
    font-weight: 600;
    color: #64748b;
    border-bottom: 1px solid #e2e8f0;
    white-space: nowrap;
  }
  .sortable { cursor: pointer; user-select: none; }
  .sortable:hover { color: #1e293b; }
  td {
    padding: 6px 12px;
    font-size: 13px;
    border-bottom: 1px solid #f1f5f9;
    max-width: 0;
  }
  .text-row { cursor: pointer; transition: background 0.1s; }
  .text-row:hover { background: #f8fafc; }
  .corpus-cell { color: #64748b; font-size: 12px; }
  .title-cell { font-weight: 500; }
  .num-cell { text-align: right; font-variant-numeric: tabular-nums; }
  .loading-cell { text-align: center; padding: 40px; color: #94a3b8; }
</style>

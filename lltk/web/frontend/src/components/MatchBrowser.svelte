<script>
  import { onMount } from 'svelte';
  import { getMatches, getMatchStats } from '../lib/api.js';
  import { formatNumber } from '../lib/utils.js';
  import { openDetail } from '../stores.js';
  import Pagination from './Pagination.svelte';

  let search = $state('');
  let groups = $state([]);
  let totalGroups = $state(0);
  let page = $state(1);
  let perPage = 50;
  let loading = $state(false);
  let stats = $state(null);

  let debounceTimer;

  async function loadMatches() {
    if (!search.trim()) { groups = []; totalGroups = 0; return; }
    loading = true;
    try {
      const data = await getMatches({ search, page, per_page: perPage });
      groups = data.groups;
      totalGroups = data.total_groups;
    } catch (e) {
      console.error(e);
    }
    loading = false;
  }

  function onSearch(e) {
    search = e.target.value;
    page = 1;
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(loadMatches, 400);
  }

  onMount(async () => {
    try { stats = await getMatchStats(); } catch (e) {}
  });
</script>

<div class="match-browser">
  {#if stats}
    <div class="stats-row">
      <span>{formatNumber(stats.total_matches)} match pairs</span>
      <span>{formatNumber(stats.total_groups)} groups</span>
      {#each stats.by_type as t}
        <span class="type-badge">{t.match_type}: {formatNumber(t.n)}</span>
      {/each}
    </div>
  {/if}

  <div class="search-bar">
    <input
      type="text"
      placeholder="Search by title (e.g. Pamela, Robinson Crusoe)..."
      value={search}
      oninput={onSearch}
    />
  </div>

  {#if loading}
    <div class="loading">Searching...</div>
  {:else if groups.length === 0 && search}
    <div class="loading">No match groups found</div>
  {:else}
    <div class="groups">
      {#each groups as group}
        <div class="group-card">
          <div class="group-header">
            Group #{group.group_id} &middot; {group.members.length} texts
          </div>
          <div class="group-members">
            {#each group.members as m}
              <button class="member" onclick={() => openDetail(m._id)}>
                <span class="member-rank">#{m.rank}</span>
                <span class="member-corpus">{m.corpus}</span>
                <span class="member-title">{m.title || '—'}</span>
                <span class="member-author">{m.author || ''}</span>
                <span class="member-year">{m.year || ''}</span>
              </button>
            {/each}
          </div>
        </div>
      {/each}
    </div>

    {#if totalGroups > perPage}
      <Pagination {page} totalPages={Math.ceil(totalGroups / perPage)}
        onPageChange={(p) => { page = p; loadMatches(); }} />
    {/if}
  {/if}
</div>

<style>
  .match-browser { display: flex; flex-direction: column; gap: 12px; }

  .stats-row {
    display: flex;
    gap: 16px;
    font-size: 13px;
    color: #64748b;
    padding: 8px 12px;
    background: white;
    border-radius: 6px;
    flex-wrap: wrap;
  }
  .type-badge {
    background: #f1f5f9;
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 12px;
  }

  .search-bar input {
    width: 100%;
    padding: 10px 14px;
    border: 1px solid #d1d5db;
    border-radius: 6px;
    font-size: 14px;
    background: white;
  }

  .loading { padding: 40px; text-align: center; color: #94a3b8; }

  .groups { display: flex; flex-direction: column; gap: 12px; }
  .group-card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    overflow: hidden;
  }
  .group-header {
    padding: 8px 14px;
    background: #f8fafc;
    font-size: 12px;
    font-weight: 600;
    color: #64748b;
    border-bottom: 1px solid #e2e8f0;
  }
  .group-members { display: flex; flex-direction: column; }
  .member {
    display: flex;
    gap: 10px;
    padding: 6px 14px;
    font-size: 13px;
    border-bottom: 1px solid #f1f5f9;
    cursor: pointer;
    align-items: baseline;
    border: none;
    background: none;
    text-align: left;
    font: inherit;
    color: inherit;
    width: 100%;
  }
  .member:hover { background: #f8fafc; }
  .member:last-child { border-bottom: none; }
  .member-rank { color: #94a3b8; font-size: 11px; min-width: 24px; }
  .member-corpus { color: #64748b; min-width: 100px; font-size: 12px; }
  .member-title { flex: 1; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .member-author { color: #64748b; max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .member-year { color: #94a3b8; min-width: 40px; text-align: right; }
</style>

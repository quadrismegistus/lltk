<script>
  import { selectedTextId, openDetail } from '../stores.js';
  import { getText } from '../lib/api.js';
  import { formatNumber } from '../lib/utils.js';

  let data = $state(null);
  let loading = $state(false);

  const SKIP_KEYS = new Set(['_id', 'meta', 'title_norm', 'author_norm']);

  selectedTextId.subscribe(async (id) => {
    if (!id) { data = null; return; }
    loading = true;
    try {
      data = await getText(id);
    } catch (e) {
      console.error('Failed to load text:', e);
      data = null;
    }
    loading = false;
  });
</script>

{#if loading}
  <div class="loading">Loading...</div>
{:else if data}
  <h3>{data.metadata.title || 'Untitled'}</h3>
  {#if data.metadata.author}
    <p class="author">{data.metadata.author}</p>
  {/if}

  <div class="meta-grid">
    {#each Object.entries(data.metadata).filter(([k]) => !SKIP_KEYS.has(k)) as [key, val]}
      {#if val != null && val !== ''}
        <div class="meta-key">{key}</div>
        <div class="meta-val">{typeof val === 'number' ? formatNumber(val) : String(val)}</div>
      {/if}
    {/each}
  </div>

  {#if data.match_group.length > 1}
    <h4>Match Group ({data.match_group.length} texts)</h4>
    <div class="match-list">
      {#each data.match_group as m}
        <button
          class="match-item"
          class:current={m._id === $selectedTextId}
          onclick={() => openDetail(m._id)}
        >
          <span class="match-corpus">{m.corpus}</span>
          <span class="match-title">{m.title || '—'}</span>
          <span class="match-year">{m.year || ''}</span>
          <span class="match-rank">#{m.rank}</span>
        </button>
      {/each}
    </div>
  {/if}

  {#if data.txt_preview}
    <h4>Text Preview</h4>
    <div class="txt-preview">{data.txt_preview.slice(0, 3000)}</div>
  {/if}
{/if}

<style>
  .loading { padding: 20px; color: #94a3b8; text-align: center; }
  h3 { font-size: 16px; font-weight: 600; margin-bottom: 4px; padding-right: 30px; }
  .author { color: #64748b; margin-bottom: 16px; font-size: 14px; }

  h4 {
    font-size: 13px;
    font-weight: 600;
    color: #475569;
    margin: 20px 0 8px;
    padding-top: 16px;
    border-top: 1px solid #e2e8f0;
  }

  .meta-grid {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 2px 12px;
    font-size: 13px;
  }
  .meta-key { color: #64748b; font-weight: 500; white-space: nowrap; }
  .meta-val { color: #1e293b; word-break: break-word; }

  .match-list { display: flex; flex-direction: column; gap: 4px; }
  .match-item {
    display: flex;
    gap: 8px;
    align-items: baseline;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
    border: 1px solid #e2e8f0;
    background: white;
    text-align: left;
    font: inherit;
    color: inherit;
  }
  .match-item:hover { background: #f8fafc; }
  .match-item.current { background: #eff6ff; border-color: #93c5fd; }
  .match-corpus { color: #64748b; min-width: 80px; }
  .match-title { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .match-year { color: #94a3b8; }
  .match-rank { color: #94a3b8; font-size: 11px; }

  .txt-preview {
    font-family: 'Georgia', 'Times New Roman', serif;
    font-size: 13px;
    line-height: 1.6;
    color: #374151;
    max-height: 400px;
    overflow-y: auto;
    white-space: pre-wrap;
    background: #fafaf9;
    padding: 12px;
    border-radius: 6px;
    border: 1px solid #e7e5e4;
  }
</style>

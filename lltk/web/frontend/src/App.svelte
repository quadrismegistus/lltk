<script>
  import { activeTab, detailOpen, closeDetail } from './stores.js';
  import Dashboard from './components/Dashboard.svelte';
  import TextsExplorer from './components/TextsExplorer.svelte';
  import TextDetail from './components/TextDetail.svelte';
  import MatchBrowser from './components/MatchBrowser.svelte';
  import CorpusList from './components/CorpusList.svelte';
  import CorpusOverlap from './components/CorpusOverlap.svelte';
  import { getStats } from './lib/api.js';
  import { formatNumber } from './lib/utils.js';
  import { onMount } from 'svelte';

  let stats = $state(null);

  const tabs = [
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'texts', label: 'Texts' },
    { id: 'matches', label: 'Matches' },
    { id: 'corpora', label: 'Corpora' },
    { id: 'overlap', label: 'Overlap' },
  ];

  onMount(async () => {
    stats = await getStats();
  });
</script>

<div class="app" class:detail-open={$detailOpen}>
  <header>
    <div class="header-left">
      <h1>LLTK Explorer</h1>
      {#if stats}
        <span class="header-stats">
          {formatNumber(stats.total_texts)} texts &middot;
          {formatNumber(stats.total_corpora)} corpora &middot;
          {stats.year_min}–{stats.year_max}
        </span>
      {/if}
    </div>
    <nav>
      {#each tabs as tab}
        <button
          class="tab"
          class:active={$activeTab === tab.id}
          onclick={() => activeTab.set(tab.id)}
        >{tab.label}</button>
      {/each}
    </nav>
  </header>

  <main>
    <div class="content">
      {#if $activeTab === 'dashboard'}
        <Dashboard />
      {:else if $activeTab === 'texts'}
        <TextsExplorer />
      {:else if $activeTab === 'matches'}
        <MatchBrowser />
      {:else if $activeTab === 'corpora'}
        <CorpusList />
      {:else if $activeTab === 'overlap'}
        <CorpusOverlap />
      {/if}
    </div>

    {#if $detailOpen}
      <aside class="detail-panel">
        <button class="close-btn" onclick={closeDetail}>&times;</button>
        <TextDetail />
      </aside>
    {/if}
  </main>
</div>

<style>
  :global(*) { box-sizing: border-box; margin: 0; padding: 0; }
  :global(body) {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #f5f5f5;
    color: #1a1a1a;
    font-size: 14px;
  }
  :global(a) { color: #2563eb; text-decoration: none; }
  :global(a:hover) { text-decoration: underline; }

  .app { display: flex; flex-direction: column; height: 100vh; }

  header {
    background: #1e293b;
    color: white;
    padding: 0 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 48px;
    flex-shrink: 0;
  }
  .header-left { display: flex; align-items: center; gap: 16px; }
  h1 { font-size: 16px; font-weight: 600; }
  .header-stats { font-size: 12px; color: #94a3b8; }

  nav { display: flex; gap: 2px; }
  .tab {
    background: none;
    border: none;
    color: #94a3b8;
    padding: 14px 16px;
    cursor: pointer;
    font-size: 13px;
    font-weight: 500;
    border-bottom: 2px solid transparent;
    transition: color 0.15s, border-color 0.15s;
  }
  .tab:hover { color: #e2e8f0; }
  .tab.active { color: white; border-bottom-color: #3b82f6; }

  main {
    flex: 1;
    display: flex;
    overflow: hidden;
  }
  .content {
    flex: 1;
    overflow-y: auto;
    padding: 20px;
  }

  .detail-panel {
    width: 440px;
    background: white;
    border-left: 1px solid #e2e8f0;
    overflow-y: auto;
    padding: 20px;
    position: relative;
    flex-shrink: 0;
  }
  .close-btn {
    position: absolute;
    top: 8px;
    right: 12px;
    background: none;
    border: none;
    font-size: 24px;
    cursor: pointer;
    color: #64748b;
    line-height: 1;
  }
  .close-btn:hover { color: #1e293b; }
</style>

<script>
  let { page = 1, totalPages = 1, onPageChange } = $props();
</script>

{#if totalPages > 1}
<div class="pagination">
  <button disabled={page <= 1} onclick={() => onPageChange(page - 1)}>&laquo; Prev</button>
  <span class="page-info">Page {page} of {totalPages}</span>
  <button disabled={page >= totalPages} onclick={() => onPageChange(page + 1)}>Next &raquo;</button>
</div>
{/if}

<style>
  .pagination {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    padding: 12px;
  }
  button {
    padding: 6px 14px;
    border: 1px solid #d1d5db;
    border-radius: 4px;
    background: white;
    cursor: pointer;
    font-size: 13px;
    color: #374151;
  }
  button:hover:not(:disabled) { background: #f3f4f6; }
  button:disabled { opacity: 0.4; cursor: not-allowed; }
  .page-info { font-size: 13px; color: #64748b; }
</style>

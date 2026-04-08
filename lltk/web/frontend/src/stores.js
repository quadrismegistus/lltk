import { writable, get } from 'svelte/store';

// ── Defaults ───────────────────────────────────────────────────────────

const DEFAULT_FILTERS = {
  search: '',
  corpus: '',
  genre: '',
  year_min: null,
  year_max: null,
  dedup: false,
  dedup_by: 'rank',
  has_freqs: false,
  sort_by: 'year',
  sort_dir: 'asc',
  page: 1,
  per_page: 100,
};

// ── Stores ─────────────────────────────────────────────────────────────

export const activeTab = writable('dashboard');
export const selectedTextId = writable(null);
export const detailOpen = writable(false);
export const filters = writable({ ...DEFAULT_FILTERS });

// Match browser has its own search
export const matchSearch = writable('');

// ── URL sync ───────────────────────────────────────────────────────────

let suppressHashUpdate = false;

/** Serialize current state to URL hash. */
function stateToHash() {
  const tab = get(activeTab);
  const params = new URLSearchParams();

  if (tab === 'texts') {
    const f = get(filters);
    if (f.search) params.set('search', f.search);
    if (f.corpus) params.set('corpus', f.corpus);
    if (f.genre) params.set('genre', f.genre);
    if (f.year_min) params.set('year_min', f.year_min);
    if (f.year_max) params.set('year_max', f.year_max);
    if (f.dedup) params.set('dedup', '1');
    if (f.sort_by !== 'year') params.set('sort_by', f.sort_by);
    if (f.sort_dir !== 'asc') params.set('sort_dir', f.sort_dir);
    if (f.page > 1) params.set('page', f.page);
  } else if (tab === 'matches') {
    const ms = get(matchSearch);
    if (ms) params.set('search', ms);
  }

  const detail = get(selectedTextId);
  if (detail) params.set('detail', detail);

  const qs = params.toString();
  return tab + (qs ? '?' + qs : '');
}

/** Parse URL hash into state. */
function hashToState(hash) {
  const raw = hash.replace(/^#\/?/, '');
  const [tab, qs] = raw.split('?', 2);
  const params = new URLSearchParams(qs || '');

  const validTabs = ['dashboard', 'texts', 'matches', 'corpora', 'overlap'];
  const resolvedTab = validTabs.includes(tab) ? tab : 'dashboard';

  suppressHashUpdate = true;

  activeTab.set(resolvedTab);

  if (resolvedTab === 'texts') {
    filters.set({
      ...DEFAULT_FILTERS,
      search: params.get('search') || '',
      corpus: params.get('corpus') || '',
      genre: params.get('genre') || '',
      year_min: params.get('year_min') ? Number(params.get('year_min')) : null,
      year_max: params.get('year_max') ? Number(params.get('year_max')) : null,
      dedup: params.get('dedup') === '1',
      sort_by: params.get('sort_by') || 'year',
      sort_dir: params.get('sort_dir') || 'asc',
      page: params.get('page') ? Number(params.get('page')) : 1,
    });
  } else if (resolvedTab === 'matches') {
    matchSearch.set(params.get('search') || '');
  }

  const detail = params.get('detail');
  if (detail) {
    selectedTextId.set(detail);
    detailOpen.set(true);
  } else {
    selectedTextId.set(null);
    detailOpen.set(false);
  }

  suppressHashUpdate = false;
}

/** Push current state to URL (creates history entry). */
export function pushState() {
  if (suppressHashUpdate) return;
  const newHash = '#' + stateToHash();
  if (window.location.hash !== newHash) {
    history.pushState(null, '', newHash);
  }
}

/** Replace current URL without creating history entry. */
export function replaceState() {
  if (suppressHashUpdate) return;
  const newHash = '#' + stateToHash();
  if (window.location.hash !== newHash) {
    history.replaceState(null, '', newHash);
  }
}

// Listen for back/forward
if (typeof window !== 'undefined') {
  window.addEventListener('popstate', () => {
    hashToState(window.location.hash);
  });

  // Initialize from URL on load
  if (window.location.hash) {
    hashToState(window.location.hash);
  }
}

// ── Actions ────────────────────────────────────────────────────────────

export function openDetail(id) {
  selectedTextId.set(id);
  detailOpen.set(true);
  pushState();
}

export function closeDetail() {
  detailOpen.set(false);
  selectedTextId.set(null);
  pushState();
}

export function switchTab(tab) {
  activeTab.set(tab);
  // Clear detail when switching tabs
  detailOpen.set(false);
  selectedTextId.set(null);
  pushState();
}

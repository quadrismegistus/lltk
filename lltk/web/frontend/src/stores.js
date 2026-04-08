import { writable } from 'svelte/store';

export const activeTab = writable('dashboard');
export const selectedTextId = writable(null);
export const detailOpen = writable(false);

// Text browser filters
export const filters = writable({
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
});

export function openDetail(id) {
  selectedTextId.set(id);
  detailOpen.set(true);
}

export function closeDetail() {
  detailOpen.set(false);
  selectedTextId.set(null);
}

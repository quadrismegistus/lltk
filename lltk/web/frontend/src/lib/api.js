const BASE = '';

async function fetchJson(url) {
  const res = await fetch(BASE + url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

export function getStats() {
  return fetchJson('/api/stats');
}

export function getOverview() {
  return fetchJson('/api/overview');
}

export function getHeatmap() {
  return fetchJson('/api/heatmap');
}

export function getGenreTimeline(corpus = '') {
  const qs = corpus ? `?corpus=${encodeURIComponent(corpus)}` : '';
  return fetchJson(`/api/genre-timeline${qs}`);
}

export function getTexts(params = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== '' && v !== null && v !== undefined) qs.set(k, v);
  }
  return fetchJson(`/api/texts?${qs}`);
}

export function getText(id) {
  return fetchJson(`/api/text/${id}`);
}

export function getCorpora() {
  return fetchJson('/api/corpora');
}

export function getCorpus(id) {
  return fetchJson(`/api/corpus/${id}`);
}

export function getGenres() {
  return fetchJson('/api/genres');
}

export function getMatches(params = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== '' && v !== null && v !== undefined) qs.set(k, v);
  }
  return fetchJson(`/api/matches?${qs}`);
}

export function getMatchStats() {
  return fetchJson('/api/match-stats');
}

export function getCorpusOverlap() {
  return fetchJson('/api/corpus-overlap');
}

export function getNgram(params = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== '' && v !== null && v !== undefined) qs.set(k, v);
  }
  return fetchJson(`/api/ngram?${qs}`);
}

export function getNgramExamples(word, params = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== '' && v !== null && v !== undefined) qs.set(k, v);
  }
  return fetchJson(`/api/ngram/${encodeURIComponent(word)}/examples?${qs}`);
}

export function getNgramCollocates(word, params = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== '' && v !== null && v !== undefined) qs.set(k, v);
  }
  return fetchJson(`/api/ngram/${encodeURIComponent(word)}/collocates?${qs}`);
}

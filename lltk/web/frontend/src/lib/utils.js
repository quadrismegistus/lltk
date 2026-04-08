export function formatNumber(n) {
  if (n == null) return '—';
  return n.toLocaleString();
}

export function truncate(s, len = 80) {
  if (!s) return '';
  return s.length > len ? s.slice(0, len) + '...' : s;
}

export function yearRange(min, max) {
  if (!min && !max) return '—';
  if (min === max) return String(min);
  return `${min || '?'}–${max || '?'}`;
}

/** Number formatting with comma separators (e.g. 1234567 → "1,234,567.00") */
export function formatNumber(num: number | undefined | null, decimals = 2): string {
  if (num == null) return '-';
  return num.toFixed(decimals).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

/** Compact volume formatting (e.g. 1500000 → "1.50M") */
export function formatVolume(vol: number | undefined | null): string {
  if (vol == null) return '-';
  if (vol >= 1e9) return `${(vol / 1e9).toFixed(2)}B`;
  if (vol >= 1e6) return `${(vol / 1e6).toFixed(2)}M`;
  if (vol >= 1e3) return `${(vol / 1e3).toFixed(2)}K`;
  return vol.toString();
}

/** Large dollar amount formatting (e.g. 1500000000 → "$1.50B") */
export function formatLargeNumber(num: number): string {
  if (num >= 1e12) return `$${(num / 1e12).toFixed(2)}T`;
  if (num >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
  if (num >= 1e6) return `$${(num / 1e6).toFixed(2)}M`;
  if (num >= 1e3) return `$${(num / 1e3).toFixed(2)}K`;
  return `$${num.toFixed(2)}`;
}

/** Format ISO date string to US Eastern time (MM/DD/YYYY, HH:MM:SS 24h ET) */
export function formatTimestampET(isoDate: string): string {
  if (!isoDate) return '-';
  const date = new Date(isoDate);
  if (isNaN(date.getTime())) return isoDate || '-';
  const formatted = date.toLocaleString('en-US', {
    timeZone: 'America/New_York',
    month: '2-digit',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  return `${formatted} ET`;
}

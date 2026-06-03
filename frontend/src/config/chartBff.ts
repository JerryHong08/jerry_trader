export function getChartBffBaseUrl(): string {
  const defaultHost =
    typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  const url =
    typeof import.meta !== 'undefined' && import.meta.env?.VITE_CHART_BFF_URL
      ? (import.meta.env.VITE_CHART_BFF_URL as string)
      : `http://${defaultHost}:5002`;
  return url || `http://${defaultHost}:5002`;
}

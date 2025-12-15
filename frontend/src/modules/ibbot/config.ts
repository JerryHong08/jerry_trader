export type IbbotConfig = {
  restBaseUrl: string;
  wsUrl: string;
};

function deriveWsUrl(restBaseUrl: string): string {
  // Supports http://host:port or https://host:port
  const url = new URL(restBaseUrl);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  // Backend WS endpoint
  url.pathname = '/ws';
  url.search = '';
  url.hash = '';
  return url.toString();
}

export function getIbbotConfig(): IbbotConfig {
  const defaultHost = typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  const restBaseUrl =
    (import.meta.env.VITE_IBBOT_REST_BASE_URL as string | undefined) ??
    `http://${defaultHost}:8888`;

  const wsUrl =
    (import.meta.env.VITE_IBBOT_WS_URL as string | undefined) ??
    deriveWsUrl(restBaseUrl);

  return {
    restBaseUrl,
    wsUrl,
  };
}

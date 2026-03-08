/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_BFF_URL?: string;
  readonly VITE_CHART_BFF_URL?: string;
  readonly VITE_TICKDATA_URL?: string;
  readonly VITE_IBBOT_URL?: string;
  readonly VITE_PRIVACY_PIN?: string;
  // Add other env variables here as needed
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

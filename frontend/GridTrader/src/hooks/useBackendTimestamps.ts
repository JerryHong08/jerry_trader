import { useState, useEffect } from 'react';

// Data domains with their update timestamps
export type DataDomain = 'market-data' | 'stock-detail' | 'orders' | 'portfolio';

// Singleton store for timestamps across all components
class TimestampStore {
  private timestamps: Map<DataDomain, Date> = new Map();
  private listeners: Set<() => void> = new Set();
  private useMockUpdates: boolean = false; // Set to false when backend is connected

  constructor() {
    // Initialize with current time
    this.timestamps.set('market-data', new Date());
    this.timestamps.set('stock-detail', new Date());
    this.timestamps.set('orders', new Date());
    this.timestamps.set('portfolio', new Date());

    // Only use mock updates if backend is not connected
    if (this.useMockUpdates) {
      this.startMockUpdates();
    }
  }

  private startMockUpdates() {
    // Market data updates every 5 seconds (mock)
    setInterval(() => {
      this.updateTimestamp('market-data');
    }, 5000);

    // Stock detail updates every 3 seconds (mock)
    setInterval(() => {
      this.updateTimestamp('stock-detail');
    }, 3000);

    // Orders update every 2 seconds (mock)
    setInterval(() => {
      this.updateTimestamp('orders');
    }, 2000);

    // Portfolio updates every 4 seconds (mock)
    setInterval(() => {
      this.updateTimestamp('portfolio');
    }, 4000);
  }

  updateTimestamp(domain: DataDomain, date?: Date) {
    this.timestamps.set(domain, date || new Date());
    this.notifyListeners();
  }

  getTimestamp(domain: DataDomain): Date | undefined {
    return this.timestamps.get(domain);
  }

  subscribe(listener: () => void) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private notifyListeners() {
    this.listeners.forEach(listener => listener());
  }

  // Enable mock updates (useful for development without backend)
  enableMockUpdates() {
    if (!this.useMockUpdates) {
      this.useMockUpdates = true;
      this.startMockUpdates();
    }
  }
}

export const timestampStore = new TimestampStore();

// Format timestamp as HH:MM:SS in US/New_York timezone
export function formatTimestamp(date: Date): string {
  // Format in America/New_York timezone
  const options: Intl.DateTimeFormatOptions = {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    timeZone: 'America/New_York',
  };
  return date.toLocaleTimeString('en-US', options);
}

// Parse ISO timestamp string to Date
export function parseTimestamp(isoString: string | null): Date | null {
  if (!isoString) return null;
  try {
    return new Date(isoString);
  } catch {
    return null;
  }
}

// Hook to use backend timestamp for a specific data domain
export function useBackendTimestamp(domain: DataDomain): string {
  const [timestamp, setTimestamp] = useState<string>(() => {
    const date = timestampStore.getTimestamp(domain);
    return date ? formatTimestamp(date) : '--:--:--';
  });

  useEffect(() => {
    const unsubscribe = timestampStore.subscribe(() => {
      const date = timestampStore.getTimestamp(domain);
      if (date) {
        setTimestamp(formatTimestamp(date));
      }
    });

    return unsubscribe;
  }, [domain]);

  return timestamp;
}

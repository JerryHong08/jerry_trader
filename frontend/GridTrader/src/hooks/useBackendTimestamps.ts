import { useState, useEffect } from 'react';

// Data domains with their update timestamps
export type DataDomain = 'market-data' | 'stock-detail' | 'orders' | 'portfolio';

// Singleton store for timestamps across all components
class TimestampStore {
  private timestamps: Map<DataDomain, Date> = new Map();
  private listeners: Set<() => void> = new Set();

  constructor() {
    // Initialize with current time
    this.timestamps.set('market-data', new Date());
    this.timestamps.set('stock-detail', new Date());
    this.timestamps.set('orders', new Date());
    this.timestamps.set('portfolio', new Date());

    // Simulate backend updates at different intervals
    this.startUpdates();
  }

  private startUpdates() {
    // Market data updates every 5 seconds
    setInterval(() => {
      this.updateTimestamp('market-data');
    }, 5000);

    // Stock detail updates every 3 seconds
    setInterval(() => {
      this.updateTimestamp('stock-detail');
    }, 3000);

    // Orders update every 2 seconds
    setInterval(() => {
      this.updateTimestamp('orders');
    }, 2000);

    // Portfolio updates every 4 seconds
    setInterval(() => {
      this.updateTimestamp('portfolio');
    }, 4000);
  }

  updateTimestamp(domain: DataDomain) {
    this.timestamps.set(domain, new Date());
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
}

const timestampStore = new TimestampStore();

// Format timestamp as HH:MM:SS
export function formatTimestamp(date: Date): string {
  const hours = date.getHours().toString().padStart(2, '0');
  const minutes = date.getMinutes().toString().padStart(2, '0');
  const seconds = date.getSeconds().toString().padStart(2, '0');
  return `${hours}:${minutes}:${seconds}`;
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

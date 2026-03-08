import React, { useState, useEffect, useRef, useCallback } from "react";

interface ClockState {
  mode: "live" | "replay";
  now_ms: number;
  speed: number;
  paused: boolean;
  data_start_ts_ns: number | null;
  session_id: string;
}

/**
 * Get the Chart BFF base URL (same logic as chartDataStore).
 */
function getChartBffBaseUrl(): string {
  const defaultHost =
    typeof window !== "undefined" ? window.location.hostname : "localhost";
  const url =
    typeof import.meta !== "undefined" && import.meta.env?.VITE_CHART_BFF_URL
      ? (import.meta.env.VITE_CHART_BFF_URL as string)
      : `http://${defaultHost}:5002`;
  return url || `http://${defaultHost}:5002`;
}

const POLL_INTERVAL_MS = 1_000; // poll /api/clock every 1s

export function TimelineClock() {
  const [displayTime, setDisplayTime] = useState<Date>(new Date());
  const [clockState, setClockState] = useState<ClockState | null>(null);
  const [connected, setConnected] = useState(false);

  // For client-side interpolation between polls
  const anchorRef = useRef<{
    serverMs: number;
    clientMs: number;
    speed: number;
    paused: boolean;
  } | null>(null);

  // Poll /api/clock
  const fetchClock = useCallback(async () => {
    try {
      const res = await fetch(`${getChartBffBaseUrl()}/api/clock`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: ClockState = await res.json();
      setClockState(data);
      setConnected(true);

      // Anchor for interpolation
      anchorRef.current = {
        serverMs: data.now_ms,
        clientMs: performance.now(),
        speed: data.speed,
        paused: data.paused,
      };
    } catch {
      setConnected(false);
      anchorRef.current = null;
    }
  }, []);

  // Poll timer
  useEffect(() => {
    fetchClock();
    const id = setInterval(fetchClock, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [fetchClock]);

  // RAF loop: interpolate between polls for smooth display
  useEffect(() => {
    let rafId: number;

    const tick = () => {
      const anchor = anchorRef.current;
      if (anchor && connected) {
        if (anchor.paused) {
          setDisplayTime(new Date(anchor.serverMs));
        } else {
          const wallElapsed = performance.now() - anchor.clientMs;
          const dataElapsed = wallElapsed * anchor.speed;
          setDisplayTime(new Date(anchor.serverMs + dataElapsed));
        }
      } else {
        // Fallback: local wall clock
        setDisplayTime(new Date());
      }
      rafId = requestAnimationFrame(tick);
    };

    tick();
    return () => cancelAnimationFrame(rafId);
  }, [connected]);

  // Format to ET
  const formatTimeET = (date: Date): string => {
    const etDate = new Date(
      date.toLocaleString("en-US", {
        timeZone: "America/New_York",
      }),
    );

    const year = etDate.getFullYear();
    const month = String(etDate.getMonth() + 1).padStart(2, "0");
    const day = String(etDate.getDate()).padStart(2, "0");
    const hours = String(etDate.getHours()).padStart(2, "0");
    const minutes = String(etDate.getMinutes()).padStart(2, "0");
    const seconds = String(etDate.getSeconds()).padStart(2, "0");

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} ET`;
  };

  const isReplay = clockState?.mode === "replay";
  const isPaused = clockState?.paused ?? false;

  return (
    <div className="flex items-center gap-2 px-4 py-2 bg-zinc-900 border border-zinc-700 text-sm font-mono">
      {isReplay ? (
        <>
          <span
            className={`inline-block w-2 h-2 rounded-full ${isPaused ? "bg-yellow-400" : "bg-orange-400 animate-pulse"}`}
            title={isPaused ? "Replay paused" : "Replay running"}
          />
          <span className="text-orange-400 font-semibold">REPLAY</span>
          {clockState && clockState.speed !== 1.0 && (
            <span className="text-orange-300 text-xs">
              {clockState.speed}×
            </span>
          )}
          <span className="text-orange-300">
            {formatTimeET(displayTime)}
          </span>
          {isPaused && (
            <span className="text-yellow-400 text-xs">⏸ PAUSED</span>
          )}
        </>
      ) : (
        <>
          <span className="text-gray-400">Market Time:</span>
          <span className="text-green-400">
            {formatTimeET(displayTime)}
          </span>
        </>
      )}
      {!connected && (
        <span className="text-red-400 text-xs ml-1" title="Cannot reach BFF /api/clock">
          ●
        </span>
      )}
    </div>
  );
}

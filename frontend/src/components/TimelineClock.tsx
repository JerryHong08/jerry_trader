import React, { useState, useEffect, useRef, useCallback } from "react";
import { getChartBffBaseUrl } from "../config/chartBff";

interface ClockState {
  mode: "live" | "replay";
  now_ms: number;
  speed: number;
  paused: boolean;
  data_start_ts_ns: number | null;
  session_id: string;
}


const POLL_INTERVAL_MS = 1_000; // poll /api/clock every 1s

export function TimelineClock({ compact }: { compact?: boolean }) {
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

  // RAF loop: interpolate between polls for smooth display.
  // Pauses when the tab is backgrounded to avoid wasted CPU.
  useEffect(() => {
    let rafId: number;
    let running = true;

    const tick = () => {
      if (!running) return;
      // Skip state updates when tab is hidden; browser throttles RAF anyway
      if (document.hidden) {
        rafId = requestAnimationFrame(tick);
        return;
      }
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
        setDisplayTime(new Date());
      }
      rafId = requestAnimationFrame(tick);
    };

    const onVisibility = () => {
      running = !document.hidden;
      if (running && !rafId) {
        tick();
      }
    };
    document.addEventListener('visibilitychange', onVisibility);

    tick();
    return () => {
      running = false;
      cancelAnimationFrame(rafId);
      document.removeEventListener('visibilitychange', onVisibility);
    };
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
    <div className={`flex items-center gap-1.5 bg-zinc-900 border border-zinc-700 font-mono whitespace-nowrap ${compact ? 'px-2 py-1 text-xs' : 'px-4 py-2 text-sm'}`}>
      {isReplay ? (
        <>
          <span
            className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${isPaused ? "bg-yellow-400" : "bg-orange-400 animate-pulse"}`}
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
          {!compact && <span className="text-zinc-400">Market Time:</span>}
          <span className="text-green-400">
            {formatTimeET(displayTime)}
          </span>
        </>
      )}
      {!connected && (
        <span className="text-red-400 text-xs ml-0.5 flex-shrink-0" title="Cannot reach BFF /api/clock">
          ●
        </span>
      )}
    </div>
  );
}

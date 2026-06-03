import { useState, useRef, useCallback, useEffect } from 'react';
import {
  ChevronUp, ChevronDown, ChevronLeft, ChevronRight,
  Plus, Minus, Maximize2, GripHorizontal, X,
} from 'lucide-react';

// ---- Constants ---------------------------------------------------------------

const PAN_STEP = 25; // px per pan tap
const LONG_PRESS_MS = 300;
const REPEAT_MS = 60;

// ---- Props -------------------------------------------------------------------

interface TouchGamepadProps {
  onPan: (dx: number, dy: number) => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFit: () => void;
}

// ---- Hook: long-press repeat -------------------------------------------------

function useLongPress(callback: () => void, enabled: boolean) {
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const delayRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const start = useCallback(() => {
    if (!enabled) return;
    callback();
    delayRef.current = setTimeout(() => {
      timerRef.current = setInterval(callback, REPEAT_MS);
    }, LONG_PRESS_MS);
  }, [callback, enabled]);

  const stop = useCallback(() => {
    if (delayRef.current) { clearTimeout(delayRef.current); delayRef.current = null; }
    if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; }
  }, []);

  useEffect(() => () => stop(), [stop]);

  return { onPointerDown: start, onPointerUp: stop, onPointerLeave: stop };
}

// ---- Component ---------------------------------------------------------------

export function TouchGamepad({ onPan, onZoomIn, onZoomOut, onFit }: TouchGamepadProps) {
  const [position, setPosition] = useState(() => {
    try {
      const saved = localStorage.getItem('jt_gamepad_pos');
      return saved ? JSON.parse(saved) : { x: 16, y: 16 };
    } catch { return { x: 16, y: 16 }; }
  });
  const [dragging, setDragging] = useState(false);
  const [minimized, setMinimized] = useState(() => {
    try { return localStorage.getItem('jt_gamepad_minimized') === 'true'; } catch { return false; }
  });

  const setAndPersistMinimized = (v: boolean) => {
    setMinimized(v);
    try { localStorage.setItem('jt_gamepad_minimized', String(v)); } catch {}
  };
  const dragStart = useRef({ x: 0, y: 0 });
  const posStart = useRef({ x: 0, y: 0 });
  const moved = useRef(false);
  const rootRef = useRef<HTMLDivElement>(null);

  const persist = (pos: { x: number; y: number }) => {
    try { localStorage.setItem('jt_gamepad_pos', JSON.stringify(pos)); } catch {}
  };

  const onDragStart = (e: React.PointerEvent) => {
    setDragging(true);
    moved.current = false;
    dragStart.current = { x: e.clientX, y: e.clientY };
    posStart.current = { ...position };
    (e.target as HTMLElement).setPointerCapture(e.pointerId);
  };

  const onDragMove = (e: React.PointerEvent) => {
    if (!dragging) return;
    const dx = e.clientX - dragStart.current.x;
    const dy = e.clientY - dragStart.current.y;
    if (Math.abs(dx) > 3 || Math.abs(dy) > 3) moved.current = true;

    // Use actual element size to compute bounds
    const el = rootRef.current;
    const elW = el ? el.offsetWidth : 160;
    const elH = el ? el.offsetHeight : 200;

    const next = {
      x: Math.max(0, Math.min(window.innerWidth - elW, posStart.current.x + dx)),
      y: Math.max(0, Math.min(window.innerHeight - elH, posStart.current.y + dy)),
    };
    setPosition(next);
  };

  const onDragEnd = () => {
    setDragging(false);
    persist(position);
  };

  // Directional pan (long-press repeats)
  const panUp = useLongPress(() => onPan(0, PAN_STEP), !minimized);
  const panDown = useLongPress(() => onPan(0, -PAN_STEP), !minimized);
  const panLeft = useLongPress(() => onPan(PAN_STEP, 0), !minimized);
  const panRight = useLongPress(() => onPan(-PAN_STEP, 0), !minimized);

  // Zoom (long-press repeats)
  const zoomIn = useLongPress(onZoomIn, !minimized);
  const zoomOut = useLongPress(onZoomOut, !minimized);

  return (
    <div
      ref={rootRef}
      className="fixed z-[9999] select-none touch-none"
      style={{ left: position.x, top: position.y }}
    >
      {minimized ? (
        <button
          onPointerDown={onDragStart}
          onPointerMove={onDragMove}
          onPointerUp={onDragEnd}
          onClick={() => { if (!moved.current) setAndPersistMinimized(false); }}
          className="flex items-center justify-center w-11 h-11 rounded-full bg-zinc-800/80 border border-zinc-600/50 backdrop-blur text-zinc-400 hover:text-white shadow-lg active:scale-95 transition-transform"
        >
          <GripHorizontal className="w-5 h-5" />
        </button>
      ) : (
        <div className="bg-zinc-900/85 backdrop-blur border border-zinc-700/60 rounded-2xl shadow-2xl p-2 flex flex-col items-center gap-1.5">
          {/* Drag handle row */}
          <div
            className="flex items-center justify-between w-full px-1 cursor-grab active:cursor-grabbing"
            onPointerDown={onDragStart}
            onPointerMove={onDragMove}
            onPointerUp={onDragEnd}
          >
            <GripHorizontal className="w-4 h-4 text-zinc-600" />
            <button
              onClick={(e) => { e.stopPropagation(); setAndPersistMinimized(true); }}
              className="p-0.5 text-zinc-600 hover:text-white rounded"
            >
              <X className="w-3.5 h-3.5" />
            </button>
          </div>

          {/* D-Pad + Zoom row */}
          <div className="flex items-center gap-3">
            {/* D-Pad */}
            <div className="grid grid-cols-3 grid-rows-3 gap-0.5">
              <div />
              <button
                {...panUp}
                className="w-10 h-10 flex items-center justify-center bg-zinc-800 hover:bg-zinc-700 active:bg-zinc-600 rounded-t-lg text-zinc-300 transition-colors"
              >
                <ChevronUp className="w-5 h-5" />
              </button>
              <div />
              <button
                {...panLeft}
                className="w-10 h-10 flex items-center justify-center bg-zinc-800 hover:bg-zinc-700 active:bg-zinc-600 rounded-l-lg text-zinc-300 transition-colors"
              >
                <ChevronLeft className="w-5 h-5" />
              </button>
              <button
                onClick={onFit}
                className="w-10 h-10 flex items-center justify-center bg-blue-700/60 hover:bg-blue-600/80 active:bg-blue-500 rounded text-zinc-200 transition-colors"
                title="Fit to screen"
              >
                <Maximize2 className="w-4 h-4" />
              </button>
              <button
                {...panRight}
                className="w-10 h-10 flex items-center justify-center bg-zinc-800 hover:bg-zinc-700 active:bg-zinc-600 rounded-r-lg text-zinc-300 transition-colors"
              >
                <ChevronRight className="w-5 h-5" />
              </button>
              <div />
              <button
                {...panDown}
                className="w-10 h-10 flex items-center justify-center bg-zinc-800 hover:bg-zinc-700 active:bg-zinc-600 rounded-b-lg text-zinc-300 transition-colors"
              >
                <ChevronDown className="w-5 h-5" />
              </button>
              <div />
            </div>

            {/* Zoom buttons */}
            <div className="flex flex-col gap-1">
              <button
                {...zoomIn}
                className="w-10 h-10 flex items-center justify-center bg-zinc-800 hover:bg-zinc-700 active:bg-zinc-600 rounded-lg text-zinc-300 transition-colors"
                title="Zoom in"
              >
                <Plus className="w-5 h-5" />
              </button>
              <button
                {...zoomOut}
                className="w-10 h-10 flex items-center justify-center bg-zinc-800 hover:bg-zinc-700 active:bg-zinc-600 rounded-lg text-zinc-300 transition-colors"
                title="Zoom out"
              >
                <Minus className="w-5 h-5" />
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

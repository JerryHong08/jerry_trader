import React, { useState, useEffect } from "react";

export function TimelineClock() {
  const [currentTime, setCurrentTime] = useState(new Date());

  useEffect(() => {
    let rafId: number;

    const tick = () => {
      setCurrentTime(new Date());
      rafId = requestAnimationFrame(tick);
    };

    tick();
    return () => cancelAnimationFrame(rafId);
  }, []);

  // Convert to ET timezone
  const formatTimeET = (date: Date): string => {
    const etDate = new Date(
      date.toLocaleString("en-US", {
        timeZone: "America/New_York",
      }),
    );

    const year = etDate.getFullYear();
    const month = String(etDate.getMonth() + 1).padStart(
      2,
      "0",
    );
    const day = String(etDate.getDate()).padStart(2, "0");
    const hours = String(etDate.getHours()).padStart(2, "0");
    const minutes = String(etDate.getMinutes()).padStart(
      2,
      "0",
    );
    const seconds = String(etDate.getSeconds()).padStart(
      2,
      "0",
    );

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} ET`;
  };

  return (
    <div className="flex items-center gap-2 px-4 py-2 bg-zinc-900 border border-zinc-700 text-sm font-mono">
      <span className="text-gray-400">Market Time:</span>
      <span className="text-green-400">
        {formatTimeET(currentTime)}
      </span>
    </div>
  );
}

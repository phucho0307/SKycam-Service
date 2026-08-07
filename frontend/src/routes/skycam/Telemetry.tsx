import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchTelemetry, type Reading } from "@/lib/skycam";

export default function Telemetry() {
  const { data: readings = [], isLoading, error } = useQuery({
    queryKey: ["skycam", "telemetry"],
    queryFn: () => fetchTelemetry(48),
    refetchInterval: 60_000,
  });

  if (isLoading) return <Note>Loading telemetry…</Note>;
  if (error) return <Note>Failed to load: {String((error as Error).message)}</Note>;
  if (readings.length === 0) return <Note>No telemetry yet.</Note>;

  const temps = readings.map((r) => r.temperature_c ?? null).filter((t): t is number => t != null);
  const min = temps.length ? Math.min(...temps) : 0;
  const max = temps.length ? Math.max(...temps) : 1;
  const span = max - min || 1;

  return (
    <div className="space-y-6">
      <div className="rounded-xl border border-slate-800 bg-slate-900/40 p-4">
        <div className="mb-3 flex items-baseline justify-between">
          <span className="text-sm text-slate-400">Temperature (last {readings.length})</span>
          <span className="text-xs text-slate-500">{min}–{max} °C</span>
        </div>
        <div className="flex h-28 items-end gap-1">
          {readings.map((r, i) => {
            const t = r.temperature_c;
            const h = t == null ? 2 : 10 + ((t - min) / span) * 90;
            return (
              <div
                key={i}
                title={t == null ? "—" : `${t} °C`}
                className="flex-1 rounded-t bg-brand-500/70"
                style={{ height: `${h}%` }}
              />
            );
          })}
        </div>
      </div>

      <div className="overflow-hidden rounded-xl border border-slate-800 bg-slate-900/40">
        <div className="border-b border-slate-800 px-4 py-2 text-sm text-slate-400">Recent readings</div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
                <th className="px-4 py-3 font-medium">Time</th>
                <th className="px-4 py-3 font-medium">Temp (°C)</th>
                <th className="px-4 py-3 font-medium">Humidity (%)</th>
              </tr>
            </thead>
            <tbody>
              {[...readings].reverse().map((r: Reading, i) => (
                <tr key={i} className="border-t border-slate-800/70">
                  <td className="px-4 py-3 text-slate-300">{new Date(r.recorded_at).toLocaleString()}</td>
                  <td className="px-4 py-3 text-slate-100">{r.temperature_c ?? "—"}</td>
                  <td className="px-4 py-3 text-slate-100">{r.humidity_pct ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function Note({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/40 p-6 text-sm text-slate-400">
      {children}
    </div>
  );
}

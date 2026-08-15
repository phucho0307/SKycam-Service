import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchLatestFrame } from "@/lib/skycam";

function timeAgo(iso: string): string {
  const s = Math.round((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  return `${Math.round(s / 3600)}h ago`;
}

export default function LiveView() {
  const { data: f, isLoading, error } = useQuery({
    queryKey: ["skycam", "latest"],
    queryFn: fetchLatestFrame,
    refetchInterval: 30_000,
  });

  if (isLoading) return <Note>Loading latest frame…</Note>;
  if (error) return <Note>Failed to load: {String((error as Error).message)}</Note>;
  if (!f) return <Note>No frames yet. Upload one to see it here.</Note>;

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-[1fr_260px]">
      <div className="overflow-hidden rounded-xl border border-slate-800 bg-slate-900/40">
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-2 text-sm text-slate-400">
          <span>Current sky</span>
          <span>{timeAgo(f.captured_at)}</span>
        </div>
        {f.preview_url ? (
          <img src={f.preview_url} alt="latest sky" className="aspect-square w-full bg-slate-950 object-cover" />
        ) : (
          <div className="flex aspect-square items-center justify-center bg-slate-950 text-slate-600">
            <div className="text-center">
              <div className="text-sm">no preview</div>
              <div className="mt-1 text-xs">frame stored, but no JPEG preview was sent</div>
            </div>
          </div>
        )}
      </div>

      <div className="space-y-3">
        <Stat label="Temperature (box)" value={f.temperature_c != null ? `${f.temperature_c} °C` : "—"} />
        <Stat label="Probe (outside)" value={f.probe_temp_c != null ? `${f.probe_temp_c} °C` : "—"} />
        <Stat
          label="Sky"
          value={
            f.is_cloudy == null
              ? "—"
              : (f.is_cloudy ? "Cloudy" : "Clear") +
                (f.cloud_score != null ? ` (${Math.round(f.cloud_score * 100)}%)` : "")
          }
          tone={f.is_cloudy ? "warn" : "ok"}
        />
        <Stat label="Last capture" value={new Date(f.captured_at).toLocaleString()} />
        {f.fits_url && (
          <a
            href={f.fits_url}
            className="block rounded-xl border border-slate-800 bg-slate-900/40 px-4 py-3 text-sm text-brand-500 hover:border-brand-500 hover:text-white"
          >
            Download FITS ↓
          </a>
        )}
        <p className="pt-1 text-xs text-slate-500">
          Refreshes the most recent uploaded frame every 30s — not a video stream.
        </p>
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

function Stat({
  label,
  value,
  tone = "default",
}: {
  label: string;
  value: string;
  tone?: "default" | "ok" | "warn";
}) {
  const toneCls =
    tone === "ok" ? "text-emerald-300" : tone === "warn" ? "text-amber-300" : "text-slate-100";
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/40 p-4">
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div className={`mt-1 text-lg font-semibold ${toneCls}`}>{value}</div>
    </div>
  );
}

import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchFrames, type Frame } from "@/lib/skycam";

export default function Images() {
  const { data: frames = [], isLoading, error } = useQuery({
    queryKey: ["skycam", "frames"],
    queryFn: () => fetchFrames(60),
  });

  if (isLoading) return <Note>Loading frames…</Note>;
  if (error) return <Note>Failed to load: {String((error as Error).message)}</Note>;
  if (frames.length === 0) return <Note>No frames yet.</Note>;

  return (
    <div>
      <div className="mb-3 text-sm text-slate-400">{frames.length} most recent frames</div>
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
        {frames.map((f) => (
          <Card key={f.id} f={f} />
        ))}
      </div>
    </div>
  );
}

function Card({ f }: { f: Frame }) {
  return (
    <div className="overflow-hidden rounded-lg border border-slate-800 bg-slate-900/40">
      {f.preview_url ? (
        <img src={f.preview_url} alt="" className="aspect-square w-full bg-slate-950 object-cover" />
      ) : (
        <div className="flex aspect-square items-center justify-center bg-slate-950 text-xs text-slate-600">
          no preview
        </div>
      )}
      <div className="flex items-center justify-between px-2 py-1.5 text-xs">
        <span className="text-slate-300">{new Date(f.captured_at).toLocaleTimeString()}</span>
        <span className="text-slate-500">{f.temperature_c != null ? `${f.temperature_c}°` : ""}</span>
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

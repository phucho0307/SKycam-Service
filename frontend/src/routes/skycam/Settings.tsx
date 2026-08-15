import { useEffect, useState, type ChangeEvent, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchSettings, saveSettings, DEVICE_ID, type CameraSettings } from "@/lib/skycam";

export default function Settings() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["skycam", "settings"],
    queryFn: () => fetchSettings(),
  });

  const [form, setForm] = useState<CameraSettings>({ device_id: DEVICE_ID });
  const [status, setStatus] = useState("");

  useEffect(() => {
    if (data) setForm({ ...data, device_id: DEVICE_ID });
  }, [data]);

  const set =
    (k: keyof CameraSettings) => (e: ChangeEvent<HTMLInputElement>) =>
      setForm({ ...form, [k]: e.target.value === "" ? null : Number(e.target.value) });

  async function onSave() {
    setStatus("saving…");
    try {
      await saveSettings({ ...form, device_id: DEVICE_ID });
      setStatus("saved ✓ — the camera applies it on its next settings fetch");
    } catch (e) {
      setStatus("failed: " + String((e as Error).message));
    }
  }

  if (isLoading) return <Note>Loading settings…</Note>;
  if (error) return <Note>Failed to load: {String((error as Error).message)}</Note>;

  return (
    <div className="max-w-lg space-y-5">
      <Section title="Lighting (capture — applied on the camera)">
        <Field label="Exposure (ms)" hint="Longer = brighter; night skies need more." value={form.exposure_ms} onChange={set("exposure_ms")} />
        <Field label="Gain" hint="Amplifies signal (like ISO); higher = brighter but noisier." value={form.gain} onChange={set("gain")} />
      </Section>

      <Section title="Preview appearance (applied to the JPEG)">
        <Field label="Gamma" hint="< 1 brighter, > 1 darker (default 1.0)." value={form.preview_gamma} onChange={set("preview_gamma")} step="0.1" />
        <Field label="Contrast" hint="1.0 = none, > 1 punchier." value={form.preview_contrast} onChange={set("preview_contrast")} step="0.1" />
        <Field label="Brightness" hint="Added to pixels (− / +)." value={form.preview_brightness} onChange={set("preview_brightness")} />
      </Section>

      <div className="flex items-center gap-3">
        <button
          onClick={onSave}
          className="rounded-lg bg-brand-500 px-4 py-2 text-sm font-medium text-white hover:opacity-90"
        >
          Save settings
        </button>
        {status && <span className="text-sm text-slate-400">{status}</span>}
      </div>

      <p className="text-xs text-slate-500">
        Note: focus/sharpness is a physical lens adjustment (turn &amp; lock the ring) — not a
        setting here. These control exposure/gain and how the preview is rendered.
      </p>
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/40 p-4">
      <div className="mb-3 text-sm font-medium text-slate-300">{title}</div>
      <div className="space-y-3">{children}</div>
    </div>
  );
}

function Field({
  label,
  hint,
  value,
  onChange,
  step = "1",
}: {
  label: string;
  hint: string;
  value?: number | null;
  onChange: (e: ChangeEvent<HTMLInputElement>) => void;
  step?: string;
}) {
  return (
    <label className="block">
      <div className="flex items-baseline justify-between">
        <span className="text-sm text-slate-200">{label}</span>
      </div>
      <input
        type="number"
        step={step}
        value={value ?? ""}
        onChange={onChange}
        className="mt-1 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 focus:border-brand-500 focus:outline-none"
      />
      <p className="mt-1 text-xs text-slate-500">{hint}</p>
    </label>
  );
}

function Note({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/40 p-6 text-sm text-slate-400">
      {children}
    </div>
  );
}

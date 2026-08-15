import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchSettings, saveSettings, DEVICE_ID, type CameraSettings } from "@/lib/skycam";

type FieldDef = {
  key: keyof CameraSettings;
  label: string;
  min: number;
  max: number;
  step: number;
  hint: string;
};

const FIELDS: FieldDef[] = [
  { key: "exposure_ms", label: "Exposure (ms)", min: 1, max: 10000, step: 10, hint: "lighting — longer = brighter" },
  { key: "gain", label: "Gain", min: 0, max: 500, step: 1, hint: "ISO-like; higher = brighter + noisier" },
  { key: "preview_gamma", label: "Gamma", min: 0.1, max: 3, step: 0.05, hint: "< 1 brighter, > 1 darker" },
  { key: "preview_contrast", label: "Contrast", min: 0.1, max: 3, step: 0.05, hint: "1.0 = none" },
  { key: "preview_brightness", label: "Brightness", min: -100, max: 100, step: 1, hint: "− / +" },
];

export default function SettingsPanel() {
  const { data } = useQuery({ queryKey: ["skycam", "settings"], queryFn: () => fetchSettings() });
  const [form, setForm] = useState<CameraSettings>({ device_id: DEVICE_ID });
  const [status, setStatus] = useState("");

  useEffect(() => {
    if (data) setForm({ ...data, device_id: DEVICE_ID });
  }, [data]);

  const setVal = (k: keyof CameraSettings, v: string) =>
    setForm((prev) => ({ ...prev, [k]: v === "" ? null : Number(v) }));

  async function onSave() {
    setStatus("saving…");
    try {
      await saveSettings({ ...form, device_id: DEVICE_ID });
      setStatus("saved ✓ — camera applies within ~30s");
    } catch (e) {
      setStatus("failed: " + String((e as Error).message));
    }
  }

  return (
    <div className="space-y-4 rounded-xl border border-slate-800 bg-slate-900/40 p-4">
      <div className="text-sm font-medium text-slate-300">Camera settings</div>
      {FIELDS.map((f) => {
        const val = form[f.key] as number | null | undefined;
        return (
          <div key={f.key}>
            <div className="flex items-baseline justify-between">
              <span className="text-sm text-slate-200">{f.label}</span>
              <input
                type="number"
                step={f.step}
                value={val ?? ""}
                onChange={(e) => setVal(f.key, e.target.value)}
                className="w-20 rounded border border-slate-700 bg-slate-950 px-2 py-0.5 text-right text-xs text-slate-100 focus:border-brand-500 focus:outline-none"
              />
            </div>
            <input
              type="range"
              min={f.min}
              max={f.max}
              step={f.step}
              value={val ?? f.min}
              onChange={(e) => setVal(f.key, e.target.value)}
              className="mt-1 w-full accent-brand-500"
            />
            <div className="text-xs text-slate-500">{f.hint}</div>
          </div>
        );
      })}
      <button
        onClick={onSave}
        className="w-full rounded-lg bg-brand-500 px-3 py-2 text-sm font-medium text-white hover:opacity-90"
      >
        Save
      </button>
      {status && <div className="text-xs text-slate-400">{status}</div>}
    </div>
  );
}

import type { Asset } from "@/lib/releases";
import { detectArch, detectPlatform, formatBytes } from "@/lib/releases";

const platformLabels: Record<string, string> = {
  windows: "Windows",
  macos: "macOS",
  linux: "Linux",
  other: "Other",
};

export default function AssetButton({ asset }: { asset: Asset }) {
  const platform = detectPlatform(asset.filename);
  const arch = detectArch(asset.filename);
  return (
    <a
      href={asset.download_url}
      className="flex flex-col gap-1 rounded-lg border border-slate-700 bg-slate-900/60 px-4 py-3 transition hover:border-brand-500 hover:bg-slate-800"
      download
    >
      <div className="flex items-center justify-between gap-3">
        <span className="text-sm font-medium text-slate-100">
          {platformLabels[platform] ?? platform}
          {arch && <span className="ml-2 text-xs text-slate-400">{arch}</span>}
        </span>
        <span className="text-xs text-slate-400">{formatBytes(asset.size_bytes)}</span>
      </div>
      <div className="truncate text-xs text-slate-500">{asset.filename}</div>
    </a>
  );
}

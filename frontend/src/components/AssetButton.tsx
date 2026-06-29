import type { Asset } from "@/lib/releases";
import { detectArch, formatBytes } from "@/lib/releases";

export default function AssetButton({ asset }: { asset: Asset }) {
  const arch = detectArch(asset.filename);
  return (
    <a
      href={asset.download_url}
      className="flex items-center gap-4 rounded-lg border border-slate-700 bg-slate-900/60 px-4 py-2.5 transition hover:border-brand-500 hover:bg-slate-800"
      download
    >
      <span className="min-w-0 flex-1 truncate text-sm text-slate-200">
        {asset.filename}
      </span>
      {arch && (
        <span className="shrink-0 rounded bg-slate-800 px-2 py-0.5 text-xs text-slate-300">
          {arch}
        </span>
      )}
      <span className="shrink-0 text-xs tabular-nums text-slate-400">
        {formatBytes(asset.size_bytes)}
      </span>
    </a>
  );
}

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  getProduct,
  groupAssetsByPlatform,
  platformLabels,
  platformOrder,
  type ProductSummary,
} from "@/lib/releases";
import AssetButton from "./AssetButton";
import FeatureRequestModal from "./FeatureRequestModal";

export default function ProductCard({ product }: { product: ProductSummary }) {
  const [expanded, setExpanded] = useState(false);
  const [featureOpen, setFeatureOpen] = useState(false);

  const detail = useQuery({
    queryKey: ["product", product.id],
    queryFn: () => getProduct(product.id),
    enabled: expanded,
  });

  const noReleases = product.release_count === 0;

  return (
    <article className="rounded-xl border border-slate-800 bg-slate-900/60 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold">{product.display_name}</h2>
          <p className="text-xs text-slate-500">{product.github_repo}</p>
        </div>
        <div className="text-right">
          {product.latest_tag ? (
            <>
              <div className="text-sm font-medium text-brand-500">{product.latest_tag}</div>
              <div className="text-xs text-slate-500">
                {product.latest_published_at &&
                  new Date(product.latest_published_at).toLocaleDateString()}
              </div>
            </>
          ) : (
            <div className="text-xs text-slate-500">No releases yet</div>
          )}
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-2 text-sm">
        <button
          type="button"
          disabled={noReleases}
          onClick={() => setExpanded((v) => !v)}
          className="rounded-lg border border-slate-700 px-3 py-1.5 hover:border-brand-500 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:border-slate-700"
        >
          {expanded ? "Hide releases" : `Releases (${product.release_count})`}
        </button>
        <button
          type="button"
          onClick={() => setFeatureOpen(true)}
          className="rounded-lg border border-slate-700 px-3 py-1.5 hover:border-brand-500"
        >
          Request a feature
        </button>
        <span className="ml-auto self-center text-xs text-slate-500">
          winget: <code className="text-slate-300">{product.winget_package_id}</code>
          {" · "}
          brew: <code className="text-slate-300">{product.homebrew_formula}</code>
        </span>
      </div>

      {expanded && (
        <div className="mt-5 space-y-6">
          {detail.isLoading && (
            <p className="text-sm text-slate-400">Loading…</p>
          )}
          {detail.isError && (
            <p className="text-sm text-red-400">
              Failed to load releases: {(detail.error as Error).message}
            </p>
          )}
          {detail.data?.releases.map((r) => (
            <div key={r.tag} className="border-t border-slate-800 pt-4">
              <div className="flex items-baseline justify-between gap-2">
                <h3 className="text-lg font-medium">
                  {r.name}{" "}
                  <span className="text-sm text-slate-500">{r.tag}</span>
                </h3>
                <span className="text-xs text-slate-500">
                  {new Date(r.published_at).toLocaleDateString()}
                </span>
              </div>
              {r.notes_md && (
                <details className="mt-2 text-sm text-slate-300">
                  <summary className="cursor-pointer text-slate-400 hover:text-slate-200">
                    Release notes
                  </summary>
                  <pre className="mt-2 whitespace-pre-wrap rounded-md bg-slate-950 p-3 text-xs">
                    {r.notes_md}
                  </pre>
                </details>
              )}
              <div className="mt-3 space-y-2">
                {(() => {
                  const groups = groupAssetsByPlatform(r.assets);
                  return platformOrder
                    .filter((p) => groups.has(p))
                    .map((p) => {
                      const assets = groups.get(p)!;
                      return (
                        <details
                          key={p}
                          open
                          className="group rounded-lg border border-slate-800 bg-slate-950/40"
                        >
                          <summary className="flex cursor-pointer items-center gap-2 px-4 py-2 text-sm font-medium text-slate-200 hover:text-white">
                            <svg
                              className="h-3 w-3 shrink-0 text-slate-500 transition-transform group-open:rotate-90"
                              viewBox="0 0 20 20"
                              fill="currentColor"
                              aria-hidden="true"
                            >
                              <path
                                fillRule="evenodd"
                                d="M7.21 14.77a.75.75 0 01.02-1.06L11.168 10 7.23 6.29a.75.75 0 111.04-1.08l4.5 4.25a.75.75 0 010 1.08l-4.5 4.25a.75.75 0 01-1.06-.02z"
                                clipRule="evenodd"
                              />
                            </svg>
                            <span>{platformLabels[p]}</span>
                            <span className="text-xs font-normal text-slate-500">
                              ({assets.length})
                            </span>
                          </summary>
                          <div className="space-y-1.5 px-2 pb-2">
                            {assets.map((a) => (
                              <AssetButton key={a.filename} asset={a} />
                            ))}
                          </div>
                        </details>
                      );
                    });
                })()}
              </div>
            </div>
          ))}
        </div>
      )}

      {featureOpen && (
        <FeatureRequestModal
          productId={product.id}
          productName={product.display_name}
          onClose={() => setFeatureOpen(false)}
        />
      )}
    </article>
  );
}

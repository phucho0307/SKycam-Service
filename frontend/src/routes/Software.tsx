import { useQuery } from "@tanstack/react-query";
import { listProducts } from "@/lib/releases";
import ProductCard from "@/components/ProductCard";

export default function Software() {
  const products = useQuery({
    queryKey: ["products"],
    queryFn: listProducts,
  });

  return (
    <section className="space-y-6">
      <header className="space-y-3">
        <h1 className="text-3xl font-bold">Software</h1>
        <p className="max-w-2xl text-slate-300">
          Cross-platform observatory software. Public releases are mirrored
          from private source repositories. Feature requests and bug reports
          are posted directly to the source repository's issue tracker.
        </p>
      </header>

      {products.isLoading && (
        <p className="text-sm text-slate-400">Loading products…</p>
      )}
      {products.isError && (
        <p className="text-sm text-red-400">
          Could not load products: {(products.error as Error).message}
        </p>
      )}
      {products.data && (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {products.data.map((p) => (
            <ProductCard key={p.id} product={p} />
          ))}
        </div>
      )}
    </section>
  );
}

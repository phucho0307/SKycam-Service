import { useCallback, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { getTurnstileSitekey, submitFeatureRequest } from "@/lib/releases";
import Turnstile from "./Turnstile";

interface Props {
  productId: string;
  productName: string;
  onClose: () => void;
}

export default function FeatureRequestModal({ productId, productName, onClose }: Props) {
  const [title, setTitle] = useState("");
  const [body, setBody] = useState("");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [token, setToken] = useState<string | null>(null);

  const sitekey = useQuery({
    queryKey: ["turnstile-sitekey"],
    queryFn: getTurnstileSitekey,
    staleTime: Infinity,
  });

  const mutation = useMutation({
    mutationFn: submitFeatureRequest,
  });

  const onToken = useCallback((t: string | null) => setToken(t), []);

  function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!token) return;
    mutation.mutate({
      product_id: productId,
      title: title.trim(),
      body: body.trim(),
      reporter_name: name.trim() || undefined,
      reporter_email: email.trim() || undefined,
      turnstile_token: token,
    });
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur"
      onClick={onClose}
    >
      <div
        className="w-full max-w-xl rounded-xl border border-slate-700 bg-slate-900 p-6"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 className="text-xl font-semibold">Request a feature for {productName}</h2>
        <p className="mt-1 text-sm text-slate-400">
          Submitted as a GitHub issue on the source repository.
        </p>
        <p className="mt-2 text-xs text-slate-500">
          Anonymous submission for now. Google sign-in is coming soon, which will pre-fill your
          name + email and link the issue to your account.
        </p>

        {mutation.isSuccess ? (
          <div className="mt-6 space-y-3 text-sm text-slate-200">
            <p>Thanks — issue created.</p>
            <a
              href={mutation.data.issue_url}
              target="_blank"
              rel="noreferrer"
              className="block break-all text-brand-500 hover:underline"
            >
              {mutation.data.issue_url}
            </a>
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg bg-slate-700 px-4 py-2 text-sm hover:bg-slate-600"
            >
              Close
            </button>
          </div>
        ) : (
          <form onSubmit={submit} className="mt-4 space-y-3 text-sm">
            <label className="block">
              <span className="block text-slate-300">Title</span>
              <input
                required
                maxLength={200}
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                className="mt-1 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100 outline-none focus:border-brand-500"
                placeholder="One-line summary"
              />
            </label>
            <label className="block">
              <span className="block text-slate-300">Description</span>
              <textarea
                required
                maxLength={8000}
                rows={6}
                value={body}
                onChange={(e) => setBody(e.target.value)}
                className="mt-1 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100 outline-none focus:border-brand-500"
                placeholder="What would you like to see? Steps to reproduce a bug, or details of the feature."
              />
            </label>
            <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
              <label className="block">
                <span className="block text-slate-300">Your name (optional)</span>
                <input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="mt-1 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100 outline-none focus:border-brand-500"
                />
              </label>
              <label className="block">
                <span className="block text-slate-300">Email (optional)</span>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="mt-1 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100 outline-none focus:border-brand-500"
                />
              </label>
            </div>

            <div className="pt-1">
              {sitekey.data && (
                <Turnstile sitekey={sitekey.data.sitekey} onToken={onToken} />
              )}
              {sitekey.isError && (
                <p className="text-xs text-red-400">
                  Could not load bot protection: {(sitekey.error as Error).message}
                </p>
              )}
            </div>

            {mutation.isError && (
              <p className="text-sm text-red-400">
                Submission failed: {(mutation.error as Error).message}
              </p>
            )}

            <div className="flex justify-end gap-2 pt-2">
              <button
                type="button"
                onClick={onClose}
                className="rounded-lg bg-slate-700 px-4 py-2 hover:bg-slate-600"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={mutation.isPending || !token}
                className="rounded-lg bg-brand-500 px-4 py-2 font-medium text-slate-950 disabled:opacity-60"
              >
                {mutation.isPending ? "Submitting…" : "Submit"}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

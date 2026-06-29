const BASE = import.meta.env.VITE_RELEASES_BASE ?? "/releases/api";

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export interface ProductSummary {
  id: string;
  display_name: string;
  github_repo: string;
  winget_package_id: string;
  homebrew_formula: string;
  latest_tag: string | null;
  latest_published_at: string | null;
  release_count: number;
}

export interface ProductDetail {
  id: string;
  display_name: string;
  github_repo: string;
  winget_package_id: string;
  homebrew_formula: string;
  releases: Release[];
}

export interface Release {
  tag: string;
  name: string;
  notes_md: string;
  published_at: string;
  assets: Asset[];
}

export interface Asset {
  filename: string;
  size_bytes: number;
  content_type: string;
  download_url: string;
}

export interface FeatureRequestPayload {
  product_id: string;
  title: string;
  body: string;
  reporter_name?: string;
  reporter_email?: string;
}

export function listProducts() {
  return call<ProductSummary[]>("/products");
}

export function getProduct(id: string) {
  return call<ProductDetail>(`/products/${encodeURIComponent(id)}`);
}

export function submitFeatureRequest(payload: FeatureRequestPayload) {
  return call<{ issue_url: string }>("/feature-requests", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export type AssetPlatform = "windows" | "macos" | "linux" | "other";

export function detectPlatform(filename: string): AssetPlatform {
  const lower = filename.toLowerCase();
  if (
    lower.endsWith(".msi") ||
    lower.endsWith(".exe") ||
    lower.includes("windows")
  ) {
    return "windows";
  }
  if (
    lower.endsWith(".dmg") ||
    lower.endsWith(".pkg") ||
    lower.includes(".app.")
  ) {
    return "macos";
  }
  if (
    lower.endsWith(".deb") ||
    lower.endsWith(".rpm") ||
    lower.endsWith(".appimage") ||
    lower.endsWith(".tar.gz") ||
    lower.endsWith(".tar.xz") ||
    lower.includes("linux")
  ) {
    return "linux";
  }
  return "other";
}

export function detectArch(filename: string): string | null {
  const lower = filename.toLowerCase();
  if (lower.includes("aarch64") || lower.includes("arm64")) return "arm64";
  if (lower.includes("x86_64") || lower.includes("amd64") || lower.includes("x64")) return "x86_64";
  return null;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

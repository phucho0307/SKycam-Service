// Sky Camera data types + API calls to the `skycam` microservice.
// Dev: Vite proxies /skycam -> http://localhost:8002 (see vite.config.ts).

export interface Frame {
  id: string;
  device_id: string;
  captured_at: string; // ISO
  received_at: string; // ISO
  temperature_c?: number | null;
  probe_temp_c?: number | null;
  cloud_score?: number | null;
  is_cloudy?: boolean | null;
  size_bytes?: number | null;
  preview_url?: string | null; // stable /skycam/frames/<id>/preview.jpg, cacheable
  fits_url?: string | null; // presigned download (only on full frames)
}

export interface Reading {
  recorded_at: string; // ISO
  temperature_c?: number | null;
  humidity_pct?: number | null;
  probe_temp_c?: number | null;
}

export const DEVICE_ID = "skycam";

const BASE = "/skycam";

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export const fetchLatestFrame = () => getJSON<Frame | null>("/frames/latest");

/** The newest frame (the picture) and the newest scored frame (sky status). */
export interface LivePayload {
  latest: Frame | null;
  last_scored: Frame | null;
}

/** How the live view learns about new frames. See SCALING.md, "Live view". */
export type LiveMode = "poll" | "sse";

/**
 * VITE_SKYCAM_LIVE picks the default; `?live=sse` or `?live=poll` in the page
 * URL overrides it, so both can be compared side by side in one build.
 */
export function liveMode(): LiveMode {
  const fromUrl = new URLSearchParams(window.location.search).get("live");
  if (fromUrl === "sse" || fromUrl === "poll") return fromUrl;
  return import.meta.env.VITE_SKYCAM_LIVE === "sse" ? "sse" : "poll";
}

const devQuery = (deviceId?: string) =>
  deviceId ? `?device_id=${encodeURIComponent(deviceId)}` : "";

/** Option A: one cacheable request (Cache-Control: max-age=2). */
export const fetchLive = (deviceId?: string) =>
  getJSON<LivePayload>(`/live${devQuery(deviceId)}`);

/** Option B: Server-Sent Events. The browser reconnects on its own. */
export const liveStreamUrl = (deviceId?: string) => `${BASE}/live/stream${devQuery(deviceId)}`;
export const fetchFrames = (limit = 60) => getJSON<Frame[]>(`/frames?limit=${limit}`);
export const fetchTelemetry = (limit = 48) => getJSON<Reading[]>(`/telemetry?limit=${limit}`);

export interface CameraSettings {
  device_id: string;
  exposure_ms?: number | null;
  gain?: number | null;
  preview_gamma?: number | null;
  preview_contrast?: number | null;
  preview_brightness?: number | null;
  updated_at?: string;
}

export const fetchSettings = (deviceId = DEVICE_ID) =>
  getJSON<CameraSettings | null>(`/settings?device_id=${encodeURIComponent(deviceId)}`);

export async function saveSettings(s: CameraSettings): Promise<CameraSettings> {
  const res = await fetch(`${BASE}/settings`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(s),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<CameraSettings>;
}

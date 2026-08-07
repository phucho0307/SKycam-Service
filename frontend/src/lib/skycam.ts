// Sky Camera data types + API calls to the `skycam` microservice.
// Dev: Vite proxies /skycam -> http://localhost:8002 (see vite.config.ts).

export interface Frame {
  id: string;
  device_id: string;
  captured_at: string; // ISO
  received_at: string; // ISO
  temperature_c?: number | null;
  is_cloudy?: boolean | null;
  size_bytes: number;
  preview_url?: string | null; // presigned, browser-reachable
  fits_url?: string | null; // presigned download
}

export interface Reading {
  recorded_at: string; // ISO
  temperature_c?: number | null;
  humidity_pct?: number | null;
}

export const DEVICE_ID = "skycam";

const BASE = "/skycam";

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export const fetchLatestFrame = () => getJSON<Frame | null>("/frames/latest");
export const fetchFrames = (limit = 60) => getJSON<Frame[]>(`/frames?limit=${limit}`);
export const fetchTelemetry = (limit = 48) => getJSON<Reading[]>(`/telemetry?limit=${limit}`);

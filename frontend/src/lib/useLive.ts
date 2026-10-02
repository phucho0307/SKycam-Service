import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchLive, liveMode, liveStreamUrl, type LivePayload } from "@/lib/skycam";

interface LiveState {
  data: LivePayload | undefined;
  isLoading: boolean;
  error: Error | null;
  mode: "poll" | "sse";
}

/**
 * The live view's data, by polling or by SSE. Same payload either way, so the
 * component does not care which, and the two can be measured against each
 * other. Both hooks are always called (rules of hooks); the unused one is idle.
 */
export function useLive(deviceId?: string): LiveState {
  const mode = liveMode();

  // A: poll every 2s, the frame cadence. With Cache-Control: max-age=2, a CDN
  // answers most of these, so the origin sees ~1 request per 2s per camera.
  const polled = useQuery({
    queryKey: ["skycam", "live", deviceId ?? "all"],
    queryFn: () => fetchLive(deviceId),
    refetchInterval: 2_000,
    enabled: mode === "poll",
  });

  // B: one long-lived SSE connection per viewer.
  const [streamed, setStreamed] = useState<LivePayload>();
  const [streamError, setStreamError] = useState<Error | null>(null);
  useEffect(() => {
    if (mode !== "sse") return;
    const es = new EventSource(liveStreamUrl(deviceId));
    es.addEventListener("live", (e) => {
      setStreamed(JSON.parse((e as MessageEvent).data) as LivePayload);
      setStreamError(null);
    });
    // EventSource retries by itself; surface the gap without tearing down.
    es.onerror = () => setStreamError(new Error("live stream interrupted; reconnecting"));
    return () => es.close();
  }, [mode, deviceId]);

  if (mode === "sse") {
    return { data: streamed, isLoading: streamed === undefined && !streamError, error: streamError, mode };
  }
  return { data: polled.data, isLoading: polled.isLoading, error: polled.error, mode };
}

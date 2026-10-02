# OpenTelemetry tracing: predictions to test

Written 2026-10-02, **before** any tracing exists. Every number here is an
**estimate with its reasoning**, made so the build can be checked against it.
When it's built, fill in the **Measured** column and the **Why it differed**
column. The misses are the learning.

Do not quote these as results. They are predictions.

Grounding facts already measured in this project: preview every 2 s and FITS
(~25 MB) every 60 s per camera; authz load test 1,663 req/s at p50 333 ms (500
users, 8-conn pool); GZIP_2 compress+verify of a real FITS 2.3–2.9 s; detection
polls every 3 s; Beat archive scan every 60 s.

---

## 1. Volume

| Quantity | Prediction | Reasoning | Measured | Why it differed |
|---|---|---|---|---|
| Frames/s at 200 sites | **~100** previews/s + **~3.3** FITS/s | 200 × (1/2 s) and 200 × (1/60 s) | | |
| Spans per preview frame (ingest + detect) | **12–15** | ingest: gRPC server, auth, Redis rate-limit `EVAL`, Postgres tx, S3 PUT ≈ 5–6; detect: claim query, Celery publish, task, S3 GET, analyze, Postgres update ≈ 6–8 | | |
| Spans/s, unsampled | **~1,200–1,500** | 100 × 12–15 | | |
| Span size on the wire | **0.5–1 KB** | name + ~10 attributes + timing, protobuf | | |
| Trace data/day, unsampled | **~50–130 GB** | 1,500 × 0.5–1 KB × 86,400 | | |
| Trace data/day, tail-sampled (5% + all errors + slow) | **~3–7 GB** | ~5% of the above plus a small error share | | |

## 2. Where time goes (one preview frame)

| Stage | Prediction (p50 / p95) | Reasoning | Measured | Why it differed |
|---|---|---|---|---|
| `UploadFrame` server-side, preview only | **15 / 50 ms** | Postgres insert 1–3 ms, S3 PUT to in-cluster MinIO 5–20 ms, auth + Redis rate limit ~1 ms | | |
| Same call seen from the Pi | **150 / 600 ms** | school WiFi dominates | | |
| **Commit → claimed by the poll** | **1.5 / 2.9 s** | uniform wait over a 3 s poll interval: mean 1.5 s, p95 ≈ 2.85 s | | |
| Celery queue wait (idle cluster) | **5 / 50 ms** | Redis broker, prefetch 1 | | |
| Detect task: S3 GET + decode + analyze + update | **60 / 200 ms** | GET 5–20 ms, OpenCV on a ~1024 px JPEG 20–100 ms, update 1–3 ms | | |
| **End to end: upload done → scored** | **1.6 / 3.2 s** | dominated by the poll gap | | |

## 3. One FITS frame

| Stage | Prediction | Reasoning | Measured | Why it differed |
|---|---|---|---|---|
| 25 MB upload from the Pi | **10–40 s** | 5–20 Mbit/s school uplink; 5 parts of 5 MB at 2–8 s each | | |
| Scored → archive claimed | **~30 s mean, ~60 s worst** | Beat archive scan every 60 s | | |
| Archive task (clear frames only) | **3–4 s** | in-cluster GET 0.3–1 s, GZIP_2 + verify 2.3–2.9 s (measured), PUT 0.2–0.5 s | | |

## 4. Cost of instrumenting

| Quantity | Prediction | Reasoning | Measured | Why it differed |
|---|---|---|---|---|
| Added server-side latency per RPC | **< 0.5 ms** | span creation is microseconds; export is batched off the request path | | |
| Ingest CPU overhead | **2–5%** | typical for gRPC + DB auto-instrumentation | | |
| Collector memory for tail sampling | **30–80 MB** | `decision_wait` 10 s × ~1,500 spans/s ≈ 15k spans held | | |
| Upload-latency histogram series if labelled by device | **~48,000** | 200 devices × ~5 methods × ~4 status codes × ~12 buckets: too many | | |

## 5. Surprises I expect (check each one)

1. **The poll gap is the biggest bar in every detection trace.** That's by
   design (polling was chosen; see CLAUDE.md, decision 3). The trace makes a
   deliberate trade-off visible; it doesn't make it a bug.
2. **Span links make ingest and detection two separate traces**, so tail
   sampling decides on each independently. Expect to keep a detection trace
   whose linked ingest trace was dropped. Likely fix: sample consistently by
   hashing the frame ID, so both halves are kept or dropped together.
3. **The `DeviceSession` stream becomes one span lasting hours.** Useless for
   latency. Expect to exclude it from tracing, or record per-message events.
4. **Background noise:** Beat's 3 s `scan_pending` makes ~28,800 near-empty
   traces a day per Beat, and health probes add one every 5 s per pod. Expect to
   filter or drop both.
5. **The Redis rate-limit `EVAL` appears on every RPC**, at ~0.3–1 ms. Small, but
   visible on every trace.
6. **Device-labelled histograms explode metric cardinality** (§4). Expect to keep
   the device label on counters only.

## 6. After building, answer these
- Which prediction was furthest off, and what assumption was wrong?
- Did anything appear in traces that no prediction mentioned?
- Was tail sampling's memory in line, and did consistent sampling (surprise 2)
  turn out to be necessary?

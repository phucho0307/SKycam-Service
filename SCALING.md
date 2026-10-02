# Scaling — what breaks, in the order it breaks

Audit of 2026-09-29, grounded in the code rather than general advice. Verified by
reading the repo: there is **no metrics, tracing or gRPC health service** in
`services/ingest`, **no partitioning or retention** in any migration, and the Go
service has no k8s manifest so no probes.

The headline: **most of these are not user-count problems.** The read path fails
on data volume at ten users, and three pieces of in-process state cap the service
at one replica regardless of load.

---

## Tier 0 — breaks before there are any users

1. ~~**No indexes on the Rust/Mongo read path.**~~ **Fixed 2026-09-30** — five
   indexes created at startup. Latest-frame query at 500k frames: 271 ms → 2 ms.
   (The earlier claim here that Mongo would *error* past its 32MB sort limit was
   wrong: this is a top-k sort holding one document, and Mongo 7 spills to disk.
   The defect was the collection scan on every poll.) Timestamps stored as
   mixed-format strings remain a follow-up.
2. ~~**Nothing is deployed.**~~ **Deploy artifacts built 2026-09-30.** Dockerfiles
   for ingest, detect and notify; kustomize component
   `infra/k8s/components/skycam-v2` (dev overlay only); CI path filters, image
   builds and overlay bumps; a new `skycam-ci.yml` that runs every suite with
   `-race` against real Postgres/Redis/MinIO. Rehearsed end to end on a Docker
   network, which caught a probe-blocking auth bug. **Still manual, once:** seal
   `skycam-v2-secrets`, and the Traefik gRPC entryPoint + Cloudflare gRPC toggle
   so cameras can reach it. See that component's README.
3. **`PUT /skycam/settings` is unauthenticated** in the Rust service.

## Tier 1 — blocks the second replica

One ingest pod today. That is an *availability* ceiling, not a throughput one:
every deploy disconnects every camera.

| In-process state | Consequence of a 2nd replica |
|---|---|
| `session.Registry` map | a command cannot reach a stream held by another pod — **correctness** |
| `ratelimit` buckets | N replicas allow N× the intended rate |
| `oidc.Pending` flows | sign-in starts on pod A, callback lands on B, fails |

**All three solved (2026-09-30).** Commands route through Redis pub/sub (see
**Cross-replica command routing** below); rate-limit buckets and sign-in flows
moved to Redis (see **Solved since this audit**). The dev manifests run
`replicas: 2`.

## Tier 2 — the connection-pooling collision

Measured: **1,663 req/s at 8 pool connections, 2,271 at 32, no gain at 64** (p99
got worse — queueing moves into Postgres).

The standard next step is PgBouncer. **It is safe to add as the design stands.**
Transaction pooling breaks `LISTEN` (a listener's connection is swapped out
between transactions, so its notifications land on a connection now serving
someone else, silently). But nothing listens: detection is triggered by polling,
deliberately (see CLAUDE.md, Key architecture decision 3). `NOTIFY` itself
works through the pooler.

*Corrected 2026-10-01.* This section used to say the detection trigger
"depends on" LISTEN/NOTIFY. It never did. If a listener is ever added, give it
a dedicated direct connection that bypasses the pooler.

**Built 2026-10-01, opt-in:** `infra/k8s/components/skycam-pgbouncer`, off
everywhere. Against Postgres capped at 40 connections, an app pool of 120 went
from **1,025 refused requests** (direct) to **0** through PgBouncer, which held
25 server connections. Required setting: `MAX_PREPARED_STATEMENTS` (with it at
0, 50 Go tests fail). Cost when not needed: ~15% throughput at an equal pool
size. Dev uses ~40 of 100 connections today, so it stays off until the budget
says otherwise. Details in that component's README.

## Tier 3 — data growth

- **No partitioning.** At 100 sites `frames` grows ~4.3M rows/day;
  `frames_device_captured_idx` grows unbounded and VACUUM cost with it. Partition
  by `captured_at` (monthly), because then **retention is `DROP TABLE`, not
  `DELETE`** — that is the real reason to do it.
- ~~**No S3 lifecycle rules.**~~ **Fixed 2026-10-01** (`infra/k8s/minio/bucket-init.yaml`).
  Worse than this list said: the 100 GiB MinIO volume is shared by all three
  environments, and one camera writes ~36 GB/day of FITS, so **one camera filled
  it in under 3 days**. Now `frames/` expires after 1 day, `previews/` after 7,
  abandoned multipart uploads after 1; `releases/` is untouched. Verified on a
  real MinIO, including that **MinIO silently drops `AbortIncompleteMultipartUpload`**
  (its `stale_uploads_expiry` setting does that job, now set explicitly), and
  the job fails loudly if the stored rules don't match. Expired previews now
  return 404 instead of 502. **Not solved by lifecycle:** 100 GiB still holds
  only a day or two of FITS from one camera. Next levers: FITS compression
  (fpack/Rice), longer retention only for clear-sky frames (tag-filtered rules),
  a bigger volume or real object storage for prod. At 200 sites the ~7 TB/day
  is a storage-architecture decision, not a lifecycle setting.
- **`settings_audit` grows forever.**

## Tier 4 — read path at user scale

- ~~**`Cache-Control` on the latest-frame endpoint**~~ **Done 2026-10-01**, as a
  new `GET /skycam/live` (latest frame + latest *scored* frame in one response,
  replacing two polls) with `Cache-Control: public, max-age=2, stale-if-error=60`.
- ~~**Presigned URLs are CDN-hostile.**~~ **Done 2026-10-01.** The signature
  differed per request, so no cache could ever reuse a preview URL. Previews are
  now served at `GET /skycam/frames/<id>/preview.jpg` with
  `max-age=31536000, immutable`: a preview is written once and never changes.
  FITS stays presigned. Measured: 18,051 image downloads by 2,000 viewers
  reached the origin **11 times**.
- **One Cloudflare setting is required, outside git:** Cloudflare caches by file
  extension by default, so the `.jpg` previews are cached automatically, but the
  extensionless `/skycam/live` JSON needs a **Cache Rule** marking it eligible
  for cache. Without it the header does nothing at the edge. See
  `infra/cloudflare/README.md`.

### Live view: decided 2026-10-01, built and measured the same day

The camera makes a new preview every **2 s**, so "live" is a 0.5 fps slideshow:
the browser needs "a new frame exists, here is its URL", one way, server to
browser. Two candidates, to be **measured against each other** before choosing:

| | A. Polling + CDN cache | B. Server-Sent Events |
|---|---|---|
| Delay before a new frame shows | up to 2–3 s | ~instant |
| Origin load at 2,000 viewers | ~0.5 req/s **per camera**, whatever the viewer count | 2,000 open connections; the CDN cannot absorb streams |
| New parts | `Cache-Control: max-age=2` + stable preview URLs | Rust `EventStream` endpoint, Redis `frames:<device>` fan-out (one subscription per camera per pod), heartbeat every ~20 s (Cloudflare drops idle responses at ~100 s), latest-frame-on-connect so a lost pub/sub message heals |

**Not WebSocket**: nothing flows browser → server continuously (settings are an
occasional `PUT`), and SSE gives auto-reconnect with `Last-Event-ID` over plain
HTTP. **Not HLS/WebRTC** unless real video (many fps) is ever wanted. Default
plan: ship A (it is also this tier's first bullet), build B only if the
measured delay is something viewers actually notice.

**Measured (2026-10-01).** One camera publishing a ~11 KB preview every 2s; N
simulated viewers behaving like the browser (poll every 2s, or hold a stream,
and download each new preview). nginx stood in for the CDN edge: plain caching
plus request collapsing (`proxy_cache_lock`). Counts are from its log. CPU is
from the unoptimised debug build, so only the ratio between options means
anything.

| 2,000 viewers, 30 s | A. Polling + edge cache | B. SSE (+ cached previews) |
|---|---|---|
| Reaching the origin | **36 `/live` + 11 images**. Same at 500 viewers (31 + 10): flat in viewers | **2,000 open streams** + 21 images. Linear in viewers |
| Origin CPU / memory | **2% / 33 MB** | 73% / 111 MB |
| Frame stored → viewer sees it | p50 **2.3 s**, p95 3.8 s | p50 **0.14 s**, p95 0.21 s |
| Frames seen per viewer (of ~15) | ~9: polling at the frame rate through a 2 s cache skips some | all |
| Errors | 0 | 0 |

Baseline, no edge cache, 500 viewers: **14,436 origin requests in 30 s** (7,495
`/live` + 6,941 images) and ~320% CPU, versus 41 with the cache.

**Origin restart mid-run (a deploy), 500 viewers.** Both were found wanting and
fixed:
- **Polling: 34 errors (502) → 0** after adding `stale-if-error=60`. The edge
  serves the last good response while the origin is down.
- **SSE: all 500 streams reconnected within 49 ms** (a thundering herd: 500
  snapshot queries in one burst), because every EventSource uses the same
  default delay. The server now sends a per-connection `retry:` spread over
  1–5 s: the same 500 reconnects spread over **4.2 s**, **peak 26 per 100 ms
  instead of 500**.

**Decision: A, polling + CDN cache, is the default.** Origin cost stays flat
however many people watch. The price is a 2–4 s delay and skipping some frames,
and for a sky that changes over minutes, nobody can tell. SSE is built, tested
and available behind a flag (`?live=sse` or `VITE_SKYCAM_LIVE=sse`) for a case
where sub-second matters. **SSE limitation:** its fan-out hub is in-process, so
with more than one skycam replica it needs a Redis channel per camera.
Harness: `liveload` (Go) + nginx config. Run in the scratchpad, not committed.

## Tier 5 — the meta-blocker

**No observability at all**: no metrics, no tracing, no `grpc.health.v1`, no
probes. Arguably #1 on this list, because every other item is something you
*diagnose* with metrics rather than guess at. Minimum: request rate/latency/error
by method, pool utilisation, queue depth, delivery success, and a health service
so Kubernetes can tell whether a pod is ready.

It is also the one thing the résumé claims (OpenTelemetry) that does not exist.

## Tier 6 — multi-tenancy, if "users" means institutions

There is no **organisation** concept. `users.is_admin` is *platform-wide* and
grants are per-user-per-device. At 200 sites across 50 institutions you want
`organizations`, admin scoped to an org, and per-org quotas so a noisy tenant
cannot starve others.

This is the one decision that is expensive to defer: adding a tenant boundary
later means touching every authorization query. Decide it on paper now.

## Single points of failure

One Postgres (under ingest, auth, detection *and* alerts), one MinIO, one Redis —
none with a replica or failover. Availability is capped by whichever fails first,
however many app replicas run.

---

## Remaining work, in priority order (as of 2026-09-30)

| # | Item | Tier | Why this position |
|---|---|---|---|
| 1 | **Observability**: metrics (rate/latency/errors per RPC, pool use, queue depth, delivery success), tracing | 5 | Every item below is diagnosed with metrics, not guessed at. And the résumé claims OpenTelemetry. gRPC health is now done; metrics and tracing are not. |
| 2 | **Finish the deploy**: seal `skycam-v2-secrets`, Traefik gRPC entryPoint (`readTimeout: 0`), Cloudflare gRPC toggle + device hostname | 0 | Everything in git is done. These three steps are outside it. |
| 3 | ~~**S3 lifecycle rules**~~ **done 2026-10-01** | 3 | Remaining: decide FITS cadence/compression/clear-sky retention; at 200 sites ~7 TB/day needs a storage decision, not just expiry. |
| 4 | **Partition `frames` by month**, plus retention on `settings_audit`, `forecast_observations`, `alerts`, `notification_deliveries` | 3 | Retention becomes `DROP TABLE` instead of `DELETE` + VACUUM. |
| 5 | **Sample + debounce cloud detection**: score ~1 frame per device per 30–60s, not every 2s preview; alarm only on agreement across samples | — | ~15–30× less detection work at 200 sites (~100 frames/s today), and fewer false alarms from planes or dew. See CLAUDE.md, *Detection cadence and transient events*. |
| 5b | ~~PgBouncer~~ **built, opt-in** | 2 | Enable when combined app pools near `max_connections`. Dev is at ~40 of 100. |
| 6 | ~~**Read path**~~ **done 2026-10-01**: `/live` cacheable, previews at stable immutable URLs | 4 | Measured: origin load flat in viewer count. **Remaining: the Cloudflare Cache Rule for `/skycam/live`** (outside git). |
| 7 | **Single points of failure**: Postgres replica or managed DB, Redis replica | — | Redis now carries control-plane state, not just the broker. |
| 8 | **Decide the org / tenant model** (on paper) | 6 | Expensive to add later: it touches every authorization query. |
| 9 | Smaller: BSON dates for Mongo timestamps; SES out of sandbox + bounce webhook; a canary email through the real mail path; auth on Rust `PUT /skycam/settings`; `ListConnectedDevices` reports zero timestamps for devices held by *another* replica | — | Real, but none compounds. |

## Not on this list, deliberately: MongoDB and DynamoDB

Neither is a skycam scaling concern, and neither is part of the new design.

**MongoDB** is the incumbent — the Rust services use it because it is what is
deployed. It is a *migration* item, not a scaling one: retire it at cutover.

**DynamoDB** was an external, platform-wide directive motivated by ops burden, not
by skycam performance. `TELEMETRY_BACKEND` defaults to `postgres` and the adapter
runs only when switched on. At 200 sites telemetry is ~100 writes/s, which
Postgres does not notice. Adding DynamoDB would *not* relieve any tier below.

## Solved since this audit

- **Shared rate limiting** (2026-09-30): `ratelimit.RedisLimiter`, the same token
  bucket as a Lua script, so every replica draws from one bucket per caller.
  Redis's own clock (not the pod's), and a key TTL equal to the refill time, so
  expiry forgets only buckets with no debt. **Fails open** when Redis is down:
  the limiter protects the service from one caller and must not refuse the whole
  fleet. Tested: 2 replicas → exactly one burst, where the in-process limiter
  (control) allows two; 200 concurrent requests across 4 replicas → exactly the
  burst, never more; the same over real gRPC with two servers.
- **Shared sign-in flows** (2026-09-30): `oidc.RedisFlows`, `SET NX EX` +
  `GETDEL`. The atomic take keeps state single-use across pods: 60 racing
  callbacks → exactly one sign-in. The full browser flow passes behind a
  round-robin balancer over two instances, and the in-memory store (control)
  fails the same test.
- **gRPC health** (2026-09-30): `grpc.health.v1`, exempt from auth by exact
  method name, reports NOT_SERVING on SIGTERM before the listeners close.
- **Race detector** first run (2026-09-30): the whole Go suite, 0 races. The load
  tests are excluded under `-race` for memory. It now runs in CI on every push.
- **Cross-replica command routing** — `internal/cluster`, Redis pub/sub plus an
  expiring `session:<device>` key. Removes Tier 1's correctness blocker. **Two of
  the three in-process states remain** (rate limiter, OIDC pending flows).
- **Rain alerts** (`notify/`, 2026-09-30) were built scale-first: the schedule lives in
  Postgres (`forecast_sites.next_poll_at` claimed with `SKIP LOCKED`), so
  staggering, per-site backoff and the staleness check fall out of the claim
  query and a duplicated scheduler is a no-op rather than a double-alert.

## Added by the 2026-09-30 work

New surface means new scaling debt. Naming it here rather than discovering it later.

- **Redis was promoted from broker to control-plane dependency.** With
  `INGEST_REDIS_URL` set, cross-replica commands and `ListConnectedDevices`
  depend on it, and startup is fatal if it is unreachable. Frames, telemetry and
  uploads are unaffected, but "Redis is just the broker" is no longer true, and
  it still has no replica.
- **Three new unbounded tables**: `forecast_observations` (one row per site per
  poll — 200 sites every 10 min is ~28k rows/day), `alerts`, and
  `notification_deliveries`. Each needs retention, same as `settings_audit`.
- **`notify/` is not deployed** — no Dockerfile, no kustomize entry, same as
  `detect/`.
- **SES must leave sandbox** (1/s, 200/day, verified recipients only) before
  alerting works at any real scale, and the **bounce webhook is unwired**: the
  store method exists but nothing calls it, so a dead address is never marked
  undeliverable in production.
- **No canary through the real mail path.** With email as the only channel there
  is no redundancy, so an untested path is how you discover a lapsed credential
  on the night it rains.

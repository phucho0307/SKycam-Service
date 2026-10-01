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

The standard next step is PgBouncer, and it collides with this design:
**PgBouncer in transaction mode does not support `LISTEN/NOTIFY`**, which the
detection trigger depends on. Already noted in
`internal/testenv/testenv.go`. The fix is a **dedicated direct connection for the
listener** that bypasses the pooler while everything else goes through it. Worth
knowing before someone adds a pooler and silently kills the trigger.

## Tier 3 — data growth

- **No partitioning.** At 100 sites `frames` grows ~4.3M rows/day;
  `frames_device_captured_idx` grows unbounded and VACUUM cost with it. Partition
  by `captured_at` (monthly), because then **retention is `DROP TABLE`, not
  `DELETE`** — that is the real reason to do it.
- **No S3 lifecycle rules.** 7.2 TB/day at 200 sites. This is the item that ends
  the project financially rather than technically.
- **`settings_audit` grows forever.**

## Tier 4 — read path at user scale

- **`Cache-Control: max-age=2` on the latest-frame endpoint** turns 667 req/s
  (2,000 browsers polling every 3s) into ~0.5 req/s at the origin.
- **Presigned URLs are CDN-hostile.** The signature differs per request, so every
  browser gets a unique URL and Cloudflare never serves a hit — ~160 Mbit/s of
  preview egress that should be free. Previews are not secret: serve them at a
  stable path with a long `max-age` and keep presigning for FITS.

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
| 3 | **S3 lifecycle rules** | 3 | ~7.2 TB/day at 200 sites. The one that ends the project financially. |
| 4 | **Partition `frames` by month**, plus retention on `settings_audit`, `forecast_observations`, `alerts`, `notification_deliveries` | 3 | Retention becomes `DROP TABLE` instead of `DELETE` + VACUUM. |
| 5 | **Dedicated LISTEN connection before any PgBouncer** | 2 | Transaction pooling silently kills `LISTEN/NOTIFY`. Measured ceiling: ~2.3k req/s at 32 conns. |
| 6 | **Read path**: `Cache-Control: max-age=2` on latest-frame; previews at stable URLs, not presigned | 4 | 667 req/s → ~0.5 req/s at origin, and the CDN starts working. |
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

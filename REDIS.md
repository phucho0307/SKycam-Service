# Redis in the skycam platform

Every place this project uses Redis, why, and what happens when Redis fails. As
of 2026-10-01. Paths are relative to the repo root.

**The rule:** Redis holds work in flight and state that pods must share.
**Postgres holds everything that must not be lost.** No record that matters
exists only in Redis, so losing Redis delays work but loses nothing.

| # | Use | Redis feature | Who | Built |
|---|---|---|---|---|
| 1 | Job queue for cloud detection | List (Celery broker) | `detect/` | ✅ |
| 2 | Job queue for rain alerts | List (Celery broker) | `notify/` | ✅ |
| 3 | Which pod holds which camera | Key with expiry (`SET … EX`) | Go ingest | ✅ |
| 4 | Commands and acks between pods | Pub/sub | Go ingest | ✅ |
| 5 | Shared rate limiting | Hash + Lua script | Go ingest | ✅ |
| 6 | Sign-in flows in progress | Key with expiry, `GETDEL` | Go ingest | ✅ |
| 7 | Live view fan-out across pods (SSE) | Pub/sub | Rust skycam | ❌ only needed if SSE is used with >1 replica |

The Rust services (`api`, `skycam`, `release-proxy`) do not use Redis today.

---

## 1–2. Celery and Beat: the job queues

### What each piece is
- **Celery workers** run tasks (score a frame, send emails).
- **Redis** is the **broker**: a queue holding task messages until a worker
  takes one. It is also the result backend (results expire after an hour,
  `result_expires=3600`).
- **Celery Beat** is the **scheduler**. On a timer it puts a task on the queue.
  It uses Redis only to publish those messages.

### Cloud detection (`detect/celery_app.py`, `detect/tasks_pg.py`)
Queue `celery` (the default), Redis database **0** in Kubernetes.

| Beat entry | Task | Every | What it does |
|---|---|---|---|
| `scan-pending-frames` | `tasks_pg.scan_pending` | 3 s (`DETECT_SCAN_INTERVAL_S`) | Claims pending frames in Postgres (`FOR UPDATE SKIP LOCKED`) and enqueues one `detect_frame` task per frame |
| `reclaim-stale-claims` | `tasks_pg.reclaim_stale` | 60 s | Returns frames claimed by a worker that died, or dead-letters them once their retry budget is spent |

The flow:

```
Beat (3s) ──► Redis queue ──► worker: scan_pending ──► Postgres: claim N frames
                                    │
                                    └── detect_frame.delay(...) × N ──► Redis queue ──► workers score frames
```

### Rain alerts (`notify/celery_app.py`, `notify/tasks.py`)
Queue **`notify`**, Redis database **1**, its own worker (`-Q notify`).

| Beat entry | Task | Every |
|---|---|---|
| `sync-sites` | `notify.sync_sites` | 5 min |
| `poll-forecasts` | `notify.poll_forecasts` | 60 s (`NOTIFY_TICK_S`) |
| `send-pending` | `notify.send_pending` | 15 s |
| `reclaim-stale` | `notify.reclaim_stale` | 2 min |
| `check-staleness` | `notify.check_staleness` | 5 min |

### Design decisions
- **Separate queues and databases for detection and alerts.** Detection is high
  volume and can wait; alerts are low volume and urgent. On one queue, thousands
  of frame-scoring tasks would sit in front of a rain warning.
- **Beat is a heartbeat, not the schedule.** The real schedule lives in Postgres
  (`forecast_sites.next_poll_at`, pending frames) and is claimed with
  `SKIP LOCKED`. So a duplicate Beat finds nothing left to claim and does
  nothing, instead of doubling the alerts. That's why plain Beat is fine here
  rather than a distributed scheduler.
- **The queue is not the record.** A frame's detection state and an alert's
  delivery state are Postgres rows. The Redis message just says "go look". So
  Redis needs no persistence.
- **One retry mechanism, not two.** Retries are counted on the Postgres row
  (`detect_attempts`, delivery attempts), not by Celery's `retry()`. Two
  independent retry budgets would disagree.
- **`task_acks_late=True`, `worker_prefetch_multiplier=1`.** A task is
  acknowledged only after it finishes, so a worker that dies mid-task has the
  task redelivered. Every task is idempotent for that reason (alarms use
  `ON CONFLICT DO NOTHING`; emails are keyed by fingerprint).
- **Polling, not LISTEN/NOTIFY.** Considered and rejected: clouds change over
  minutes, and the indexed poll costs microseconds. See CLAUDE.md, Key
  architecture decision 3.

---

## 3–4. Pub/sub: commands between ingest pods

**Code:** `services/ingest/internal/cluster/` (`redis.go`). Enabled by
`INGEST_REDIS_URL`; Redis database **2** in Kubernetes.

### The problem
Each camera holds **one** long-lived gRPC stream to **one** ingest pod. An
operator's "capture now" can arrive at **any** pod. That pod needs to know
where the camera is, and to get the command there.

### Two Redis features, two jobs

| Job | Key / channel | Mechanism |
|---|---|---|
| **Where is camera X?** | `skycam:session:<device>` = `<replica id>` | `SET … EX 30`, renewed every 10 s while the stream is open; deleted on disconnect only if still ours (a Lua compare-and-delete) |
| **Deliver the command** | channel `skycam:cmd:<device>` | `PUBLISH`; only the pod holding that camera is subscribed |
| **Return the ack** | channel `skycam:ack:<replica>` | `PUBLISH` to the asking pod, which matches it to the waiting caller by command id |
| **List connected cameras** | `skycam:session:*` | `SCAN` (never `KEYS`, which blocks Redis) |

```
operator ──► pod B: SendCommand(cam-7)
               │ GET skycam:session:cam-7  → "pod-A"
               │ PUBLISH skycam:cmd:cam-7  {cmd, reply_to: pod-B}
               ▼
             pod A (subscribed) ──► camera's gRPC stream ──► camera acks
               │ PUBLISH skycam:ack:pod-B {command_id, ok}
               ▼
             pod B ──► operator gets the ack
```

### Why both a key and pub/sub
- **Pub/sub can't tell you whether anyone was listening.** Without the presence
  key, a command to an offline camera would vanish and fail only after the ack
  timeout.
- **The expiry cleans up after crashes.** A pod that dies without
  disconnecting cleanly stops renewing, and its claim expires in 30 seconds. No
  sweeper process is needed.

### Why lossy pub/sub is acceptable here, and nowhere else
Pub/sub doesn't store messages: if nobody receives one, it's gone. That's
acceptable **only** because commands are already defined as "now or never" ("abort
exposure" is meaningless an hour later), and `SendCommand` already reports
"sent but not acknowledged" separately from failure. Everything that must arrive
(frames, telemetry, alerts) is a Postgres row instead.

### Three ordering rules, each one a bug first
1. **Subscribe before announcing.** Otherwise the presence key points at this
   pod before it can receive commands.
2. **Presence beats a local session.** A camera that moved pods leaves a stale
   local session for up to 90 s; trusting it sends commands into a dead stream.
3. **Register for the ack per command id before publishing.** With one shared
   reader, concurrent callers consumed each other's acks.

---

## 5. Shared rate limiting

**Code:** `services/ingest/internal/ratelimit/redis.go`.

**Problem:** with counters in each pod's memory, N pods allow N times the
configured rate.

| | |
|---|---|
| Keys | `skycam:rl:device:<id>`, `skycam:rl:user:<id>`: a hash of `{tokens, ts}` |
| Algorithm | Token bucket (devices: 2/s, burst 60; users: 20/s, burst 100) |
| Atomicity | The whole read-refill-take-write runs as **one Lua script**, so two pods can't both spend the last token |
| Clock | Redis's own (`TIME`), not each pod's, because pod clocks drift |
| Cleanup | Each key expires once its bucket would be full again, so only buckets with no outstanding debt are forgotten |
| If Redis fails | **Fails open**: requests are allowed and logged. The limiter guards against one bad caller and must not refuse the whole fleet |

Measured: two pods → exactly one shared burst (the per-pod version allowed two);
200 concurrent requests across 4 pods → exactly the burst.

---

## 6. Sign-in flows in progress

**Code:** `services/ingest/internal/oidc/redis.go`.

**Problem:** OAuth sign-in starts on one pod (`/auth/login`) and finishes
wherever the identity provider's redirect lands (`/auth/callback`), often a
different pod.

| | |
|---|---|
| Key | `skycam:oidc:flow:<state>` → the PKCE verifier, nonce and return URL |
| Write | `SET NX EX 600`: expires after 10 minutes, and never overwrites another login |
| Read | **`GETDEL`**: read and delete in one atomic step, so a replayed callback can't sign in twice |
| If Redis fails | Sign-in returns 500. Already signed-in users are unaffected (tokens are verified without Redis) |

Measured: 60 racing replays of one callback → exactly one sign-in; the full
browser flow across two pods passes.

---

## 7. Not built: live view fan-out

The live view polls a CDN-cached endpoint by default, which needs no Redis.
The optional SSE mode (`?live=sse`) uses an **in-process** broadcast, so a
frame stored on pod A reaches only pod A's viewers. Running SSE with more than
one skycam replica would need a Redis channel per camera
(`frames:<device>`), with one subscription per camera per pod.

---

## Where Redis is deliberately NOT used

| Not used for | Used instead | Why |
|---|---|---|
| **Caching the live view** | The CDN (`Cache-Control`) | The data is public and identical for every viewer. The CDN stops requests before they reach the servers; a Redis cache would only save a 2 ms database query |
| **Alerts and anything durable** | Postgres rows | Pub/sub can't retry, acknowledge or audit. Silent loss is the one unacceptable failure |
| **Waking the detector** (LISTEN/NOTIFY → Redis) | Polling Postgres | The domain doesn't need sub-second reaction, and the poll costs microseconds |
| **Storing images** | S3 (+ CDN) | Already stored and cached elsewhere |

**When a Redis cache would be right:** per-user responses a CDN can't share
(for example "my cameras" filtered by permissions), or expensive aggregates
(for example a week of cloud statistics).

---

## Deployment (`infra/k8s/components/skycam-v2/redis.yaml`)

| Setting | Value | Why |
|---|---|---|
| Instances | **1** (`Recreate`, never two) | Two would be two separate brokers, each holding half the queue |
| Persistence | **None** (`--save ""`, `--appendonly no`) | Nothing durable lives here; everything is rebuilt from Postgres |
| Memory policy | **`noeviction`**, 200 MB | When full, writes fail loudly. Under LRU, Redis would silently drop a queued task or a presence key |
| Databases | 0 = detect, 1 = notify, 2 = ingest | Isolation. (Pub/sub channels ignore the database number; the `skycam:` prefix separates those) |

---

## What happens when Redis fails

| Part | Effect | Recovery |
|---|---|---|
| Detection | Beat can't enqueue; workers idle. Frames already claimed when Redis died have lost their task messages | When Redis returns, new frames flow within 3 s. Orphaned claims come back via `reclaim_stale` once older than `DETECT_STALE_CLAIM_S` (5 min). Nothing is lost; some frames score up to ~5 min late |
| Rain alerts | Same shape: polls and sends pause | Resumes on the next tick; deliveries are rows, stale claims reclaimed. Delayed, not lost |
| Ingest at startup | **Fatal** if `INGEST_REDIS_URL` is set but unreachable | Deliberate: running silently without the bus would look healthy while commands vanish |
| Ingest while running | Cross-pod commands can't be routed; rate limiting fails open; sign-in returns 500 | **Uploads, frames and telemetry are unaffected**: that path never touches Redis |
| Cameras' connections | Stay up. Presence renewals fail and keys expire after 30 s, so other pods consider the camera unreachable | Renewals resume when Redis does |

**The honest gap:** this is one Redis with no replica, and it now carries
control-plane state (presence, commands), not just the queues. It's a single
point of failure for operator commands and sign-in. A replica, or a managed
Redis, is item 7 in SCALING.md's remaining work.

### Next to learn, if time allows: Redis replication and failover
Goal: when the one Redis crashes, a replica takes over and the platform keeps
routing commands and signing people in, instead of waiting for a restart.

What to learn and build, in order:
1. **Replication.** One primary takes writes, one or more replicas copy it
   (`replicaof`). On its own this gives a spare copy but no automatic switch.
2. **Redis Sentinel for automatic failover.** Three Sentinel processes watch the
   primary. If it dies, they agree on it (a quorum) and promote a replica.
   Clients ask Sentinel "who is primary now?" instead of using a fixed address
   (go-redis: `NewFailoverClient`; Celery: a `sentinel://` broker URL).
3. **What failover does to *this* project specifically,** which is the part
   worth testing rather than assuming:
   - **Pub/sub subscriptions don't move** to the new primary. Each ingest pod
     must resubscribe to its `cmd:` and `ack:` channels after a failover.
   - **Replication is asynchronous**, so the last moments of writes can be lost.
     Here that's acceptable by design (presence keys renew within 10 s,
     rate-limit buckets refill, a lost sign-in is retried, queue messages are
     re-derived from Postgres), but it should be measured.
   - **Persistence:** with no persistence, a primary that restarts *empty* and
     is still treated as primary can wipe its replicas. Sentinel setups need
     either persistence or care with automatic restarts.
4. **Test it like the rest of the project:** kill the primary under load (the
   fake Pi uploading, commands crossing pods, sign-ins running) and measure how
   long until commands route again and what, if anything, was lost.

Not needed at this scale: **Redis Cluster** (sharding data across several
primaries). The data here is tiny; the problem is availability, not size.
**Alternative:** a managed Redis (for example AWS ElastiCache, or a hosted
provider) does steps 1–2 for you; steps 3–4 still apply.

---

## Interview version
> "Redis does two different jobs. It's the Celery broker for two queues kept
> apart so a detection backlog can't delay a rain alert. And it's the shared
> state between Go ingest pods: presence keys with expiry plus pub/sub for
> routing commands, a Lua token bucket so N pods enforce one rate limit, and
> GETDEL for single-use sign-in state. Nothing durable lives in it. Detection
> state and alerts are Postgres rows, so Redis runs without persistence and a
> Redis loss delays work but never loses it. And I deliberately didn't use it
> as a response cache: the live view is public, so the CDN absorbs it before it
> reaches us."

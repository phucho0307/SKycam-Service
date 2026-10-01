# Cloud Detection & Redis — Design Notes

Why cloud detection is built the way it is: a Redis queue behind a pool of Celery
workers, and what changes when the trigger moves from polling to Postgres
`LISTEN/NOTIFY`. Companion to `SKYCAM_WRITEUP.md` and `GO_GRPC_AND_POSTGRES.md`.

**Built:** the detector, Celery workers, the Redis broker, Beat polling.
**Not built:** retries, a DLQ, idempotent alarm writes, indexes, and any deployment —
the worker runs on a laptop only. Details in "Known issues" below.

---

## Redis is both — it's a general in-memory store

Redis isn't "for" caching *or* queuing — it's a **fast in-memory key-value store**
with rich data structures (strings, lists, hashes, sets, streams, pub/sub). People
use that one tool for several patterns. The two relevant here:

| Use | What it does | Typical for |
|---|---|---|
| **Cache** | store a computed/fetched result temporarily (with a TTL) so you don't recompute/refetch | read-heavy endpoints, cutting DB/S3 load |
| **Queue / broker** | pass work between services — producer pushes a job, worker pops and processes it | async background processing, decoupling services |

For cloud detection, you'd use the **queue** pattern. (You could also use Redis as a
cache elsewhere — more on that at the end.)

## How Redis (as a queue) fits cloud detection

The point is to **not block the Pi's upload** on a potentially slow detection step.
Instead of skycam calling detection inline, it drops a job on a Redis queue and
returns immediately; a worker processes it in the background. *(As built, that worker
is Python/Celery — this section was drafted when a Go worker was still the plan.)*

```
Pi ──upload──► skycam service
                  ├─ store FITS+preview → S3
                  ├─ store metadata → MongoDB
                  ├─ LPUSH frame_id → Redis queue "detect"     ← drops a job
                  └─ return 200 to Pi   (fast — doesn't wait for detection)

                         Redis "detect" queue:  [id3][id2][id1]
                                                        │
Celery worker (Python) ──BRPOP "detect"──► gets frame_id ─┘
                  ├─ fetch the preview/frame
                  ├─ run detection (brightness / star-count / ML) → cloud_score
                  ├─ update the Mongo `frames` doc (cloud_score, is_cloudy)
                  └─ if cloudy → create an `alarms` record (→ notify later)
```

**Mechanically:** the queue is just a Redis list. skycam does `LPUSH detect <frame_id>`
(push a job); the worker does `BRPOP detect` (blocking pop — it sleeps until a job
arrives, then grabs it). That's the whole broker. (For a more robust version later,
Redis Streams add acknowledgements + multiple workers; plain Pub/Sub is not right for
a work queue because messages are lost if no worker is listening.)

## Why the queue beats calling detection directly

- **Fast uploads:** the Pi gets its 200 right away; detection (especially if it's ML)
  runs after, not in the request path.
- **Independent scaling:** detection slow? Run 3 worker copies all BRPOP-ing the same
  queue.
- **Fault isolation:** if the detection worker is down, uploads still succeed — jobs
  just buffer in Redis until the worker comes back.
- **Decoupling:** skycam doesn't know or care what detection does; it just drops an
  id. Classic microservice separation.

## Redis as a cache — considered, not implemented

The same Redis could cache `/skycam/frames/latest` and its presigned URL with a short
TTL, so many browsers refreshing don't hammer the database and S3 every time. **Not
built** — with the read path indexed, the query is cheap enough that caching would add
an invalidation problem for no measured gain. Worth revisiting when there are enough
concurrent viewers to measure.

So: **Redis = both**, and here it is a **queue** that decouples slow cloud detection
from fast uploads.

---

## Redis queue vs pub/sub

The core difference is **who gets the message** and **what happens if no one is
listening.**

| | **Queue** (List: `LPUSH`/`BRPOP`) | **Pub/Sub** (`PUBLISH`/`SUBSCRIBE`) |
|---|---|---|
| Delivery | to **exactly one** consumer (workers compete) | to **every** subscriber (broadcast) |
| Persistence | message **stored** until a worker pops it | **none** — vanishes instantly |
| If no consumer is ready | **buffers** and waits | **dropped forever** (missed) |
| Model | pull — worker asks for the next job | push — Redis shoves it to all listeners |
| Good for | **work queues** (do each job once) | **live notifications** (miss-it-if-away is fine) |

**Analogy:**
- **Queue = the order spike at a diner.** Tickets pile up; the next free cook grabs
  the next one. Each order cooked **once**. Cooks all busy? Tickets **wait**.
- **Pub/Sub = a loudspeaker announcement.** Everyone hears it *at that instant*.
  Stepped out? You **missed it** — no replay. Everyone hears the *same* thing.

**Why cloud detection needs a queue, not pub/sub:**
- Each frame must be detected **once, by some worker, and never lost** → queue.
- Pub/sub breaks it two ways: if the worker is **restarting** when a frame arrives,
  the message is **gone** → that frame never gets detected; and with **3 workers**,
  *all three* receive the same message → the frame is detected **3×** (duplicate work).

**Where pub/sub *is* right here:** live GUI updates — when a new frame lands,
`PUBLISH` a "new_frame" event so connected browsers refresh in real time. Ephemeral +
broadcast + "fine to miss if not connected" = exactly its use.

**Middle ground for later:** Redis **Streams** are persistent like a queue **and**
support multiple consumer groups with acknowledgements — the robust upgrade when a
plain list isn't enough.

---

## Redis Streams vs Kafka

Both share the same core idea — an **append-only log** with **consumer groups**,
offsets, acknowledgements, and replay. The difference is **scale, durability, and
operational weight.**

| | **Redis Streams** | **Kafka** |
|---|---|---|
| What it is | a data structure **inside Redis** | a distributed **streaming platform** (broker cluster) |
| Storage | **in-memory** first (RAM-bound; trim with `MAXLEN`) | **on disk**, replicated across brokers |
| Scale | **single node** (one stream lives on one node) | **horizontal** — partitions across brokers → millions/sec |
| Retention | keep it **small**; not for big history | **long** (hours→forever); store & replay |
| Durability | weaker (RDB/AOF, replica failover) | **strong** (replicated, survives broker loss) |
| Ops | **trivial** (just run Redis) | **heavy** (cluster, KRaft/ZooKeeper, tuning) |
| Ecosystem | minimal | huge (Connect, Kafka Streams, ksqlDB) |
| Latency | very low (in-memory) | low, optimized for **throughput** |

**Analogy:** Redis Streams = a shared logbook in one room (fast, limited pages, one
machine). Kafka = a replicated archive across many buildings (keeps everything,
firehose scale, needs an ops team).

**For this project (a preview every 2s, a 25MB FITS every 60s):** Redis Streams already gives Kafka-like semantics
at tiny ops cost. Kafka would be renting a distributed warehouse to store one
notebook — it only wins at genuine firehose scale with long retention/replay.

## Is Redis + Celery a production pattern?

Yes — Redis as a job broker behind a worker pool is one of the most common shapes in
production (Celery, Sidekiq, BullMQ), and every cloud sells it managed (ElastiCache,
Memorystore, Azure Cache). If this platform moves to AWS, **ElastiCache** is the
no-ops way to keep the same design.

One currency note: Redis Inc. relicensed in 2024, and **Valkey** is the BSD-licensed
fork (drop-in, same protocol) that AWS and Google now offer managed. Saying "Redis or
Valkey" is the current phrasing.

---

## Known issues in the current implementation

Two real defects found reading the shipped code. Both are cheap to fix and both are
worth fixing *before* any database migration, so the migration gets measured against a
fair baseline.

### 1. The Beat scan is a full collection scan

`detect/tasks.py` runs this every `DETECT_SCAN_INTERVAL_S` (default **3s**):

```python
_db.frames.find({"cloud_score": {"$exists": False}}).limit(SCAN_LIMIT)
```

`frames` has **no indexes** — the only `create_index` in the repo is in
`backend/crates/release-proxy/src/db.rs`. So this predicate is unindexed and Mongo
scans the whole collection, every 3 seconds, degrading linearly as frames accumulate.

At the 2s preview cadence that's roughly **43,000 docs/day**. After a week Mongo is
scanning ~300,000 documents every 3 seconds to find the handful that are unscored.

**Fix:** a partial index on `cloud_score` so the query is O(unscored) instead of
O(total). Unscored is normally single digits.

### 1b. The read path is unindexed too — and this one can hard-fail

Same root cause, worse consequence. `backend/crates/skycam/src/routes/read.rs`:

```rust
.find_one(doc! {}).sort(doc! { "captured_at": -1 })   // latest frame
.find(filter).sort(doc! { "captured_at": -1 })        // ?from&to range
```

With no index on `captured_at`, "give me the latest frame" is a **full collection scan
plus an in-memory sort** — to return one document. The GUI Live View polls it every 3s,
so this is user-facing latency, not just background load.

It also doesn't merely get slow. Large unindexed sorts can exceed MongoDB's blocking-sort
memory limit and **error outright** rather than degrade. Worth verifying against our
Mongo 7 settings, but the direction is clear: this query gets worse until it breaks.

Telemetry has the identical pattern on `recorded_at`.

### 2. Poison-pill loop on the error path

In `detect_frame`, the *no-preview* path deliberately writes a `None` sentinel so the
frame won't be picked up again. The *error* path doesn't. If the S3 fetch or the
`cv2` decode throws, `cloud_score` is never set — so the next scan re-enqueues the
same frame 3 seconds later, forever.

With `SCAN_LIMIT=50`, **fifty permanently-failing frames starve out every real one**.

**Fix:** catch failures and write a sentinel (or a `detect_error` field + attempt
counter) so a frame can't be retried indefinitely.

### 3. Alarm writes aren't idempotent

`detect_frame` ends with a plain `_db.alarms.insert_one(...)`. The `update_one` before
it is idempotent (`$set` twice = same result); the insert is not.

This is reachable **today**, without event-driven. `task_acks_late=True` means a worker
that dies mid-task gets its message redelivered and re-runs from the top — producing a
**second alarm record for one cloudy frame**. Under any at-least-once broker this stops
being an edge case and becomes routine.

**Fix:** unique index on `alarms.frame_id`, and upsert on `{frame_id, kind}` instead of
a raw insert.

---

## Required indexes (none of these exist yet)

| Collection | Index | Serves |
|---|---|---|
| `frames` | `{captured_at: -1}` | latest-frame + `?from&to` range (issue 1b) |
| `telemetry` | `{recorded_at: -1}` | telemetry range queries |
| `frames` | partial on `cloud_score` (unscored only) | detection scan / reconciliation sweep |
| `alarms` | unique on `frame_id` | idempotent alarm writes (issue 3) |

The partial index is tiny — it only contains unscored docs, normally single digits — so
it's close to free to maintain.

---

## Messaging vocabulary: what we already have vs. what we'd build

Acks, consumer groups and DLQs sound Kafka-specific but they're **generic messaging
patterns**. Redis Streams (Redis 5.0, 2018) imported them deliberately from Kafka's
design. Note DLQ is *not* a Kafka feature — Kafka makes you build it; native DLQ is a
RabbitMQ/SQS thing.

| | Competing consumers | Per-message ack | Redelivery on crash | DLQ |
|---|---|---|---|---|
| Redis **list** (`LPUSH`/`BRPOP`) | yes, naturally | no | no — pop and die = lost | no |
| Redis **Streams** | yes (`XREADGROUP`) | yes (`XACK`) | yes (`XAUTOCLAIM`) | DIY |
| Kafka | yes | offsets, not per-message | yes | DIY |
| RabbitMQ / SQS | yes | yes | yes | built in |
| **Celery** (our stack) | yes | yes | yes | **DIY — missing** |

### Consumer groups: already solved

Celery over a Redis list gives competing consumers for free — N workers blocking on the
same key, each message to exactly one. `task_acks_late=True` is already set, so a dead
worker's task is redelivered. To scale, just add workers:

```bash
celery -A celery_app worker --concurrency=4   # or bump k8s replicas
```

`worker_prefetch_multiplier=1` (already set) is correct here — it stops one worker
greedily reserving a batch while others idle, which matters for slow tasks like image
decode.

### DLQ: the actual gap

```python
@app.task(bind=True, max_retries=3, name="tasks.detect_frame")
def detect_frame(self, frame_id):
    try:
        ...
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            _db.detect_failures.insert_one({
                "frame_id": frame_id, "error": str(exc),
                "failed_at": datetime.now(timezone.utc),
            })
            _db.frames.update_one({"_id": ObjectId(frame_id)},
                                  {"$set": {"cloud_score": None, "is_cloudy": None}})
            return {"frame_id": frame_id, "dead_lettered": True}
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)
```

That sentinel write also fixes the poison-pill loop (issue 2) — the frame stops being
rescanned, and we keep a visible record of *why* rather than losing it silently. One
change closes both.

### Do we need Redis Streams?

Not for this. Celery already provides the three things that matter (competing consumers,
acks, crash redelivery). Streams would earn their place only if we dropped Celery for the
raw primitives, or wanted **replay** — re-running detection over last week's frames with
recalibrated thresholds. That last one is plausible once we start tuning on real night
data.

### Where Streams *would* earn their place: live telemetry fanout — PROPOSED, NOT BUILT

The one genuine gap Streams fit. There is no live dashboard today, and the GUI polls
`/frames/latest` every 3s — the naive shape this would replace.

Telemetry arrives every 2s per device on the gRPC `DeviceSession`. Each reading writes to
Postgres (durable, source of truth) **and** `XADD telemetry:{device_id} MAXLEN ~ 1000`.
A WebSocket server tails the streams with `XREADGROUP` and fans out to browser dashboards.

Why each piece:
- **`MAXLEN ~ 1000`** bounds memory per device. The `~` makes trimming approximate, which
  lets Redis drop whole nodes instead of counting exactly — cheaper, and 1000-ish is as
  good as 1000 for a dashboard.
- **Consumer groups** let several WebSocket servers share the load without each one
  delivering every message.
- **Streams, not pub/sub**, because pub/sub is fire-and-forget: a dashboard that
  reconnects has missed whatever arrived while it was gone. A bounded log lets it replay
  the last N and catch up.
- **A third `TelemetryStore` adapter**, not a rewrite — the interface already has Postgres
  and DynamoDB implementations and a `both` dual-write mode.

**Say this before anyone else does: it is a dual write**, the exact thing transactional
`pg_notify` exists to avoid. It is acceptable *here* for a specific reason — telemetry is
lossy by nature. The next reading is 2 seconds away, so a dropped publish costs a dashboard
one stale tick. A dropped `frame_ingested` event means a frame is never scored, and that is
why *that* one is inside the transaction. Different consistency needs, different mechanism;
the same split, one level down.

**Scale honesty:** one device at 0.5 msg/s. This is the shape market-data fanout uses, and
the volume here does not require it. Knowing which part would bind first (WebSocket server
fan-out, not Redis) is worth more than inflating the number.

**Not built:** no WebSocket server, no Redis Streams adapter, no dashboard.

---

## The dual-write problem (read before building event-driven)

Writing to Mongo and publishing to Redis are **two operations with no transaction across
them**:

```
write frame to Mongo   ✓
   ← process dies here
LPUSH to Redis         ✗ never happens
```

That frame exists and no event was emitted; nothing will ever process it. Not rare —
every deploy, OOM kill, and node drain is a chance to hit it. Reordering doesn't help
(publish first → worker gets an id for a doc that isn't written yet).

**The fix is CDC — let the database emit the events.** One write, one source of truth, no
gap to die in.

- **DynamoDB Streams** — the real skycam upside of the platform migration.
- **MongoDB Change Streams** — *available to us without migrating*, with one catch:
  change streams require a replica set, and `infra/k8s/base/mongodb.yaml` runs `mongo:7`
  standalone with no `--replSet`. A **single-node replica set** is the normal way to
  unlock them — add `--replSet rs0`, run `rs.initiate()` once, same single pod.

Worth having in hand for the DynamoDB conversation: *event-driven detection does not
require migrating off Mongo.*

## Poll vs event-driven: the honest trade

The current design is poll-based, and that was the right call for shipping. But the
trade is more subtle than "push is faster," and the subtlety matters if detection ever
moves to events (Redis Streams, or DynamoDB Streams if the platform migrates).

| | **Poll** (current) | **Event-driven** |
|---|---|---|
| Latency | bounded by scan interval (~3s) | near zero |
| Missed work | **self-healing** — next scan picks it up | **lost silently** unless you add a backstop |
| Failure mode | loud (frame visibly unscored, retried) | quiet (event dropped, nothing notices) |
| DB load | constant scan pressure | none on the hot path |
| Ops | trivial | needs DLQ + reconciliation |

**The key asymmetry:** polling is *self-healing*. A frame that gets missed for any
reason is picked up on the next scan automatically. Event-driven is not — if the event
is dropped (worker dies mid-task, consumer outage, retention window expires, bad
deploy drops a batch), that frame stays unscored **forever and nothing ever notices**.

Two things are therefore mandatory before going event-driven:

### Reconciliation sweep

A slow periodic job that asks *"did anything get missed?"* and re-enqueues it. Events
do the fast path; the sweep catches whatever fell through.

It is **the current Beat scan, demoted** — from every 3s to every ~5min, and from
primary mechanism to safety net. Note this means the index from issue #1 is still
wanted; it just serves a sweep every few minutes instead of a query every 3 seconds.

### Dead letter queue (DLQ)

Where a message goes after it exhausts its retries. Without one, a poison frame is
**silently dropped** after N attempts — which trades the loud failure loop of issue #2
for silent data loss. For an observatory that's the worse failure.

**So the accurate claim** (worth stating carefully if pitching this): event-driven
removes the *hot* 3-second scan and the latency it costs, and demotes the index to
serving a reconciliation sweep. It does **not** remove index design as a concern.

## What the DynamoDB migration means for detection

**Where it landed:** `telemetry` moved to DynamoDB; `frames` stayed in Postgres. See
`DYNAMODB_TELEMETRY_MIGRATION.md`. That split matters here, because **detection reads
and writes `frames`** — so the pipeline is unaffected, and the transactional
`NOTIFY frame_ingested` that replaces Beat's polling is still possible.

Had `frames` moved instead, three things would have followed:

- **`detect_status = 'pending'` has no efficient DynamoDB equivalent.** The reconciliation
  sweep would need a sparse GSI on a `pending` attribute, deleted on scoring.
- **Sort keys must be designed upfront** for the `?from&to` range queries — no
  retrofitting an index later.
- **DynamoDB Streams would replace `NOTIFY`** as the trigger. Workable, but back to
  events that can be lost, rather than one that commits with the row.

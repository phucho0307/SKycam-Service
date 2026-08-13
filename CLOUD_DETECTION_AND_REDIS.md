# Cloud Detection & Redis — Design Notes

Forward-looking design notes for the next feature (cloud detection) and how Redis
fits. Companion to `SKYCAM_WRITEUP.md`.

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
returns immediately; the Go worker processes it in the background.

```
Pi ──upload──► skycam service
                  ├─ store FITS+preview → S3
                  ├─ store metadata → MongoDB
                  ├─ LPUSH frame_id → Redis queue "detect"     ← drops a job
                  └─ return 200 to Pi   (fast — doesn't wait for detection)

                         Redis "detect" queue:  [id3][id2][id1]
                                                        │
Go detection worker ──BRPOP "detect"──► gets frame_id ─┘
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

## Honest note: you might not need Redis on day one

The simplest cloud-detection MVP needs **no broker at all** — two options:

1. skycam calls the Go detector **synchronously** after upload (fine if detection is
   fast), or
2. the Go detector **polls Mongo** for frames missing a `cloud_score`.

Redis earns its place **when detection is slow or you want clean async** — then you
add it as the queue. So the honest sequence is: **build detection first; add Redis the
moment you want it off the request path.**

## Where Redis-as-a-cache would help (ties to "multiuser")

Separately from detection, once the GUI has many users hitting `/skycam/frames/latest`
constantly, you could **cache** that response (and presigned URLs) in Redis with a
short TTL — so 100 users refreshing don't hammer Mongo/S3 every time. That's the
**cache** use of the same Redis, serving your multiuser concern.

So: **Redis = both**, and here it's a **queue** that decouples slow cloud detection
from fast uploads (and could double as a read cache later).

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

**For this project (~1 frame/min):** Redis Streams already gives Kafka-like semantics
at tiny ops cost. Kafka would be renting a distributed warehouse to store one
notebook — it only wins at genuine firehose scale with long retention/replay.

## Is Redis used in production? Yes — and what's Valkey?

Redis is one of the most-used data stores in production, almost always as the **fast
layer in front of a primary DB** (not the system of record):

- **caching** (query results, API responses, sessions) — the #1 use
- **background job queues** (Sidekiq, BullMQ, Celery/RQ)
- **rate limiting**, leaderboards, distributed locks, pub/sub real-time

Every cloud sells it managed: **AWS ElastiCache**, **Google Memorystore**, **Azure
Cache for Redis**, **Upstash**, **Redis Cloud**.

**Valkey** = the **open-source fork of Redis**. Story: Redis was BSD (open source)
for years; in **March 2024 Redis Inc. relicensed** it to a source-available license
(RSALv2/SSPL), which restricts cloud providers. In response the **Linux Foundation +
AWS, Google, Oracle, Snap, Ericsson** forked the last open version into **Valkey**
(BSD-licensed, truly open). It's a **drop-in replacement** — same RESP protocol, same
commands, existing clients just work. AWS/Google now offer Valkey managed. Cousins:
**KeyDB**, **Dragonfly**.

**Takeaway:** the "Redis pattern" is more entrenched than ever; some teams now run
**Valkey** for licensing reasons. Saying *"Redis or Valkey"* signals you're current.
If the platform goes AWS (the DynamoDB thread), **ElastiCache (Redis/Valkey)** is the
managed, no-ops way to add it.

# Go / gRPC Ingest + PostgreSQL — Design Notes

> **STATUS (2026-09-25): M1–M6 BUILT AND TESTED LOCALLY — NOT DEPLOYED.** The contract,
> resumable `UploadFrame` into S3, Postgres metadata + `NOTIFY`, the bidirectional
> `DeviceSession`, device/operator auth, and settings + commands all exist in `proto/` and
> `services/ingest/`, with integration tests passing against real MinIO and Postgres.
> **M0 (network go/no-go) is PART-RUN (2026-09-27): Traefik+TLS passes, Cloudflare blocks
> gRPC at the edge until the zone's gRPC setting is enabled** — see M0 RESULTS below.
> M8–M9 are not started. The
> *deployed* system is still Rust/Rocket + MongoDB (see `SKYCAM_WRITEUP.md`). Keep the
> distinction when describing the project: "built and tested locally" ≠ "in production".
>
> **Update (2026-09-26): the device half of the contract now exists** as a reference
> client, `tools/fake-pi/` — Python stubs generated from the same `.proto`, driving a
> real `DeviceSession` and real resumable `UploadFrame` against the Go service. 50 frames
> and 50 telemetry rows through the full path, plus a SIGKILL'd upload resumed from 5MB
> **in a different process**. It is a synthetic camera, not the real Pi: `skycam-pi/send.py`
> is still unported and still POSTs multipart HTTP to Rust. So M7 is "proven with a
> reference client", not "the Pi speaks gRPC".

Companion to `CLOUD_DETECTION_AND_REDIS.md` (detection pipeline) and
`SKYCAM_WRITEUP.md` (what's actually deployed).

---

## The problem being solved

An observatory can't image through clouds, and a dome left open in bad weather risks
the optics. So something must continuously answer: **is the sky clear right now?**

Four constraints that fight each other:

1. **The camera is unreachable.** A Pi at a remote site behind NAT — it can dial out,
   nothing can dial in. Any config change has to reach a device that cannot be addressed.
2. **Data is large and continuous.** A ~25MB FITS every 60s (ZWO ASI676MC; 1,440/day
   ≈ **36GB/day/site**) plus a JPEG preview every 2s (43,200/day). *(Verified against
   `skycam-pi/send.py`; earlier drafts wrongly said 2MB.)*
3. **Analysis must be fast enough to act on.** A cloud alarm five minutes late is useless.
4. **Nothing can be silently lost.** It's a science archive; a quietly dropped frame is a
   hole nobody notices for months.

---

## Architecture

### Today

```
Pi (Python) ──HTTP/JSON──► Rust skycam ──► Mongo + S3
GUI (TS)    ──REST───────► Rust skycam ──► Mongo + S3
```

One Rust service does both jobs. Schema drift risk is Python↔Rust.

### Planned

```
                ┌──── ONE outbound HTTP/2 connection (NAT-friendly) ────┐
                │                                                       │
                │  DeviceSession (bidi, long-lived)                     │
 Pi (Python) ───┤    telemetry + heartbeats ──►                         │
                │    ◄── settings pushes                                │
                │                                                       │
                │  UploadFrame (client-streaming, one call per frame)   │
                │    frame chunks ──►  ◄── ack                          │
                └───────────────────► Go ingest ◄───────────────────────┘
                                          │
                            ┌─────────────┴─────────────┐
                            ▼                           ▼
                     S3 (blob bytes)          Postgres (metadata)
                                                 │   │
                                                 │   └─ NOTIFY (same txn)
                                                 │            │
 GUI (TS) ──REST──► Rust api ──read──────────────┘            ▼
                                                     Python detect worker
                                                       (LISTEN, no poll)
```

**Split by responsibility:**

| | Write path (Go) | Read path (Rust) |
|---|---|---|
| Client | one Pi, long-lived connection | many browsers, short requests |
| Protocol | gRPC (browsers can't speak it) | REST (browsers can) |
| Work | stream bytes, hold connections | query, render, presign URLs |

Browsers cannot speak gRPC natively, so the read path stays REST regardless. The device
path needs a persistent connection, which REST handles badly. They were always going to
want different transports; separate services make that explicit.

---

## RPC design — which of the four types, and why

gRPC has four RPC shapes. This design uses three:

| RPC | Type | Direction | Job |
|---|---|---|---|
| `DeviceSession` | **Bidirectional** | Pi ⇄ Go | always-open control channel |
| `UploadFrame` | **Client streaming** | Pi → Go | one call per frame, chunked |
| `UpdateDeviceSettings` | **Unary** | browser (via grpc-gateway) or Rust → Go | store settings + push to device X |
| — | Server streaming | — | **not used** |

As built in `proto/skycam/v1/skycam.proto` (see the file for message definitions):

```proto
service SkycamService {
  // Opened once when the Pi boots, held open.
  // Up: hello, telemetry, heartbeats.  Down: settings + commands (abort, capture-now).
  rpc DeviceSession(stream DeviceSessionRequest) returns (stream DeviceSessionResponse);

  // One call per frame: FrameHeader (preview inline), then FitsChunks.
  // Resumable: frame_id is the key, chunks start at GetUploadStatus's offset.
  rpc UploadFrame(stream UploadFrameRequest) returns (UploadFrameResponse);

  // How many FITS bytes are durably stored (a part boundary)?
  rpc GetUploadStatus(GetUploadStatusRequest) returns (GetUploadStatusResponse);
}

service SkycamControlService {   // internal-only listener
  // Store settings and push them to the device if it's connected.
  rpc UpdateDeviceSettings(UpdateDeviceSettingsRequest) returns (UpdateDeviceSettingsResponse);
  // Reach a connected device now. Not stored: an offline device is an error.
  rpc SendCommand(SendCommandRequest) returns (SendCommandResponse);
  rpc ListConnectedDevices(ListConnectedDevicesRequest) returns (ListConnectedDevicesResponse);
}
```

- **Bidirectional** solves NAT — the Pi opens it outbound and the server pushes down it.
- **Client streaming** gives chunked, resumable, pipelined upload of 25MB frames into S3.
- **Unary** is the settings write — from the browser via grpc-gateway (option B) or from
  Rust (option A); either way Go pushes down the session only it holds.
- **Server streaming** would fit if the Pi only *received* — but it also sends telemetry
  and heartbeats, so bidi supersedes it.

### Why frames are NOT on the bidi session (control plane vs data plane)

An earlier draft put frames *and* settings on `DeviceSession`. That's wrong.

HTTP/2 multiplexes **separate streams** without blocking each other, but **within one
gRPC stream, messages are strictly ordered.** Put a 25MB frame's chunks on the bidi session
and a command queued behind them (e.g. abort exposure) waits for the whole frame — tens of
seconds on a slow uplink.

Separate RPCs fix it: each `UploadFrame` is its own HTTP/2 stream, so uploads and settings
pushes flow independently. Failure semantics are cleaner too — one bad frame errors and
retries on its own, without tearing down the control session.

**The NAT story survives**, because a gRPC channel multiplexes every RPC over **one TCP
connection**. The Pi opens one outbound connection, holds one `DeviceSession`, and fires
short-lived `UploadFrame` calls alongside. NAT still sees one connection.

**Principle:** control plane and data plane go on separate streams. Small, latency-sensitive
messages must not queue behind bulk transfers.

---

## What gRPC actually solves

> **Corrected 2026-09-21 after checking `skycam-pi/send.py`.** Two earlier claims were
> wrong: the FITS is **~25MB**, not 2MB; and the settings poll runs **once per 2s capture
> cycle** (`refresh_settings()` is called once in `run_once()`), so it's ~43,200 req/day,
> not 86,400 — the 1s throttle never binds. Worse, push gives **no visible settings
> latency win** (see #3). The ranking below reflects the corrected facts.

### 1. Resumable, non-blocking upload of 25MB frames — the real headline

A 25MB FITS every minute over school WiFi or cellular takes seconds to tens of seconds.
Two concrete problems today:

- **A failed upload restarts from zero.** Drop the link at 90% and all 25MB goes again.
- **The upload stalls detection.** `send.py` runs capture → send serially, so while a FITS
  is uploading, no previews are captured or sent. On a slow link, the cloud detector
  starves for the length of every FITS upload.

A chunked client stream makes the fixes natural — **but gRPC enables them, it doesn't
provide them. They must be designed:**

- **Resumable uploads.** The client-generated `frame_id` keys the upload; the server maps
  chunks onto S3 multipart parts (5MiB minimum part size). After a drop, the client calls
  `GetUploadStatus` and resumes from the last completed part instead of restarting 25MB.
  *(Built in M2 — S3's own part list is the record of progress.)*
- **Pipelining.** S3 upload of part N overlaps receipt of part N+1, shortening
  time-to-durable. Today the whole file lands in a `TempFile` before the S3 upload starts.
- **Integrity.** The final chunk carries a SHA-256; the server verifies before committing
  the row.

**Memory is *not* the win** — don't claim it. Rocket's `TempFile` already spools to disk,
so today's memory is bounded; the Go uploader holds `PartSize × Concurrency` in memory
(~25MiB at SDK defaults — tune down).

**And the preview stall is fixed by client-side concurrency** (upload FITS in a background
worker on the Pi), not by the protocol. REST could do that too. gRPC's part is resumability.

### 2. A contract that can't silently drift

See the schema drift section below.

### 3. A push channel — for commands, not for settings latency

The camera can't accept inbound connections, so today everything toward it is polled:
once per capture cycle, **~43,200 requests/day/device** at the 2s default.

**Be precise about what push buys.** Exposure and gain only take effect at the *next
capture*, and the poll already runs immediately before each capture. So pushing them gives
**no visible latency improvement.** Do not claim "instant settings."

Push is genuinely needed for:
- **Commands that can't wait for the next cycle** — abort a long exposure, capture now,
  restart, reload config
- **Device presence** — the server knows which devices are connected *right now*, not
  "last seen N seconds ago"
- Removing the poll — real, but ~43k tiny GETs/day is ~0.5 req/s. Minor load; don't oversell.

The bidirectional stream still solves NAT: the device opens **one outbound connection and
holds it**, commands flow down `DeviceSession`, frames go up on separate `UploadFrame`
calls over the same connection.

### 4. Multiplexing

HTTP/2 carries frame uploads, telemetry, and settings pushes over one connection
concurrently. Note the precise claim: *separate streams* don't block each other, but
messages *within* one stream are ordered — which is exactly why frames and settings are
on different RPCs rather than sharing the bidi session.

## What gRPC does NOT solve — do not claim these

- **Serialisation size.** The payload is already raw binary in a multipart body. Protobuf
  saves nothing on image bytes; metadata is a few hundred bytes either way.
- **Throughput.** ~0.4MB/s sustained (≈36GB/day of FITS), bursting to 25MB each minute.
  REST moves that fine. The problem is reliability on a bad link, not volume.
- **Upload latency.** The network is the constraint, not the protocol.
- **Settings latency.** Settings apply at the next capture either way (see #3 above).
- **Memory.** Already bounded today via `TempFile` spooling to disk.

If we claim gRPC made ingestion faster, anyone who knows the protocol will ask *how* — and
the honest answer is that it didn't. It made uploads *resumable* and gave the server a way
to *reach* the device, which is a different and better story.

**Also honest:** WebSockets would solve the NAT problem too. gRPC's advantage over a raw
WebSocket is the typed schema and generated clients — no hand-rolled message framing and
dispatch. That's "better fit," not "only option."

---

## Schema drift, and what protobuf fixes

Today the Pi builds a JSON body by hand and the server parses it by hand. Nothing connects
those two pieces of code:

- Pi sends `probe_temp` instead of `probe_temp_c` → server silently reads null, the field
  quietly disappears from the data
- A float becomes a string → parse fails at runtime, in production
- A field is added server-side and not on the Pi → no error anywhere, just missing data

With protobuf, the `.proto` is the single definition and both sides *generate* their code
from it. A rename breaks the build, not the data.

**The value is not "different languages can communicate"** — HTTP already did that. It's
that the contract is enforced by the compiler instead of by memory.

### Who gets what

| Side | Language | Talks | Gets from protobuf |
|---|---|---|---|
| Pi | Python | gRPC | generated client + types |
| Ingest | Go | gRPC | generated server + types |
| GUI | TypeScript | REST | **types only** |

Two generated clients, not three. The GUI still calls REST against the Rust API.

---

## Why Go — and why that's a separate question

**These are two independent decisions. Don't conflate them:**

1. **HTTP/JSON → gRPC/protobuf** — fixes drift, enables server push, enables streaming
2. **Rust → Go for that service** — a language choice

#1 could be done in Rust (`tonic`) with every benefit described above. When asked "why
gRPC?" the answer is NAT/push and the enforced contract. When asked "why Go?" the answer
is different — answering the second question with the first question's reasons is
noticeable.

**Defensible reasons for Go here:**
- **Goroutine-per-stream.** Long-lived concurrent connections are what Go's scheduler is
  built around. Fifty sites = fifty parked streams, no thread-pool tuning.
- **`context.Context` cancellation.** When a device's stream drops, the cancellation
  propagates into the in-flight S3 part upload and DB call automatically. (An earlier
  draft cited `io.Pipe` here; the built service doesn't use it — see M2's deviation note.)
- **Build/deploy simplicity.** Static binary, distroless image, compiles in seconds —
  relevant given Rust builds here need a 20GB WSL2 allocation to avoid OOM.
- **Honest framing:** this service's hard parts are I/O concurrency and connection
  lifecycle, not memory safety or CPU-bound work.

---

## Where Go and Rust actually talk

Mostly they don't — they share Postgres (Go writes, Rust reads). That shared-database
coupling is the weakest joint in the design: schema changes require coordinated edits in
two languages, enforced only by discipline. **Mitigation: Go owns the schema and all
migrations; Rust connects with a read-only role.**

### The one genuine cross-service call: `PushSettings`

> **As built, neither option was taken.** `SkycamControlService` runs on its own
> loopback-only listener with an operator token, so settings and commands go operator →
> Go directly, and Rust is not involved. grpc-gateway (option B) would only be needed to
> let the browser call it without a Rust hop. The replica-pinning issue below still
> applies to any push.

When someone drags the exposure slider in the web UI, the request arrives at **Rust**, but
the open connection to the device lives in **Go**:

```
Browser ──REST──► Rust api ──gRPC──► Go ingest ──stream──► Pi
                     │
                     └── writes audit row to Postgres
```

`PushSettings(device_id, settings)` — a real cross-language call with a real reason: the
service receiving the request isn't the service holding the connection.

### The replica wrinkle (know this before someone asks)

A device's stream is pinned to **one specific pod**. Rust calling the k8s Service hits a
*random* pod — four times out of five it doesn't hold that stream and the push silently
goes nowhere.

| Approach | How | Cost |
|---|---|---|
| **Single replica** | don't scale it | no HA; fine at 1–2 devices |
| **Redis pub/sub fan-out** | any pod publishes; the pod holding the stream sends | needs Redis (already run) |
| **Consistent routing** | route by `device_id` to a predictable pod | more infra, brittle on restarts |

Single replica is the honest answer at current scale, with pub/sub as the documented path
when scaling. Saying so shows we know the limit of what we built.

---

## PostgreSQL — two jobs

### 1. The data is genuinely relational

Sites own devices. Users hold roles on sites. Settings changes need an audit trail (who
changed gain, when, from what). "Which users may view this site's cameras" is a join.
Modelling that in a document store means hand-rolling integrity the database should enforce.

### 2. `LISTEN/NOTIFY` eliminates the dual-write problem

The naive event-driven pipeline writes the row, then publishes an event. Two operations,
no transaction between them. Crash in the gap — a deploy, an OOM kill, a node drain — and
the frame exists with no event. Nothing processes it; nothing notices.

```sql
BEGIN;
  INSERT INTO frames (...) VALUES (...);
  NOTIFY frame_ingested, '<frame_id>';   -- same txn, atomic with the insert
COMMIT;
```

Either both commit or neither does. The gap doesn't exist.

**Caveat:** `NOTIFY` is *not* durable. Notifications vanish if no listener is connected,
payloads cap at 8KB, nothing survives a restart. The dual-write problem is solved; the
lost-listener problem is not — hence the reconciliation sweep below.

---

## How Redis + Celery still fit

`NOTIFY` is a doorbell, not a queue. It doesn't retry, buffer, or survive a restart. So a
thin listener holds the `LISTEN` connection and enqueues to Redis on each notification:

```
Postgres NOTIFY ──► listener ──► Redis queue ──► Celery workers
```

| Component | Job |
|---|---|
| Postgres | source of truth + transactional event emission |
| `LISTEN/NOTIFY` | low-latency trigger |
| Redis | durable queue — buffers bursts, survives worker restarts |
| Celery | worker pool, bounded retries w/ backoff, DLQ, scheduling |
| S3 | blob bytes, keeping DB rows small |

**Celery Beat isn't removed — it's demoted.** It stops being the primary mechanism
(scanning every 3s) and becomes the safety net (reconciliation sweep every ~5min).

### Failure handling

| Failure | Handling |
|---|---|
| Crash between write and event | impossible — `NOTIFY` is transactional |
| No listener connected, notification lost | reconciliation sweep re-enqueues |
| Transient S3/network error | Celery retry, exponential backoff, 3 attempts |
| Permanently corrupt frame | DLQ record + sentinel — stops retrying, stays visible |
| Worker dies mid-task | `acks_late` → Redis redelivers |
| Duplicate delivery | idempotent writes — upsert on `frame_id`, unique index on alarms |
| Device connection dies silently | gRPC keepalives + exponential-backoff reconnect |
| Systemic failure (creds expired) | DLQ depth alert → fix cause → manual replay |

---

## Downsides — the honest list

### Go
- **Fourth language** (Rust, TS, Python, Go) maintained by one person.
- **`Frame` defined three times** — Rust `domain`, `.proto`, generated Go. *Mitigation:
  make `.proto` canonical and generate the Rust side from it too, from day one.*
- **Alden works in Rust.** A Go service is a component only one person can maintain or
  review. Raise this before building, not after.
- **Loses Rust's guarantees on the partial-upload path.** `Result` forces handling a
  truncated stream; `if err != nil` lets you forget.
- Fourth Dockerfile, CI job, k8s Deployment, `go.mod`.

### gRPC
- **Browsers can't speak it** — shapes the whole design; read path stays REST.
- **h2c through Cloudflare Tunnel → Traefik is the biggest schedule risk.** Needs HTTP/2
  end to end; Cloudflare's gRPC support is opt-in, Traefik needs `h2c` on the service.
  **Prove this day one with a hello-world; everything after depends on it.**
- **Debugging is worse** — no curl, need `grpcurl`, binary payloads aren't readable in logs.
- **Schema evolution is permanent** — protobuf field numbers can never be reused.
- **Long-lived bidi streams are real work** — keepalives, backoff reconnect, half-open
  detection (the Pi's connection dies silently and the server still thinks it's alive).
- **gRPC load balancing trap** — one long-lived HTTP/2 connection pins a client to one
  backend. Doesn't bite at two devices; know it before being asked.

### Postgres
- **Adds a StatefulSet while the boss is trying to remove them.** Self-managed Postgres is
  the same ops burden as self-managed Mongo. Use managed, or be ready to explain.
- **Time-series needs a plan** — 15M rows/year. BRIN indexes or monthly partitioning, not
  just a btree.
- **`LISTEN/NOTIFY` isn't durable** (see above).

### The combination
- **Two services, one database.** Schema changes need coordinated edits in two languages.
- **The transition is the hard part** — REST and gRPC both live, Mongo and Postgres both
  holding frames, dual-write then cut over. Most of the real risk lives here.

---

## Plan

### Phase 0 — Baseline (½ day)
Four indexes, the date-vs-string bug in `frames_list`, `device_id` filter on
`/frames/latest`, measure with `explain("executionStats")`. **Without before-numbers, no
later claim is defensible.**

### Phase 1 — Postgres + schema + auth (~1 week)
1. Postgres (managed preferred), sealed credentials
2. Schema: `sites`, `devices`, `users`, `roles`, `user_site_roles`, `frames`, `telemetry`,
   `device_settings`, `settings_audit`, `alarms`, `detect_failures`
3. `golang-migrate`, **owned by the Go service** since it owns writes
4. Google OIDC + JWT issuance in Rust `api`. Verification: Rust services via the `domain`
   crate — but **Go can't import a Rust crate**, so the Go service verifies the same JWT
   independently (`golang-jwt` against the same public key / JWKS)
5. Read-only Postgres role for Rust
6. If using a managed pooler (e.g. Supabase / PgBouncer in transaction mode): **`LISTEN`
   does not work through it.** The listener needs a direct connection.

### Phase 2 — Go gRPC service (~4–5 weeks part-time), milestone by milestone

Repo layout: `proto/skycam/v1/skycam.proto` (with `buf.yaml` / `buf.gen.yaml`) and a Go
module at `services/ingest/` (`cmd/ingest`, `internal/{server,session,store,blob}`,
`migrations/`, `Dockerfile`). Use `buf` for codegen, lint, and breaking-change detection.

**M0 — Prove the network path (1–3 days, GO/NO-GO).** A hello-world unary RPC is **not a
sufficient test.** Verified against the repo, four things must work:
- **Dedicated hostname** (e.g. `ingest.dev.observatory.services`). gRPC request paths look
  like `/skycam.v1.Skycam/UploadFrame`, which does **not** match the `/skycam/` Prefix rule
  in `infra/k8s/base/ingress.yaml` — it falls through to `/` and hits the **frontend**.
- **cloudflared → Traefik over HTTP/2.** The tunnel currently targets
  `http://traefik.ingress.svc.cluster.local:80` (HTTP/1.1). It needs HTTP/2 to origin, and
  whether cloudflared supports cleartext h2c origins (vs requiring TLS) must be verified.
  The Cloudflare zone's gRPC setting must be on. Public hostnames live in the Cloudflare
  dashboard (token-managed tunnel) — not in git; document them in
  `infra/k8s/cloudflare-tunnel/README.md`.
- **A long-lived *idle* bidi stream held open > 100s** — Cloudflare applies proxy timeouts;
  confirm the stream survives, and plan app-level heartbeat messages regardless.
- **A 25MB client stream end to end**, and **`pip install grpcio` on the actual Pi**
  (32-bit Pi OS may lack wheels and compile for a very long time).
If any fails, stop and resolve before building. Fallback if Cloudflare can't carry it:
wait for Mode B (direct DNS + Origin Cert) or reconsider the transport.

#### M0 RESULTS (2026-09-27) — Traefik PASS, Cloudflare BLOCKED

Run locally, not against the cluster: there is no kubectl context, SSH to
`10.101.229.77` times out from outside its network, and `https://dev.observatory.services`
returns **HTTP 530** (Cloudflare 1033, tunnel not running) — **the deployed environment is
currently down**. So the two halves were tested separately.

**Traefik + TLS + h2c: PASS, but only after fixing a 60-second stream killer.**

Traefik v3 in Docker, TLS on the entrypoint, `h2c://` to the Go service — the same scheme
the k8s Service will need. First run, the bidi `DeviceSession` was reset at *exactly* 60s,
repeatedly:

```
opened 22:06:21.631 → RST_STREAM (INTERNAL_ERROR) 22:07:21.692    60.06s
opened 22:07:22.693 → RST_STREAM (INTERNAL_ERROR) 22:08:22.692    59.99s
```

Cause: **Traefik v3 defaults `entryPoints.<name>.transport.respondingTimeouts.readTimeout`
to 60s** (v2 defaulted to no limit). It caps the lifetime of a *request*, and a long-lived
stream is one request. Setting it to `0` fixed it — the same session then held **5 minutes
with zero drops** (1 connection, 0 reconnects), carrying 150 uploads, two 24MB FITS
transfers, a `CaptureNow` command acked, and two settings pushes.

This would have been a miserable production bug: the stream churns every 60s, commands are
undeliverable during each gap, and it presents as an intermittent network fault.

**M8 requirement:** give gRPC its **own entryPoint** with `readTimeout: 0`, not the shared
web entrypoint. An unlimited read timeout on the browser-facing entrypoint is a slowloris
invitation; on a device-only entrypoint it is correct. MicroK8s' Traefik is Helm-managed, so
this is static config, not an Ingress annotation.

**Cloudflare: BLOCKED at the edge.** Through a `trycloudflare.com` quick tunnel
(`cloudflared --url http://localhost:9090 --http2-origin`), two requests differing *only* in
`Content-Type`:

| Content-Type | Result | Reached origin? |
|---|---|---|
| `application/grpc` | **403 Forbidden**, `server: cloudflare` | **no** |
| `application/octet-stream` | 502 (origin rejected as non-gRPC) | yes |

The 502 proves the tunnel path itself works; the 403 is Cloudflare's edge refusing gRPC.
**gRPC must be enabled per-zone in Cloudflare's Network settings**, and a quick tunnel runs
on Cloudflare's own zone where that cannot be set. So this is *not* evidence that
`observatory.services` will fail — it is evidence that gRPC is **off by default** and that
the toggle is mandatory, plus a reminder that the dashboard holds config git does not.

**Still unproven, and it is the real risk:** whether Cloudflare's proxy carries
**client-streaming and bidirectional** streams, and for how long, on a zone with gRPC
enabled. Cloudflare has historically documented limits on streaming gRPC. Verify before
depending on it; Mode B (direct DNS + Origin Cert, already documented in
`infra/cloudflare/README.md`) is the fallback, and it removes Cloudflare from the data path
entirely.

**Also confirmed:** `grpcio` installs and runs fine on CPython 3.14 (1.84.0) — but that was
on Windows, **not** on 32-bit Pi OS, which is still untested.

**Client change this required:** `fake_pi.py` gained `OBS_GRPC_TLS` and `OBS_GRPC_CA`, since
anything past a local h2c listener needs a verified TLS channel.

**M1 — Contract. ✅ BUILT.** `proto/skycam/v1/skycam.proto`: `SkycamService`
(`DeviceSession`, `UploadFrame`, `GetUploadStatus`) and `SkycamControlService`
(`UpdateDeviceSettings` — the settings RPC; works for either option A or B). `buf lint`
(STANDARD) passes; Go stubs via `buf generate`, Python stubs via `proto/gen-python.sh`.
Verified: a generated Python client talks to the Go server over TCP.
*Not done:* `buf breaking` in CI — moved to M8 with the other CI changes (it also needs a
baseline on `dev` to compare against).

**M2 — Resumable `UploadFrame` into S3. ✅ BUILT.** 256KB chunks cut into fixed 5MiB
multipart parts (one part buffer in memory per upload); SHA-256 of the assembled object
verified before commit; mismatch deletes it (`DataLoss`).
**Deviation from the plan:** no `io.Pipe` → `manager.Uploader`. The upload manager owns a
multipart upload for the duration of *one* call and aborts it on failure, so it cannot
resume. Instead the service manages parts directly, and **S3 is the source of truth for
progress**: the object key is deterministic per frame, so `ListMultipartUploads` +
`ListParts` recover the in-flight upload after a drop *or a server restart*, with no
separate state to keep consistent. Resume points are part boundaries.
**Done criterion met:** integration test cuts a 26MB upload at 80%, resumes from the last
completed part (15MiB), checksum matches — 8/8 repeated runs. Also: wrong offset →
`FailedPrecondition`; object assembled but row missing (crash window) → retry commits it.

**M3 — Postgres + `FrameStore`. ✅ BUILT.** `frames` table via embedded `golang-migrate`
migrations (`ingest migrate`); explicit `detect_status` column (`pending/scored/failed/
skipped`) instead of null sentinels, with a partial index on `pending`; per-device
`(device_id, captured_at DESC)` index. Blob first, then row + `pg_notify` in one
transaction; `ON CONFLICT DO NOTHING` makes retries no-ops.
**Done criterion met:** tests show NOTIFY fires exactly once per committed frame, not on a
duplicate, and **not on a rolled-back insert**. Scope note: only `frames` so far —
`devices`/`users`/roles arrive with auth (M5 / Phase 1).

**M4 — `DeviceSession`. ✅ BUILT.** `internal/session` holds the registry; the handler runs
one receive and one send goroutine per device, because gRPC allows concurrent Recv and Send
but never two Senders — so every outbound message goes through the session's queues and only
the send loop touches the stream. Settings **coalesce** (last-write-wins, so a slow device
gets the newest value rather than a backlog); commands **queue** with a bounded depth and
return `ResourceExhausted` when full. A reconnect **replaces** the older session, and
`Remove` checks identity so a late teardown cannot evict the newer one. The idle deadline
lives in the send loop's `select`, so a half-open socket is dropped without an extra goroutine.
*Bug found and fixed by the tests:* waiting for both goroutines (`errgroup.Wait`) hung a
replaced session forever — cancelling the context does **not** unblock `stream.Recv()`; only
returning from the handler makes gRPC tear the stream down. The handler now returns on the
first of {recv done, send done, context cancelled}, with buffered channels so the orphaned
loop exits cleanly.

**M5 — Auth. ✅ BUILT.** A gRPC interceptor, so no future RPC can forget the check. Devices
present a bearer token; the token decides identity, and a `device_id` in the message body is
only accepted when it agrees. Tokens are stored as SHA-256 digests (high-entropy strings, so
no password hashing is needed and a leaked table yields nothing usable) with a short cache to
keep per-frame auth off the database. Operators use a **separate** token on
`SkycamControlService`, which binds to its own loopback listener — a placeholder until Phase 2
JWT + RBAC. *Why it shipped with M4 rather than later:* the registry's last-connection-wins
rule means an unauthenticated session is a remote kill switch — anyone could open a session
claiming a device id and evict the real camera.

**M6 — Settings and commands. ✅ BUILT.** `UpdateDeviceSettings` writes the row and the audit
entry in one transaction, then pushes to the device if it happens to be connected;
`delivered=false` is a normal outcome, not an error. **The database is the source of truth and
push is only an optimisation:** a device is sent its current settings the moment it connects,
so a lost push cannot leave a camera on stale values. Commands are deliberately *not* stored —
"abort the current exposure" is meaningless an hour later — so an offline device returns
`FailedPrecondition` rather than queueing a surprise. `SendCommand` correlates the device's
`CommandAck` back to the waiting caller, and reports "sent but unacknowledged" distinctly from
failure. `ListConnectedDevices` exposes the registry (per-replica).

**M7 — Pi client. Reference client DONE (2026-09-26); real Pi port outstanding.**

`tools/fake-pi/fake_pi.py` implements the device side against the real service: three
threads (session / capture / uploader), frames spooled to disk before upload, resume via
`GetUploadStatus`, bounded retries with backoff, and a `spool/failed/` dead-letter
directory. `tools/fake-pi/control_cli.py` is the operator side. Verified end to end:

| Claim | Evidence from the run |
|---|---|
| Frames reach Postgres + S3 | 50 `frames`, 50 `telemetry`, 60 objects, 0 server errors |
| Capture doesn't wait on upload | `capture` and `uploader` interleave in the log |
| Upload resumes after a drop | `resuming … from 5.0MB of 24.0MB` |
| Resume survives losing the client | frame SIGKILL'd, resumed by a **new process** |
| Nothing is lost on failure | spool retains the frame; `recovered 1 frame(s)` on restart |
| Settings push reaches a live device | `pushed_now=True`, device logs the new values |
| A push to an offline device isn't lost | `pushed_now=False`, then delivered on reconnect |
| Commands round-trip | `CaptureNow` → off-cycle capture → `acknowledged=True` |
| No storage garbage | zero incomplete multipart uploads after every run |

Two failure modes turned out to be distinct: a client that *stops sending* half-closes the
stream and the server reports `FailedPrecondition "stream ended at N of M"`, while a client
that *dies* cancels the transport and the server logs `fits upload interrupted` with its
committed offset. Both resume correctly; only the second is what a dropped link looks like.

Still to do on the real Pi: in `send.py`, delete `refresh_settings()` (the per-cycle poll),
move `send_telemetry()` onto `DeviceSession` and `send_frame()` onto `UploadFrame`, add the
spool + uploader thread, reconnect with backoff **+ jitter** (the reference client has plain
backoff, which would synchronise a fleet after an outage), and a `USE_GRPC` flag for
fallback. Done when an overnight run survives a server restart and a network drop with the
preview cadence holding.

**M8 — Deploy (2 days).** Multi-stage Dockerfile → distroless. CI changes the current
workflow needs (verified):
- `build-and-deploy.yml` `on.push.paths` only watches `backend/**` and `frontend/**` —
  **add `services/**` and `proto/**`** or the Go service never builds
- add a matrix entry for `ingest`
- add `ingest` to the hard-coded `kustomize edit set image` list, or its tag never bumps
- add a Go test job (`go test -race`) alongside `backend-ci.yml`
Plus: Deployment (single replica), Service with Traefik h2c scheme, dedicated-host Ingress,
sealed Postgres/S3 creds, kustomize entry. **Add S3 lifecycle rules before production** —
~36GB/day/site of FITS is not optional to manage.

**M9 — Measure + cut over (2–3 days).** Metrics that match the corrected claims: resume
success after a mid-upload drop, preview cadence held during FITS uploads,
time-to-durable per frame, poll requests 43,200/day → 0, ingest p50/p99. Then retire the
REST ingest path.

### Phase 3 — Detection on LISTEN/NOTIFY (~2 days)
1. Listener process holds `LISTEN`, enqueues to Redis
2. Keep Celery for the worker pool and retries
3. DLQ + idempotent alarm writes
4. Reconciliation sweep every ~5min

### Phase 4 — Cutover (~3 days)
Dual-write, verify counts, flip reads, retire Mongo from skycam, delete REST ingest.

---

## Is there an API gateway?

**No, and a dedicated one isn't needed.** Every gateway job already has an owner:

| Gateway job | Who does it |
|---|---|
| TLS termination, DDoS protection | Cloudflare (edge) |
| Routing by host / path | Traefik Ingress |
| Auth | each service verifies the JWT itself |
| Rate limiting, body limits | Traefik middleware, if ever needed |

The Go service is an **ingest backend**, not a gateway. A Go gateway in front of everything
would add a hop and a single point of failure while duplicating what Traefik already does.
Worth adding only with many services and cross-cutting policy (per-client quotas, request
aggregation, protocol translation for external API consumers) — none of which exist here.

### Where a Go "API" legitimately fits: grpc-gateway

`grpc-gateway` generates a REST/JSON reverse proxy from annotations in the `.proto`, served
by the Go service itself. That creates a real choice for how settings writes reach the device:

| | **A. `PushSettings` from Rust** | **B. grpc-gateway on Go** (recommended) |
|---|---|---|
| Path | browser → Rust → internal gRPC → Go | browser → Traefik → Go REST → session |
| Hops | 2 services | 1 service |
| Security surface | internal-only listener + service-to-service auth | none extra; Go verifies the user JWT |
| Owner of device state | split across Rust + Go | Go alone |
| Go↔Rust gRPC call | yes | **removed** |

**Recommendation: B.** The service that holds the device connection should own device
settings. It removes an internal RPC and the port that must be locked down. The trade: the
Go↔Rust gRPC link disappears, and Go must verify user JWTs (needed for Go anyway).

---

## Open decisions

**Collection destinations — decide once, move once.** The failure mode is migrating
`telemetry` Mongo→Postgres in Phase 1, then Postgres→DynamoDB in Phase 3: two migrations,
two backfills, two cutovers, for one net move. Write the destination table before Phase 1's
schema work.

**Reconciling with the DynamoDB directive.** Mongo holds two data shapes; the migration is
a decomposition, not a lift-and-shift:

| | Relational | Time-series | NOTIFY survives? | Ops |
|---|---|---|---|---|
| **A. Split by workload** | Postgres | DynamoDB (telemetry) | ✅ frames stay in PG | 2 DBs |
| **B. All Postgres** | Postgres | Postgres (+partitioning) | ✅ | 1 DB |
| **C. All DynamoDB** | DynamoDB | DynamoDB | ❌ → Streams | 1 DB |
| **D. Managed Mongo (Atlas)** | Mongo | Mongo | ❌ → Change Streams | zero migration |

**A** reconciles this design with the directive: Postgres takes relational data + `frames`
(preserving transactional NOTIFY); DynamoDB takes `telemetry` (pure time-series, highest
volume, no joins, clean `PK=device_id, SK=recorded_at`). Mongo retires entirely.

**C** has a specific cost worth naming: `frames` in DynamoDB kills transactional `NOTIFY`,
and RBAC in DynamoDB means multiple round trips or hand-maintained denormalisation.

**D** deserves saying once: if the driver is purely ops burden, managed Atlas removes it
with zero code change.

**Build storage behind an interface** so the decision doesn't block work. ✅ **Done** —
`internal/store` defines `FrameStore`, `TelemetryStore`, `DeviceStore` and `SettingsStore`,
and that is exactly what made the DynamoDB work a second adapter rather than a rewrite
(`TELEMETRY_BACKEND=postgres|dynamo|both`). Ports and adapters, testable without a
database, and both backends benchmarkable instead of arguable.

**Where this landed (2026-09-25):** option **A**. Relational data and `frames` are in
Postgres — which is what keeps the transactional `NOTIFY` possible — and `telemetry` moved
to DynamoDB, migrated and verified against DynamoDB Local. See
`DYNAMODB_TELEMETRY_MIGRATION.md`.

**Questions still open for the decision-maker:**
1. What's driving it — ops burden, cost, or a broader AWS move?
2. Whole platform, or is one collection enough to prove it?
3. Is auth/RBAC data in scope? *(The sharpest question — it's the one workload DynamoDB
   handles badly, and the one we're about to build.)*

**`UpdateDeviceSettings` needs a `FieldMask` — known bug, not yet fixed.** Found on
2026-09-26 by driving the control API from `tools/fake-pi/control_cli.py`: a call that set
only `gain` wiped `exposure_ms` and `preview_gamma` to NULL, and `settings_audit` recorded
the loss. `internal/server/control.go` passes all five `optional` fields into
`UpsertSettings`, so unset becomes NULL.

The fault is in the contract, not the handler. With bare `optional` fields, unset means both
"leave it alone" and "clear it" and the server cannot distinguish them — which is exactly
what `google.protobuf.FieldMask` exists for (`update_mask: {paths: ["gain"]}`). A GUI with
five sliders that PUTs only the one the user moved would silently reset the other four.

Fixing it means adding a field to a message a running service already uses, so it wants
doing deliberately: add `update_mask`, treat an absent mask as "replace all" for backward
compatibility, and only then move callers to partial updates. Until then any caller must
read current settings and send all five fields back.

---

## Interview stories — what to say when they grill you

Every story below is about code that exists in `services/ingest/` with a passing test. The
follow-ups are the questions an interviewer who knows the material will actually ask.

---

### 1. "Tell me about a hard technical problem"

**Resumable 25MB uploads over a link that drops.**

*Setup.* The camera sends a ~25MB science frame every minute over school WiFi. If the link
dropped at 90%, the whole 25MB went again. On a slow uplink a single frame takes tens of
seconds, so retries were eating a large share of the budget.

*What I did.* Uploads arrive as 256KB gRPC chunks and get assembled into 5MiB S3 multipart
parts. After a drop, the client calls `GetUploadStatus` and resumes from the last part that
landed. **S3's own part list is the record of progress** — the object key is derived from the
frame id, so `ListMultipartUploads` + `ListParts` recover the state.

*Why that way.* The AWS SDK has an upload manager, and I couldn't use it: it owns a multipart
upload for the duration of one call and aborts on failure, so it cannot resume across
connections. Managing parts directly also meant no second copy of progress to keep in sync —
if I had tracked it in Postgres, a crash between the S3 write and the DB write would desync
them. There is exactly one source of truth and it is the one holding the bytes.

*Result.* An upload cut at 80% resumes from 15MiB with a matching SHA-256. It survives a
**server restart** too, because nothing was in server memory.

**Follow-ups they will ask:**
- *"How do you know which parts to trust?"* Walk parts from 1, stop at the first gap, and
  accept a short part only if it ends the file — every part but the last must be exactly
  5MiB. Ten unit cases cover it (gap, missing first part, short part mid-run, unknown total).
- *"What if the client lies about the size, or reuses a frame id for a different file?"*
  Stored parts exceeding the declared size return `FailedPrecondition`, and the final
  SHA-256 check catches a mismatched file: the object is deleted and the client restarts at 0.
- *"Why verify the hash by re-reading the object?"* Parts from an earlier connection never
  passed through this process, so a running hash is impossible. It costs one extra read of a
  25MB file; correctness of a science archive is worth more than that.
- *"Two uploads of the same frame at once?"* A per-frame lock returns `Aborted` and the client
  backs off. It is in-process — correct for one replica; several would need `pg_advisory_lock`.

---

### 2. "How do you make sure nothing gets lost?"

**The dual-write problem, and why `NOTIFY` lives inside the transaction.**

*Setup.* When a frame lands, two things must happen: record it, and tell the detector. The
obvious design writes the row then publishes an event. Those are two operations with no
transaction between them. A crash in the gap — a deploy, an OOM kill, a node drain — leaves a
frame that exists with no event. Nothing processes it, and **nothing notices**, which for a
science archive means a hole discovered months later.

*What I did.* The insert and `pg_notify` commit together in one transaction, so either both
happen or neither does. `ON CONFLICT DO NOTHING` keyed on the client-generated `frame_id`
makes retries no-ops, and a retry emits no second event.

*Result.* The test asserts all three properties: one event per committed frame, none on a
duplicate, and **none at all when the insert rolls back**.

**Follow-ups:**
- *"`NOTIFY` isn't durable — what if nobody is listening?"* Correct, and that is the limit of
  the design. It closes the write-then-publish gap; it does not make delivery guaranteed.
  That is why a reconciliation sweep still re-queues anything left `pending`.
- *"Why not an outbox table?"* Same guarantee, more moving parts. `NOTIFY` is already
  transactional in Postgres, and a sweep covers the lost-listener case either way.
- *"Why blob first, then the row?"* A crash after the upload leaves an orphaned object —
  harmless, reused by the retry, cleaned by a lifecycle rule. The reverse order can leave a
  row pointing at a file that does not exist, which breaks the UI and the detector.

---

### 3. "Tell me about a bug you found" — the best story here

**A cancelled context does not unblock `stream.Recv()`.**

*Setup.* Each device session runs two goroutines, one receiving and one sending, because gRPC
permits a concurrent Recv and Send but **never two concurrent Sends**. I originally waited for
both with `errgroup.Wait()`.

*The bug.* When a camera reconnected, the registry cancelled the older session's context. The
send loop returned immediately. The receive loop was parked in `stream.Recv()` — and
cancelling *my* context does not unblock it, because only returning from the handler makes
gRPC tear the stream down. So `Wait()` waited for a goroutine that was waiting for the handler
to return. The displaced client hung, and the test timed out after ten minutes.

*The fix.* The handler returns on the **first** of: receive done, send done, or context
cancelled. The loops report into buffered channels, so the orphaned one finishes and exits on
its own once gRPC closes the stream.

*Why it matters.* Sessions are long-lived, so this leaks one goroutine per reconnect —
invisible in a demo, fatal over weeks. A test now runs five connect/disconnect cycles and
asserts the goroutine count returns to baseline.

**Follow-ups:**
- *"How did you find it?"* The test hung rather than failed. Go's panic dump on timeout prints
  every goroutine stack, and the parked one was sitting in `waitOnHeader`.
- *"Why not signal the receive loop with a channel?"* It is blocked in a network read, not on
  a select. Nothing short of closing the stream unblocks it.

---

### 4. "Why did you add auth there?"

**The registry made an unauthenticated session a remote kill switch.**

*Setup.* Auth was planned for a later milestone. Then the session registry gained
last-connection-wins: a new session for a device evicts the old one, which is right when a
camera reconnects after a half-open socket.

*The realisation.* Without authentication, anyone who could reach the port could open a
session claiming to be `skycam`, **boot the real camera off**, and start receiving the
settings pushes meant for it. Upload without auth is bad; a session without auth is a
denial-of-service on physical hardware. So it shipped with the session, not after.

*What I did.* An interceptor, so no future RPC can forget the check. The token decides
identity; a `device_id` in the body is only accepted when it agrees, which also means the
storage key can never be forged. Devices and operators hold **separate** credentials, and the
operator API binds to its own loopback listener.

**Follow-ups:**
- *"Why SHA-256 and not bcrypt?"* These are 32 bytes of randomness, not passwords. There is no
  dictionary to attack, so the slow-hash argument does not apply, and bcrypt on every frame
  upload would be a real cost. A leaked table still yields nothing usable.
- *"Doesn't a cache mean revoked tokens keep working?"* Yes, for up to the TTL. That is the
  trade for keeping per-frame auth off the database; the TTL is the bound.
- *"What is still missing?"* Operator auth is a shared token — a placeholder. Real user
  accounts and RBAC are Phase 2, and that is the kind of gap to name yourself before they
  find it.

---

### 5. "How do you handle a device that's offline?"

**The database is the source of truth; pushing is an optimisation.**

*Setup.* An operator moves a slider. The camera might be connected, asleep, or behind a dead
link. A design where the push *is* the update loses the change whenever delivery fails.

*What I did.* The change is written to Postgres first, with an audit row in the same
transaction, then pushed if a session happens to exist. `delivered=false` is a normal outcome,
not an error. A device is sent its current settings **the moment it connects**, so a lost push
cannot leave a camera running stale values.

*The contrast that makes the point.* Commands are the opposite: they are **not** stored, and
an offline device gets `FailedPrecondition`. "Abort the current exposure" is meaningless an
hour later, so queueing it would be worse than refusing it.

**Follow-ups:**
- *"Why do settings coalesce but commands queue?"* Settings are last-write-wins state — a
  backlog of stale exposures helps nobody. Commands are discrete actions, so they queue, with
  a bounded depth and `ResourceExhausted` when full rather than unbounded memory.
- *"Two operators editing at once?"* The row is locked `FOR UPDATE` inside the transaction, so
  the changes serialise and the audit trail shows both.
- *"How does an operator know it landed?"* `delivered` distinguishes live push from stored,
  and `SendCommand` distinguishes acknowledged from "sent, no ack within the timeout".

---

### 6. "What did you get wrong?" — have this one ready

Two answers, both real, and volunteering them lands better than being caught.

**The frame size.** Early write-ups said 2MB. Reading `skycam-pi/send.py` showed the ZWO
ASI676MC produces **~25MB** per FITS — about 36GB/day/site. Everything downstream changed:
resumability went from nice-to-have to the main point, and retention became mandatory rather
than optional.

**The headline benefit was wrong.** I had claimed gRPC would make settings changes
instantaneous versus a 1-second poll. Reading the client showed the poll runs **once per
capture cycle, immediately before the capture** — so a pushed setting still takes effect at
the same capture. Push buys **nothing** for settings latency. It buys commands that cannot
wait, and live knowledge of which devices are connected. The request count was also ~43,200 a
day, not 86,400.

*The point to make:* I went looking in the client code rather than repeating my own design
doc, and the correction made the story smaller but true. **Do not say gRPC made ingestion
faster** — anyone who knows the protocol will ask how, and the honest answer is that it did
not. It made uploads resumable and gave the server a way to reach a device it cannot dial.

---

### 7. "Why gRPC?" and "Why Go?" — two questions, two answers

Answering the second with the first's reasons is noticeable, so keep them apart.

**Why gRPC:** the camera is behind NAT and cannot accept connections, so a bidirectional
stream the *device* opens is the only way for the server to reach it. Plus one schema
generating both the Go server and the Python client, so the two cannot silently disagree.
*Be honest:* WebSockets would also solve NAT. gRPC's edge is the typed contract and generated
clients, not raw capability.

**Why Go:** this service's hard parts are connection lifecycle and I/O concurrency, not memory
safety or CPU-bound work. Goroutine-per-connection maps directly onto long-lived sessions,
`context` cancellation propagates into the S3 upload and the DB calls, the binary deploys in a
scratch container, and it compiles in seconds — which matters when the rest of the workspace
needs 20GB of RAM to build. `tonic` would have given the same gRPC story in Rust; this was a
fit-and-iteration-speed call, not a "Rust can't".

---

### 8. The architecture question: "Why not put everything on one stream?"

HTTP/2 multiplexes **separate streams** without head-of-line blocking, but messages **within
one stream are ordered**. Putting a 25MB frame on the control stream would park an abort
command behind it for tens of seconds on a slow uplink. So frames get their own RPC, and both
share one TCP connection — which keeps the NAT story intact, since the camera still opens
exactly one connection.

**The principle:** control plane and data plane go on separate streams; small,
latency-sensitive messages must never queue behind bulk transfers.

---

### 9. "How did you test it?" — the answer that separates you

Integration tests run against **real MinIO and real Postgres**, not mocks, because the
behaviour under test *is* the storage behaviour: which parts S3 reports after a drop, and
whether Postgres delivers a notification for a rolled-back transaction. A mock would have
asserted my assumptions rather than the system's.

Worth mentioning:
- The resume test had to be made deterministic. HTTP/2 flow control lets the client run
  megabytes ahead of the server, so "cut at 80%" did not pin down how much had actually
  landed. The test now waits until the server reports three complete parts, then cuts.
- Tests share one database, so each one generates its own device id. A fixed device let rows
  from one test (a settings row) change what another saw on connect — two tests failed for
  that reason and the fix was isolation, not a sleep.
- `-race` needs a C compiler, which this Windows box lacks, so it runs in CI on Linux. Worth
  saying plainly rather than implying it ran.

---

### What to say about status — and what not to claim

**True:** "Built and tested locally against real MinIO and Postgres; not deployed." ~3,300
lines of Go, 40+ tests, three consecutive clean runs.

**Do not claim:** that it runs in production, that it replaced the REST path (the Pi still
uses REST — that is M7), that it runs on AWS (it is MinIO on a VPS), that gRPC works through
Cloudflare (untested — that is M0), or any percentage that has not been measured.

**The strongest honest framing:** *"I rebuilt the ingest path as a Go/gRPC service: resumable
25MB uploads that survive a dropped link, a transactional event so no frame is silently lost,
and a bidirectional session so the server can reach a camera behind NAT. It runs end to end
locally with a fake camera; deploying it and moving the real Pi over are the next steps."*

---

## Explaining it to non-engineers

**Situation.** An observatory photographs the night sky. Clouds ruin the photos, and an
open dome in bad weather risks the equipment. Something must answer "is the sky clear?"
all night. The camera sits at a remote site on an ordinary connection — it can reach out,
but nothing can reach in, like a phone that makes calls but can't receive them. A small
preview every 2 seconds plus a 25MB science image every minute — about 36GB a day — over a
connection that drops. Losing an image quietly means a gap in the scientific record nobody
notices for months.

**Task.** Collect images reliably, store them safely, check each for clouds within
seconds, and show results on a site where different people see different observatories.

**Action.**
- *Big uploads that survive a bad connection.* A 25MB image over flaky WiFi used to restart
  from zero if the link dropped at 90%, and while it uploaded, the camera stopped sending
  the small previews the cloud detector depends on. Now images go up in pieces that resume
  where they left off, in the background, so the previews never stop.
- *Keep one line open so the server can reach the camera.* The camera can't receive calls,
  so it used to check in every cycle — ~43,000 times a day. Now it holds one line open, and
  the server can send commands down it, like "stop this exposure," the moment they're needed.
- *Write it down and ring the bell as one action.* Recording an image and telling the
  analyser about it must be indivisible, or a crash in between loses the image silently.
- *A ticket system for the analysis.* Work queues up like order tickets; the next free
  worker takes the next one.
- *Assume things break.* Temporary glitches retry with growing pauses. Genuinely broken
  images go to a labelled pile — not deleted, not retried forever. On a slow timer the
  system re-checks its own books for anything that slipped through.

**Result.**
- A dropped connection resumes a 25MB upload instead of restarting it
- The previews that drive cloud detection keep flowing while big images upload
- The server can reach the camera with commands; ~43,000 daily check-ins gone
- Cloud detection within seconds of an image arriving — fast enough to close a dome
- No image silently lost: every one is analysed, retried, or visibly set aside

### Which claims are safe

**Safe now** (arithmetic or structural): the ~43,200/day poll figure (one per 2s cycle);
"resumes instead of restarting" (once built); "nothing is silently lost"; "the server can
reach the device."

**Do NOT claim** (checked and false): "settings apply instantly / faster" — they take
effect at the next capture either way; "flat memory from streaming" — memory was already
bounded; "86,400 requests/day" — the real figure is ~43,200; "2MB frames" — they're ~25MB.

**Needs measurement first**: anything phrased as "X% faster." Run the before/after and use
the real number — that's the claim someone will ask you to justify.

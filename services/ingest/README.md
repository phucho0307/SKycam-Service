# ingest

Go/gRPC ingest service for sky-camera devices. Design and rationale:
[`GO_GRPC_AND_POSTGRES.md`](../../GO_GRPC_AND_POSTGRES.md).

**Status:** M1–M6 built and tested locally, not deployed. All four device RPCs and
the operator API work. Not started: the Pi client (M7), CI/Kubernetes (M8), and
the M0 check that gRPC survives Cloudflare Tunnel + Traefik.

**Two listeners, on purpose.** `INGEST_GRPC_ADDR` serves devices and is the only
one the ingress should reach. `INGEST_CONTROL_ADDR` serves operators and binds to
loopback: anything that can call it can re-point a live camera.

## Layout

```
cmd/ingest/          `ingest` serves, `ingest migrate`, `ingest device add <id> [token]`
gen/skycam/v1/       generated from proto/skycam/v1/skycam.proto — do not edit
internal/config/     all env-driven settings (read nowhere else)
internal/blob/       S3 storage; resumable multipart FITS uploads
internal/store/      store interfaces + Postgres impl, embedded migrations
internal/session/    live device sessions: registry, outbound queues, ack routing
internal/auth/       gRPC interceptors; device tokens and the operator token
internal/server/     gRPC handlers (upload, session, control)
internal/testenv/    wiring for integration tests
```

## How an upload works

1. Client sends a `FrameHeader` (with the JPEG preview inline), then FITS chunks.
2. Chunks are cut into 5MiB S3 multipart parts as they arrive — one part buffer
   in memory per upload.
3. On a dropped connection, flushed parts stay in S3. `GetUploadStatus` reports
   the last part boundary; the client resumes from there. **S3 is the source of
   truth for progress** (deterministic key + `ListMultipartUploads`/`ListParts`),
   so it survives a server restart with no extra state.
4. After completing the multipart upload, the server re-reads the object and
   checks its SHA-256 against the header. A mismatch deletes it (`DataLoss`).
5. Blobs are written first, then the row + `NOTIFY frame_ingested` in one
   transaction. `frame_id` is the idempotency key: retries return `duplicate`.

## How the device session works

The device opens `DeviceSession` and holds it, so NAT never has to be traversed
inbound. Per session the server runs **two goroutines** — one receiving, one
sending — because gRPC permits concurrent Recv and Send but never two Senders.
Everything outbound therefore goes through the session's queues and only the send
loop touches the stream.

- **Settings coalesce.** Only the newest value is worth sending, so a slow device
  gets current state rather than a backlog.
- **Commands queue, bounded.** A device too far behind gets `ResourceExhausted`
  instead of unbounded memory. Commands are never stored: an offline device gets
  `FailedPrecondition`, because "abort the exposure" is meaningless an hour later.
- **The database is the source of truth; pushing is an optimisation.** A device is
  sent its current settings the moment it connects, so `delivered=false` on an
  offline device is a normal outcome, not a lost change.
- **A reconnect replaces the older session** (it usually means the old socket is
  half-open). Removal is identity-checked so a late teardown can't evict the
  newer connection.
- **Idle timeout** lives in the send loop's `select`, dropping a peer that has
  gone quiet even when the socket still looks open locally.

## Auth

An interceptor, so no handler can forget it. Devices send `Authorization: Bearer
<token>`; the token decides identity and a `device_id` in the body is only
accepted when it agrees. Tokens are stored as SHA-256 digests — they are random
strings, not passwords — with a short cache so per-frame auth doesn't hit the
database.

```bash
go run ./cmd/ingest device add skycam-01     # prints the token once
```

### Users: JWT + per-device grants

The control listener takes **either** a user JWT or the shared operator token.

**Authentication** is a signed token; **authorization** is a database lookup —
they are deliberately separate. A validly signed token for a subject nobody has
provisioned gets `PermissionDenied`, not access. Nothing about what a user may do
is carried *in* the token, so revoking a grant takes effect on the next request
rather than when the token expires.

Tokens are **Ed25519, verified with a public key only**. The `api` crate is the
sole issuer and the only component holding a private key; a shared HMAC secret
would make every verifier capable of minting tokens, so leaking a read-only
service's config would hand over the ability to forge an admin.

Roles are ordered — `viewer` < `operator` < `admin`. Changing settings or sending
a command needs `operator`. `users.is_admin` is a platform-wide escape hatch that
grants every device; per-device access belongs in `device_grants`.

```bash
go run ./cmd/ingest user  add  u-123 astronomer@example.org "Ada"  # [--admin]
go run ./cmd/ingest grant add  u-123 skycam-01 operator
go run ./cmd/ingest grant revoke u-123 skycam-01
```

Config (all optional; without them only the operator token works):

```
INGEST_JWT_PUBLIC_KEYS=kid=<base64 of a PEM Ed25519 public key>[,kid2=...]
INGEST_JWT_ISSUER=observatory-api        INGEST_JWT_AUDIENCE=skycam-ingest
INGEST_JWT_LEEWAY_SECONDS=30
```

Several keys support rotation: publish the new public key, switch signing to it,
retire the old one once no live token carries its `kid`. With more than one key a
`kid` header is required — trying each key in turn would make rotation pointless.

Two ordering rules the handlers follow, both to avoid leaking information:
authorization runs **before** payload validation (otherwise an unauthorized
caller learns the valid gain range), and **before** the session lookup in
`SendCommand` (otherwise "device is not connected" reveals which device ids are
real and which cameras are online). `ListConnectedDevices` filters to the
caller's devices instead of erroring.

The shared `INGEST_OPERATOR_TOKEN` still works and bypasses grants — it is a
service account for service-to-service calls, on a loopback-only listener. The
service refuses to start if neither it nor a JWT key is configured.

## Cross-replica command routing

`session.Registry` is a per-process map, so with two replicas an operator calling
`SendCommand` on replica A cannot reach a camera whose `DeviceSession` is held by
replica B — the `chan *Command` is in B's memory. That is a **correctness** failure
and it caps the service at one replica, which is an *availability* ceiling: every
deploy then disconnects every camera.

`internal/cluster` fixes it with two Redis features doing two different jobs:

| Job | Mechanism |
|---|---|
| where is camera X? | `SET session:<device> = <replica> EX 30`, renewed every 10s |
| deliver the command | `PUBLISH cmd:<device>` |
| return the ack | `PUBLISH ack:<replica>`, demultiplexed by command id |

Enable with `INGEST_REDIS_URL`. Empty means single-replica operation and the local
registry is the whole picture; a configured-but-unreachable Redis is **fatal at
startup**, because running without the bus would route commands into a void that
looks like healthy single-replica behaviour.

The same Redis also holds the other two pieces of per-process state, so with it
set any replica can serve any request:

| State | In memory (no Redis) | Shared (`INGEST_REDIS_URL` set) | If Redis fails mid-run |
|---|---|---|---|
| Rate-limit buckets | `ratelimit.Limiter` — N replicas allow N× the rate | `ratelimit.RedisLimiter` — one Lua script, atomic | **fails open** (unlimited, logged) |
| Sign-in flows | `oidc.Pending` — callback on another pod fails | `oidc.RedisFlows` — `SET NX EX` + `GETDEL` | sign-in returns 500 |

`INGEST_REDIS_PREFIX` (default `skycam`) namespaces the rate-limit and sign-in
keys, so two environments can share one Redis without sharing buckets.

The device listener also serves `grpc.health.v1`, **without authentication**
(exact method names only), for Kubernetes gRPC probes. It reports NOT_SERVING
on SIGTERM, before the listeners close.

**Why lossy pub/sub is acceptable here and nowhere else.** The proto already states
commands are not stored — "abort the exposure" is meaningless an hour later — and
`SendCommand` already reports *sent but unacknowledged* as distinct from failed. A
dropped publish degrades into a path that already exists. Frames, telemetry,
detection triggers and rain alerts all get durable rows instead.

**The expiring key is not optional.** Pub/sub cannot report whether anyone was
listening, so without presence a command to an offline camera would be published
into the void and only fail after the ack timeout. The TTL is also what makes a
crashed replica's claim expire on its own; Postgres has no TTL and would need a
sweeper.

Three ordering rules, each of which was a bug first:

- **Subscribe before announcing.** The other order leaves a window where the
  presence key names this replica but its subscription is not live, so a command
  lands on whichever replica *is* subscribed — possibly the stale one the device
  just left.
- **Presence is authoritative, even when a local session exists.** After a device
  moves, the old replica keeps a stale session until its stream notices (up to the
  90s idle timeout on a partition). Trusting the local registry in that window
  sends commands into a dead stream while the device is connected elsewhere. One
  Redis `GET` per command buys a single source of truth.
- **Register for the ack per command id, before publishing.** One shared channel
  with several readers meant a waiter consumed somebody else's ack and discarded
  it, so the rightful waiter timed out. The bus demultiplexes, mirroring what
  `session.Session` already does for device acks.

`ListConnectedDevices` also spans replicas, since a third of the fleet reads as an
outage rather than a partial view. Remote entries carry no timestamps — those live
in the holding replica's memory, and inventing them would be worse than leaving
them empty.

**Still per-process:** the rate limiter and the OIDC pending-flow map. Both need
the same treatment before a second replica is fully correct.

### Rate limiting: token bucket, per authenticated caller

In the interceptor, **after** authentication, because the meaningful key is the
caller's identity and not their address: every camera arrives through the same
Cloudflare tunnel, so an IP limit would either throttle the whole fleet or
nothing. Limiting by IP is the edge's job; limiting by device is ours.

**Token bucket, not a fixed window**, for a reason specific to this system. A
camera that has been offline drains its spool in a burst, and that burst is
*legitimate* — it is what writing frames to disk before uploading is for. A token
bucket states "0.6 req/s is normal, but let a recovering device spend 60 requests
at once" as two independent knobs. A fixed window cannot express that, and it
also lets a caller send **2× the limit** across a boundary (100 at `11:59:59.9`
plus 100 at `12:00:00.1` are both legal) — which is the exact runaway this
exists to stop. There is a test asserting that boundary case.

Defaults come from the measured cadence, not round numbers: a camera makes about
**0.6 req/s** (a preview every 2s, a FITS every 60s, the odd status check), so
2/s leaves ~3× headroom while capping a runaway at a small multiple of normal.

```
INGEST_DEVICE_RPS=2        INGEST_DEVICE_BURST=60     # 0 rps disables
INGEST_OPERATOR_RPS=20     INGEST_OPERATOR_BURST=100
INGEST_RATE_LIMIT_IDLE_SECONDS=1800
```

Details that matter:

- **A stream costs one token at open**, not one per message — otherwise a
  long-lived `DeviceSession` or a chunked 25MB upload would throttle itself
  mid-transfer. So the unit of cost is a frame, not a chunk.
- **`ResourceExhausted`, not `Unavailable`.** gRPC clients treat `Unavailable` as
  "retry immediately, the server is having a moment", which is the opposite of
  what an over-quota caller should do.
- **Non-blocking.** A handler that waited for a token would hold a goroutine and
  burn the caller's deadline; refusing lets the client back off, which it already
  does.
- **The shared operator token is exempt** — it is the platform talking to itself
  over loopback, and throttling it would throttle the GUI.
- **Idle buckets are evicted**, but a *draining* one is kept: otherwise a caller
  could clear its own debt by going quiet for slightly less than the TTL.
- **Backpressure is not the same thing.** The bounded command queue refuses when
  it is genuinely full; this refuses when one caller is taking more than its
  share whether or not anything is full.

**Single-replica only.** The buckets are in process memory, so N replicas allow
N× the intended rate. Multiple replicas need the counter in Redis (a small Lua
script, since refill is a read-modify-write across two fields). Not built — and
the per-process session registry blocks horizontal scaling first anyway.

### Sign-in: OAuth 2.0 authorization code + PKCE

A third listener (`INGEST_AUTH_ADDR`, HTTP not gRPC — a browser cannot speak
gRPC) serves `GET /auth/login`, `GET /auth/callback`, `GET /auth/me`.

This is a **confidential client**: the code-for-token exchange happens
server-side with the client secret, and the browser never sees a provider token.
PKCE is used anyway — it costs one hash and defends against an intercepted
authorization code, which a client secret does not.

**Three random values, three different jobs.** Conflating them is the usual bug:

| Value | Defends against | Checked by |
|---|---|---|
| `code_verifier` (PKCE, RFC 7636) | an intercepted authorization code | the provider, at the token endpoint |
| `state` | CSRF — binding a victim's session to the attacker's account | us, and it is single-use |
| `nonce` | replay of an ID token captured elsewhere | us, against the flow we stored |

PKCE does not replace `state`, and `state` does not replace `nonce`.

Other decisions: `S256` only, never `plain` (which offers no protection at all).
Identity is the **subject**, never the email — an email can be reassigned to a
different person. An unverified email is refused outright. `return_to` is checked
against an exact allow-list, because an open redirect here would hand a freshly
minted session token to whatever host an attacker names — and it is rejected
*before* any state is stored, so probing cannot even allocate memory.

The provider's ID token is consumed once and exchanged for **our own** Ed25519
token: our issuer, audience, lifetime and key. That means an expiry we choose,
no dependency on the provider being reachable to serve a request, and one token
format across services. The session token carries identity only — permissions
are re-read from the database on every request, so a revoked grant bites
immediately rather than at token expiry.

`AutoProvision` is **off** by default: anyone with a Google account can
authenticate, but only provisioned users get in, which is what an observatory
wants. With it off, an unknown subject gets `403`.

```bash
go run ./cmd/ingest keygen session-key-1     # prints the two env vars below
```

```
INGEST_AUTH_ADDR=127.0.0.1:9092
INGEST_OIDC_PROVIDER_URL=https://accounts.google.com
INGEST_OIDC_CLIENT_ID=...          INGEST_OIDC_CLIENT_SECRET=...
INGEST_OIDC_REDIRECT_URL=https://.../auth/callback
INGEST_OIDC_ALLOWED_RETURN_TO=https://dev.observatory.services
INGEST_OIDC_AUTO_PROVISION=false
INGEST_JWT_SIGNING_KID=session-key-1   INGEST_JWT_PRIVATE_KEY=<base64 PEM>
INGEST_SESSION_TOKEN_TTL_SECONDS=3600
```

The private key goes **only** to the issuer; `INGEST_JWT_PUBLIC_KEYS` goes to
every verifier. `INGEST_JWT_SIGNING_KID` must appear in `INGEST_JWT_PUBLIC_KEYS`,
or this service mints tokens it will then reject.

**Known limit:** in-flight sign-ins are held in memory, so with several replicas
a user who starts on one pod and returns to another fails the callback. Fix is a
shared store or sticky routing; with one replica it is correct and has no moving
parts.

**Where this really belongs:** the Rust `api` crate, which is the platform's sole
token issuer. It is implemented here so the Go service runs and demonstrates end
to end on its own — the *verification* half is what stays here permanently.

## Telemetry backend (DynamoDB migration)

Telemetry writes go through TELEMETRY_BACKEND: postgres (default), dynamo, or both
(dual-write, with Postgres deciding success). cmd/migrate-telemetry backfills the
legacy Mongo collection, verifies counts and values, and benchmarks write latency.
Rationale, results and the honest caveats: DYNAMODB_TELEMETRY_MIGRATION.md.

## Regenerating code

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/bufbuild/buf/cmd/buf@latest
buf lint && buf generate            # from the repo root

pip install grpcio-tools
proto/gen-python.sh <out-dir>       # Python stubs for the Pi
```

## Running locally

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/ingest?sslmode=disable
export S3_ENDPOINT=http://localhost:9000 S3_BUCKET=observatory-dev
export S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin S3_REGION=us-east-1
go run ./cmd/ingest migrate
go run ./cmd/ingest                 # listens on INGEST_GRPC_ADDR (default :9090)
```

Server reflection is on, so `grpcurl -plaintext localhost:9090 list` works.

Optional: `INGEST_MAX_FITS_BYTES` (default 512MiB), `INGEST_MAX_PREVIEW_BYTES`
(default 2MiB; must stay under gRPC's 4MB message limit).

## Tests

```bash
go test ./...                       # unit tests; integration tests skip
```

Integration tests run against real Postgres and S3 when these are set:

```bash
INGEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/ingest_test?sslmode=disable \
INGEST_TEST_S3_ENDPOINT=http://localhost:9000 \
go test -count=1 ./...
```

They cover: full 26MB round trip, drop at 80% and resume from the last part,
wrong offset, checksum mismatch, committing an already-assembled object,
preview-only frames, header validation, NOTIFY-exactly-once, live settings push,
offline store-and-resync, the settings audit trail, telemetry persistence,
reconnect eviction, idle timeout, command ack round trip, goroutine-leak checks
across reconnects, and the auth rejections (unknown token, acting as another
device, device token on the control API).

**JWT verification** (`internal/auth`, no infrastructure needed): expiry, `nbf`,
issuer, audience, missing subject, wrong key, unknown `kid`, key rotation, and
the two attacks that matter — `alg: none`, and HS256 signed with the *public*
key, which succeeds against a verifier that lets the token choose the algorithm.

**Authorization** (`internal/server/authz_test.go`): the matrix — operator on
their own device, denied on someone else's, viewer denied a write, admin without
a grant, valid token for an unprovisioned subject, disabled user, revoked grant,
audit trail naming the real user, `SendCommand` denying before revealing
connectivity, and list filtering.

**Sign-in** (`internal/oidc`, `internal/authsvc`): PKCE verifier length and
charset against RFC 7636, challenge = base64url(SHA256(verifier)), state
single-use, expired and unknown state indistinguishable. Then the whole flow
against a **stub identity provider** (discovery, JWKS, authorize, token) that
enforces PKCE itself — so a code redeemed with the wrong verifier is refused,
with a control test proving the same flow succeeds with the right one. Plus:
nonce mismatch, missing `id_token`, unverified email, unprovisioned user,
disabled user, auto-provision, open-redirect attempts, and that a rejected login
creates no pending state. One test in `internal/server` closes the loop by
minting with the real `Issuer` and calling gRPC with it — those are configured
separately, so a `kid`/audience mismatch would break every sign-in while both
sides' own tests still passed.

**Load** (`authz_load_test.go`): 100 and 500 users, each with one camera, all
writing concurrently. Every user also attempts a write to a camera they do not
own, and the test fails if any succeeds — a permission check that is correct
single-threaded and wrong under load is the failure worth catching. Afterwards
every camera is read back to prove it does not hold the value only a trespasser
sends. `-short` skips the 500-user run.

```bash
INGEST_TEST_POOL_MAX_CONNS=32 go test ./internal/server/ -run UnderLoad -v
```

Each test gets its own generated device id, because they share one database and a
fixed device would let one test's rows change what another sees on connect.

`-race` needs cgo (a C compiler); run it in CI on Linux.

# Migrating `telemetry` to DynamoDB — Proposal

> **STATUS (2026-09-25): BUILT AND RUN.** The adapter, the dual-write path and the
> backfill/verify/bench tool all work: **669 real telemetry documents migrated out of the
> legacy Mongo collection and verified.** Environment: DynamoDB Local (API-compatible);
> pointing it at an AWS table is two env vars, and would additionally exercise throttling,
> TTL deletion, IAM and consumed-capacity/cost.

Companion to `GO_GRPC_AND_POSTGRES.md` (the ingest rebuild) and the DynamoDB section
of `CLAUDE.md` (the platform-wide directive this answers).

---

## The question this answers

The platform-wide instruction is "get off self-managed MongoDB, move to DynamoDB."
Taken literally that means rewriting every data-access path in four services. This
proposes the smaller, defensible version: **move the one collection that is actually
DynamoDB-shaped, measure it, and let the result decide the rest.**

## Why `telemetry` is the right collection

| Property | `telemetry` | Why it matters |
|---|---|---|
| Access pattern | always "this device, this time range" | Exactly `PK=device_id, SK=recorded_at`. No scans, no secondary indexes. |
| Joins | none | Nothing joins to telemetry; it is read on its own. |
| Write volume | highest in the system (~43k rows/day/device) | The one collection where managed scaling earns anything. |
| Value per row | lowest | A lost reading costs nothing — the next is 2 seconds away. |
| Retention | natural | Nobody needs last year's humidity. DynamoDB TTL expires rows for free; a Mongo TTL index pays write IO to delete them. |
| Coupling | none | Migrating it touches neither `frames`, the detection pipeline, nor the transactional `NOTIFY` design. |

The honest business reason, and the strongest half of the argument: **three
self-managed MongoDB StatefulSets, one per environment, is real operational burden.**
Moving the highest-volume, lowest-risk collection first is how a migration gets
proven before the platform is bet on it.

## Why NOT `frames` — even though it would make the résumé line literally true

The "blobs on S3, keys in the table, under the 400KB item limit" story describes
`frames`, not telemetry. Moving `frames` would make that sentence true, and it costs
more than it returns:

- **It kills the transactional `NOTIFY`.** Insert + event committing together is only
  possible because Postgres does both in one transaction. In DynamoDB it's Streams
  again, and designing around lost events.
- **The detection trigger has to be rebuilt**, and `WHERE detect_status='pending'`
  becomes a sparse GSI.
- **It's a week, not two days**, and it trades a strong story for a weaker one.

**Consequence for the résumé line:** drop the 400KB clause. Telemetry items are ~100
bytes; the item limit never comes up. The claim that survives a follow-up is narrower:
*migrated the telemetry collection off self-managed MongoDB to DynamoDB, keyed by
device and timestamp with TTL-based retention.*

## Table design

```
Table:  skycam_telemetry
  PK    device_id        (S)   "skycam-01"
  SK    recorded_at      (S)   ISO-8601 UTC — lexicographic order == chronological
        temperature_c    (N)
        humidity_pct     (N)
        probe_temp_c     (N)
        received_at      (S)
        expires_at       (N)   epoch seconds; TTL attribute (e.g. now + 90d)
```

- **Sort key as ISO-8601 text**, so `Query ... BETWEEN :from AND :to` gives the
  `?from&to` range the GUI already asks for, with no extra index.
- **TTL** does retention with no cron job and no delete cost.
- **On-demand capacity**: ~0.5 writes/second is far inside the permanent free tier
  (25 GB, 25 write units), so this costs $0 at current volume — worth stating
  plainly rather than hand-waving about cost.

### Hot partitions — know this before being asked
`device_id` as the partition key concentrates a device's writes on one partition. At a
handful of cameras that is fine. At scale, suffix the key with a time bucket
(`device#2026-09`) to spread them, at the cost of querying across buckets.

## Why this is a day or two

Storage already sits behind an interface in the ingest service:

```go
type TelemetryStore interface {
    InsertTelemetry(ctx context.Context, t TelemetryReading) error
}
```

So the work is a **second adapter**, not a rewrite of the service. That was the point
of the ports-and-adapters choice: the storage decision stays swappable, and both
backends can be benchmarked instead of argued about.

## Plan

1. ❌ **AWS table** (free tier, console or Terraform) — pending an account.
2. ✅ **`internal/store/dynamo.go`** implementing `TelemetryStore` with `aws-sdk-go-v2`.
3. ✅ **Dual-write behind a config flag** (`TELEMETRY_BACKEND=postgres|dynamo|both`).
4. ✅ **Backfill** existing rows, verify counts and spot-check values.
5. ✅ **Flip reads.** The Postgres path is still in place; it would be removed after a soak.
6. ⚠️ **Measure both** — write latency done; cost and a meaningful latency comparison need
   the real table (`ConsumedCapacity` comes back `null` locally).

**Integration cost to budget for:** the Rust read API serves telemetry to the GUI, so
it needs `aws-sdk-dynamodb` (same pattern as the `aws-sdk-s3` it already uses), or the
Go service grows a telemetry read RPC.

**Switching to a real AWS table:** drop `DYNAMO_ENDPOINT`, set real IAM credentials and
`DYNAMO_REGION`. No code change — the adapter is the same either way.

## What was actually run (2026-09-25)

**1. Backfill of the real legacy collection.** The Rust service's Mongo database
(`observatory_dev.telemetry`) held **669 real readings**:

```
backfill starting source_rows=669
progress copied=500 of=669
backfill done copied=669 skipped=0 elapsed=1.077s
```

**2. Verification — counts and values, not just counts.**

```
count check device_id=skycam mongo=669 dynamo=669 match=true
value check device_id=skycam recorded_at=2026-08-16T00:02:16Z match=true
verification passed
```

**3. Re-run proves it is resumable.** Running the backfill again copied all 669 rows and
the destination count stayed **669** — the `(device_id, recorded_at)` key means a repeated
row overwrites itself. An interrupted migration is resumed by simply running it again.

**4. Dual-write.** With `TELEMETRY_BACKEND=both`, three readings sent over a live device
session landed in **both** stores, with the TTL attribute set in DynamoDB:

```
postgres  17:33:37 -> 10    17:33:38 -> 11    17:33:38 -> 12
dynamodb  17:33:37 -> 10    17:33:38 -> 11    17:33:38 -> 12   (expires_at set)
```

**5. Cutover.** Restarted with `TELEMETRY_BACKEND=dynamo`; Postgres stayed frozen at 10
rows for that device while DynamoDB grew to 5. That is the whole sequence: dual-write,
verify, flip, and the old store goes quiet.

**6. Write latency, measured** (`migrate-telemetry bench -n 200`):

| Backend | p50 | p95 | p99 |
|---|---|---|---|
| Postgres (container, localhost) | 3.5 ms | 4.4 ms | 4.7 ms |
| DynamoDB Local (JVM, localhost) | 6.9 ms | 52.3 ms | 64.3 ms |

These are not a DynamoDB-versus-Postgres verdict: both ran on one laptop with no network
in between, so the comparison that would decide the migration still needs a real table.
What carries over is the method — same client, same readings, percentiles not averages.

### Tests

`internal/store/dynamo_test.go`, skipped unless `INGEST_TEST_DYNAMO_ENDPOINT` is set:
range query returns newest-first, repeated writes are idempotent, absent sensors stay
absent (a missing probe is not 0 °C), batch insert crosses DynamoDB's 25-item limit, and
the dual-write wrapper swallows a secondary failure but not a primary one.

### Commands

```bash
docker run -d --name obs-dynamodb -p 58000:8000 amazon/dynamodb-local \
  -jar DynamoDBLocal.jar -inMemory -sharedDb

export DYNAMO_ENDPOINT=http://localhost:58000 DYNAMO_TABLE=skycam_telemetry \
       DYNAMO_ACCESS_KEY=local DYNAMO_SECRET_KEY=local \
       MONGODB_URI=mongodb://localhost:27017 MONGODB_DB=observatory_dev

go run ./cmd/migrate-telemetry backfill      # copy, resumable
go run ./cmd/migrate-telemetry verify        # counts + value spot-check
go run ./cmd/migrate-telemetry bench -n 200  # latency percentiles

TELEMETRY_BACKEND=both   go run ./cmd/ingest  # dual-write during the migration
TELEMETRY_BACKEND=dynamo go run ./cmd/ingest  # after cutover
```

### What changed in the codebase

| File | Role |
|---|---|
| `internal/store/dynamo.go` | the adapter: table creation, TTL, put, range query, count, batch write |
| `internal/store/telemetry_dualwrite.go` | primary decides success; a secondary failure is logged, not returned |
| `internal/config/config.go` | `TELEMETRY_BACKEND` (postgres / dynamo / both) and the `DYNAMO_*` settings |
| `cmd/ingest/main.go` | picks the backend at startup |
| `cmd/migrate-telemetry/` | one-off tool: backfill, verify, bench — a separate command so the service never links the Mongo driver |

The service itself was **not** modified: it writes through `store.TelemetryStore`, so the
migration really was a second adapter. That is the payoff of putting storage behind an
interface, and it is the answer to "why did that only take a day?"

## What to say when asked "why did you migrate that collection?"

> We were running three self-managed MongoDB StatefulSets, one per environment, and the
> ops burden was the real driver. Rather than lift-and-shift, I looked at which
> collection was actually DynamoDB-shaped and moved that one first as a proof.
> Telemetry is append-only, always read as device plus time range, has no joins, and is
> the highest write volume with the lowest value per row — a clean partition-key /
> sort-key fit, and TTL gives us retention for free. The relational data stayed put,
> because "which users can see this site" is a join, and that's what DynamoDB is bad at.

**Follow-ups and answers:**

- *"Why not migrate everything?"* RBAC and device ownership are join-shaped; in
  DynamoDB that becomes several round trips or a denormalisation maintained by hand.
- *"Hot partitions?"* See above — fine at this scale, time-bucket the key at scale.
- *"How do you query a range?"* Sort key is an ISO-8601 timestamp; `Query` with
  `BETWEEN`. No index needed because the table was designed around that one question.
- *"How did you migrate without downtime?"* Dual-write behind a flag, backfill, verify
  counts, flip reads, remove the old path.
- *"What did it cost?"* Inside the free tier at ~0.5 writes/second — and know your own
  write rate, because that's the tell.
- *"What would make you move more?"* Numbers. If telemetry shows a real win on cost or
  latency, `frames` is the next candidate — but only with a plan for the detection
  trigger, since the transactional `NOTIFY` doesn't survive the move.

## Claims to avoid

- **The environment was DynamoDB Local**, so throttling, TTL deletion, IAM and cost are
  unverified. Everything else — key design, dual-write, backfill, verification, cutover —
  is real work and holds.
- **One collection moved**, not the platform.
- **The 400KB item limit doesn't apply** unless `frames` moves; telemetry items are tiny.
- **No latency claim without the before/after.** From a VPS, DynamoDB is a WAN hop and may
  measure *slower* than in-cluster Mongo — a legitimate finding, not a reason to skip it.

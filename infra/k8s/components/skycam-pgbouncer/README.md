# skycam-pgbouncer component (opt-in, off everywhere)

PgBouncer in **transaction mode** in front of the skycam-v2 Postgres. It patches
the ingest, detect and notify Deployments' `DATABASE_URL` to `pgbouncer:6432`.
The ingest `migrate` init container keeps a direct connection.

## When to turn it on
Turn it on when the apps' **combined pool sizes approach Postgres
`max_connections`** (default 100). Don't turn it on before that.

**Dev today, roughly:**

| Process | Max connections |
|---|---|
| ingest × 2 (pgx default: one per CPU, minimum 4; 8-core node assumed) | ~16 |
| detect worker (2 processes × pool of 4) + Beat | ~12 |
| notify worker + Beat | ~12 |
| **Total** | **~40 of 100**. Not needed yet |

It becomes necessary as ingest replicas and Celery processes grow: ~10 ingest
pods alone would be ~80.

**It is not free.** At an equal pool size, when connections were *not* the
limit, it measured about 15% lower throughput (~3,250 vs ~3,800 req/s) from the
extra network hop. With a pool smaller than the app's, the difference was ~40%.

To enable, list it after skycam-v2 in the overlay:

```yaml
components:
- ../../components/skycam-v2
- ../../components/skycam-pgbouncer
```

## What was measured (2026-10-01)
Postgres was capped at `max_connections=40`. The test was the 500-user
authorization load test (5,000 requests).

| | App pool | Result | Connections held by Postgres |
|---|---|---|---|
| Direct | 120 | **FAIL: 1,025 requests refused**, matching 1,025 `sorry, too many clients already` in the Postgres log | up to the cap |
| Through PgBouncer | 120 | **PASS, 0 errors**, 1,810 req/s | **25** |

Correctness through PgBouncer:
- **Go:** the whole suite passes.
- **Python:** 27/27 detect tests and 35/35 notify tests pass.
- **Full deployment rehearsal:** two ingest replicas, the Celery apps and the
  fake Pi, all through PgBouncer with these exact settings. 20/20 frames scored,
  0 errors. Postgres saw **2 connections**, both from PgBouncer.

## The settings that matter, and why
- **`MAX_PREPARED_STATEMENTS=200` is required.** pgx caches named prepared
  statements per connection, and transaction pooling hands it a different
  server connection. Control run with it at 0: **50 Go tests failed**,
  `prepared statement "stmtcache_..." already exists`. psycopg prepares a query
  on its fifth execution, so it needs this too. Requires PgBouncer 1.21 or newer.
- **Migrations bypass it.** golang-migrate holds a session-level advisory lock,
  which transaction pooling cannot keep. That's why the init container is not
  patched.
- **Nothing uses `LISTEN`.** It would break silently here, because a
  notification lands on a server connection now serving someone else. Detection
  is triggered by polling, by design. `NOTIFY` works fine through the pooler.
- **Connection budget:** PgBouncer's idle server connections count against
  `max_connections`. Found the hard way: with PgBouncer holding 25 idle, a
  direct client had only ~12 left. Keep `replicas × MAX_DB_CONNECTIONS` (2 × 25)
  plus migrations and admin sessions under the limit.
- **`IDLE_TRANSACTION_TIMEOUT=60`:** a client that opens a transaction and goes
  quiet would pin a server connection. This releases it.
- **Two replicas plus a PDB**, so the pooler is not a new single point of
  failure in front of every query.

CI runs the whole Go suite through PgBouncer on every push (`skycam-ci.yml`).

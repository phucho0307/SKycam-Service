# detect — cloud-detection worker (Python + Celery + Redis)

Scores each sky frame for cloudiness and raises alarms. Classical CV heuristic
(star count + brightness + R/B ratio) on the JPEG preview — no ML, fast, explainable.

Two backends, chosen by `DETECT_BACKEND`, because both ingest pipelines exist:

| `DETECT_BACKEND` | Pipeline | Tasks | State |
|---|---|---|---|
| `mongo` (default) | Rust ingest → MongoDB | `tasks.py` | shipped; known defects below |
| `postgres` | Go ingest → Postgres | `tasks_pg.py` | **built + tested 2026-09-27**, not deployed |

```
Beat --claim pending frames--> Redis --> worker
  worker: get preview from S3 -> analyze() -> write cloud_score/is_cloudy
          -> if cloudy, insert alarm (idempotently)
Beat also runs a sweep that returns frames claimed by a worker that died.
```

## Why this shape

- **Poll, not push.** Polling is *self-healing*: a missed scan is picked up on the
  next one, whereas a dropped event leaves a frame unscored forever with nothing
  to notice. Latency does not matter here — sky conditions change over minutes.
  What made polling expensive in the Mongo version was the **missing index**, not
  polling itself. Postgres has a partial index on the pending rows, so the same
  poll is an index scan over the handful still waiting.
- **`NOTIFY` is an optimisation, not the mechanism.** The Go service already emits
  `NOTIFY frame_ingested` inside the insert transaction. A listener would let the
  poll interval grow to minutes while keeping scoring prompt — with the poll still
  there as the correctness backstop. **Not built yet**; nothing LISTENs today.
- **Preview, not FITS.** The JPEG is small and OpenCV-decodable; FITS needs astropy.

## What the Postgres backend fixes

The three defects of the Mongo version, each fixed structurally rather than with
a `try`/`except`:

**Poison pills.** Failures are *classified*. No preview, or a JPEG that will not
decode, can never succeed, so the frame goes straight to a terminal state
(`skipped` / `failed`) and leaves the pending index. Only genuinely transient
failures (S3 unreachable) are retried, on a budget counted in
`frames.detect_attempts`. The Mongo query was "has no `cloud_score`" and the error
path wrote nothing, so a bad frame was re-enqueued every 3 seconds forever.

**Double-enqueue.** `claim_pending` marks rows `claimed` in the same statement
that selects them, using `FOR UPDATE SKIP LOCKED`. Two pollers get disjoint sets
and neither blocks. Previously a frame stayed "unscored" while a worker was
mid-task, so the next scan handed it to a second worker.

**Duplicate alarms.** `ON CONFLICT (frame_id, kind) DO NOTHING` against a unique
index. With `task_acks_late`, a worker dying after analysing gets the task
redelivered, and a plain `insert_one` raised the alarm twice.

A worker that dies between claiming and finishing is caught by
`reclaim_stale`: claims older than `DETECT_STALE_CLAIM_S` return to `pending`,
unless the attempt budget is spent, in which case they are dead-lettered with
`detect_error` set. The dead letter is a row state, not a separate queue — so
"why did this frame never score" is answerable in SQL months later.

## Run locally (Postgres backend)

Needs Postgres + MinIO + Redis, and the Go ingest service to have created frames
(see `tools/fake-pi/README.md` for a synthetic camera).

```bash
docker run -d --name obs-redis -p 6379:6379 redis:7   # or valkey/valkey

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export DETECT_BACKEND=postgres
export DATABASE_URL=postgres://postgres:postgres@localhost:55432/skycam_demo
export REDIS_URL=redis://localhost:6379/0
export S3_ENDPOINT=http://localhost:9000 S3_BUCKET=skycam-demo
export S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin S3_REGION=us-east-1

celery -A celery_app worker --loglevel=info      # one terminal
celery -A celery_app beat   --loglevel=info      # another — Beat cannot be
                                                 # embedded with -B on Windows
```

## Config (env)

Shared: `REDIS_URL`, `S3_*`, `DETECT_SCAN_INTERVAL_S` (3), `DETECT_SCAN_LIMIT` (50),
`DETECT_EXPECTED_STARS` (40), `DETECT_CLOUD_THRESHOLD` (0.6). The star and
threshold values are placeholders — calibrate on real night data. `frames` now
stores `star_count`, `brightness` and `rb_ratio`, so recalibration does not mean
re-reading every JPEG out of object storage.

Postgres backend: `DATABASE_URL`, `DETECT_MAX_ATTEMPTS` (3),
`DETECT_STALE_CLAIM_S` (300), `DETECT_SWEEP_INTERVAL_S` (60).
`DETECT_STALE_CLAIM_S` must exceed the task's own time limit, or the sweep will
resurrect frames that are still being processed.

Mongo backend: `MONGODB_URI`, `MONGODB_DB`.

**Mongo backend known defects (unchanged):** `detect_frame` has no exception
handling, so an S3 or decode failure is re-enqueued forever; alarm writes are a
plain `insert_one`, so a redelivered task duplicates them; and the scan query is
unindexed. Details in `CLOUD_DETECTION_AND_REDIS.md`.

## Tests

```bash
# unit: analyze() against the latest stored frame's preview
python test_detector.py

# Postgres layer + tasks, against a real database, MinIO and Redis.
# Use a THROWAWAY database: the fixtures truncate `frames`.
createdb skycam_detect_test   # then: ingest migrate
DETECT_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/skycam_detect_test \
  python -m pytest test_store_postgres.py test_tasks_pg.py -v
```

27 tests, no mocks: every behaviour under test is a property of the SQL
(`SKIP LOCKED`, `ON CONFLICT`, the partial index) or of a real OpenCV decode, and
a mock would assert my beliefs about Postgres rather than Postgres. One test reads
`EXPLAIN` output and fails if the claim query stops using the partial index —
because that index is the whole reason polling is defensible here.

The tests use Redis **db 15**, not db 0. `scan_pending` publishes real messages,
and sharing a database with a running worker left orphaned tasks carrying
frame_ids from the test database, which a worker pointed at another database then
tried to process.

## Still missing before this runs anywhere real

`detect/Dockerfile`, a kustomize entry, and a Redis manifest — the worker runs in
no cluster environment. Plus the `LISTEN` bridge, if the poll interval is to grow.

## Keeping clear-sky FITS (Postgres backend)

Raw FITS expire after a day (bucket lifecycle rule). Frames scored **clear** get a
losslessly compressed copy under `archive/frames/<device>/<id>.fits.fz`, kept
longer (`ARCHIVE_EXPIRE_DAYS` on the MinIO bucket-init job). Cloudy, unscored and
failed frames are never kept.

| Setting | Default | Meaning |
|---|---|---|
| `DETECT_ARCHIVE_CLEAR_FITS` | `true` | Schedule the archive scan at all |
| `FITS_COMPRESSION` | `GZIP_2` | `GZIP_2`, `RICE_1`, or `none` |
| `DETECT_ARCHIVE_INTERVAL_S` | `60` | How often Beat looks for clear frames to keep |
| `DETECT_ARCHIVE_WINDOW_S` | `72000` (20 h) | Only frames this recent; must stay under the raw FITS lifetime |
| `DETECT_ARCHIVE_LIMIT` | `10` | Frames per scan (each holds ~25 MB in memory while compressing) |

`fits_archive.py` never stores anything that does not decompress to exactly the
original pixels and header; if compression can't be proven lossless, the original
bytes are archived instead. State per frame is in the `fits_archive` table
(migration `0006_fits_archive`), including the reason a file was kept uncompressed.

Read an archived file with any FITS reader (`astropy.io.fits.open`, DS9); the
compressed image is in HDU 1. Measured on the camera's own frames: GZIP_2
2.07–2.25×, Rice 1.66–1.70×.

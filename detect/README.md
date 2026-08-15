# detect — cloud-detection worker (Python + Celery + Redis)

Scores each sky frame for cloudiness and raises alarms. Classical CV heuristic
(star count + brightness + R/B ratio) on the JPEG preview — no ML, fast, explainable.

```
Celery Beat --poll Mongo for un-scored frames--> Redis --> Celery worker
  worker: get preview from S3 -> analyze() -> write cloud_score/is_cloudy to `frames`
          -> if cloudy, insert into `alarms`
```

## Why this shape
- **Poll (not push):** skycam is Rust; enqueuing a Celery task from Rust means
  replicating Celery's protocol. Beat polling keeps the broker/worker world in Python.
  At ~1 frame/min the poll latency is irrelevant. (Event-driven later: skycam LPUSHes
  an id + a small bridge dispatches.)
- **Preview, not FITS:** the JPEG preview is small and OpenCV-decodable; FITS needs
  astropy.

## Run locally
Needs the local stack (MongoDB + MinIO with frames) + a Redis:

```bash
docker run -d --name obs-redis --network obs-local -p 6379:6379 redis:7   # or valkey/valkey

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export MONGODB_URI=mongodb://localhost:27017 MONGODB_DB=observatory_dev
export S3_ENDPOINT=http://localhost:9000 S3_BUCKET=observatory-dev
export S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin S3_REGION=us-east-1
export REDIS_URL=redis://localhost:6379/0

# worker + beat (one process for local dev):
celery -A celery_app worker --beat --loglevel=info
```

## Config (env)
`REDIS_URL`, `MONGODB_URI`, `MONGODB_DB`, `S3_*`, `DETECT_SCAN_INTERVAL_S` (30),
`DETECT_EXPECTED_STARS` (40), `DETECT_CLOUD_THRESHOLD` (0.6). The star/threshold
values are placeholders — calibrate on real night data.

## Tests
`python test_detector.py` runs `analyze()` on the latest stored frame's preview.

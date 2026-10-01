# fake-pi — a sky camera that isn't there

A Python gRPC client that speaks the real contract in `proto/skycam/v1/skycam.proto`
to the Go ingest service, with no camera and no sensors. It exists so the ingest
pipeline can be exercised end to end on a laptop, and so the device half of the
contract is written down before the real Pi is ported (M7).

It is **not** what runs on the Pi today. The real Pi (`skycam-pi/send.py`) still
POSTs multipart HTTP to the Rust skycam service. That is the shipped path; this
is the replacement path.

```
fake_pi.py ──DeviceSession (bidi)────> telemetry, heartbeats, command acks up
           <───────────────────────── settings, commands down
           ──UploadFrame (client-stream)─> preview + FITS in 512KB chunks
           ──GetUploadStatus (unary)────> where to resume after a failure

control_cli.py ──SkycamControlService──> settings, commands, connected devices
                                         (loopback port, operator token)
```

## What it demonstrates

| Behaviour | How to see it |
|---|---|
| Two-tier cadence | preview every cycle, FITS every `--fits-every` seconds |
| Frames spooled to disk before upload | kill the process mid-upload; the frame is still in `spool/` |
| Crash recovery | restart — `recovered N frame(s) from a previous run` |
| Resumable FITS upload | `--fail-upload-at-mb 10 --throttle-mbps 100` |
| Upload retry with backoff | same run: `attempt 1/5`, then `resuming … from 10.0MB` |
| Device-side DLQ | 5 failed attempts moves the frame to `spool/failed/` |
| Capture never blocks on upload | separate threads; watch `capture` and `uploader` interleave |
| Settings push | `control_cli.py settings … --preview-gamma 0.45` → preview size changes |
| Command + ack | `control_cli.py command … capture-now` → off-cycle capture |
| Lost push is harmless | stop the client, change settings, restart: it gets them on connect |

## Generating the stubs

The stubs are generated from the same `.proto` the Go server is built from —
that is the point of having a contract. Regenerate after any proto change:

```bash
pip install grpcio grpcio-tools pillow
PYTHON=python bash ../../proto/gen-python.sh gen
```

Output lands in `gen/skycam/v1/`, imported as `from skycam.v1 import skycam_pb2`.

## Running it

Needs Postgres and MinIO, and the ingest service. From the repo root:

```bash
# 1. stack
docker run -d --name obs-postgres -p 55432:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=ingest_test postgres:16-alpine
docker run -d --name obs-minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio:latest server /data --console-address ":9001"
docker exec obs-postgres psql -U postgres -c "CREATE DATABASE skycam_demo"
docker exec obs-minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec obs-minio mc mb --ignore-existing local/skycam-demo

# 2. service config
export DATABASE_URL="postgres://postgres:postgres@localhost:55432/skycam_demo?sslmode=disable"
export S3_ENDPOINT=http://localhost:9000 S3_BUCKET=skycam-demo S3_REGION=us-east-1
export S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin
export INGEST_GRPC_ADDR=127.0.0.1:9090 INGEST_CONTROL_ADDR=127.0.0.1:9091
export INGEST_OPERATOR_TOKEN=demo-operator-token

# 3. schema, then a device (the token is printed once)
go run ./services/ingest/cmd/ingest migrate
go run ./services/ingest/cmd/ingest device add fake-skycam
go run ./services/ingest/cmd/ingest          # leave running

# 4. the camera
export OBS_INGEST_TOKEN=<the token from step 3>
export OBS_DEVICE_ID=fake-skycam OBS_GRPC_TARGET=localhost:9090
python tools/fake-pi/fake_pi.py --cycles 3 --fits-every 1
```

### The resumable-upload demo

```bash
python fake_pi.py --cycles 1 --fits-every 1 --fail-upload-at-mb 10 --throttle-mbps 100
```

`--fail-upload-at-mb` raises inside the request generator, so the RPC fails
mid-transfer. The retry calls `GetUploadStatus`, gets back the number of bytes S3
has durably committed, seeks the spool file to that offset and continues.
Observed offsets are always multiples of 5MB because that is the multipart part
size — a partial part is not durable, so it is re-sent.

There are **two distinct failure paths**, and they hit different server code:

| How it fails | What the server sees | Logged as |
|---|---|---|
| `--fail-upload-at-mb` (generator raises) | request stream half-closes, `Recv` returns `io.EOF` short of the declared size | nothing; returns `FailedPrecondition "stream ended at N of M bytes"` |
| the client process is killed | transport cancel, `Recv` returns `Canceled` | `fits upload interrupted` with `committed` bytes |

Only the second is what a dropped link really looks like, so test it too:

```bash
# start an upload, then kill the process a few seconds in
timeout -s KILL 4 python fake_pi.py --cycles 1 --fits-every 1 --throttle-mbps 20
# restart: the frame is recovered from the spool and resumed
python fake_pi.py --cycles 1 --fits-every 99999
```

Observed: `fits upload interrupted … committed: 5242880` on the server, then
`resuming 6bc54721 from 5.0MB of 24.0MB` in the *new* process. The resume state
lives in S3's own multipart part list, not in the client and not in a database,
which is why it survives losing the client entirely.

`--throttle-mbps` matters for this demo: unthrottled, the client hands 10MB to
gRPC faster than the server can flush 5MB parts to S3, so the failure can land
before *anything* is committed and the resume offset is 0. Throttling models a
real uplink and makes the demo deterministic.

## A bug this client found

`UpdateDeviceSettings` is a **whole-row replace**, but the CLI (and a GUI with
five sliders) makes it look like a partial update. Observed in `settings_audit`:

```
1  before: —                                    after: gain=300, exposure_ms=2000, preview_gamma=0.45
2  before: gain=300, exposure_ms=2000, gamma=0.45  after: gain=777, exposure_ms=null, preview_gamma=null
```

The second call passed only `--gain 777` and silently wiped exposure and gamma.
`internal/server/control.go` copies all five optional fields straight into
`UpsertSettings`, so an unset field becomes `NULL`.

The cause is in the contract, not the handler: with bare `optional` fields, unset
means both "leave it alone" and "clear it", and the server cannot tell which the
caller meant. The standard gRPC answer is a `google.protobuf.FieldMask` naming
the fields to change (`update_mask: {paths: ["gain"]}`). Until the proto has one,
any caller must read current settings and send all five fields back.

Not yet fixed — it needs a proto change, and the field numbers are already in use
by a running service, so it wants doing deliberately.

## Testing through TLS and a reverse proxy

`OBS_GRPC_TLS=1` switches to a verified TLS channel; `OBS_GRPC_CA=<pem>` trusts a specific
certificate instead of the system store, for a self-signed proxy. This is how M0 was run —
Traefik v3 terminating TLS with an `h2c://` backend to the Go service:

```bash
OBS_GRPC_TARGET=localhost:8443 OBS_GRPC_TLS=1 OBS_GRPC_CA=certs/server.crt \
  python fake_pi.py --fits-every 90
```

**Watch the session-reconnect lines.** That run is what caught Traefik v3's default
`readTimeout` of 60s: the `DeviceSession` was `RST_STREAM`ed at exactly 60.0s every time,
and the client's reconnect loop hid it as a warning rather than a failure. If you see a
session reopening on a suspiciously round interval, a proxy is timing out the request — a
long-lived stream *is* one request. With `respondingTimeouts.readTimeout: 0` the same
session held 5 minutes with zero drops.

## Notes

- **Synthetic FITS.** Built by hand (valid header cards, 2880-byte blocks,
  `BZERO=32768` for unsigned 16-bit) rather than with astropy, to keep the
  dependencies to grpcio + pillow. Pixel data is noise, and `ORIGIN` says
  `fake-pi` so a synthetic file is never mistaken for an observation. Nothing
  downstream reads FITS pixels yet — cloud detection works on the JPEG preview.
- **Don't name a script after a stdlib module.** This CLI was briefly called
  `operator.py`, which shadowed the stdlib `operator` that `collections`
  imports, and broke every script in the directory with a circular-import error.
- **Keepalive.** The client pings every 30s because the server's
  `KeepaliveEnforcementPolicy.MinTime` is 20s; pinging faster earns a
  `GOAWAY ENHANCE_YOUR_CALM`, which looks like a network fault.
- **One sender per stream.** gRPC forbids concurrent `Send`s on one stream, so
  every outbound session message goes through `outbox` and is yielded by the
  single request generator. The same rule shapes the server (see
  `internal/server/session.go`).

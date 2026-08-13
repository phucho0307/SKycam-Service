# Sky Camera — Project Writeup

*Personal reference for SWE interviews. Covers what was built, why, the design
decisions and tradeoffs, the debugging challenges, and current status.*

---

## 1. One-line summary

I built a **sky-camera microservice** inside an existing observatory web platform:
a Raspberry Pi 5 pushes **temperature readings** and **FITS astronomy images** to
a dedicated backend service, which stores images in **object storage (S3)** and
metadata/telemetry in **MongoDB**, and serves a **React admin GUI** where users
see the live sky, browse the archive, and view temperature history.

## 2. The problem

- A Raspberry Pi 5 with a **ZWO ASI676MC** camera (~25 MB FITS files) and an
  **AHT10** temp/humidity sensor needed to send data somewhere.
- The platform (**Observatory Services**) had a website + health checks but **no
  way to receive data and no GUI** for it. I built the whole ingest + storage +
  read + UI path.
- Goal shape: keep the Pi a *thin capture agent*; do all storage, processing, and
  the multi-user GUI **server-side** (k8s in prod, laptop in dev).

## 3. System architecture

```
Pi (thin agent)                      skycam microservice (Rust/Rocket)
──────────────                       ─────────────────────────────────
capture + read sensor                WRITE  (Pi, bearer token):
  │  HTTPS POST (+ Bearer token)       POST /skycam/frames     FITS(+JPEG preview) → S3
  │  NO cloud keys on the Pi           POST /skycam/telemetry  JSON → MongoDB
  ▼
Cloudflare edge → tunnel → Traefik → skycam service ──► S3 / MinIO   (image bytes)
                                            └─────────► MongoDB       (metadata, telemetry)

React GUI  ──GET──►  skycam service   READ (GUI):
(browser)                               GET /skycam/frames/latest
   ▲                                    GET /skycam/frames?from=&to=
   └── loads images DIRECTLY ◄──────    GET /skycam/telemetry?from=&to=
       from S3 via presigned URL        (responses embed presigned S3 URLs)
```

**Core principle:** *big binary and small facts go to different stores.* Images →
S3 (blobs). Metadata + telemetry → MongoDB (queryable). The `s3_key` / `preview_key`
strings in the Mongo `frames` document are the pointers that link a record to its
files in S3.

## 4. Tech stack

| Layer | Tech | Notes |
|---|---|---|
| Backend | **Rust**, **Rocket 0.5** | dedicated `skycam` crate/microservice |
| Database | **MongoDB** (official Rust driver) | `telemetry` + `frames` collections |
| Object storage | **S3-compatible** via `aws-sdk-s3`; **MinIO** in dev | two clients: internal (upload) + public (presign) |
| Frontend | **React 19 + TypeScript + Tailwind v4**, **react-query** | admin GUI under `/skycam` |
| Infra | **MicroK8s**, **Traefik**, **Cloudflare Tunnel**, **ArgoCD** GitOps, **Sealed Secrets** | microservices, one Deployment per crate |
| Pi client | **Python** (`requests`, `zwoasi`, `astropy`, `smbus2`) | separate repo |

## 5. What I built

**Started** by adding ingest to the existing `api` crate, then **extracted it into
its own `skycam` microservice** (the platform is a microservices project — one
binary + one k8s Deployment per crate). `api` went back to just health checks.

**Write side** (`skycam/routes/ingest.rs`): a bearer-token request guard (fails
**closed** → 503 if no token configured); `POST /skycam/telemetry` (JSON → Mongo);
`POST /skycam/frames` (multipart FITS **+ optional JPEG preview** → S3, metadata →
Mongo). 64 MiB body limit for large FITS.

**Storage** (`skycam/storage.rs`): `aws-sdk-s3` with **two clients** — an internal
one for uploads, and a **presigning client bound to a public endpoint** so signed
URLs are browser-reachable.

**Read side** (`skycam/routes/read.rs`): `frames/latest`, `frames` (time-range
list), `telemetry` (range) — clean JSON DTOs that **embed presigned URLs**
(`preview_url`, `fits_url`).

**Shared types** (`domain`): `TelemetryReading`, `Frame` (`s3_key`, `preview_key`,
size, capture metadata).

**Frontend GUI** (`frontend/src/routes/skycam/`): an admin panel *modeled on
Allsky* but adapted — **Live View** (latest frame via presigned `<img>`, temp, sky
status, FITS download), **Images** (thumbnail grid), **Telemetry** (temp chart +
table). Data via react-query; Vite proxies `/skycam` → the service.

## 6. Key design decisions & tradeoffs *(good interview material)*

1. **Images in S3, not MongoDB.** 25 MB blobs in Mongo docs wreck the DB at volume
   (~36 GB/day @ 1 frame/min). S3 for bytes, Mongo for a pointer.

2. **The Pi never holds S3 keys** — only a bearer token. The service is the sole
   holder of S3 credentials. Steal the Pi, and cloud storage is still safe. This is
   *why* an "API in front of S3" exists instead of the Pi uploading directly.

3. **S3 first, then Mongo.** Upload the file *then* write the DB record → no "ghost"
   records pointing at missing files.

4. **HTTP POST, not WebSocket.** Captures are discrete request/response events → HTTP
   (status codes, multipart, retries for free). WebSocket is reserved for a *future
   live preview*. (Also: **S3 has no WebSocket API** — HTTP only.) The GUI "live
   view" is just the latest uploaded frame re-fetched, not a stream.

5. **Bearer token, not JWT.** One device → its own API: a static token is simpler and
   correct. JWT earns its cost with many users / expiring creds / roles — reserved
   for future human login.

6. **Microservice decomposition — and *not over-splitting*.** `skycam` is its own
   service (own Deployment, `/skycam/` ingress path) alongside `api`, `frontend`.
   But I deliberately kept ingest + read + (future) light detection in **one**
   service, and will only split a worker out **when the work is actually heavy** —
   premature fragmentation is a classic mistake.

7. **Reuse Allsky as a *reference*, not run its code.** Allsky (the standard DIY
   all-sky system) is a Pi-local monolith (Bash/C/PHP/Python, local disk, single
   admin). Running it server-side against S3 for multi-user = gutting it. Instead I
   built the GUI into the existing React+Rust platform and borrow Allsky's *ideas*
   (keograms, cloud detection, overlays).

8. **FITS isn't browser-viewable → JPEG previews (decision "A1").** FITS is a data
   format browsers can't render. So the Pi sends a small **JPEG preview** alongside
   the FITS; the GUI shows the JPEG, the FITS stays for science/download.

9. **Serving image bytes: presigned URL ("B1") over proxy ("B2").** B2 = bytes flow
   *through* the API (simple, keeps S3 private, per-request auth). B1 = API returns a
   short-lived **signed S3 URL** and the browser downloads **directly from S3**
   (offloads traffic, needs a browser-reachable endpoint). Chose **B1** because it's
   the long-term design (the repo already had an `s3_public_endpoint` slot) — no
   throwaway B2 work. Tradeoff accepted: signed links are "anyone-with-link until
   expiry," fine for a public sky-cam. Implemented via a **second S3 client** signing
   against the public endpoint (internal `obs-minio:9000` for uploads, public
   `localhost:9000` / `s3.observatory.services` for the browser).

10. **Separate repo for the Pi client, linked only by an HTTP contract.** Pi code
    lives in `ObservatoryServices/Skycam---Pi`; depends on the platform only through
    documented endpoints (URL + token), no shared code. Clean decoupling.

11. **Deferred: image id scheme.** UUID vs content hash (SHA-256 — integrity +
    idempotent retries, but not time-sortable) vs hybrid (timestamp path + stored
    `sha256`). Recommended hybrid; deferred.

12. **Cloud detection deferred, planned as a separate Go microservice** (polyglot
    services are fine in a microservices setup; a deliberate divergence from the
    Rust-only convention).

## 7. The Pi client (`Skycam---Pi` repo)

`send.py` each cycle: reads AHT10 (raw I²C via `smbus2`), captures a frame
(`zwoasi`), writes temp/exposure/gain into the **FITS header** (self-describing
frames), then POSTs telemetry + image. It also emits a debayered **color JPEG
preview** (OpenCV, RGGB) alongside the FITS and POSTs to `/skycam/frames` +
`/skycam/telemetry`. **Graceful degradation:** missing sensor or lib → skip that
part, keep going (works during incremental hardware bring-up). The repo also carries
the standalone `capture_one.py` (single-capture test) and `live_view.py` (dual-mode
preview).

## 8. Testing approach

Full **local stack in Docker**, no hardware, no production:
- `skycam` image + **MongoDB** + **MinIO** containers on a shared Docker network.
- **`curl` as the fake Pi:** no token → **401**; with token → stored; telemetry →
  row in `telemetry`; frame+preview → file in MinIO + `s3_key`/`preview_key`
  pointers in `frames`.
- **B1 proof:** fetched the returned **presigned `preview_url`** → `HTTP 200,
  image/jpeg` served straight from MinIO; `fits_url` → 200, full bytes.
- Ran real **`send.py`** against the API to validate its HTTP/auth/JSON layer.
- Frontend: typechecked, ran the dev server, confirmed the GUI shows **real** data
  through the Vite `/skycam` proxy.
- **Real hardware, end to end:** ran `send.py --once` on the actual Pi → a real
  **25 MB FITS + color preview + 24 °C reading** flowed through to MinIO + MongoDB
  and rendered in the GUI at `/camera`.

## 9. Engineering challenges I debugged *(great "hard bug" stories)*

The code compiled fine; the **environment** fought back. Isolating *where* a failure
lives was the whole skill:

1. **TLS-intercepting network broke cargo downloads.** A proxy MITMs HTTPS with a CA
   the fresh Linux build container didn't trust → `unable to get local issuer
   certificate`. **Fix:** exported the host's trusted CA bundle, injected it into the
   build; verified with one `curl` (HTTP 200) before the slow rebuild.

2. **No Windows C++ linker** (`link.exe`). **Fix:** build inside Docker (the real
   build path) instead of natively.

3. **Disk hit 0 bytes → Docker crashed** (I/O error, corrupted layer blob). **Fix:**
   freed space, restarted the engine, pruned the corrupt build cache.

4. **OOM compiling `aws-sdk-s3`.** 32 GB RAM but ~25 GB already used; the huge crate
   spiked past the free headroom and the **WSL2 VM behind Docker crashed** ("error
   running a WSL command"), taking all containers down. **Fixes, layered:** debug/
   unoptimized build, dropped debug symbols, capped parallel compilers; killed the
   Node/Vite dev server to free RAM; and switched the local Dockerfile from
   `cargo-chef` (which compiled the heavy crate **twice**) to a **single-pass** build.
   Then it completed.

5. **School WiFi isolated the Pi from the laptop.** Client isolation blocks
   device-to-device traffic, so the Pi couldn't reach the local backend directly.
   **Fix:** a `cloudflared` **quick tunnel** — the laptop dials out to Cloudflare
   (which assigns a public URL), the Pi hits that URL; both sides make only
   *outbound* connections, so isolation doesn't apply. Same reverse-tunnel trick as
   the production ingress, stood up on the laptop in one command.

**Interview takeaway:** I separated code failures from environment failures by
reading each error, forming a hypothesis, testing it *cheaply* before committing to
a slow rebuild, and fixing one root cause at a time.

## 10. Current status

| Piece | Status |
|---|---|
| `skycam` microservice — write endpoints | ✅ built, runs, verified locally |
| `skycam` — read endpoints (latest/list/telemetry) | ✅ built, verified |
| A1 — JPEG previews | ✅ endpoint accepts `preview`, stored in `previews/` |
| B1 — presigned URLs (browser → S3 direct) | ✅ proven (`preview_url` loads image/jpeg) |
| React admin GUI wired to real data | ✅ typechecks, live at `/camera` |
| Pi client updated to `/skycam` + color preview | ✅ done, pushed |
| Real Pi hardware capture | ✅ validated end-to-end (via cloudflared tunnel) |
| Deploy to `dev.observatory.services` | ✅ pushed to `dev`, CI/CD deploying |
| dev ingest accepting data | ⏳ needs `skycam-secrets` Sealed Secret (cluster access) |
| Cloud detection (Go service) | ⏳ deferred |
| Image id = UUID/hash | ⏳ deferred |

## 11. What's next

1. Create the **`skycam-secrets`** Sealed Secret (token + S3 keys) so dev ingest
   goes live end-to-end.
2. **Cloud detection** as a separate **Go** microservice + alarms + an Alarms page —
   the concrete next feature (likely the first *async* workload).
3. Under consideration (see roadmap notes): a **broker** (Redis / Kafka) to run
   detection & data products async; **keograms / timelapse**; a possible **DynamoDB**
   migration (business-driven).
4. Image-id scheme (hybrid timestamp + `sha256`); live preview over WebSocket;
   presigned *uploads* if body-size limits bite.

## 12. Concepts I can speak to

Microservice decomposition (and when *not* to split) · object storage vs database ·
presigned URLs vs proxying · content-addressed storage & integrity hashing ·
bearer-token vs JWT · request/response vs streaming (HTTP vs WebSocket) · NAT
traversal via reverse tunnels (Cloudflare Tunnel) · client isolation on shared WiFi ·
Kubernetes ingress routing · GitOps (ArgoCD) · Sealed Secrets · multi-stage Docker
builds & the cargo-chef double-compile tradeoff · decoupling services via HTTP
contracts · graceful degradation in clients · isolating environment vs code failures.

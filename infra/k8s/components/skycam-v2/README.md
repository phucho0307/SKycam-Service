# skycam-v2 component

The Go/gRPC skycam stack: `ingest` (2 replicas), `postgres`, `redis`, and the
`detect` and `notify` Celery apps (a worker and a Beat each). Included by the
**dev overlay only**. Promote by adding the same `components:` entry to
`overlays/release`, then `overlays/prod`.

## What is in git and what is not

| Step | Where | Status |
|---|---|---|
| Images built and pushed to GHCR | `build-and-deploy.yml` (ingest, detect, notify) | in git |
| Tag pinned in the overlay | same workflow's `bump-overlay` job | in git |
| Tests incl. `-race` | `skycam-ci.yml` | in git |
| Manifests | this directory | in git |
| **`skycam-v2-secrets`** | Sealed Secret, per namespace | **you, once** (below) |
| **Devices reaching ingest from the internet** | Traefik entryPoint + Cloudflare hostname | **you, once** (below) |

Until the secret exists, the new pods sit in `CreateContainerConfigError`.
Nothing else in the namespace is affected.

## 1. Seal the secret

The shape is in `infra/k8s/base/secrets.example.yaml`. Run this on a machine
with `kubeseal` and access to the cluster's public cert (see
`infra/bootstrap/README.md`):

```bash
NS=observatory-dev
kubectl create secret generic skycam-v2-secrets -n $NS --dry-run=client -o yaml \
  --from-literal=postgres_password="$(openssl rand -base64 24)" \
  --from-literal=s3_access_key='<MinIO user for observatory-dev>' \
  --from-literal=s3_secret_key='<its secret>' \
  --from-literal=operator_token="$(openssl rand -base64 32)" \
| kubeseal --format=yaml --cert sealed-secrets-pub.pem \
> infra/k8s/overlays/dev/skycam-v2-secrets.sealed.yaml
```

Then add `skycam-v2-secrets.sealed.yaml` to `overlays/dev/kustomization.yaml`
`resources:` and commit. Optional keys: `jwt_public_keys` (public half only, from
`ingest keygen`), `smtp_host`, `smtp_user`, `smtp_pass`.

`postgres_password` is read once, when the Postgres volume is first
initialised. Changing it later changes what clients send, not what Postgres
accepts. Rotate it with `ALTER USER`, then re-seal.

## 2. Let cameras reach it

In-cluster, `ingest:9090` serves plaintext HTTP/2 (h2c). Getting device traffic
to it needs two changes outside this repo, both found during the M0 test (see
`GO_GRPC_AND_POSTGRES.md`):

- **Traefik: a dedicated entryPoint with `readTimeout: 0`.** Traefik v3 defaults
  `respondingTimeouts.readTimeout` to 60s, which resets every long-lived
  `DeviceSession` at exactly 60 seconds. Never disable it on the browser-facing
  entryPoint (it is the slowloris defence). It must be a separate entryPoint.
- **Cloudflare: enable gRPC for the zone** (Network settings), and route a device
  hostname through the tunnel to that entryPoint. With gRPC off, Cloudflare
  returns 403 to `application/grpc` before the request reaches the origin.

Still unproven: whether Cloudflare carries client-streaming and bidi streams,
and for how long. If it does not, fall back to Mode B (direct DNS + Origin Cert),
which takes Cloudflare out of the device path.

## Design notes

- **Two ingest replicas is only correct because of Redis.** Session presence,
  command routing, rate-limit buckets and sign-in flows are all shared there.
  Without `INGEST_REDIS_URL` it must be `replicas: 1`.
- **Redis has no persistence and `noeviction`.** Everything durable is a
  Postgres row that the apps re-derive work from. Under memory pressure a write
  fails loudly, where LRU would silently drop a queued task or a presence key.
- **Health probes are native gRPC** (`grpc.health.v1`), exempt from auth by exact
  method name. On SIGTERM the pod reports NOT_SERVING before its listeners close.
- **Migrations run in an init container** with only `DATABASE_URL`. golang-migrate
  takes an advisory lock, so two pods starting together serialise.
- **Beat Deployments use `Recreate`.** Duplicate Beats are harmless here anyway,
  since both apps claim work from Postgres with `SKIP LOCKED`.
- **Single points of failure remain:** one Postgres, one Redis, no replicas. Fine
  for dev; before prod, use a managed database or CloudNativePG.

## Verified locally (2026-09-30)

No cluster was reachable, so every image, command, env var, user and read-only
root filesystem in these manifests was run on a Docker network instead. Results:

- Two migrations started at once gave one clean schema (`version=5 dirty=false`).
- Both replicas were SERVING on `grpc.health.v1`.
- The fake Pi uploaded 20 frames to replica A. All 20 were stored, 2 FITS were
  in MinIO, and all 20 were scored by the detect worker.
- A command sent through replica **B's** control API reached the camera
  connected to **A**, and was acknowledged.
- One rate-limit bucket for that camera existed in Redis, not one per pod.
- Both Celery liveness commands returned `1 node online`. There were 0 errors
  across six app logs. SIGTERM gave a clean exit (code 0).
- `kubeconform -strict`: 24 valid, 0 invalid. The 4 skipped are the
  SealedSecret CRDs.

The rehearsal found one deploy-blocking bug: the health service sat behind the
auth interceptor, so every probe would have failed and no pod would ever have
become Ready. Fixed, with a regression test.

# Cloudflare configuration

Two modes — pick the one matching where the VPS lives.

## Caching the skycam live view (needed from 2026-10-01)

The skycam service sends `Cache-Control` headers designed for the edge to
absorb viewer load. Measured: 2,000 polling viewers → ~47 origin requests in
30 s with caching, against ~14,400 for 500 viewers without. Cloudflare caches
by file extension by default:

- `/skycam/frames/<id>/preview.jpg`: **cached automatically** (`.jpg`), for a
  year (`immutable`).
- `/skycam/live`: **not cached until you add a Cache Rule.** Rules → Cache
  Rules → URI path starts with `/skycam/live` (and is not `/skycam/live/stream`)
  → *Eligible for cache*, *Use cache-control header if present*. The origin sends
  `max-age=2, stale-if-error=60`.
- `/skycam/live/stream` (SSE, opt-in) must **not** be cached or buffered.

## gRPC needs extra setup (tested 2026-09-27)

The Go ingest service (`services/ingest/`) speaks gRPC, and **Cloudflare blocks gRPC at the
edge by default**. Measured: two requests differing only in `Content-Type` —
`application/grpc` got a **403 from Cloudflare** without reaching the origin, while
`application/octet-stream` reached it (502). Before pointing a camera at a Cloudflare
hostname:

1. **Zone → Network → gRPC: on.** Not on by default. Without it, every RPC is a 403.
2. **A dedicated Public Hostname** (e.g. `ingest.dev.observatory.services`). gRPC paths look
   like `/skycam.v1.SkycamService/UploadFrame`, which does not match the `/skycam/` Prefix
   rule in `infra/k8s/base/ingress.yaml` — it falls through to `/` and hits the *frontend*.
3. **cloudflared needs `http2Origin: true`** for the gRPC hostname (the `--http2-origin`
   flag in `--url` mode). The existing tunnel targets Traefik over HTTP/1.1, which cannot
   carry gRPC.
4. **Traefik: give gRPC its own entryPoint with `respondingTimeouts.readTimeout: 0`.**
   Traefik v3 defaults it to 60s and a long-lived stream is a single request, so the
   `DeviceSession` is reset at exactly 60 seconds, repeatedly. With the timeout disabled the
   same stream held 5 minutes with no drops. Do **not** disable it on the browser-facing
   entrypoint — an unlimited read timeout there invites slowloris.

**Still unverified:** whether Cloudflare's proxy carries *client-streaming* and
*bidirectional* gRPC, and for how long, even with the zone setting on. If it does not,
Mode B removes Cloudflare from the data path.

## Mode A — Cloudflare Tunnel (current: VPS behind NAT)

All inbound traffic enters via an outbound-initiated cloudflared connection from inside the cluster. No public IP, no port forwarding, no Origin Cert.

See `infra/k8s/cloudflare-tunnel/README.md` for the full walkthrough. Quick summary:

1. Cloudflare Zero Trust → create a tunnel, copy the install token.
2. Add Public Hostnames for `observatory.services`, `dev.observatory.services`, `release.observatory.services` (and optionally `argocd.observatory.services`), all pointing at `http://traefik.ingress.svc.cluster.local:80`. Set the HTTP Host header to the public hostname on each one.
3. Seal the token as `cloudflared-token`, commit. ArgoCD's `cloudflare-tunnel` Application brings it up.
4. Cloudflare SSL/TLS mode: **Flexible** is fine while using the tunnel (Cloudflare encrypts to the cluster via the tunnel regardless).

## Mode B — Direct (future: colocation with public IP)

When the VPS has a routable IP, you can switch from tunnel to standard DNS:

1. DNS records (proxied) → A records pointing at the public IP:

   | Hostname                       | Type | Value          | Proxy |
   |--------------------------------|------|----------------|-------|
   | `observatory.services`         | A    | `<PUBLIC_IP>`  | yes   |
   | `dev.observatory.services`     | A    | `<PUBLIC_IP>`  | yes   |
   | `release.observatory.services` | A    | `<PUBLIC_IP>`  | yes   |

2. Generate a Cloudflare Origin Certificate covering `observatory.services` and `*.observatory.services` (15-year validity).

3. Install the cert as a TLS secret in each namespace:

   ```bash
   for ns in observatory-dev observatory-release observatory-prod; do
     kubectl -n "$ns" create secret tls cloudflare-origin-tls \
       --cert=origin.pem --key=origin.key
   done
   ```

4. Add a `tls:` block back to `infra/k8s/base/ingress.yaml`:

   ```yaml
   spec:
     tls:
       - hosts: [HOST_PLACEHOLDER]
         secretName: cloudflare-origin-tls
   ```

   And re-add the corresponding patch op in each overlay (`/spec/tls/0/hosts/0`).

5. Cloudflare SSL/TLS mode: **Full (strict)**.

6. Optionally enable **Authenticated Origin Pulls** so only Cloudflare can reach the origin.

7. Once verified, you can delete the `cloudflare-tunnel` Application (or keep cloudflared as a failover path).

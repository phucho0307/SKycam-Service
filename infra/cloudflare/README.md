# Cloudflare configuration

Two modes — pick the one matching where the VPS lives.

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

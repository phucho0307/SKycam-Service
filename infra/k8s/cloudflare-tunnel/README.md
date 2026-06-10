# Cloudflare Tunnel

Outbound-only TLS-terminated path from Cloudflare's edge to the cluster. Required while the VPS sits behind NAT; can be removed (or kept) after the colocation move.

## One-time setup

1. **Create the tunnel** in Cloudflare Zero Trust dashboard:
   - Networks → Tunnels → Create a tunnel → "Cloudflared" connector
   - Name it `observatory-services` (or similar)
   - Copy the install token shown for "Docker" / "Kubernetes" — that long base64 string is what we need

2. **Add public hostnames** on the tunnel — all three point at the same in-cluster Traefik service:

   | Subdomain    | Domain                 | Service                                       |
   | ------------ | ---------------------- | --------------------------------------------- |
   | (apex — `@`) | `observatory.services` | `http://traefik.ingress.svc.cluster.local:80` |
   | `dev`        | `observatory.services` | `http://traefik.ingress.svc.cluster.local:80` |
   | `release`    | `observatory.services` | `http://traefik.ingress.svc.cluster.local:80` |

   For each, expand **Additional application settings → HTTP Settings** and set the **HTTP Host Header** to the public hostname (e.g. `dev.observatory.services`). Traefik routes by Host, so this header must survive the tunnel hop.

3. **Seal the token** and commit it:

   ```bash
   kubectl create secret generic cloudflared-token \
     --namespace=cloudflare-tunnel \
     --from-literal=token='eyJhIjoi...' \
     --dry-run=client -o yaml \
   | kubeseal --format=yaml \
     --controller-namespace=kube-system \
     --controller-name=sealed-secrets-controller \
   > infra/k8s/cloudflare-tunnel/cloudflared-token.sealed.yaml
   ```

   Add `cloudflared-token.sealed.yaml` to `kustomization.yaml` resources and commit. ArgoCD picks it up on next sync.

## Colocation migration

When the VPS gets a public IP:

- Keep the tunnel running for failover, OR delete the cloudflared Deployment and switch Cloudflare DNS records to A records pointing at the public IP.
- If switching to direct: generate a Cloudflare Origin Certificate, store as a `cloudflare-origin-tls` Secret in each namespace, and add a `tls:` block back to `infra/k8s/base/ingress.yaml`. Set Cloudflare SSL mode to **Full (strict)**.

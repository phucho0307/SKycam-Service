# Observatory Services

Public site and platform for Observatory Solutions LLC at `observatory.services`.

## Repository layout

```text
backend/        Rust workspace (Rocket + MongoDB)
  crates/
    api/            HTTP API: auth, users, observatories, imaging, feature requests
    release-proxy/  Pulls releases from private GitHub repos, caches to S3, serves binaries
    domain/         Shared domain types
frontend/       Vite + React 19 + TypeScript + Tailwind v4
infra/
  k8s/
    base/             Shared manifests
    overlays/         dev / release / prod kustomize overlays
    cloudflare-tunnel/ cloudflared Deployment
  argocd/             AppProject, root app-of-apps, env Applications
  bootstrap/          One-time VPS setup walkthrough
  cloudflare/         Tunnel + future direct-mode setup notes
.github/
  workflows/      backend-ci, frontend-ci, infra-validate, build-and-deploy
```

## Environments

| Branch    | Host                            | Namespace             | Sync       |
|-----------|---------------------------------|-----------------------|------------|
| `dev`     | `dev.observatory.services`      | `observatory-dev`     | auto       |
| `release` | `release.observatory.services`  | `observatory-release` | auto       |
| `main`    | `observatory.services`          | `observatory-prod`    | manual     |

Each environment has its own MongoDB StatefulSet inside its namespace.

## Deploy flow

1. Push to `dev` / `release` / `main` →
2. `build-and-deploy` workflow builds container images (api, release-proxy, frontend), pushes to `ghcr.io/observatoryservices/...:<branch>-<sha>` →
3. Workflow runs `kustomize edit set image` on the matching overlay and commits the new SHA tag back to the same branch with `[skip ci]` →
4. ArgoCD on the cluster detects the kustomize change and reconciles. Dev and release auto-sync; prod waits for a human click.

## Inbound traffic (Mode A, current)

Cluster sits behind NAT. **Cloudflare Tunnel** (cloudflared, deployed inside the cluster) terminates TLS at Cloudflare's edge and forwards to Traefik (MicroK8s' default ingress controller) over the outbound connection. No public IP needed.

When the VPS moves to colocation, switch to **Mode B** in `infra/cloudflare/README.md` (direct A records + Origin Cert + Full-strict).

## Bootstrap

Brand-new VPS → fully GitOps cluster: follow `infra/bootstrap/README.md`. After that, only git pushes are needed.

## Local development

### Backend

```bash
cd backend
cargo run -p api
```

Requires `MONGODB_URI` (defaults to `mongodb://localhost:27017`) and `MONGODB_DB` (defaults to `observatory`).

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Vite proxies `/api/*` to `http://localhost:8000` so the backend runs without CORS.

## Phase status

- [x] **Phase 1a** — monorepo skeleton
- [x] **Phase 1b** — Cloudflare Tunnel + ArgoCD GitOps + build-and-deploy pipeline
- [ ] **Phase 1c** — release proxy + winget feed + Homebrew tap + feature-request → GH Issues
- [ ] **Phase 2** — Google sign-in + RBAC + marketing pages
- [ ] **Phase 3** — imaging request + image database + analysis tools
- [ ] **Phase 4** — observatory status pages + feeds
- [ ] **Phase 5** — hardware PC-parts-picker + observatory designer

## Placeholders to fill before deploy

- `<S3_ENDPOINT>` — your S3-compatible endpoint URL (in `infra/k8s/overlays/*/kustomization.yaml`)
- `<S3_BUCKET>` — bucket per env (defaults to `observatory-{env}` in each overlay)

Repo: `https://github.com/ObservatoryServices/ObservatoryServices`.
GHCR uses lowercase: `ghcr.io/observatoryservices/observatory-services-{api,release-proxy,frontend}`.

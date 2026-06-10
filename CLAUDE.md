# Claude Code notes for this repo

## Stack

- Backend: Rust workspace, Rocket 0.5, official `mongodb` driver, tokio runtime.
- Frontend: Vite + React 19 + TypeScript + Tailwind v4 (CSS-based config).
- Infra: MicroK8s on VPS (`obsserv@10.101.229.77`), Traefik ingress (MicroK8s default; IngressClass `public` is registered by Traefik), Cloudflare Tunnel terminating at Cloudflare's edge (Mode A while behind NAT; switches to Origin Cert + direct DNS for colo move — see `infra/cloudflare/README.md`).
- GitOps: ArgoCD on the cluster, app-of-apps pattern, three env Applications + cloudflare-tunnel Application.
- Secrets: Sealed Secrets controller; everything encrypted in git.
- CI/CD: GitHub Actions → GHCR images → workflow bumps kustomize image tag in the matching overlay → ArgoCD syncs.

## Conventions

- Workspace crates live under `backend/crates/<name>`.
- Each backend crate has its own binary and its own k8s Deployment.
- Frontend pages live in `frontend/src/routes/`, components in `frontend/src/components/`.
- All env-driven config goes through `crates/api/src/config.rs` (and equivalents per crate). Do not read env vars inline elsewhere.
- Domain types shared between crates live in `crates/domain`.

## Environments and branches

- `dev` branch → `dev.observatory.services` → namespace `observatory-dev` (ArgoCD auto-sync)
- `release` branch → `release.observatory.services` → namespace `observatory-release` (ArgoCD auto-sync)
- `main` branch → `observatory.services` → namespace `observatory-prod` (ArgoCD manual sync)

Never merge to `main` directly; promote through `dev` → `release` → `main`.

GitHub org slug is `ObservatoryServices`. GHCR is case-insensitive but image refs MUST use lowercase `observatoryservices`.

## Avoid

- Postgres / SQL — this project is MongoDB only.
- Axum / Actix — backend is Rocket.
- Plain `Secret` resources committed to git — always go through Sealed Secrets (kubeseal). `infra/k8s/base/secrets.example.yaml` shows the shapes but is reference-only.
- Bypassing ArgoCD by running `kubectl apply` against the cluster — ArgoCD will revert it on the next sync. Make changes in git.
- Pinning images to `:latest` in overlays — the build-and-deploy workflow pins to `:<branch>-<sha>` immutable tags.

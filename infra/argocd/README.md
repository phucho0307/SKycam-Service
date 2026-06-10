# ArgoCD setup

## Topology

```
root (Application, tracks main:infra/argocd/applications/)
 ├── observatory-dev     → dev branch     → infra/k8s/overlays/dev      → observatory-dev ns
 ├── observatory-release → release branch → infra/k8s/overlays/release  → observatory-release ns
 ├── observatory-prod    → main branch    → infra/k8s/overlays/prod     → observatory-prod ns
 └── cloudflare-tunnel   → main branch    → infra/k8s/cloudflare-tunnel → cloudflare-tunnel ns
```

`dev` and `release` auto-sync on git push. `prod` requires a human click (the `syncPolicy.automated` block is intentionally omitted). Flip it on later if you trust the promotion pipeline.

## How deploys flow

1. Developer pushes to `dev` branch.
2. `.github/workflows/build-and-deploy.yml` builds + pushes container images to GHCR tagged `:dev-{sha}`.
3. Same workflow runs `kustomize edit set image` against `infra/k8s/overlays/dev/kustomization.yaml`, pinning the new SHA tag, and commits back to `dev` with `[skip ci]`.
4. ArgoCD's `observatory-dev` Application detects the git change and reconciles. New image rolls out.

Promotion to `release` and `main` is a git merge from `dev` → `release` → `main`. Each merge updates the corresponding overlay's image tag (via the same CI workflow on the target branch) and ArgoCD reconciles.

## Bootstrap

See `infra/bootstrap/README.md`. After ArgoCD is installed and has a credential for this repo, the only thing you need to apply by hand is `infra/argocd/project.yaml` and `infra/argocd/root.yaml`. Everything else is pulled in.

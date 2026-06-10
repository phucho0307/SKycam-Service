# VPS bootstrap

One-time setup to get the cluster from clean Ubuntu + MicroK8s installed to "ArgoCD takes over from here."

Target: `ssh obsserv@10.101.229.77` (Ubuntu Server, MicroK8s pre-installed).

## 1. MicroK8s addons

On the VPS:

```bash
sudo microk8s enable dns
sudo microk8s enable hostpath-storage
sudo microk8s enable ingress
sudo microk8s enable metrics-server
sudo microk8s enable helm3
sudo microk8s status --wait-ready
```

Set up the kubeconfig:

```bash
mkdir -p ~/.kube
sudo microk8s config > ~/.kube/config
chmod 600 ~/.kube/config

# Either alias microk8s.kubectl as kubectl, or install kubectl directly:
sudo snap install kubectl --classic
```

Confirm:

```bash
kubectl get nodes
kubectl -n ingress get ds          # nginx-ingress-microk8s-controller
```

## 2. Sealed Secrets controller

Lets you commit encrypted secrets to git that only this cluster can decrypt.

```bash
kubectl apply -f https://github.com/bitnami-labs/sealed-secrets/releases/latest/download/controller.yaml

# kubeseal CLI on your laptop (not the VPS):
#   macOS:   brew install kubeseal
#   Windows: scoop install kubeseal     (or download from the GitHub release)
#   Linux:   download the kubeseal binary from the same release
```

Fetch the public cert (used to encrypt secrets offline):

```bash
kubeseal --controller-namespace=kube-system --controller-name=sealed-secrets-controller \
  --fetch-cert > sealed-secrets-pub.pem
```

Keep `sealed-secrets-pub.pem` on your workstation; it's safe to commit but doesn't need to be.

## 3. ArgoCD

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# Wait for it to come up
kubectl -n argocd rollout status deploy/argocd-server
```

Initial admin password:

```bash
kubectl -n argocd get secret argocd-initial-admin-secret \
  -o jsonpath='{.data.password}' | base64 -d; echo
```

### Expose ArgoCD via the tunnel (optional, recommended)

Add a 4th public hostname on the tunnel, e.g. `argocd.observatory.services` →
`https://argocd-server.argocd.svc.cluster.local:443`, with **No TLS Verify** on
(self-signed internal cert) under Additional application settings.

### Give ArgoCD read access to this repo

Since `ObservatoryServices/ObservatoryServices` is private:

1. Generate a fine-grained PAT or deploy key with **read-only access to this repo**.
2. In ArgoCD UI → Settings → Repositories → Connect Repo via HTTPS:
   - URL: `https://github.com/ObservatoryServices/ObservatoryServices.git`
   - Username: anything (e.g. `argocd`)
   - Password: the PAT

Or via CLI:

```bash
argocd repo add https://github.com/ObservatoryServices/ObservatoryServices.git \
  --username argocd --password "$GH_PAT"
```

## 4. Apply the AppProject and the root Application

```bash
kubectl apply -f infra/argocd/project.yaml
kubectl apply -f infra/argocd/root.yaml
```

That's it. ArgoCD now reconciles everything else:

- `cloudflare-tunnel` Application → installs cloudflared (once the sealed token is committed; see `infra/k8s/cloudflare-tunnel/README.md`)
- `observatory-dev` / `observatory-release` → auto-sync on branch pushes
- `observatory-prod` → manual sync (sync button in ArgoCD UI or `argocd app sync observatory-prod`)

## 5. Commit the sealed secrets

For each environment namespace you need:

- `mongodb-credentials` (StatefulSet init user/pass)
- `api-secrets` (mongodb_uri)
- `release-proxy-secrets` (S3 + GitHub App — Phase 1c)

For the tunnel namespace you need:

- `cloudflared-token` (see `infra/k8s/cloudflare-tunnel/README.md`)

Example for `observatory-dev` Mongo creds:

```bash
kubectl create secret generic mongodb-credentials \
  --namespace=observatory-dev \
  --from-literal=username='observatory' \
  --from-literal=password="$(openssl rand -base64 24)" \
  --dry-run=client -o yaml \
| kubeseal --format=yaml \
  --controller-namespace=kube-system \
  --controller-name=sealed-secrets-controller \
> infra/k8s/overlays/dev/mongodb-credentials.sealed.yaml
```

Then add `mongodb-credentials.sealed.yaml` to the overlay's `kustomization.yaml` resources list and commit. Repeat for each Secret in each overlay.

## 6. Verify

```bash
argocd app list
argocd app sync observatory-dev   # if not already auto-synced
kubectl -n observatory-dev get pods
```

Hit `https://dev.observatory.services/` once cloudflared is up.

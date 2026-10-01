# syntax=docker/dockerfile:1.7
#
# Go ingest service. Build context is services/ingest (the generated protobuf
# code is vendored under gen/, so proto/ is not needed here).
#
#   docker build -t ingest services/ingest
#
# Behind a TLS-intercepting proxy, pass its CA as a build secret; CI passes
# nothing and the step is a no-op:
#
#   docker build --secret id=extra_ca,src=corp-ca-bundle.crt -t ingest services/ingest

FROM golang:1.27 AS build
RUN --mount=type=secret,id=extra_ca,required=false \
    if [ -s /run/secrets/extra_ca ]; then \
      cat /run/secrets/extra_ca >> /etc/ssl/certs/ca-certificates.crt; \
    fi
WORKDIR /src
# Modules first, so a source-only change reuses the download layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# Static binary: no cgo, so it runs on distroless with no libc.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingest ./cmd/ingest

# distroless/static: no shell, no package manager, CA certificates included
# (needed for S3 over HTTPS and the OIDC provider). Nothing to exec into is a
# feature for an internet-facing service.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ingest /ingest
USER nonroot:nonroot
# 9090 devices (gRPC), 9091 control (loopback only), 9092 sign-in (HTTP)
EXPOSE 9090 9092
ENTRYPOINT ["/ingest"]

# syntax=docker/dockerfile:1

# =============================================================================
# redimos proxy container image (task 24.1)
#
# Multi-stage build:
#   1. `build`  - compiles a static Linux binary for cmd/redimos with the Go
#                 toolchain.
#   2. `final`  - copies only the binary into a distroless static image that
#                 runs as a non-root user and exposes the RESP2 port 6379.
#
# Build context is the redimos repository root; the redimo/v2 dependency resolves
# from the Go module proxy at the version pinned in go.mod (github.com/aura-studio/
# redimo/v2, currently v2.8.1):
#
#     docker build -t redimos:latest .
# =============================================================================

# ----------------------------- build stage ----------------------------------
# Matches go.mod's `go 1.25`; an older toolchain would download an unpinned one.
FROM golang:1.25-alpine AS build

# git is occasionally needed for module resolution; ca-certificates lets the
# build fetch modules over TLS when the module cache is cold.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Manifests first to leverage layer caching for deps.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Now copy the full redimos source and build.
COPY . ./

# Static, stripped binary: no CGO, trimmed paths, no symbol/debug tables.
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/redimos ./cmd/redimos

# ----------------------------- final stage ----------------------------------
# distroless static: minimal image, no shell, runs as non-root (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot AS final

LABEL org.opencontainers.image.title="redimos" \
      org.opencontainers.image.description="RESP2-compatible proxy backed by DynamoDB" \
      org.opencontainers.image.source="https://github.com/aura-studio/redimos"

# RESP2 endpoint (matches the -addr default of :6379 in cmd/redimos/main.go).
EXPOSE 6379
# Observability endpoint: /metrics (Prometheus) + /healthz. Matches the
# -metrics-addr default of :9121 in cmd/redimos/main.go (requirement 18.5).
EXPOSE 9121

COPY --from=build /out/redimos /usr/local/bin/redimos

# Non-root by default (distroless nonroot => uid/gid 65532).
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/redimos"]
# Default args; override table/auth/consistency/ports at runtime via the task
# definition. Bind both listeners on all interfaces so the container is
# reachable from the NLB (RESP2) and the metrics scraper (/metrics, /healthz).
CMD ["-addr=:6379", "-metrics-addr=:9121", "-table=redis-data", "-consistency=strong"]

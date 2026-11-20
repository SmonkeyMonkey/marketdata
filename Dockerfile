# syntax=docker/dockerfile:1

# One build for every service: all binaries come out of a single `go build`,
# and each image is a thin stage that copies its own binary (compose picks
# it with `target:`). Build stages run on the build machine's platform and
# cross-compile to the target one, so an amd64 image builds natively on an
# arm64 Mac (and the other way round) without emulation.

# Frontend bundle; only the api image embeds it.
FROM --platform=$BUILDPLATFORM node:24-alpine AS web
WORKDIR /web
COPY frontend/package.json frontend/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci
COPY frontend/index.html frontend/vite.config.ts frontend/tsconfig.json ./
COPY frontend/src ./src
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS go
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
# Only Go sources: SQL, dashboards or frontend changes don't invalidate this.
COPY cmd ./cmd
COPY internal ./internal
COPY frontend/embed.go ./frontend/
ARG TARGETOS TARGETARCH VERSION=dev
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
# Every service except api (which needs the frontend, built below).
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w -X marketdata/internal/ops.Version=${VERSION}" \
      -o /out/ $(go list ./cmd/... | grep -v '/cmd/api$')

FROM go AS go-api
COPY --from=web /web/dist ./frontend/dist
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w -X marketdata/internal/ops.Version=${VERSION}" -o /out/api ./cmd/api

FROM gcr.io/distroless/static-debian12:nonroot AS base
ENTRYPOINT ["/app"]

FROM base AS ingester
COPY --from=go /out/ingester /app

FROM base AS writer
COPY --from=go /out/writer /app

FROM base AS aggregator
COPY --from=go /out/aggregator /app

FROM base AS alerts
COPY --from=go /out/alerts /app

FROM base AS arbitrage
COPY --from=go /out/arbitrage /app

FROM base AS gateway
COPY --from=go /out/gateway /app

FROM base AS loadgen
COPY --from=go /out/loadgen /app

FROM base AS replay
COPY --from=go /out/replay /app

# Last, so a plain `docker build .` produces the api image.
FROM base AS api
COPY --from=go-api /out/api /app

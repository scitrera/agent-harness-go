# syntax=docker/dockerfile:1
#
# agent-harness — the OSS reference build.
#
# Every dependency resolves from the public Go proxy, so the build context is
# just this module: no named build contexts, no replace directives to recreate.
# (The Scitrera distribution's image needs those; this one deliberately does
# not, which is the point of keeping the OSS module dependency-clean.)
#
#   docker build -t agent-harness:local .

ARG GO_IMAGE=golang:1.25

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS builder

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /src

# Dependencies first, so a source-only change does not re-download them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /out/agent-harness ./cmd/agent-harness

FROM alpine:3.23.4

RUN apk add --no-cache ca-certificates tzdata python3 && \
    addgroup -g 1000 harness && \
    adduser -D -u 1000 -G harness harness && \
    mkdir -p /workspace && chown harness:harness /workspace

COPY --from=builder /out/agent-harness /usr/local/bin/agent-harness

USER harness
WORKDIR /workspace

# The web UI, when --web is used. Aether modes dial out and expose nothing.
EXPOSE 8787

ENTRYPOINT ["agent-harness"]
CMD ["--serve"]

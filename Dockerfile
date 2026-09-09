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

RUN GOBIN=/tmp/release-tools \
    go install github.com/google/go-licenses/v2@v2.0.1 && \
    /tmp/release-tools/go-licenses report \
      ./cmd/agent-harness ./cmd/tool-catalog-service \
      --ignore github.com/scitrera/agent-harness-go > /tmp/third-party-licenses.csv && \
    ! grep -Eq ',Unknown$' /tmp/third-party-licenses.csv && \
    /tmp/release-tools/go-licenses save \
      ./cmd/agent-harness ./cmd/tool-catalog-service \
      --ignore github.com/scitrera/agent-harness-go \
      --save_path /out/third_party_licenses && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      go build -trimpath -ldflags="-s -w" -o /out/agent-harness ./cmd/agent-harness && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      go build -trimpath -ldflags="-s -w" -o /out/tool-catalog-service ./cmd/tool-catalog-service

FROM alpine:3.23.4

RUN apk add --no-cache ca-certificates tzdata python3 && \
    addgroup -g 1000 harness && \
    adduser -D -u 1000 -G harness harness && \
    mkdir -p /workspace && chown harness:harness /workspace

COPY --from=builder /out/agent-harness /usr/local/bin/agent-harness
COPY --from=builder /out/tool-catalog-service /usr/local/bin/tool-catalog-service
COPY --from=builder /src/LICENSE /src/NOTICE /usr/share/licenses/agent-harness/
COPY --from=builder /out/third_party_licenses /usr/share/licenses/agent-harness/third_party/

USER harness
WORKDIR /workspace

# The web UI, when --web is used. Aether modes dial out and expose nothing.
EXPOSE 8787

ENTRYPOINT ["agent-harness"]
CMD ["--serve"]

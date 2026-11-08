# Runs tools/netsim/compare/compare.py: hikyaku, satmux and TUIC v5 over netsim's
# emulated paths. Build from the repository root; the container needs TUN and
# network namespaces, which --privileged grants:
#
#   docker build -f tools/netsim/Dockerfile -t hikyaku-netsim .
#   docker run --rm --privileged -v "$PWD/netsim-out:/tmp/netsim-out" hikyaku-netsim
#
# Options such as --profiles 620ms or --systems hikyaku,tuic go after the image name.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/hikyaku-server ./cmd/hikyaku-client ./tools/netsim

FROM debian:trixie-slim
ARG TARGETARCH
ARG SATMUX_COMMIT=2922ebb835d0eb939ddbdfa3b5e7e0f247a0dc0c
ARG TUIC_VERSION=v2.0.0-dev10
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl iproute2 openssl python3 \
 && rm -rf /var/lib/apt/lists/*
RUN arch=$([ "$TARGETARCH" = arm64 ] && echo aarch64 || echo x86_64) \
 && for side in server client; do \
      curl -fsSL -o /usr/local/bin/tuic-$side \
        https://github.com/Itsusinn/tuic/releases/download/$TUIC_VERSION/tuic-$side-$arch-linux-musl; \
    done \
 && chmod +x /usr/local/bin/tuic-* \
 && mkdir -p /opt/satmux \
 && curl -fsSL -o /opt/satmux/satmux.py https://raw.githubusercontent.com/Th0rgal/satmux/$SATMUX_COMMIT/satmux.py
COPY --from=build /out/ /usr/local/bin/
COPY tools/netsim/compare/ /opt/compare/
ENTRYPOINT ["python3", "/opt/compare/compare.py"]

# Runs tools/netsim/compare/compare.py: hikyaku, satmux and TUIC v5 over netsim's
# emulated paths. Build from the repository root; the container needs TUN and
# network namespaces, which --privileged grants:
#
#   docker build -f tools/netsim/Dockerfile -t hikyaku-netsim .
#   docker run --rm --privileged -v "$HOME/hikyaku-netsim-out:/tmp/netsim-out" hikyaku-netsim
#
# Options such as --profiles 620ms or --systems hikyaku,tuic go after the image name.
# Results go outside the repository, which the build copies in.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/hikyaku-server ./cmd/hikyaku-client ./tools/netsim

FROM debian:trixie-slim
ARG TARGETARCH
# Changing either pin needs new sums in tools/netsim/checksums.sha256.
ARG SATMUX_COMMIT=2922ebb835d0eb939ddbdfa3b5e7e0f247a0dc0c
ARG TUIC_VERSION=v2.0.0-dev10
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl iproute2 openssl python3 \
 && rm -rf /var/lib/apt/lists/*
COPY tools/netsim/checksums.sha256 /tmp/dl/
RUN arch=$([ "$TARGETARCH" = arm64 ] && echo aarch64 || echo x86_64) \
 && cd /tmp/dl \
 && for side in server client; do \
      curl -fsSLO https://github.com/Itsusinn/tuic/releases/download/$TUIC_VERSION/tuic-$side-$arch-linux-musl \
      || exit 1; \
    done \
 && curl -fsSLO https://raw.githubusercontent.com/Th0rgal/satmux/$SATMUX_COMMIT/satmux.py \
 && sha256sum --check --strict --ignore-missing checksums.sha256 \
 && install -m 755 tuic-server-$arch-linux-musl /usr/local/bin/tuic-server \
 && install -m 755 tuic-client-$arch-linux-musl /usr/local/bin/tuic-client \
 && install -D -m 644 satmux.py /opt/satmux/satmux.py \
 && cd / && rm -r /tmp/dl
COPY --from=build /out/ /usr/local/bin/
COPY tools/netsim/compare/ /opt/compare/
ENTRYPOINT ["python3", "/opt/compare/compare.py"]

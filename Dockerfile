# Build on the runner's own architecture and cross-compile, so an arm64 image
# costs a compile instead of an emulated build.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY src ./src
ARG TARGETOS
ARG TARGETARCH
# TARGETVARIANT carries the ARM level (v6, v7); Go wants it as a bare number in
# GOARM. It is empty for every other architecture, which Go ignores.
ARG TARGETVARIANT
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOARM=${TARGETVARIANT#v} go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /gha-docker-scaleset ./src/cmd/gha-docker-scaleset

# Runs as root: it talks to the Docker socket and removes work directories
# that jobs filled with root-owned files.
FROM gcr.io/distroless/static-debian12
LABEL org.opencontainers.image.source=https://github.com/feyst/gha-docker-scaleset
LABEL org.opencontainers.image.description="Ephemeral GitHub Actions runners as Docker containers, one per job, under one cap"
LABEL org.opencontainers.image.licenses=MIT
COPY --from=build /gha-docker-scaleset /gha-docker-scaleset
ENTRYPOINT ["/gha-docker-scaleset"]

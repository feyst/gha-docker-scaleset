FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o /gha-docker-scaleset .

# Runs as root: it talks to the Docker socket and removes work directories
# that jobs filled with root-owned files.
FROM gcr.io/distroless/static-debian12
COPY --from=build /gha-docker-scaleset /gha-docker-scaleset
ENTRYPOINT ["/gha-docker-scaleset"]

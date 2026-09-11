GO_IMAGE ?= golang:1.26
IMAGE ?= gha-docker-scaleset:dev
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

# Go runs in a container, so the host needs nothing but Docker.
GO = docker run --rm -u $$(id -u):$$(id -g) -e HOME=/tmp \
	-e GOCACHE=/cache/build -e GOMODCACHE=/cache/mod -e GOFLAGS=-buildvcs=false \
	-v $(HOME)/.cache/gha-docker-scaleset:/cache -v $(CURDIR):/src -w /src $(GO_IMAGE)

.PHONY: test vet fmt tidy image

test:
	$(GO) go test -race ./...

vet:
	$(GO) go vet ./...

fmt:
	$(GO) gofmt -l -w .

tidy:
	$(GO) go mod tidy

image:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t $(IMAGE) .

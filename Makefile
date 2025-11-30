.PHONY: help build build-scanner push-scanner clean

# Variables
DOCKER_IMAGE_NAME ?= scanasservice-scanner
DOCKER_REGISTRY ?= docker.io
DOCKER_IMAGE_TAG ?= latest
DOCKER_IMAGE_FULL ?= $(DOCKER_REGISTRY)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)

help:
	@echo "ScanAsService Build Tasks"
	@echo ""
	@echo "Targets:"
	@echo "  build-scanner      Build the scanner image locally"
	@echo "  build-scanner-tag  Build scanner with custom tag (TAG=myregistry/image:v1.0)"
	@echo "  push-scanner       Push scanner image to registry (requires TAG set)"
	@echo "  clean              Remove built images"
	@echo ""
	@echo "Examples:"
	@echo "  make build-scanner"
	@echo "  make build-scanner TAG=myregistry.com/scanasservice:v1.0"
	@echo "  make push-scanner TAG=myregistry.com/scanasservice:v1.0"

build-scanner:
	@echo "Building scanner image: $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)"
	docker build -t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) -f Dockerfile .

build-scanner-tag:
	@if [ -z "$(TAG)" ]; then \
		echo "Error: TAG variable not set. Usage: make build-scanner-tag TAG=myregistry.com/scanasservice:v1.0"; \
		exit 1; \
	fi
	@echo "Building scanner image with tag: $(TAG)"
	docker build -t $(TAG) -f Dockerfile .

push-scanner:
	@if [ -z "$(TAG)" ]; then \
		echo "Error: TAG variable not set. Usage: make push-scanner TAG=myregistry.com/scanasservice:v1.0"; \
		exit 1; \
	fi
	@echo "Pushing scanner image: $(TAG)"
	docker push $(TAG)

clean:
	@echo "Removing scanner images..."
	docker rmi -f $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) || true
	@if [ -n "$(TAG)" ]; then docker rmi -f $(TAG) || true; fi

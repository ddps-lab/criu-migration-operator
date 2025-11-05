.PHONY: all build test docker-build docker-push install uninstall generate manifests help download-criu

# Image registry and tags
REGISTRY ?= 192.168.0.253:5000
AGENT_IMG ?= $(REGISTRY)/criu-agent:latest
CONTROLLER_IMG ?= $(REGISTRY)/criu-migration-controller:latest
MONITOR_IMG ?= $(REGISTRY)/criu-node-monitor:latest

# CRIU binary URL
CRIU_URL ?= https://mhsong-criu-s3-data.s3.us-west-2.amazonaws.com/criu
CRIU_DIR = criu
CRIU_BIN = $(CRIU_DIR)/criu

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

all: build

##@ General

help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

generate: ## Generate code (protobuf, deepcopy, etc.)
	@echo "Generating protobuf code..."
	./scripts/generate-proto.sh
	@echo "Generating deepcopy code..."
	controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

fmt: ## Run go fmt against code.
	go fmt ./...

vet: ## Run go vet against code.
	go vet ./...

test: fmt vet ## Run tests.
	go test ./... -coverprofile cover.out

##@ Build

build: fmt vet ## Build binaries.
	go build -o bin/agent cmd/agent/main.go
	go build -o bin/controller cmd/controller/main.go
	go build -o bin/node-monitor cmd/node-monitor/main.go

##@ Docker

download-criu: ## Download CRIU binary from S3.
	@echo "Downloading CRIU binary from $(CRIU_URL)..."
	@mkdir -p $(CRIU_DIR)
	@curl -L -o $(CRIU_BIN) $(CRIU_URL)
	@chmod +x $(CRIU_BIN)
	@echo "CRIU binary downloaded to $(CRIU_BIN)"
	@$(CRIU_BIN) --version || echo "Warning: CRIU version check failed"

docker-build: download-criu ## Build docker images.
	docker build -t ${AGENT_IMG} -f deploy/agent/Dockerfile .
	docker build -t ${CONTROLLER_IMG} -f deploy/controller/Dockerfile .
	docker build -t ${MONITOR_IMG} -f deploy/node-monitor/Dockerfile .

docker-push: docker-build ## Push docker images.
	docker push ${AGENT_IMG}
	docker push ${CONTROLLER_IMG}
	docker push ${MONITOR_IMG}

##@ Deployment

manifests: ## Generate Kubernetes manifests.
	controller-gen crd paths="./api/..." output:crd:artifacts:config=config/crd
	controller-gen rbac:roleName=migration-controller paths="./pkg/controller/..." output:rbac:artifacts:config=config/rbac

install: manifests ## Install CRDs into the K8s cluster.
	kubectl apply -f config/crd/

uninstall: ## Uninstall CRDs from the K8s cluster.
	kubectl delete -f config/crd/

deploy: manifests ## Deploy controller to the K8s cluster.
	kubectl apply -f config/crd/
	kubectl apply -f config/rbac/
	kubectl apply -f config/manager/

undeploy: ## Undeploy controller from the K8s cluster.
	kubectl delete -f config/manager/ --ignore-not-found
	kubectl delete -f config/rbac/ --ignore-not-found
	kubectl delete -f config/crd/ --ignore-not-found

##@ Dependencies

controller-gen: ## Download controller-gen if necessary.
	@test -s $(GOBIN)/controller-gen || \
	go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest

protoc-gen-go: ## Download protoc-gen-go if necessary.
	@test -s $(GOBIN)/protoc-gen-go || \
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

protoc-gen-go-grpc: ## Download protoc-gen-go-grpc if necessary.
	@test -s $(GOBIN)/protoc-gen-go-grpc || \
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# CRIU Migration Operator for Kubernetes

A Kubernetes operator that enables zero-downtime live migration of applications using CRIU (Checkpoint/Restore In Userspace) with Object Storage integration. Designed specifically for Spot/Preemptible instances.

## Overview

This operator provides:
- **Automatic Migration**: Detects spot instance interruptions and automatically migrates workloads
- **Incremental Checkpoints**: Regular pre-checkpoints with minimal overhead
- **Object Storage Integration**: Stores checkpoints in S3/MinIO/GCS for cross-node migration
- **Lazy Page Loading**: Fast restore with on-demand page fetching from object storage
- **Kubernetes Native**: CRD-based API with familiar kubectl workflows

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                 Kubernetes Cluster                       │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │          Migration Controller                  │    │
│  │  - Reconciles MigratableApp resources          │    │
│  │  - Orchestrates migrations                     │    │
│  │  - Manages Pod lifecycle                       │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │         Node Monitor (DaemonSet)               │    │
│  │  - Detects spot interruptions                  │    │
│  │  - Triggers migrations                         │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │           Application Pod                      │    │
│  │  ┌──────────────┐   ┌────────────────────┐    │    │
│  │  │ App Container│   │ CRIU Agent Sidecar │    │    │
│  │  │              │   │ - gRPC Server      │    │    │
│  │  │ your-app     │◄──│ - Checkpoint       │    │    │
│  │  │              │   │ - Restore          │    │    │
│  │  └──────────────┘   └────────────────────┘    │    │
│  └────────────────────────────────────────────────┘    │
│                       │                                  │
│                       ▼                                  │
│               Object Storage (S3)                       │
└─────────────────────────────────────────────────────────┘
```

## Implementation Details

### Sleep Infinity Approach

The operator uses a "sleep infinity" pattern to avoid checkpointing the container's PID 1 process directly:

1. **Pod starts with `sleep infinity` as PID 1** (specified in MigratableApp spec)
2. **Agent launches the actual application via `nsenter`** during restore
3. **CRIU only checkpoints the child process**, not PID 1

Benefits:
- PID 1 (sleep) remains unchanged across migrations
- Avoids complications with container runtime expectations
- Maintains namespace sharing for kubelet compatibility

### Mount Namespace Handling

The operator implements CRIU's `--join-ns mnt` feature to handle Kubernetes-injected mounts:

**Challenge**: Kubernetes injects various mounts into containers:
- `/dev/termination-log`
- `/etc/hosts`, `/etc/resolv.conf`, `/etc/hostname`
- ConfigMap/Secret volumes
- Service account tokens

**Solution**: Join the target pod's existing mount namespace instead of restoring:
- **Dump**: Mark specific mounts as external (`--external mnt[path]:id`)
- **Restore**: Use `--join-ns mnt:/proc/1/ns/mnt` to join target's mount namespace
- **Result**: Target pod's mounts (managed by kubelet) are used directly

**CRIU Bug Fix**: Fixed a bug in CRIU 4.0 where `--join-ns mnt` was not working correctly. See [CRIU_JOIN_NS_MNT_BUG_FIX.md](../criu_build/CRIU_JOIN_NS_MNT_BUG_FIX.md) for details.

### Storage Strategy

**During Dump**:
- Upload ALL checkpoint files to S3, including `pages-*.img`
- Even though pages are served via page-server during migration, they must be in S3 for lazy-pages daemon

**During Restore**:
- Download only metadata files from S3 (core, mm, files, etc.)
- Skip downloading `pages-*.img` (too large, loaded on-demand)
- Lazy-pages daemon fetches pages from S3 as needed

**Benefit**: Fast restore startup time (~1-2 seconds) with on-demand page loading

### AWS Credentials Strategy

- **Regular S3**: Uses IAM roles or public access (no credentials needed in CRIU command)
- **Express One Zone**: Requires explicit credentials (`--aws-access-key`, `--aws-secret-key`)
- Agent conditionally includes credentials based on storage type

## Prerequisites

### Development Environment
- **Go**: 1.25.3+ (required for building)
- **Docker**: For building container images
- **Protobuf Compiler**: `protoc` (for generating gRPC code)
- **controller-gen**: For generating CRD manifests
- **kubectl**: For deploying to Kubernetes

### Kubernetes Cluster
- **Kubernetes**: v1.20+
- **Container Runtime**: containerd (with CRIU support) or CRI-O
- **Object Storage**: S3, MinIO, or GCS
- **Linux Kernel**: 4.x+ (with CRIU support)

## Building from Source

### Step 1: Install Go Dependencies

```bash
# Install Go 1.25.3 or later
# Ubuntu/Debian example:
wget https://go.dev/dl/go1.25.3.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.25.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
export PATH=$PATH:$(go env GOPATH)/bin
source /etc/profile  # Or add to ~/.bashrc
```

### Step 2: Install Build Tools

```bash
# Install protobuf compiler
sudo apt update && sudo apt install -y protobuf-compiler

# Install Go tools
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest
```

### Step 3: Download Dependencies

```bash
cd kubernetes_integration
go mod download
go mod tidy
```

### Step 4: Generate Code

```bash
# Generate protobuf code
./scripts/generate-proto.sh

# Or generate manually:
export PATH=$PATH:$(go env GOPATH)/bin
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  pkg/proto/agent.proto
```

### Step 5: Generate Kubernetes Manifests

```bash
# Generate CRD manifests
make manifests

# This creates:
# - config/crd/migration.io_migratableapps.yaml
# - config/rbac/role.yaml
```

### Step 6: Build Binaries

```bash
# Build all binaries
make build

# Output:
# - bin/agent         (CRIU Agent)
# - bin/controller    (Migration Controller)
# - bin/node-monitor  (Node Monitor)
```

### Step 7: Build Docker Images

```bash
# Download CRIU binary and build all images
make docker-build

# This will:
# 1. Download CRIU binary from S3
# 2. Build agent image (with CRIU)
# 3. Build controller image
# 4. Build node-monitor image

# Images created:
# - 192.168.0.253:5000/criu-agent:latest
# - 192.168.0.253:5000/criu-migration-controller:latest
# - 192.168.0.253:5000/criu-node-monitor:latest
```

### Step 8: Push to Registry (Optional)

```bash
# Push all images to registry
make docker-push

# Or customize registry:
make docker-push REGISTRY=your-registry.com/yourorg
```

## Complete Build Workflow

```bash
# Full build from scratch
source /etc/profile
cd kubernetes_integration

# 1. Install dependencies
go mod tidy

# 2. Generate code
./scripts/generate-proto.sh
make manifests

# 3. Build binaries
make build

# 4. Build and push Docker images
make docker-push
```

## Installation

### Prerequisites
- Kubernetes cluster running (v1.20+)
- `kubectl` configured to access the cluster
- Docker images pushed to registry (or use `make docker-push`)

### Method 1: Using Makefile (Recommended)

```bash
# 1. Install CRDs
make install

# 2. Deploy namespace, RBAC, controller and monitor
make deploy

# Or with custom registry:
make deploy REGISTRY=192.168.0.253:5000

# 3. Create storage credentials (see below)
```

**Note**: The `make deploy` command will automatically substitute the correct image references based on the REGISTRY variable. If you built and pushed images with a custom registry (e.g., `make docker-push REGISTRY=192.168.0.253:5000`), use the same REGISTRY value when deploying.

### Method 2: Manual Installation

#### Step 1: Install CRDs

```bash
kubectl apply -f config/crd/migration.io_migratableapps.yaml
```

#### Step 2: Create Namespace, ServiceAccount and RBAC

```bash
# This will create:
# - Namespace: migration-system
# - ServiceAccount: migration-controller
# - ClusterRole and ClusterRoleBinding
# - Leader election Role and RoleBinding
kubectl apply -f config/rbac/rbac.yaml
```

#### Step 3: Deploy Controller and Node Monitor

```bash
# Deploy the controller deployment and node-monitor daemonset
kubectl apply -f config/manager/manager.yaml
```

#### Step 4: Verify Installation

```bash
# Check if pods are running
kubectl get pods -n migration-system

# Expected output:
# NAME                                     READY   STATUS    RESTARTS   AGE
# migration-controller-xxxxxxxxxx-xxxxx    1/1     Running   0          30s
# node-monitor-xxxxx                       1/1     Running   0          30s
# node-monitor-yyyyy                       1/1     Running   0          30s
```

#### Step 5: Configure Object Storage Credentials

**Important**: Create the secret in the `migration-system` namespace (where the controller is deployed). The controller will automatically inject these credentials into all MigratableApp pods, regardless of which namespace they run in.

**For AWS S3:**
```bash
kubectl create secret generic s3-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=your-access-key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your-secret-key \
  -n migration-system
```

**For MinIO:**
```bash
kubectl create secret generic s3-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=minioadmin \
  --from-literal=AWS_SECRET_ACCESS_KEY=minioadmin \
  -n migration-system
```

**Note**: Only one secret in `migration-system` namespace is needed. All MigratableApps in any namespace will use this secret.

## Quick Start

### 1. Create a MigratableApp

```yaml
# example-app.yaml
apiVersion: migration.io/v1alpha1
kind: MigratableApp
metadata:
  name: my-web-app
  namespace: default
spec:
  template:
    metadata:
      labels:
        app: my-web-app
    spec:
      containers:
      - name: app
        image: python:3.9-slim
        command: ["python", "-c"]
        args:
        - |
          import time
          counter = 0
          while True:
              counter += 1
              print(f"Counter: {counter}")
              time.sleep(5)
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"

  checkpointPolicy:
    interval: "30s"
    autoAdjust: true
    memoryThresholdMB: 100
    maxCheckpointChainDepth: 10

  migrationPolicy:
    autoMigrate: true
    preferOnDemand: true
    migrationTimeoutSeconds: 300

  storage:
    type: s3
    bucket: my-checkpoint-bucket
    endpoint: http://minio.default.svc.cluster.local:9000
    region: us-east-1
    credentialsSecret: s3-credentials
```

### 2. Deploy the Application

```bash
kubectl apply -f example-app.yaml
```

### 3. Monitor the Application

```bash
# Watch MigratableApp status
kubectl get mapp my-web-app -w

# Get detailed status
kubectl describe mapp my-web-app

# View logs
kubectl logs -l migration.io/app=my-web-app -c criu-agent
kubectl logs -l migration.io/app=my-web-app -c app
```

### 4. Check Checkpoint Status

```bash
# View checkpoint information
kubectl get mapp my-web-app -o jsonpath='{.status.checkpointStatus}' | jq

# Output example:
# {
#   "lastCheckpointID": "abc123-1234567890",
#   "lastCheckpointTime": "2024-11-05T08:00:00Z",
#   "checkpointChainDepth": 3,
#   "checkpointChainRoot": "xyz789-1234567890"
# }
```

### 5. View Migration History

```bash
kubectl get mapp my-web-app -o jsonpath='{.status.migrationHistory}' | jq

# Output example:
# [
#   {
#     "fromNode": "node-1",
#     "toNode": "node-2",
#     "timestamp": "2024-11-05T08:05:00Z",
#     "reason": "spot-interrupt",
#     "duration": "15.2s",
#     "success": true
#   }
# ]
```

### 6. Trigger Manual Migration

```bash
# Add migration trigger annotation
POD_NAME=$(kubectl get pod -l migration.io/app=my-web-app -o jsonpath='{.items[0].metadata.name}')
kubectl annotate pod $POD_NAME migration.io/trigger=requested
kubectl annotate pod $POD_NAME migration.io/reason=manual
```

## Configuration

### Checkpoint Policy

```yaml
checkpointPolicy:
  # Interval between pre-checkpoints
  interval: "30s"

  # Automatically adjust interval based on memory changes
  autoAdjust: true

  # Trigger checkpoint when memory changes exceed this threshold (MB)
  memoryThresholdMB: 100

  # Maximum checkpoint chain depth before full checkpoint
  maxCheckpointChainDepth: 10
```

### Migration Policy

```yaml
migrationPolicy:
  # Enable automatic migration on spot interrupt
  autoMigrate: true

  # Node selector for migration target
  targetNodeSelector:
    node-type: on-demand

  # Prefer on-demand nodes over spot
  preferOnDemand: true

  # Migration timeout (seconds)
  migrationTimeoutSeconds: 300
```

### Storage Configuration

**AWS S3:**
```yaml
storage:
  type: s3
  bucket: my-bucket
  region: us-east-1
  credentialsSecret: aws-credentials
```

**MinIO:**
```yaml
storage:
  type: minio
  bucket: my-bucket
  endpoint: http://minio.default.svc.cluster.local:9000
  region: us-east-1
  credentialsSecret: minio-credentials
```

**GCS:**
```yaml
storage:
  type: gcs
  bucket: my-bucket
  credentialsSecret: gcs-credentials
```

## Makefile Targets

```bash
# Development
make help              # Show all available targets
make generate          # Generate protobuf and deepcopy code
make fmt               # Format Go code
make vet               # Run Go vet
make test              # Run tests

# Build
make build             # Build binaries (agent, controller, node-monitor)

# Docker
make download-criu     # Download CRIU binary from S3
make docker-build      # Build Docker images (includes download-criu)
make docker-push       # Build and push Docker images

# Deployment
make manifests         # Generate CRD and RBAC manifests
make install           # Install CRDs to cluster
make uninstall         # Uninstall CRDs from cluster
make deploy            # Deploy controller and monitor
make undeploy          # Remove controller and monitor

# Dependencies
make controller-gen    # Install controller-gen
make protoc-gen-go     # Install protoc-gen-go
make protoc-gen-go-grpc # Install protoc-gen-go-grpc
```

## Customization

### Custom CRIU Binary

```bash
# Use custom CRIU binary URL
make docker-build CRIU_URL=https://your-server.com/criu
```

### Custom Registry

```bash
# Build and push with custom registry
make docker-push REGISTRY=your-registry.com/yourorg

# Deploy with same custom registry
make deploy REGISTRY=your-registry.com/yourorg

# Complete workflow:
make docker-push REGISTRY=192.168.0.253:5000
make deploy REGISTRY=192.168.0.253:5000
```

The `REGISTRY` variable affects:
- **docker-build/docker-push**: Sets the image tags for building and pushing
- **deploy**: Automatically replaces image references in deployment YAML before applying to cluster

### Custom Image Tags

Edit the Makefile:
```makefile
AGENT_IMG ?= $(REGISTRY)/criu-agent:v1.0.0
CONTROLLER_IMG ?= $(REGISTRY)/criu-migration-controller:v1.0.0
MONITOR_IMG ?= $(REGISTRY)/criu-node-monitor:v1.0.0
```

## Troubleshooting

### Build Issues

**Problem: `go: command not found`**
```bash
# Install Go and add to PATH
export PATH=$PATH:/usr/local/go/bin
export PATH=$PATH:$(go env GOPATH)/bin
source /etc/profile
```

**Problem: `protoc: command not found`**
```bash
# Install protobuf compiler
sudo apt install -y protobuf-compiler
```

**Problem: `controller-gen: command not found`**
```bash
# Install controller-gen
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest
```

### Docker Build Issues

**Problem: CRIU download fails**
```bash
# Check CRIU URL and download manually
curl -L -o criu/criu https://mhsong-criu-s3-data.s3.us-west-2.amazonaws.com/criu
chmod +x criu/criu
```

**Problem: Go version mismatch in Docker**
```bash
# Dockerfiles use golang:1.25.3-alpine
# Make sure go.mod requires go >= 1.25.1
```

### Runtime Issues

**Problem: Agent connection failed**
```bash
# Check agent pod logs
kubectl logs <pod-name> -c criu-agent

# Verify agent is running
kubectl exec <pod-name> -c criu-agent -- ps aux | grep agent
```

**Problem: Checkpoint failed**
```bash
# Check CRIU logs in the pod
kubectl exec <pod-name> -c criu-agent -- ls /checkpoints
kubectl exec <pod-name> -c criu-agent -- cat /checkpoints/<dump-id>/criu.log

# Verify CRIU is available
kubectl exec <pod-name> -c criu-agent -- criu check --all
```

**Problem: Migration timeout**
```bash
# Increase migration timeout
kubectl edit mapp <app-name>
# Set spec.migrationPolicy.migrationTimeoutSeconds to higher value
```

## Project Structure

```
kubernetes_integration/
├── api/v1alpha1/              # CRD API definitions
├── cmd/                        # Main applications
│   ├── agent/                 # CRIU Agent
│   ├── controller/            # Migration Controller
│   └── node-monitor/          # Node Monitor
├── pkg/                        # Libraries
│   ├── agent/                 # Agent implementation
│   ├── controller/            # Controller implementation
│   ├── scheduler/             # Checkpoint scheduler
│   ├── monitor/               # Spot monitor
│   └── proto/                 # gRPC definitions
├── config/                     # Kubernetes manifests
│   ├── crd/                   # CRD definitions
│   ├── rbac/                  # RBAC configs
│   ├── manager/               # Controller deployment
│   └── samples/               # Example applications
├── deploy/                     # Dockerfiles
│   ├── agent/
│   ├── controller/
│   └── node-monitor/
├── scripts/                    # Build scripts
├── Makefile                    # Build automation
└── README.md                   # This file
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

Apache License 2.0

## Recent Updates

### 2025-11-12: Re-Migration Support and Production Hardening
- **Fixed**: Multiple critical issues preventing re-migration (gen0 → gen1 → gen2)
- **Major Changes**:
  - S3 path consistency: Use MigratableApp name instead of pod name across all generations
  - SOURCE_POD_IP injection: Proper lazy-pages connection for re-migration
  - Generation number tracking: Fixed via Downward API annotation reading
  - PID layout consistency: Added PID booster init container for reproducible PIDs
  - Enhanced namespace handling: Comprehensive external mount detection and mapping
  - Robust lazy-pages lifecycle: Proper readiness detection and health checks
- **Results**: Successful multi-generation migration with 43-file checkpoint chains
- **Performance**: ~7s restore time with lazy-pages, continuous pre-checkpoints working
- **Commit**: [c07b93f](../../commit/c07b93f)

### 2025-11-11: Page-Server Lifecycle Fix
- **Fixed**: TCP health check killing page-server prematurely
- **Solution**: Removed TCP dial from `waitForPageServerReady()` function
- **Impact**: Stable zero-downtime migrations achieved
- **Performance**: 1.8s restore time, 15.96s total migration time
- **Details**: See [CRIU_MIGRATION_OPERATOR_DOCS.md](../../CRIU_MIGRATION_OPERATOR_DOCS.md#2025-11-11-오후-page-server-lifecycle-문제-해결-및-migration-성공)

### 2025-11-11: CRIU `--join-ns mnt` Bug Fix
- **Fixed**: CRIU 4.0 `--join-ns mnt` not working correctly
- **Solution**: Clear `root_ns_mask` for joined namespaces in `prepare_namespace_before_tasks()`
- **Impact**: Successful mount namespace handling in Kubernetes
- **Details**: See [CRIU_JOIN_NS_MNT_BUG_FIX.md](../../criu_build/CRIU_JOIN_NS_MNT_BUG_FIX.md)

## References

- [CRIU Documentation](https://criu.org/)
- [Kubernetes Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime)
- [Kubebuilder](https://book.kubebuilder.io/)
- [Full Documentation](../../CRIU_MIGRATION_OPERATOR_DOCS.md)

## Contact

For questions or support:
- GitHub: [github.com/ddps-lab/criu-migration-operator](https://github.com/ddps-lab/criu-migration-operator)
- Issues: [GitHub Issues](https://github.com/ddps-lab/criu-migration-operator/issues)

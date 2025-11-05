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

## Features

### Core Features
- ✅ Zero-downtime migration for stateful applications
- ✅ Automatic spot instance interrupt detection (AWS/GCP/Azure)
- ✅ Incremental checkpoint chain for minimal overhead
- ✅ Object storage integration (S3/MinIO/GCS)
- ✅ Lazy page loading for fast restore
- ✅ Kubernetes-native CRD API

### Migration Policies
- Configurable checkpoint intervals
- Auto-adjustment based on memory changes
- Target node selection (on-demand preference)
- Migration timeout settings

## Prerequisites

- Kubernetes cluster (v1.20+)
- CRIU-capable container runtime (containerd recommended)
- Object storage (S3, MinIO, or GCS)
- Linux kernel with CRIU support (4.x+)

## Installation

### 1. Install CRDs

```bash
kubectl apply -f config/crd/
```

### 2. Create namespace and RBAC

```bash
kubectl apply -f config/rbac/rbac.yaml
```

### 3. Deploy controller and node monitor

```bash
kubectl apply -f config/manager/manager.yaml
```

### 4. Configure object storage credentials

```bash
# Create S3 credentials secret
kubectl create secret generic s3-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=your-access-key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your-secret-key \
  -n default
```

## Quick Start

### 1. Create a MigratableApp

```yaml
apiVersion: migration.io/v1alpha1
kind: MigratableApp
metadata:
  name: my-app
spec:
  template:
    metadata:
      labels:
        app: my-app
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

  checkpointPolicy:
    interval: "30s"
    autoAdjust: true
    memoryThresholdMB: 100

  migrationPolicy:
    autoMigrate: true
    preferOnDemand: true

  storage:
    type: s3
    bucket: my-checkpoint-bucket
    endpoint: http://minio.default.svc.cluster.local:9000
    region: us-east-1
    credentialsSecret: s3-credentials
```

### 2. Apply the resource

```bash
kubectl apply -f my-app.yaml
```

### 3. Monitor the application

```bash
# Watch the MigratableApp
kubectl get mapp my-app -w

# Check status
kubectl describe mapp my-app

# View migration history
kubectl get mapp my-app -o jsonpath='{.status.migrationHistory}' | jq
```

## Usage Examples

### Manual Migration Trigger

```bash
# Trigger manual migration by adding annotation
kubectl annotate pod my-app migration.io/trigger=requested
kubectl annotate pod my-app migration.io/reason=manual
```

### Check Checkpoint Status

```bash
kubectl get mapp my-app -o jsonpath='{.status.checkpointStatus}' | jq
```

### View Migration History

```bash
kubectl get mapp my-app -o yaml | yq '.status.migrationHistory'
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

#### AWS S3

```yaml
storage:
  type: s3
  bucket: my-bucket
  region: us-east-1
  credentialsSecret: aws-credentials
```

#### MinIO

```yaml
storage:
  type: minio
  bucket: my-bucket
  endpoint: http://minio.default.svc.cluster.local:9000
  region: us-east-1
  credentialsSecret: minio-credentials
```

#### GCS

```yaml
storage:
  type: gcs
  bucket: my-bucket
  credentialsSecret: gcs-credentials
```

## Development

### Build

```bash
# Build all binaries
make build

# Build Docker images
make docker-build

# Push images
make docker-push
```

### Generate Code

```bash
# Generate protobuf code
./scripts/generate-proto.sh

# Generate CRD manifests
make manifests

# Generate deepcopy code
make generate
```

### Testing

```bash
# Run tests
make test

# Run with local Kubernetes cluster (kind/minikube)
make install
make run
```

## Troubleshooting

### Agent Connection Failed

```bash
# Check agent pod logs
kubectl logs <pod-name> -c criu-agent

# Verify agent is running
kubectl exec <pod-name> -c criu-agent -- ps aux | grep agent
```

### Checkpoint Failed

```bash
# Check CRIU logs in the pod
kubectl exec <pod-name> -c criu-agent -- cat /checkpoints/<dump-id>/criu.log

# Verify CRIU is available
kubectl exec <pod-name> -c criu-agent -- criu check
```

### Migration Timeout

```bash
# Increase migration timeout
kubectl edit mapp <app-name>
# Set spec.migrationPolicy.migrationTimeoutSeconds to higher value

# Check controller logs
kubectl logs -n migration-system deployment/migration-controller
```

### S3 Upload Failed

```bash
# Verify S3 credentials
kubectl get secret s3-credentials -o yaml

# Test S3 connectivity from pod
kubectl exec <pod-name> -c criu-agent -- \
  aws s3 ls s3://<bucket-name> --endpoint-url <endpoint>
```
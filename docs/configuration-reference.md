# Configuration Reference

Complete reference for MigratableApp CRD fields and environment variables.

## MigratableApp Spec

### `spec.template`

Standard Kubernetes PodTemplateSpec. The operator replaces the app container's command with `sleep infinity` and launches the original command via the CRIU agent sidecar using `nsenter`.

### `spec.storage`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | (required) | Storage backend: `s3`, `minio`, `gcs` |
| `bucket` | string | (required) | Bucket name for checkpoints |
| `endpoint` | string | | S3-compatible endpoint URL (upload endpoint) |
| `downloadEndpoint` | string | | CDN endpoint for restore (e.g., CloudFront). Falls back to `endpoint` |
| `region` | string | | Cloud region |
| `credentialsSecret` | string | | Kubernetes Secret name (must exist in `migration-system` namespace). Keys: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| `expressOneZone` | bool | false | S3 Express One Zone optimization |
| `directUpload` | bool | false | CRIU native `--object-storage-upload` during dump. Zero local disk I/O for pages |
| `asyncPrefetch` | bool | false | Enable async prefetch in lazy-pages daemon |
| `prefetchWorkers` | int | 4 | Number of async prefetch worker threads |
| `semiSyncIOV` | *bool | nil (enabled) | Semi-synchronous IOV fetch. Set `false` for ablation |
| `hotVMASeed` | *bool | nil (enabled) | Hot VMA priority seeding in prefetch queue. Set `false` for ablation |
| `logUpload` | bool | false | Upload all raw CRIU logs to S3 after dump/restore for experiment data collection |

### `spec.checkpointPolicy`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `interval` | string | "30s" | Pre-checkpoint interval (Go duration format) |
| `autoAdjust` | bool | true | Dynamic interval based on dirty page volume |
| `memoryThresholdMB` | int | 100 | Memory change threshold for adaptive checkpoint (MB) |
| `maxCheckpointChainDepth` | int | 10 | Max incremental chain depth before full checkpoint |
| `deadlineScheduler` | object | | Agent-side deadline-driven scheduler (see below) |

### `spec.checkpointPolicy.deadlineScheduler`

When enabled, the controller's periodic pre-checkpoint is disabled. The agent-side scheduler uses dirty page profiling to decide when to pre-dump, ensuring migration can complete within the cloud provider's termination deadline.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Activate deadline scheduler |
| `dryRun` | bool | false | Log decisions without triggering actual pre-dumps |
| `deadlineSeconds` | int | 120 | Termination deadline (AWS=120, Azure=30) |
| `bandwidthMBps` | int | 100 | Estimated upload bandwidth in MB/s |
| `scanIntervalMs` | int | 2000 | Evaluation interval in ms |
| `tFreezeMs` | int | 50 | Estimated CRIU process freeze time in ms |
| `tMarginMs` | int | 5000 | Safety margin in ms |

### `spec.migrationPolicy`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | string | "lazy-storage" | Migration strategy: `full`, `lazy-storage`, `lazy-direct`, `lazy-hybrid` |
| `autoMigrate` | bool | true | Auto-migrate on spot interruption |
| `targetNodeSelector` | map | | Node labels for target selection |
| `preferOnDemand` | bool | true | Prefer on-demand nodes as migration targets |
| `migrationTimeoutSeconds` | int | 300 | Migration timeout |

### Migration Strategies

| Strategy | Description | Page Transfer | S3 Upload |
|----------|-------------|---------------|-----------|
| `full` | Dump all pages, upload synchronously, restore all | Synchronous | All pages before restore |
| `lazy-storage` | Dump all pages to S3, restore with lazy-pages from S3 | On-demand from S3 | All pages before restore |
| `lazy-direct` | Dump with page-server, lazy-pages fetches from page-server | On-demand from source | Metadata only |
| `lazy-hybrid` | Page-server + async S3 upload, lazy-pages uses both | On-demand (server+S3) | Async in background |

## Agent Environment Variables

These are set automatically by the controller via PodBuilder:

| Env Var | Description |
|---------|-------------|
| `S3_BUCKET` | Storage bucket name |
| `S3_ENDPOINT` | Upload endpoint URL |
| `S3_REGION` | Cloud region |
| `DOWNLOAD_ENDPOINT` | Download/CDN endpoint |
| `STORAGE_TYPE` | `s3`, `minio`, or `gcs` |
| `DIRECT_UPLOAD` | `true` to enable CRIU native S3 upload |
| `ASYNC_PREFETCH` | `true` to enable async prefetch in lazy-pages |
| `PREFETCH_WORKERS` | Number of prefetch workers |
| `LOG_UPLOAD` | `true` to upload all raw logs to S3 |
| `NO_SEMI_SYNC_IOV` | `true` to disable semi-sync IOV (ablation) |
| `NO_HOT_VMA_SEED` | `true` to disable hot VMA seeding (ablation) |
| `AWS_ACCESS_KEY_ID` | From credentials secret |
| `AWS_SECRET_ACCESS_KEY` | From credentials secret |
| `BANDWIDTH_MBPS` | Override auto-detected bandwidth (MB/s). If unset, auto-detected |

## Network Bandwidth Auto-Detection

The agent automatically detects network bandwidth on startup:

1. **AWS**: Queries IMDS for instance type → calls `ec2:DescribeInstanceTypes` → reads `NetworkCards[].BaselineBandwidthInGbps`
2. **On-premise**: Reads `/sys/class/net/*/speed` for the fastest active NIC
3. **Manual override**: Set `BANDWIDTH_MBPS` env var (via CRD `deadlineScheduler.bandwidthMBps`)
4. **Default fallback**: 100 MB/s

The detected bandwidth is used by the deadline scheduler for F_op feasibility evaluation.
Gbps → MB/s conversion applies 80% TCP efficiency factor.

## Example: Minimal

```yaml
apiVersion: migration.io/v1alpha1
kind: MigratableApp
metadata:
  name: my-app
spec:
  template:
    spec:
      containers:
        - name: app
          image: my-app:latest
  storage:
    type: minio
    bucket: checkpoints
    endpoint: http://minio.minio.svc:9000
    credentialsSecret: minio-credentials
```

## Example: Full Experiment Setup

```yaml
apiVersion: migration.io/v1alpha1
kind: MigratableApp
metadata:
  name: my-app
spec:
  template:
    spec:
      containers:
        - name: app
          image: my-app:latest
  storage:
    type: s3
    bucket: my-checkpoints
    endpoint: https://s3.us-east-1.amazonaws.com
    downloadEndpoint: https://d1234.cloudfront.net
    region: us-east-1
    credentialsSecret: aws-credentials
    directUpload: true
    asyncPrefetch: true
    prefetchWorkers: 4
    logUpload: true
  checkpointPolicy:
    interval: "30s"
    autoAdjust: false
    maxCheckpointChainDepth: 5
  migrationPolicy:
    strategy: lazy-storage
```

## Example: Ablation Study (5 modes)

```yaml
# Mode 1: baseline (no prefetch, no semi-sync, no hot VMA)
storage:
  asyncPrefetch: false
  semiSyncIOV: false
  hotVMASeed: false
  logUpload: true

# Mode 2: async prefetch only
storage:
  asyncPrefetch: true
  semiSyncIOV: false
  hotVMASeed: false

# Mode 3: async + semi-sync IOV
storage:
  asyncPrefetch: true
  # semiSyncIOV defaults to enabled
  hotVMASeed: false

# Mode 4: async + hot VMA seed (no semi-sync)
storage:
  asyncPrefetch: true
  semiSyncIOV: false
  # hotVMASeed defaults to enabled

# Mode 5: full (all features)
storage:
  asyncPrefetch: true
  # semiSyncIOV defaults to enabled
  # hotVMASeed defaults to enabled
```

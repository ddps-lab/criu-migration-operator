# Architecture

## Components

### Migration Controller (`pkg/controller/`)

The Kubernetes controller-runtime reconciler that manages the MigratableApp lifecycle.

**Reconcile loop:**
1. Ensure pod exists (create if missing)
2. Check for migration trigger annotation (`migration.io/trigger=requested`)
3. If triggered, orchestrate migration via `performMigration()`
4. Otherwise, evaluate periodic pre-checkpoint (time-based or adaptive)

**Key files:**
- `reconciler.go` — main reconcile loop, checkpoint scheduling
- `migration.go` — migration orchestration (FinalDump → Create target → Restore → Delete source)
- `pod_builder.go` — Pod spec construction with agent sidecar injection
- `client.go` — gRPC client wrapper for agent communication

### CRIU Agent Sidecar (`pkg/agent/`)

A privileged sidecar container that runs alongside the application and handles all CRIU operations via gRPC.

**Lifecycle:**
1. On startup: find main container PID, launch user process via `nsenter`, start gRPC server
2. Auto-start write profiler (uffd-wp) for dirty page tracking
3. Handle gRPC requests: PreCheckpoint, FinalDump, Restore, GetStatus, GetDirtyVolume

**Key files:**
- `server.go` — gRPC server, process lifecycle, profiler management
- `checkpoint.go` — CRIU pre-dump and final dump with S3 direct upload
- `restore.go` — CRIU restore with lazy-pages daemon, async prefetch
- `s3.go` — S3/MinIO client, CRIU CLI arg generation
- `deadline_scheduler.go` — F_op feasibility model for deadline-driven pre-dumps
- `metrics.go` — lazy-pages log parser for per-fault metrics

### Write Profiler (`pkg/profiler/`)

Tracks memory write patterns using Linux userfaultfd write-protect (uffd-wp) via ptrace syscall injection.

**How it works:**
1. Attach to target process via `PTRACE_SEIZE`
2. Inject `userfaultfd()` syscall into target's address space
3. Register writable VMAs with `UFFDIO_REGISTER` + `UFFD_FEATURE_WP`
4. Periodically scan via `PAGEMAP_SCAN` ioctl to detect dirty pages
5. Classify VMAs as hot/cold using sliding window (theta=0.3, N=3 consecutive)

**Integration with CRIU:**
- Before dump: `CleanupBeforeCRIU()` — unregister all VMAs, close uffd fds via ptrace injection
- After dump: `ReinitAfterCRIU()` — full re-setup via ptrace (new uffd creation)

**Key files:**
- `profiler.go` — main loop, lifecycle (Start/Stop/CleanupBeforeCRIU/ReinitAfterCRIU)
- `ptrace.go` — ptrace attach/inject, uffd creation, close target fd
- `scan.go` — PAGEMAP_SCAN ioctl for dirty page detection
- `heat.go` — sliding window heat classifier (ConsecutiveHot counter)
- `vma.go` — /proc/PID/maps parser, VMA diffing

### Node Monitor (`pkg/monitor/`)

DaemonSet that polls cloud provider metadata for spot interruption notices.

**Supported clouds:** AWS (IMDSv2), GCP, Azure

### Deadline Scheduler (`pkg/agent/deadline_scheduler.go`)

Agent-side scheduler that uses the F_op (Operational Feasibility) model to decide when pre-dumps are needed:

```
alpha = R_cold * P_size / B_eff
T_transfer = P_size / B_eff + D_cold_ss    (where D_cold_ss = R_cold * I * alpha / (1 - alpha))
feasible = T_transfer + T_freeze + T_margin < D (deadline)
```

When not feasible, triggers a pre-dump to reduce dirty page volume.

## Data Flow

### Pre-dump (Periodic Checkpoint)

```
Controller → gRPC PreCheckpoint → Agent
  → Profiler.CleanupBeforeCRIU() (unregister uffd)
  → CRIU pre-dump --object-storage-upload --exclude-range <hot VMAs>
  → Upload hot-vmas.json to S3
  → Upload raw logs to S3 (if logUpload=true)
  → Profiler.ReinitAfterCRIU() (re-setup uffd)
```

### Migration

```
Controller detects trigger annotation
  → gRPC FinalDump to source agent
    → Profiler.CleanupBeforeCRIU()
    → CRIU dump --object-storage-upload --lazy-pages
    → Save hot-vmas.json + upload to S3
    → Upload logs (if logUpload=true)
  → Create target pod (restore mode)
  → gRPC Restore to target agent
    → Download metadata from S3 (skip pages-*.img)
    → Start lazy-pages daemon (--async-prefetch --prefetch-workers N)
    → CRIU restore --lazy-pages --enable-object-storage
    → Lazy-pages reads hot-vmas.json for priority seeding
    → Wait for lazy-pages completion
    → Parse per-fault metrics
    → Upload restore logs (if logUpload=true)
  → Delete source pod
  → Update MigratableApp status
```

## S3 Key Layout

```
<bucket>/
  <app-name>/
    <generation>/<node-name>/<dump-id>/
      core-*.img, inventory.img, pages-*.img, ...   (CRIU files)
      hot-vmas.json                                  (agent: prefetch seed)
      hot_vma_metadata.json                          (agent: profiler details)
      logs/                                          (if logUpload=true)
        criu.log
        restore.log
        lazy-pages.log
        stats-dump
        stats-restore
        lazy-pages-metrics.json
```

## Pod Structure

```
Pod: my-app-gen0
├── Init: pid-booster (busybox: sleep 0.1 — for PID layout consistency)
├── Container: app
│   ├── Command: sleep infinity (PID 1)
│   └── Volumes: /tmp/.criu-checkpoints (shared)
├── Container: criu-agent (privileged, hostPID)
│   ├── Sidecar gRPC server on :8080
│   ├── Launches user process via nsenter
│   ├── Profiler: uffd-wp dirty tracking
│   └── Volumes: /tmp/.criu-checkpoints (shared)
└── Annotations:
    ├── migration.io/app: my-app
    ├── migration.io/generation: "0"
    ├── migration.io/original-command: /app/counter
    └── migration.io/trigger: requested (set to trigger migration)
```

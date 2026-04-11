# criu-migration-operator 업데이트 요약

## 개요

criu-s3 (async-prefetch-rework branch)의 기능들을 Kubernetes operator에 통합.
S3 direct upload, hot VMA seeding, async prefetch 설정, per-fault metrics, adaptive scheduling, deadline scheduler 구현.

## 테스트 결과

QEMU 3-node 클러스터 (master + worker1 + worker2)에서 검증:
- Pre-dump with S3 direct upload: ✅
- Migration (worker1 → worker2, lazy-storage strategy): ✅ (1.86s total)
- Counter workload: state 보존 (counter 2660+ 계속 실행)

---

## 1. CRIU 바이너리 업데이트

**파일**: `criu-binary`, `deploy/agent/criu`

criu-s3 async-prefetch-rework 최신 빌드 (commit 1dcfd0d54). 포함 기능:
- `relocate_internal_fd()`: restore 시 fd 충돌 방지 (dynamic threshold)
- CLI post-processing: `--no-semi-sync-iov`, `--no-hot-vma-seed` 순서 무관 동작
- `--object-storage-upload`: dump 시 S3 직접 업로드 (zero disk I/O)
- `--object-storage-path-style`: MinIO 지원
- UFFDIO_COPY EAGAIN fix: zero page corruption 방지
- Prefetch controller 단순화 + global metadata 누적

---

## 2. S3 Direct Upload

**파일**: `pkg/agent/s3.go`, `pkg/agent/checkpoint.go`

### 변경 내용
- `BuildCRIUUploadArgs(s3Prefix)`: dump 시 `--object-storage-upload` + credentials + path-style 생성
- `UploadFile(localPath, s3Key)`: 단일 파일 S3 업로드 (agent metadata용)
- `uploadAgentMetadata()`: CRIU가 모르는 agent 생성 파일 (hot-vmas.json) 별도 업로드
- Pre-dump과 FinalDump 모두 direct upload 지원

### 동작
```
DIRECT_UPLOAD=true 일 때:
1. CRIU dump args에 --object-storage-upload 추가
2. CRIU가 pages + metadata를 S3에 직접 전송 (memfd + multipart)
3. Go 측 UploadCheckpoint() skip
4. Agent가 생성한 hot-vmas.json만 별도 업로드
```

---

## 3. Hot VMA → hot-vmas.json 브릿지

**파일**: `pkg/agent/server.go`

### 변경 내용
- `saveHotVMAsJSON(dumpDir, hotRegions)`: profiler의 HotRegion → CRIU 호환 JSON
- FinalDump handler에서 `saveVMAMetadata()` 옆에 호출

### 포맷
```json
{
  "excluded": [
    {"start": "0x7f000000", "end": "0x7f100000"},
    ...
  ],
  "no_parent": []
}
```

CRIU `prefetch.c`의 `load_hot_vma_metadata()`가 local 파일 또는 S3에서 읽어 hot IOV를 priority queue 최상위에 배치.

---

## 4. Path-style S3 URL

**파일**: `pkg/agent/s3.go`

### 변경 내용
- `BuildCRIUObjectStorageArgs()`에 `--object-storage-path-style` 추가 (isMinIO일 때)
- 기존 workaround (bucket을 prefix에 포함) 대신 명시적 flag 사용
- CloudFront CDN은 bucket 옵션 제외 (기존 동작 유지)

---

## 5. Async Prefetch 설정 확장

**파일**: `pkg/agent/restore.go`, `api/v1alpha1/migratableapp_types.go`, `pkg/controller/pod_builder.go`

### CRD 필드
```yaml
storage:
  asyncPrefetch: true        # --async-prefetch
  prefetchWorkers: 4         # --prefetch-workers N
  directUpload: true         # --object-storage-upload
  semiSyncIOV: false         # --no-semi-sync-iov (ablation)
  hotVMASeed: false          # --no-hot-vma-seed (ablation)
```

### 환경변수 전달
```
ASYNC_PREFETCH, PREFETCH_WORKERS, DIRECT_UPLOAD
NO_SEMI_SYNC_IOV, NO_HOT_VMA_SEED
```

### 로직
- `--async-prefetch`와 `--no-semi-sync-iov`는 독립 — semi-sync는 async 없이도 동작
- `--no-hot-vma-seed`는 `--async-prefetch` 블록 안에서만 전달

---

## 6. Lazy-pages 소켓 Polling

**파일**: `pkg/agent/restore.go`

### 변경 내용
- `time.Sleep(1 * time.Second)` → `lazy-pages.socket` 파일 존재 확인 (50ms poll, 5s max)
- 기존 log 기반 `waitForLazyPagesReady()`와 별도로 socket 파일 체크

---

## 7. Per-fault Metrics 수집

**파일**: `pkg/agent/metrics.go` (신규), `pkg/agent/server.go`

### ParseLazyPagesLog()
criu-lazy-pages.log에서 추출:
- **Per-fault**: stall_ms, source (S3/CACHE), pages per fault
- **Aggregates**: avg/p50/max stall, S3 vs cache breakdown
- **UFFD**: total pages transferred/expected
- **Cache**: hit rate
- **Daemon**: duration

### 호출 시점
lazy-pages daemon 종료 후 goroutine에서 자동 파싱 + 로그 출력:
```
[TARGET-AGENT] Lazy-pages metrics: faults=245 (S3=163 cache=82)
  stall_avg=4.3ms p50=2.7ms max=34.5ms daemon=2.5s cache_hit=33.5%
```

---

## 8. Adaptive Checkpoint Scheduling (autoAdjust)

**파일**: `pkg/controller/reconciler.go`, `pkg/controller/client.go`

### 동작
```
controller reconcile loop:
  if autoAdjust:
    agent.GetDirtyVolume() via gRPC
    if cumulative_dirty > memoryThresholdMB:
      → trigger pre-dump ("dirty_threshold")
    elif dirty_rate < 10 pages/s:
      → skip cycle (workload idle)
    else:
      → fallback to interval
  else:
    → time-based (interval)
```

### CRD 설정
```yaml
checkpointPolicy:
  interval: "30s"
  autoAdjust: true
  memoryThresholdMB: 50
```

---

## 9. Deadline Scheduler

**파일**: `pkg/agent/deadline_scheduler.go` (신규), `pkg/agent/server.go`, `pkg/controller/pod_builder.go`

### F_op 모델 구현
논문의 Operational Feasibility Score를 agent 내부에서 주기적 평가:

```
Available = Deadline - T_freeze - T_margin
α = R_cold × P_size / B_eff
D_cold_ss = R_cold × I × α/(1-α)   (geometric series)
D_min = D_hot + D_cold_ss
T_required = D_min / B_eff
F_op = Available / T_required
```

### 결정 로직
- F_op < 1.0: infeasible → pre-dump trigger
- F_op < 2.0: marginal → pre-dump trigger
- F_op ≥ 2.0: comfortable → no action

### CRD 설정
```yaml
checkpointPolicy:
  deadlineScheduler:
    enabled: true
    dryRun: false
    deadlineSeconds: 120      # AWS spot: 2분
    bandwidthMBps: 100
    scanIntervalMs: 2000
    tFreezeMs: 50
    tMarginMs: 5000
```

### 환경변수
```
DEADLINE_SCHEDULER_ENABLED, DEADLINE_SECONDS, BANDWIDTH_MBPS
DEADLINE_SCAN_INTERVAL_MS, DEADLINE_TFREEZE_MS, DEADLINE_TMARGIN_MS
DEADLINE_SCHEDULER_DRY_RUN
```

---

## 10. Profiler 설정 변경

**파일**: `pkg/profiler/profiler.go`

- Default scan interval: 1000ms → **5000ms** (논문 기준 5초)
- Default theta: 0.3 (유지)
- Default N: 3 (유지)
- `CleanupBeforeCRIU()`: multi-process safety 문서화

---

## 테스트 환경

```
QEMU VMs:
  master (172.16.0.1): control-plane, 16GB RAM, 4 vCPU
  worker1 (172.16.0.2): worker, 16GB RAM, 4 vCPU
  worker2 (172.16.0.3): worker, 16GB RAM, 4 vCPU

Kubernetes: v1.34.1 (kubeadm)
CNI: Flannel
Registry: 172.16.0.254:5000 (host Docker)
MinIO: in-cluster (NodePort 30900)
CRIU: v4.0 (criu-s3 commit 1dcfd0d54)
```

## Migration 결과 (counter workload, 64MB mmap)

```
Strategy: lazy-storage
Total duration: 1.86s
  Final dump: ~1s (S3 direct upload)
  Restore: 570ms (lazy-pages from MinIO)
  Lazy-pages: completed immediately (small workload)
Source: worker1 → Target: worker2
Counter state: preserved (2660+)
```

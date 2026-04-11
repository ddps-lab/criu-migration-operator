# Operator Update — 2026-04-11

## Summary

criu-migration-operator에 논문 설계 전체를 구현하고 QEMU 3-node K8s 클러스터에서 E2E 검증 완료.
기존 MigratableApp CRD 방식 외에 **Webhook 기반 Pod injection**을 추가하여, 일반 Deployment에 annotation 하나만 추가하면 migration이 활성화됨.

## 주요 구현 사항

### 1. Dirty Volume Invariant Scheduler (논문 §3.2)

논문 Eq.4-5 수식 구현:
```
T_remain = T_freeze + (D_current × P_size) / B_upload + T_margin
Invariant: T_remain < Deadline (항상 유지)
```

- `autoAdjust: true` → agent-side invariant scheduler가 pre-dump 전담 (controller skip)
- `autoAdjust: false` → controller가 fixed interval pre-dump (기존 동작)
- Initial pre-dump: Pod 배치 즉시 전체 writable memory 사전 전송
- F_op: read-only feasibility score, `F_op < 1` 시 경고 로그만 (pre-dump trigger 아님)

**검증 로그 (dirty-counter workload, bandwidth=1MB/s, deadline=15s):**
```
T_remain=5874ms   D_current=979 pages (3.8MB)    → OK
T_remain=9698ms   D_current=1958 pages (7.6MB)   → OK
T_remain=13522ms  D_current=2937 pages (11.5MB)  → OK
T_remain=17346ms  D_current=3916 pages (15.3MB)  → VIOLATED → Pre-dump!
T_remain=2050ms   D_current=0 pages (0.0MB)      → RESET (cycle 반복)
```

**Commits:** `8526643`, `c2a5335`

### 2. Webhook Pod Injection

Mutating Admission Webhook으로 기존 Deployment/StatefulSet/bare Pod에 sidecar 자동 주입.

```yaml
# 기존 Deployment에 annotation 2개만 추가
annotations:
  migration.io/enabled: "true"
  migration.io/config: "migration-defaults"  # ConfigMap 이름
```

이것만으로:
- criu-agent sidecar 자동 inject (sleep infinity + SYS_PTRACE + shared PID ns)
- MigratableApp CR 자동 생성 (`webhook-managed` label)
- Credentials secret 자동 mirror (migration-system → pod namespace)
- Invariant scheduler or controller checkpoint 자동 시작

**검증:**

| 리소스 | Injection | CR 생성 | App name |
|--------|-----------|---------|----------|
| Deployment | ✅ 2/2 | ✅ | Deployment name |
| StatefulSet | ✅ 2/2 | ✅ | StatefulSet name |
| Bare Pod | ✅ 2/2 | ✅ | Pod name |

**Commits:** `afa415f`, `08849d6`

### 3. S3 Direct Upload + Hot VMA

- CRIU `--object-storage-upload`으로 dump 시 S3 직접 업로드 (disk I/O 제거)
- `--object-storage-path-style` for MinIO
- Write profiler (uffd-wp): θ=0.3, N=3, 5s interval
- Hot VMA: pre-dump `--exclude-range` + final dump `hot-vmas.json` (lazy-pages seed)

**Profiler lifecycle:**
```
Profiler start → CleanupBeforeCRIU (uffd 해제) → CRIU dump → ReinitAfterCRIU (uffd 재설정)
```

### 4. Async Prefetch + Ablation Control

- `--async-prefetch --prefetch-workers 4` for lazy-pages
- Ablation flags: `semiSyncIOV`, `hotVMASeed` (CRD에서 제어)
- 5-mode 실험 설정 가능

### 5. Per-fault Metrics + Log Upload

- `lazy-pages-metrics.json`: per-fault stall time, S3 vs cache, pages per fault
- `logUpload: true` → 모든 raw CRIU 로그 S3 자동 업로드
- Pre-dump 후: criu.log, hot-vmas.json
- Restore 후: restore.log, lazy-pages.log, stats-dump, stats-restore, lazy-pages-metrics.json

### 6. Network Bandwidth Auto-Detection

- AWS: IMDS → `ec2:DescribeInstanceTypes` API → `BaselineBandwidthInGbps`
- On-premise: `/sys/class/net/*/speed` (negotiated link speed)
- Manual override: `bandwidthMBps` CRD 필드

**로그:**
```
[AWS-DETECT] Not on AWS or IMDS unavailable
[NET-DETECT] On-premise NIC: eth0, speed=10000 Mbps → baseline=1250 MB/s
```

### 7. Mock IMDS Spot Interrupt E2E

Mock IMDS 서버를 worker 노드에 배포하여 full E2E spot interrupt simulation:

```
Mock IMDS (169.254.169.254) → Node Monitor (1초 polling) → Pod annotation
→ Controller migration → CRIU dump → S3 upload → Target restore → Counter 연속
```

**Full E2E 결과:**
```
[WEBHOOK] Created MigratableApp default/test-e2e
[INVARIANT-SCHEDULER] Initial pre-dump completed (32703 bytes)
[INVARIANT-SCHEDULER] Profiler started after initial dump
Spot interruption simulated → Node Monitor 감지 (2초)
worker2 → SchedulingDisabled
Migration: worker2 → worker1
Restore: 574ms, faults=0 (initial pre-dump 덕분에 page fault 없음)
[LOG_UPLOAD] Uploaded 4 restore log files
Phase: Running, Generation: 1
```

## Commit History

| Commit | Description |
|--------|-------------|
| `18501a1` | Fix migration race condition, full E2E verification |
| `c2a5335` | Cleanup: unit docs, remove unused code, update E2E report |
| `8526643` | Dirty volume invariant scheduler (T_remain < Deadline) |
| `08849d6` | Fix webhook migration: generateName, volume dedup, Pod watch |
| `afa415f` | Webhook sidecar injection, log upload, network auto-detect |
| `2543e6b` | Auto-start profiler + close target uffd before CRIU dump |
| `bce8760` | Fix periodic checkpoint: CRD default, debug logging |

## 테스트 환경

- QEMU 3-node K8s v1.34.1 (master + worker1 + worker2)
- MinIO in-cluster (NodePort 30900)
- Registry: 172.16.0.254:5000
- Mock IMDS: systemd-run on worker nodes (169.254.169.254:80)
- Workloads: simple-counter (4MB), dirty-counter (256MB)

## 남은 작업

- AWS terraform + 본실험 (500 runs, 26 instance types × 6 workloads)
- F_op < 1 시 instance selection 제안 (확장 — 미구현)

# Migration Strategy System

## Overview

CRIU migration operator는 4가지 migration strategy를 지원한다. 사용자는 MigratableApp CR의 `spec.migrationPolicy.strategy` 필드로 선택한다.

## Strategy 정의

| Strategy | Source Dump | Target Restore | 페이지 소스 | Source 수명 |
|----------|-----------|----------------|------------|------------|
| `full` | 일반 dump → storage 동기 업로드 | storage에서 전부 다운로드 → restore | storage (전체) | dump 후 즉시 해제 |
| `lazy-storage` | 일반 dump → storage 동기 업로드 | lazy-pages → storage on-demand | storage (on-demand) | dump 후 즉시 해제 |
| `lazy-direct` | lazy dump + page-server + metadata 비동기 업로드 | lazy-pages → source page-server (TCP) | TCP (direct) | 전송 완료까지 유지 |
| `lazy-hybrid` | lazy dump + page-server + 전체 비동기 업로드 | lazy-pages → page-server + storage fallback | TCP + storage fallback | 전송 완료까지 유지 |

기본값: `lazy-storage` (strategy 미지정 시)

## YAML 사용 예시

```yaml
apiVersion: migration.io/v1alpha1
kind: MigratableApp
metadata:
  name: my-app
spec:
  migrationPolicy:
    strategy: lazy-storage  # full | lazy-storage | lazy-direct | lazy-hybrid
```

## Strategy별 동작 흐름

### full

1. Source agent: `criu dump` (일반, `--lazy-pages` 없음)
2. Source agent: storage에 모든 파일 **동기** 업로드 (완료까지 대기)
3. Controller: source pod 즉시 삭제 가능
4. Target agent: storage에서 **모든 파일** 다운로드 (metadata + pages-*.img)
5. Target agent: `criu restore` (`--lazy-pages` 없음, `--enable-object-storage` 없음)
6. 프로세스 즉시 실행 (모든 페이지가 로컬에 존재)

**특징**: downtime이 가장 길지만 가장 단순하고 안정적. restore 후 page fault 없음.

### lazy-storage

1. Source agent: `criu dump` (일반, `--lazy-pages` 없음)
2. Source agent: storage에 모든 파일 **동기** 업로드 (완료까지 대기)
3. Controller: source pod 즉시 삭제 가능
4. Target agent: storage에서 **metadata만** 다운로드
5. Target agent: `criu lazy-pages` 시작 (`--page-server`/`--address` 없음, storage에서만 fetch)
6. Target agent: `criu restore --lazy-pages --enable-object-storage`
7. 프로세스 즉시 실행, page fault 시 storage에서 on-demand fetch

**특징**: source를 빨리 해제 가능. restore 후 page fault 발생 시 storage latency.

### lazy-direct

1. Source agent: `criu dump --lazy-pages --address 0.0.0.0 --port 9999` (page-server 모드)
2. Source agent: metadata를 storage에 **비동기** 업로드
3. Controller: page-server readiness 대기
4. Target agent: storage에서 **metadata만** 다운로드
5. Target agent: `criu lazy-pages --page-server --address <source-ip> --port 9999` (source에 TCP 연결)
6. Target agent: `criu restore --lazy-pages --enable-object-storage`
7. 프로세스 즉시 실행, page fault 시 source page-server에서 TCP fetch

**특징**: page-server TCP 직접 전송으로 latency 최소. source가 전송 완료까지 유지되어야 함.

### lazy-hybrid

1. Source agent: `criu dump --lazy-pages --address 0.0.0.0 --port 9999` (page-server 모드)
2. Source agent: 전체 파일을 storage에 **비동기** 업로드
3. Controller: page-server readiness 대기
4. Target agent: storage에서 **metadata만** 다운로드
5. Target agent: `criu lazy-pages --page-server --address <source-ip> --port 9999 --enable-object-storage` (TCP + storage fallback)
6. Target agent: `criu restore --lazy-pages --enable-object-storage`
7. 프로세스 즉시 실행, page fault 시 TCP 우선, 실패 시 storage fallback

**특징**: lazy-direct + storage fallback. page-server가 죽어도 storage에서 복구 가능.

## 구현 위치

| 파일 | 역할 |
|------|------|
| `api/v1alpha1/migratableapp_types.go` | `MigrationPolicy.Strategy` 필드 정의 |
| `pkg/proto/agent.proto` | `FinalDumpRequest.migration_strategy`, `RestoreRequest.migration_strategy` |
| `pkg/agent/checkpoint.go` | FinalDump에서 strategy별 dump 분기 (page-server 유무, upload 방식) |
| `pkg/agent/restore.go` | Restore에서 strategy별 다운로드/lazy-pages/object-storage 분기 |
| `pkg/agent/server.go` | RPC handler에서 strategy 전달 |
| `pkg/controller/client.go` | strategy 파라미터를 FinalDump/Restore RPC에 전달 |
| `pkg/controller/migration.go` | strategy별 오케스트레이션 (page-server 대기, lazy-pages 완료 대기 등) |

## Pre-dump과의 관계

Pre-dump (`CheckpointPolicy.Interval`)은 strategy와 **독립**적으로 동작한다. 모든 strategy에서 pre-dump을 통해 incremental checkpoint chain을 구축할 수 있다. FinalDump 시 `--prev-images-dir`로 이전 pre-dump을 참조한다.

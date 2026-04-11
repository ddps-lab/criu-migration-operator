# Resolved Issues

CRIU migration operator 개발 과정에서 발생한 이슈와 해결 방법을 기록한다.

---

## Issue 1: CRIU Restore Zombie Process

### 현상

`criu restore` 프로세스가 종료 후 zombie(`[criu] <defunct>`)로 남아 `/proc` 테이블을 차지.

### 원인

`restore.go`에서 `cmd.Start()` 후 `cmd.Wait()`를 호출하지 않음. 주석에 "We MUST NOT call cmd.Wait() because that would terminate the restore process"로 되어있었으나 이는 오해. `Wait()`는 프로세스를 죽이지 않고 exit status를 수거(reap)할 뿐.

비교: `pageServerCmd`와 `lazyPagesCmd`는 이미 goroutine에서 `Wait()`를 호출하고 있었음.

### 해결

`server.go`의 Restore RPC handler에서 restoreCmd에 대한 reaper goroutine 추가:
```go
go func() {
    if restoreCmd != nil && restoreCmd.Process != nil {
        restoreCmd.Wait()
    }
}()
```

### 검증

`kubectl exec <pod> -c criu-agent -- ps aux | grep defunct` → 0건

---

## Issue 2: SIGPIPE로 인한 복원된 프로세스 즉시 종료

### 현상

CRIU restore가 성공하고 lazy-pages가 모든 페이지를 전송(17871/17871)하지만, 복원된 프로세스가 수 초 내에 종료됨. 로컬 환경에서는 정상 생존.

### 원인 분석 과정

1. stdout에 write하지 않는 `while True: time.sleep(1)` → restore 후 정상 생존
2. stdout에 write하는 `memwrite` (매 반복 `print()`) → restore 후 즉시 종료
3. `dmesg`에 segfault/SIGBUS 없음 → 프로세스 자발적 종료

### 근본 원인

K8s에서 컨테이너 프로세스의 stdout/stderr는 containerd 로그 수집 데몬에 연결된 anonymous pipe. Cross-pod restore 시 원래 pipe의 read-end가 존재하지 않아, write 시 커널이 SIGPIPE 전송 → 프로세스 종료.

### 해결

CRIU의 `--inherit-fd fd[N]:pipe:[inode]` 메커니즘 활용:

1. **Dump 시**: 워크로드 프로세스의 stdout/stderr pipe inode를 `/proc/{pid}/fd/` 심볼릭 링크에서 읽어 기록
2. **Pipe inode 전달**: FinalDumpResponse → Controller → RestoreRequest 경로로 전달
3. **Restore 시**: `os.Pipe()`로 새 pipe pair 생성, write-end를 CRIU ExtraFiles로 전달, `--inherit-fd fd[N]:pipe:[inode]` 옵션으로 원래 pipe를 새 pipe로 교체
4. **Agent drain**: read-end를 goroutine에서 drain하여 SIGPIPE 방지

**핵심 형식**: `pipe:[inode]` (콜론 포함). `pipe[inode]` (콜론 없음)은 CRIU가 매칭하지 못함.

참고: `--external pipe[inode]`는 CRIU에서 지원하지 않음 (dump 시 pipe external 마킹 불가). `--inherit-fd`만으로 restore 시 교체 가능.

### 상세 문서

[pipe_inherit_fd.md](pipe_inherit_fd.md)

---

## Issue 3: MinIO에서 `--express-one-zone` 사용 시 세션 생성 실패

### 현상

```
Session URL: https://(null).http://minio.migration-system:9000/?session
Error: objstor: SESSION_ERROR http_code=0
```

### 원인

`--express-one-zone` 플래그가 S3 Express One Zone 세션을 생성하려고 `https://{bucket}.{endpoint}/?session` 형태 URL을 만드는데, MinIO에서는 bucket이 null이고 endpoint가 `http://`라서 URL이 깨짐.

원래 `--express-one-zone`은 SigV4 인증 활성화를 위해 MinIO에도 사용했으나, CRIU 소스 수정(GitID: 24cece02a)으로 SigV4가 기본 활성화되어 더 이상 불필요.

### 해결

`s3.go`의 `needsCRIUExpressOneZone()`에서 MinIO 제외:
```go
func (c *S3Client) needsCRIUExpressOneZone() bool {
    return c.expressOneZone  // isMinIO() 제거
}
```

---

## Issue 4: lazy-pages와 Pre-dump 충돌 (Deadlock)

### 현상

Restore 직후 baseline checkpoint(pre-dump)이 실행되면 복원된 프로세스가 죽음.

### 원인

Restore 직후 lazy-pages가 아직 페이지를 전송 중인데, pre-dump이 `criu pre-dump -t {pid}`로 프로세스를 freeze. Freeze된 프로세스는 page fault를 발생시킬 수 없고, lazy-pages가 page fault를 처리할 수 없어 deadlock 발생.

### 해결

1. `lazyPagesActive` 플래그 추가: restore 시 `true`, lazy-pages `Wait()` 완료 후 `false`
2. `PreCheckpoint`, `FinalDump` RPC에서 `lazyPagesActive` 체크 → lazy-pages 진행 중이면 거부
3. `StatusResponse`에 `lazy_pages_active` 필드 추가
4. Controller에서 `waitForPageServerCompletion` (source zombie PID 체크) → `waitForLazyPagesCompletion` (target agent GetStatus polling)으로 교체

---

## Issue 5: PID Namespace fd 조기 Close

### 현상

`defer pidNsFd.Close()`가 `Restore()` 함수 반환 시 PID namespace fd를 닫아버림.

### 원인

`restore.go`에서 PID namespace fd를 `os.Open()`으로 열고 `defer Close()`를 사용. CRIU ExtraFiles로 전달했지만, `Restore()` 함수가 반환되면 fd가 닫힘.

### 해결

`defer pidNsFd.Close()` 제거. fd를 `RestoreResult.PidNsFd`로 반환하여 Agent에서 관리. CRIU restore 프로세스 종료 시 goroutine에서 close.

---

## Issue 6: `full` Strategy에서 nil pointer panic

### 현상

`full` strategy에서 restore 시 agent crash (EOF error). Panic: `nil pointer dereference at server.go:397`.

### 원인

`full` strategy에서는 lazy-pages를 시작하지 않으므로 `lazyPagesCmd`가 nil. 하지만 로그 출력에서 `lazyPagesCmd.Process.Pid`에 무조건 접근.

### 해결

nil 체크 추가:
```go
if lazyPagesCmd != nil && lazyPagesCmd.Process != nil {
    log.Printf("Lazy-pages daemon PID: %d", lazyPagesCmd.Process.Pid)
}
```

`lazyPagesActive`, lazy-pages Wait goroutine도 `needsLazy` 조건으로 감싸서 `full` strategy에서 불필요한 코드 실행 방지.

---

## Issue 7: `full` Strategy에서 `--enable-object-storage` 불필요

### 현상

`full` strategy에서 모든 파일을 로컬로 다운로드했는데도 `--enable-object-storage` args가 추가되어 CRIU가 S3에 접근 시도.

### 해결

```go
if m.s3Client != nil && strategy != "full" {
    // object-storage args 추가
}
```

---

## Issue 8: `lazy-direct`에서 checkpoint metadata 미업로드

### 현상

`lazy-direct` strategy에서 storage 업로드를 건너뛰었지만, target의 lazy-pages가 metadata 파일을 로컬에서 읽어야 함 → `Can't open dir: No such file or directory`.

### 원인

`lazy-direct`의 원래 의도는 storage를 안 쓰는 것이었지만, CRIU lazy-pages는 `--images-dir`로 로컬에 metadata 파일이 있어야 함. metadata는 반드시 storage를 경유해야 target에 전달 가능.

### 해결

`lazy-direct`에서도 checkpoint 파일을 storage에 비동기 업로드. `--lazy-pages` dump의 pages 파일은 minimal (lazy stubs)이므로 업로드 부담 적음. Controller에서 5초 대기 후 restore 진행.

**정의 재정립**: `lazy-direct` = "pages는 page-server TCP로 전송, metadata는 storage 경유"

# Pipe File Descriptor Inheritance on CRIU Restore in Kubernetes

## Abstract

CRIU(Checkpoint/Restore In Userspace)를 사용한 Kubernetes 컨테이너 live migration에서 복원된 프로세스가 즉시 종료되는 문제를 발견하고 해결하였다. 근본 원인은 Kubernetes 환경에서 컨테이너 프로세스의 stdout/stderr가 containerd 로그 수집 데몬에 연결된 anonymous pipe인데, cross-pod restore 시 해당 pipe의 read-end가 존재하지 않아 SIGPIPE가 발생하는 것이다. CRIU의 `--inherit-fd fd[N]:pipe:[inode]` 메커니즘을 활용하여 restore 시 원래 pipe를 새로운 pipe pair로 교체하는 방법으로 해결하였다.

---

## 1. 문제 정의

### 1.1 현상

CRIU restore가 성공적으로 완료되고 (`Restore finished successfully. Tasks resumed.`), lazy-pages가 모든 페이지를 정상 전송 (예: 17871/17871)하지만, 복원된 프로세스가 수 초 내에 종료됨.

- `dmesg`에 segfault/SIGBUS 기록 없음
- 로컬 환경 (`criu dump` + `criu restore --shell-job`)에서는 동일 워크로드가 정상 생존
- Kubernetes 환경의 `--join-ns` + `--inherit-fd` 조합에서만 발생

### 1.2 원인 분석 과정

1. **Zombie 의심**: `cmd.Wait()` 미호출로 인한 zombie 확인 → zombie는 발생했으나 프로세스 종료와 무관
2. **PID namespace fd 수명 의심**: `defer pidNsFd.Close()` → fd 수정 후에도 동일 현상
3. **`--restore-detached` 옵션 테스트**: 유무와 무관하게 동일 현상
4. **stdout 미사용 프로세스 비교 테스트**: `while True: time.sleep(1)` (stdout 미사용) → restore 후 정상 생존. `memwrite` (매 반복 `print()`) → restore 후 즉시 종료

이 비교를 통해 **stdout/stderr에 write하는 프로세스만 종료됨**을 확인.

### 1.3 근본 원인

Kubernetes 환경에서 컨테이너 프로세스의 fd 구조:

```
fd 0 → /dev/null
fd 1 → pipe:[232671]   (write-end, stdout → containerd log collector가 read-end 보유)
fd 2 → pipe:[232672]   (write-end, stderr → containerd log collector가 read-end 보유)
```

CRIU dump 시 이 pipe 상태가 checkpoint 이미지에 저장됨. Restore 시 CRIU가 pipe를 **새로 생성**하지만, 새 pipe의 read-end를 아무 프로세스도 소유하지 않음 (원래의 containerd log collector는 다른 pod에 있으므로).

복원된 프로세스가 `print()` → fd 1 (stdout pipe) write → read-end 없음 → **커널이 SIGPIPE 전송** → 프로세스 종료.

---

## 2. 해결 방법

### 2.1 CRIU `--inherit-fd` 메커니즘

CRIU는 `--inherit-fd fd[N]:RES` 옵션으로 restore 시 특정 file descriptor를 교체할 수 있다. RES 형식:

| RES 형식 | 용도 | 예시 |
|----------|------|------|
| `pipe:[inode]` | anonymous pipe 교체 | `fd[4]:pipe:[232671]` |
| `file[mnt_id:inode]` | 일반 파일 교체 | `fd[4]:file[72:a3e7]` |
| `tty[rdev:dev]` | TTY 교체 | `fd[4]:tty[136:1]` |
| `path/to/file` | 경로 기반 교체 | `fd[4]:tmp/old` |

**핵심**: pipe 매칭에서 inode 번호 앞에 **콜론이 필수**이다.

```
올바른 형식: --inherit-fd fd[4]:pipe:[232671]   (콜론 포함)
잘못된 형식: --inherit-fd fd[4]:pipe[232671]    (콜론 없음 → 매칭 실패)
```

이 형식은 `/proc/{pid}/fd/` 심볼릭 링크의 형식 (`pipe:[232671]`)과 정확히 일치한다.

### 2.2 dump/restore 흐름

#### Dump 측 (Source Agent)

1. 워크로드 프로세스의 fd 1, 2를 `/proc/{pid}/fd/` 심볼릭 링크로 읽어 pipe inode 확인
2. Pipe inode를 `CheckpointResult.PipeInodes`에 저장 (`{"stdout": "232671", "stderr": "232672"}`)
3. **`--external` 옵션은 사용하지 않음** — CRIU의 external resource 지원에 pipe가 포함되지 않음
4. Pipe inode를 gRPC response (`FinalDumpResponse.pipe_inodes`)로 controller에 전달

```go
// checkpoint.go — dump 시 pipe inode 기록
pipeInodes := make(map[string]string)
fdLabels := map[int]string{1: "stdout", 2: "stderr"}
for fd, label := range fdLabels {
    link, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", pid, fd))
    if strings.HasPrefix(link, "pipe:[") {
        inode := strings.TrimPrefix(strings.TrimSuffix(link, "]"), "pipe:[")
        pipeInodes[label] = inode
    }
}
```

#### Controller

FinalDumpResponse의 `pipe_inodes`를 RestoreRequest의 `pipe_inodes`로 전달.

```
FinalDumpResponse.PipeInodes → controller → RestoreRequest.PipeInodes
```

#### Restore 측 (Target Agent)

1. 각 pipe inode에 대해 `os.Pipe()`로 새 anonymous pipe pair 생성
2. Write-end를 CRIU의 ExtraFiles로 전달 (fd 4, fd 5 등)
3. `--inherit-fd fd[4]:pipe:[inode]` 옵션으로 CRIU에 pipe 교체 지시
4. Read-end를 agent goroutine에서 drain하여 SIGPIPE 방지

```go
// restore.go — restore 시 pipe 교체
if stdoutInode, ok := pipeInodes["stdout"]; ok {
    stdoutR, stdoutW, _ := os.Pipe()
    extraFiles = append(extraFiles, stdoutW)
    args = append(args, "--inherit-fd",
        fmt.Sprintf("fd[%d]:pipe:[%s]", fdIndex, stdoutInode))

    // Agent가 read-end를 drain — SIGPIPE 방지
    go func() {
        io.Copy(os.Stdout, stdoutR)
        stdoutR.Close()
    }()
}
```

### 2.3 데이터 흐름 다이어그램

```
[Source Pod]
  app process: fd 1 → pipe:[232671] (containerd read-end)
       ↓ CRIU dump
  pipe inode 기록: {"stdout": "232671", "stderr": "232672"}
       ↓ gRPC FinalDumpResponse
[Controller]
       ↓ gRPC RestoreRequest (pipe_inodes 포함)
[Target Pod]
  Agent: os.Pipe() → stdoutR (read-end), stdoutW (write-end)
  CRIU restore: --inherit-fd fd[4]:pipe:[232671]
       ↓ CRIU가 pipe:[232671]을 fd 4 (stdoutW)로 교체
  restored process: fd 1 → new pipe (Agent가 read-end drain)
       ↓ print() → new pipe write → Agent read → no SIGPIPE
```

---

## 3. CRIU 내부 동작

### 3.1 Pipe 매칭 로직

CRIU restore 시 `--inherit-fd fd[N]:pipe:[inode]`가 지정되면:

1. CRIU가 checkpoint 이미지에서 pipe entry를 읽음 (`Collected pipe entry ID 0x12 PIPE ID 0x411dc`)
2. 각 pipe entry의 inode와 `--inherit-fd`의 inode를 매칭 (`Found id pipe:[266716] (fd 4) in inherit fd list`)
3. 매칭된 pipe는 새로 생성하지 않고, inherit fd를 사용하여 복원 (`File pipe:[266716] will be restored from fd 1 dumped from inherit fd 1`)

### 3.2 `--external`과의 차이

| | `--external` (dump) | `--inherit-fd` (restore) |
|---|---|---|
| **pipe 지원** | 미지원 | 지원 (`pipe:[inode]` 형식) |
| **mnt 지원** | 지원 (`mnt[path]:label`) | 미사용 (restore에서 `--external mnt[label]:path`) |
| **pid 지원** | 지원 (`pid[inode]:label`) | 지원 (`fd[N]:label`) |
| **용도** | dump 시 external resource 마킹 | restore 시 fd 교체 |

Pipe는 dump 시 `--external`로 마킹하지 않고, restore 시 `--inherit-fd`만으로 교체 가능하다. 이는 CRIU가 pipe를 checkpoint 이미지에 완전히 저장하되, restore 시 inherit-fd로 지정된 pipe를 우선적으로 사용하기 때문이다.

---

## 4. 구현 세부사항

### 4.1 Proto 정의

```protobuf
// FinalDumpResponse
message FinalDumpResponse {
  // ...
  map<string, string> pipe_inodes = 6;  // "stdout" → "232671"
}

// RestoreRequest
message RestoreRequest {
  // ...
  map<string, string> pipe_inodes = 9;  // dump에서 전달받은 pipe inode
}
```

### 4.2 ExtraFiles fd 번호 관리

Go의 `exec.Cmd.ExtraFiles`에 추가된 파일은 fd 3부터 순서대로 할당된다:

```
ExtraFiles[0] → fd 3  (PID namespace fd)
ExtraFiles[1] → fd 4  (stdout pipe write-end)
ExtraFiles[2] → fd 5  (stderr pipe write-end)
```

`--inherit-fd`의 fd 번호는 이 순서와 일치해야 한다.

### 4.3 Drain Goroutine

Agent가 pipe의 read-end를 goroutine에서 drain한다. 이 goroutine은:
- 복원된 프로세스의 출력을 agent의 stdout/stderr로 전달
- Read-end가 살아있으므로 write-end에 SIGPIPE가 발생하지 않음
- 프로세스가 종료되면 write-end가 닫히고, `io.Copy`가 EOF를 반환하여 goroutine 종료

---

## 5. 제한사항 및 향후 과제

### 5.1 현재 제한

- **fd 0 (stdin)**: `/dev/null`로 설정되어 있으므로 교체 불필요
- **fd 1, 2만 처리**: stdout/stderr 이외의 pipe fd는 현재 미처리
- **Pipe 외 fd**: 일반 파일 fd (예: 로그 파일 핸들)는 `--join-ns mnt`로 mount namespace를 공유하므로 자동 복원

### 5.2 향후 개선

- 복원된 프로세스의 출력을 target pod의 containerd 로그 수집 시스템에 연결하면 `kubectl logs`에서 확인 가능
- 현재는 agent의 stdout으로 drain하므로 agent container의 로그에서 확인 가능

---

## 6. 참고 자료

- CRIU Inheriting FDs on Restore: https://criu.org/Inheriting_FDs_on_restore
- CRIU External Resources: https://criu.org/External_resources
- CRIU `--inherit-fd` 형식: `fd[N]:pipe:[inode]` (콜론 필수)
- Linux pipe(2): anonymous pipe의 read-end가 모두 닫히면 write 시 SIGPIPE 전송

# Write Profiler: Independent Memory Write Pattern Analysis for CRIU Live Migration
# CRIU Live Migration을 위한 독립적 메모리 쓰기 패턴 분석기

## Abstract

본 문서는 CRIU의 자체 dirty page 추적 메커니즘과 독립적으로 동작하는 메모리 쓰기 프로파일러의 설계와 구현을 제시한다. 이 프로파일러는 Linux 커널의 userfaultfd write-protection (uffd-wp) async 모드를 활용하며, PTE bit 57 (`PAGE_IS_WRITTEN`)을 사용하여 CRIU의 soft-dirty 추적 (PTE bit 55)과 간섭 없이 메모리 쓰기 패턴을 추적한다. 빈번하게 쓰기가 발생하는 hot VMA를 식별함으로써, CRIU가 incremental pre-dump 시 `--exclude-range`를 통해 해당 영역을 건너뛸 수 있게 하여 체크포인트 크기와 live migration 중 전송 시간을 줄인다. 전체 구현은 CGO 의존성 없이 순수 Go로 작성되었으며, ptrace 기반 syscall injection을 통해 외부 프로파일러 프로세스에서 대상 프로세스의 uffd-wp를 설정한다.

## 1. Motivation

### 1.1 CRIU's Incremental Checkpoint Mechanism

CRIU (Checkpoint/Restore In Userspace)는 pre-dump 메커니즘을 통한 incremental checkpointing을 지원한다. 연속적인 pre-dump 사이에서, CRIU는 Linux soft-dirty PTE bit (bit 55)를 사용하여 마지막 체크포인트 이후 수정된 페이지를 식별한다. 워크플로우는 다음과 같다:

1. CRIU가 `/proc/pid/clear_refs`에 `4`를 써서 모든 soft-dirty 비트를 클리어한다.
2. 프로세스가 계속 실행되며, 커널이 쓰기가 발생한 모든 페이지에 PTE bit 55를 설정한다.
3. 다음 pre-dump 시, CRIU가 `/proc/pid/pagemap`을 읽어 bit 55가 설정된 페이지를 찾고 해당 페이지만 덤프한다.

이 메커니즘은 쓰기 비율이 낮은 프로세스에서 잘 동작한다. 그러나 쓰기 집약적인 워크로드의 경우, 특정 VMA는 pre-dump 사이에 항상 dirty 상태가 되어, 매 pre-dump 반복마다 이러한 hot 영역을 포함하는 것은 비효율적이다: 다음 pre-dump가 실행될 때쯤이면 해당 페이지는 다시 dirty 상태가 되기 때문이다.

### 1.2 The Conflict Problem

soft-dirty를 사용하는 (즉, `/proc/pid/clear_refs`에 쓰는) 다른 외부 dirty 추적 도구는 CRIU의 추적과 충돌한다. 프로파일링 목적으로 soft-dirty 비트를 클리어하면, CRIU가 실제로 수정된 페이지를 놓치게 되어 불완전하거나 손상된 체크포인트가 생성될 수 있다.

### 1.3 The uffd-wp Async Solution

Linux 커널 6.7+에서 userfaultfd write-protection의 async 모드 (`UFFD_FEATURE_WP_ASYNC`)가 도입되었다. 이 모드는 별도의 PTE bit -- bit 57 (`PAGE_IS_WRITTEN`) --을 사용하여 쓰기를 추적한다. 주요 특성은 다음과 같다:

- **비트 독립성**: PTE bit 57은 PTE bit 55 (soft-dirty)와 완전히 분리되어 있다. 한쪽 비트의 설정, 클리어, 읽기는 다른 쪽에 영향을 주지 않는다.
- **원자적 scan-and-clear**: `PAGEMAP_SCAN` ioctl의 `PM_SCAN_WP_MATCHING`은 단일 시스템 호출에서 어떤 페이지에 쓰기가 발생했는지 읽고 write protection을 재설정하는 작업을 원자적으로 수행한다.
- **시그널 미발생**: 전통적인 uffd-wp와 달리, async 모드는 쓰기 시 userfault 이벤트를 생성하지 않는다. 쓰기는 정상적으로 진행되며, 커널은 단순히 PTE에 기록한다.

### 1.4 Design Goal

Write Profiler의 목표는 CRIU와 함께 실행되면서, 메모리 쓰기 패턴을 지속적으로 프로파일링하여 hot VMA를 식별하는 것이다. 이렇게 식별된 hot VMA는 `--exclude-range` 인자로 CRIU에 전달되어, pre-dump 시 해당 영역을 건너뛰도록 지시한다. Hot VMA는 불가피한 최종 dump 시에 전송된다.

## 2. Architecture Overview

### 2.1 Component Diagram

```
+-------------------------------------------------------------------+
|  Agent Pod (sidecar container)                                    |
|                                                                   |
|  +---------------------------+    +-----------------------------+ |
|  |       gRPC Server         |    |    CheckpointManager        | |
|  |  (StartProfiling,         |    |  (PreCheckpoint,            | |
|  |   StopProfiling,          |--->|   FinalDump,                | |
|  |   GetHotRegions,          |    |   appendExcludeArgs)        | |
|  |   GetDirtyVolume)         |    +-----------------------------+ |
|  +---------------------------+                                    |
|              |                                                    |
|              v                                                    |
|  +---------------------------+                                    |
|  |     Write Profiler        |                                    |
|  |  +---------------------+ |    +-----------------------------+ |
|  |  | ptrace Injector     | |    |  Target Process (main       | |
|  |  | (one-time setup)    |----->|  container, shared PID ns)  | |
|  |  +---------------------+ |    |                             | |
|  |  +---------------------+ |    |  uffd fd (in-process)       | |
|  |  | PAGEMAP_SCAN loop   |<---->|  /proc/pid/pagemap          | |
|  |  +---------------------+ |    |  /proc/pid/maps             | |
|  |  +---------------------+ |    +-----------------------------+ |
|  |  | Heat Classifier     | |                                    |
|  |  | (sliding window)    | |                                    |
|  |  +---------------------+ |                                    |
|  +---------------------------+                                    |
+-------------------------------------------------------------------+
```

### 2.2 Operational Flow

1. **초기화**: agent가 `StartProfiling` gRPC 호출을 수신한다. 공유 PID namespace에서 대상 프로세스의 PID를 찾는다.
2. **ptrace Injection**: 전용 goroutine (OS 스레드에 고정됨)이 `PTRACE_SEIZE`를 통해 대상에 attach하고, userfaultfd를 생성하여 모든 쓰기 가능한 anonymous VMA를 `UFFDIO_REGISTER_MODE_WP` 모드로 등록하는 일련의 syscall을 inject한 후, `pidfd_getfd`를 통해 uffd file descriptor를 프로파일러 프로세스로 복사한다.
3. **Baseline 설정**: 프로파일러가 `PM_SCAN_WP_MATCHING`과 `ReturnMask=0`으로 초기 `PAGEMAP_SCAN`을 수행하여, 결과를 읽지 않고 현재 존재하는 모든 페이지에 write-protect를 건다. 이것이 baseline이 되어, 이후의 모든 쓰기가 `PAGE_IS_WRITTEN`을 설정하게 된다.
4. **주기적 Scan**: 백그라운드 goroutine이 설정 가능한 간격 (기본값: 1000ms)으로 실행된다. 각 주기마다:
   - `/proc/pid/maps`를 다시 파싱하여 새로운 VMA (`mmap`, `brk` 등으로 인한)를 감지한다.
   - 새로운 VMA를 tracker 측 uffd fd의 `UFFDIO_REGISTER`를 통해 등록한다 (ptrace 불필요).
   - 각 쓰기 가능한 anonymous VMA에 대해 `PM_SCAN_WP_MATCHING`으로 `PAGEMAP_SCAN`을 수행하여, dirty 페이지를 원자적으로 읽고 write protection을 재설정한다.
5. **Heat Classification**: scan 결과가 VMA별 sliding window classifier에 입력되어, 설정 가능한 연속 구간 동안 설정 가능한 쓰기 비율 임계값을 초과하는 VMA를 식별한다.
6. **CRIU 통합**: 각 pre-dump 전에 프로파일러가 VMA를 해제 (`UFFDIO_UNREGISTER`)하고, hot 영역을 `--exclude-range` 인자로 제공하며, dump 완료 후 재등록한다.

### 2.3 Key Design Decisions

- **순수 Go, CGO 미사용**: 모든 ptrace 연산, ioctl 호출, 구조체 직렬화가 `syscall.RawSyscall6`, `syscall.Syscall`, `unsafe.Pointer`를 사용하여 구현되었다. CGO 오버헤드를 피하고 크로스 컴파일을 단순화한다.
- **PTRACE_ATTACH 대신 PTRACE_SEIZE 사용**: `PTRACE_SEIZE`는 attach 시 대상을 정지시키지 않아 방해를 최소화한다. 대상은 `PTRACE_INTERRUPT`를 통한 syscall injection 중에만 잠시 정지된다.
- **fd 전송에 pidfd_getfd 사용**: Unix domain socket `SCM_RIGHTS`를 통한 uffd fd 전송 대신, 프로파일러가 `pidfd_open` + `pidfd_getfd`를 사용하여 대상의 uffd fd를 프로파일러의 fd 테이블에 직접 복제한다. 이 방식이 더 간단하며 추가 IPC 설정이 필요 없다.

## 3. PTE Bit Independence

### 3.1 Page Table Entry Layout (x86_64)

x86_64 page table entry에는 Linux 커널이 페이지 상태 추적에 사용하는 여러 소프트웨어 정의 비트가 포함되어 있다:

| Bit | Name | Mechanism | Used By |
|-----|------|-----------|---------|
| 55 | `PTE_SOFT_DIRTY` | Set by kernel on write, cleared via `/proc/pid/clear_refs` | CRIU pre-dump |
| 57 | `PTE_UFFD_WP` | Managed by uffd-wp subsystem, tracked as `PAGE_IS_WRITTEN` | Write Profiler |

### 3.2 Kernel-Level Separation

두 비트는 완전히 독립적인 커널 서브시스템에 의해 관리된다:

- **Soft-dirty (bit 55)**: `mm/memory.c`에 의해 관리된다. `/proc/pid/clear_refs`에 `4`를 쓰면 모든 PTE를 순회하며 bit 55를 클리어한다. `/proc/pid/pagemap`의 bit position 55를 통해 읽는다.
- **uffd-wp (bit 57)**: userfaultfd 서브시스템 (`fs/userfaultfd.c` 및 `mm/mprotect.c`)에 의해 관리된다. `UFFD_FEATURE_WP_ASYNC`가 활성화되면, write-protected 페이지에 대한 쓰기가 fault 없이 진행되며, 커널이 WP 비트를 클리어하고 `PAGE_IS_WRITTEN` 마커를 설정한다. 이 마커는 `PAGEMAP_SCAN` ioctl의 category bitmask에서 bit position 1 (`PAGE_IS_WRITTEN`)을 통해 읽는다.

### 3.3 Non-Interference Guarantee

핵심 속성은 다음과 같다:

- `/proc/pid/clear_refs`에 `4`를 쓰면 **bit 55만 클리어**된다. bit 57은 건드리지 않는다.
- `PM_SCAN_WP_MATCHING`을 사용한 `PAGEMAP_SCAN`은 **bit 57에만 작용**한다. bit 55는 건드리지 않는다.
- 단일 페이지 쓰기는 **두 비트 모두 독립적으로** 설정한다: bit 55는 soft-dirty 메커니즘을 통해, bit 57은 uffd-wp async 메커니즘을 통해.

따라서 Write Profiler와 CRIU는 동일한 프로세스에서 어떠한 간섭도 없이 동시에 동작할 수 있다. 각각은 마지막 clear/scan 연산 이후 어떤 페이지에 쓰기가 발생했는지에 대한 자체적으로 일관된 뷰를 가진다.

### 3.4 PAGEMAP_SCAN Atomicity

`PAGEMAP_SCAN` ioctl (`0xC0606610`)은 원자적 scan-and-clear 시맨틱을 제공한다. `PM_SCAN_WP_MATCHING` 플래그와 함께 호출되면:

1. 커널이 지정된 주소 범위의 PTE를 순회한다.
2. category 필터와 일치하는 각 페이지에 대해 `PAGE_IS_WRITTEN`이 설정되어 있는지 확인한다.
3. 설정되어 있으면 출력 벡터에 페이지를 기록하고 **원자적으로 write protection을 재설정**한다 (`PAGE_IS_WRITTEN`을 클리어하고 WP 비트를 복원).
4. 해당 페이지에 대한 다음 쓰기가 다시 `PAGE_IS_WRITTEN`을 설정하게 된다.

이 원자적 연산은 별도의 read-then-clear 방식에 내재하는 race condition을 제거한다.

## 4. Implementation Details

### 4.1 ptrace Syscall Injection

프로파일러는 대상 프로세스의 주소 공간 내에서 userfaultfd를 생성하고 VMA를 write-protection에 등록해야 한다. 프로파일러가 별도의 프로세스(agent sidecar 컨테이너)에서 실행되므로, ptrace를 사용하여 대상에 syscall을 inject한다.

#### 4.1.1 Attachment

```
PTRACE_SEIZE(pid)       // Non-stopping attach (unlike PTRACE_ATTACH)
PTRACE_INTERRUPT(pid)   // Stop the target for syscall injection
wait4(pid)              // Wait for stop notification
PTRACE_GETREGS(pid)     // Save original register state
PTRACE_PEEKDATA(RIP)    // Save original instruction bytes at RIP
PTRACE_POKEDATA(RIP, 0x050F)  // Write 'syscall' instruction (0x0F 0x05)
```

`PTRACE_SEIZE`(`PTRACE_ATTACH` 대신)의 사용은 중요하다: `PTRACE_ATTACH`는 대상에 `SIGSTOP`을 보내고 대상의 부모에서 `wait()`를 트리거하여 프로세스 관리를 방해할 수 있다. `PTRACE_SEIZE`는 정지나 시그널 없이 attach한다.

`PTRACE_SEIZE`와 `PTRACE_INTERRUPT` 모두 `syscall.RawSyscall6`을 사용하여 구현되는데, Go의 `x/sys/unix` 패키지가 이러한 연산을 직접 노출하지 않기 때문이다:

```go
syscall.RawSyscall6(syscall.SYS_PTRACE,
    uintptr(unix.PTRACE_SEIZE), uintptr(pid), 0, 0, 0, 0)
```

#### 4.1.2 Syscall Injection Sequence

대상이 RIP의 `syscall` 명령어로 정지되면, 프로파일러는 다음 순서로 syscall을 inject한다:

| Step | Syscall | Purpose |
|------|---------|---------|
| 1 | `mmap(NULL, 4096, PROT_READ\|PROT_WRITE, MAP_PRIVATE\|MAP_ANONYMOUS, -1, 0)` | Allocate a scratch page in the target's address space for ioctl argument structs |
| 2 | `userfaultfd(O_CLOEXEC \| O_NONBLOCK \| UFFD_USER_MODE_ONLY)` | Create userfaultfd with user-mode-only flag |
| 3 | `ioctl(uffd, UFFDIO_API, &{api=0xAA, features=WP_ASYNC\|WP_UNPOPULATED})` | Negotiate API version and enable required features |
| 4 | `ioctl(uffd, UFFDIO_REGISTER, &{range, mode=WP})` | Register each writable anonymous VMA (repeated per VMA) |

3단계와 4단계에서, ioctl 인자 구조체는 `PTRACE_POKEDATA`를 통해 scratch 페이지에 쓰여진 후, scratch 페이지 주소를 인자 포인터로 하는 ioctl syscall이 inject된다.

각 inject된 syscall은 동일한 패턴을 따른다:

```go
func (p *ptraceInjector) injectSyscall(nr, a1, a2, a3, a4, a5, a6 uint64) (uint64, error) {
    // Set registers: RAX=nr, RDI=a1, RSI=a2, RDX=a3, R10=a4, R8=a5, R9=a6
    // Set RIP back to the saved RIP (where 'syscall' instruction was poked)
    // PTRACE_SINGLESTEP to execute exactly one instruction
    // wait4() for SIGTRAP
    // PTRACE_GETREGS to read return value from RAX
}
```

#### 4.1.3 fd Transfer via pidfd_getfd

대상 내부에서 uffd를 생성한 후, 프로파일러는 자신의 프로세스에 해당 fd의 사본이 필요하다. 이는 `pidfd_open` + `pidfd_getfd` syscall 쌍을 통해 수행된다:

```go
pidfd, _ := unix.PidfdOpen(pid, 0)      // Get a pidfd for the target
trackerFd, _ := unix.PidfdGetfd(pidfd, int(uffd), 0)  // Duplicate target's uffd into profiler
```

이 방식은 Unix domain socket `SCM_RIGHTS`보다 선호되는데, 대상 프로세스와의 조율이 필요 없기 때문이다 -- 대상이 fd를 보낼 필요 없이, 프로파일러가 단순히 복사한다.

#### 4.1.4 UFFD_USER_MODE_ONLY

`UFFD_USER_MODE_ONLY` 플래그 (값 `1`)는 `userfaultfd()` syscall에 전달된다. 이 플래그는 Linux 5.11에서 도입되어, sysctl `vm.unprivileged_userfaultfd`가 `0`으로 설정되어 있을 때 비특권 userfaultfd 생성을 허용한다. 이 플래그를 사용하면, uffd는 사용자 모드 코드의 fault만 처리하며 (커널 모드 fault 제외), 이는 write-protection 추적에 충분하다.

구현에는 fallback 경로가 포함되어 있다: `UFFD_USER_MODE_ONLY`를 사용한 `userfaultfd()`가 실패하면, 플래그 없이 재시도한다 (sysctl이 비특권 userfaultfd를 허용하는 커널용).

#### 4.1.5 Cleanup and Restoration

모든 syscall injection이 완료된 후:

1. scratch 페이지가 inject된 `munmap()`을 통해 해제된다.
2. RIP의 원래 명령어 바이트가 `PTRACE_POKEDATA`를 통해 복원된다.
3. 원래 레지스터 상태가 `PTRACE_SETREGS`를 통해 복원된다.
4. `PTRACE_DETACH`를 통해 대상이 분리되고 실행이 재개된다.

대상 프로세스는 injection이 발생했음을 인식하지 못한다. uffd fd는 대상에서 열린 상태로 유지되며 (`O_CLOEXEC` 설정됨), 등록된 모든 VMA에 `VM_UFFD_WP` 플래그가 설정된다.

#### 4.1.6 OS Thread Locking

모든 ptrace 연산은 동일한 OS 스레드에서 수행되어야 하는데, Linux 커널이 ptrace attachment를 특정 스레드 (task)와 연관시키기 때문이다. Go에서 goroutine은 OS 스레드 간에 다중화되므로, 프로파일러는 ptrace 연산 전에 `runtime.LockOSThread()`를 호출하고 전체 injection 시퀀스를 전용 goroutine에서 실행한다:

```go
go func() {
    runtime.LockOSThread()
    defer runtime.UnlockOSThread()
    trackerFd, targetFd, registered, err = setupUffdWP(pid, vmas)
}()
```

### 4.2 PAGEMAP_SCAN ioctl

#### 4.2.1 ioctl Number

`PAGEMAP_SCAN` ioctl 번호는 `0xC0606610`이며, Linux ioctl 인코딩 매크로로부터 계산된다:

```
_IOWR('f', 16, sizeof(struct pm_scan_arg))
= _IOWR(0x66, 0x10, 96)
= 0xC0606610
```

여기서 `'f' = 0x66`은 file ioctl 타입이고, `16 = 0x10`은 명령 번호이며, `96`은 인자 구조체 크기이다.

#### 4.2.2 pm_scan_arg Structure

인자 구조체는 96바이트 (12 x uint64)이며, 컴파일 타임 크기 검증을 포함한다:

```go
type pmScanArg struct {
    Size              uint64  // sizeof(pm_scan_arg), must be 96
    Flags             uint64  // PM_SCAN_WP_MATCHING | PM_SCAN_CHECK_WPASYNC
    Start             uint64  // Scan range start address
    End               uint64  // Scan range end address
    WalkEnd           uint64  // Output: address where scan stopped
    Vec               uint64  // Pointer to pageRegion output array
    VecLen            uint64  // Length of output array
    MaxPages          uint64  // Maximum pages to scan (0 = unlimited)
    CategoryInverted  uint64  // Inverted category filter mask
    CategoryMask      uint64  // Required category filter mask
    CategoryAnyofMask uint64  // Any-of category filter mask
    ReturnMask        uint64  // Categories to include in output
}

// Compile-time size assertion
var _ [96]byte = [unsafe.Sizeof(pmScanArg{})]byte{}
```

#### 4.2.3 Category Masks

프로파일러는 다음 page category 플래그를 사용한다:

| Flag | Value | Purpose |
|------|-------|---------|
| `PAGE_IS_WPALLOWED` | `1 << 0` | Page supports write-protection (verification) |
| `PAGE_IS_WRITTEN` | `1 << 1` | Page has been written since last WP (the dirty indicator) |
| `PAGE_IS_FILE` | `1 << 2` | Page is file-backed (excluded from tracking) |
| `PAGE_IS_PRESENT` | `1 << 3` | Page is present in RAM |
| `PAGE_IS_SWAPPED` | `1 << 4` | Page is swapped out |
| `PAGE_IS_PFNZERO` | `1 << 5` | Page maps to PFN zero (excluded from tracking) |

#### 4.2.4 Two Scan Modes

프로파일러는 `PAGEMAP_SCAN`을 두 가지 모드로 사용한다:

**Baseline WP Scan** (초기화 시 및 CRIU reinit 후 호출):
```go
arg := pmScanArg{
    Flags:             PM_SCAN_WP_MATCHING,
    CategoryInverted:  PAGE_IS_PFNZERO | PAGE_IS_FILE,  // Exclude zero/file pages
    CategoryMask:      PAGE_IS_PFNZERO | PAGE_IS_FILE,  // Filter condition
    CategoryAnyofMask: PAGE_IS_PRESENT | PAGE_IS_SWAPPED, // Must be present or swapped
    ReturnMask:        0,  // Don't return results, just write-protect
}
```

이 scan은 출력을 수집하지 않고 모든 적격 페이지에 write-protect를 건다. `CategoryInverted`와 `CategoryMask`의 조합은 제외 필터를 생성한다: `PAGE_IS_PFNZERO` 또는 `PAGE_IS_FILE`이 설정된 페이지는 건너뛴다 (이 비트들이 inverted mask와 required mask 모두에 나타나므로, `(categories & ~inverted) & mask == mask` 조건이 해당 페이지에 대해 실패한다).

**Dirty Page Scan** (각 프로파일링 간격마다 호출):
```go
arg := pmScanArg{
    Flags:             PM_SCAN_WP_MATCHING,
    CategoryInverted:  PAGE_IS_PFNZERO | PAGE_IS_FILE,
    CategoryMask:      PAGE_IS_PFNZERO | PAGE_IS_FILE | PAGE_IS_WRITTEN,
    CategoryAnyofMask: PAGE_IS_PRESENT | PAGE_IS_SWAPPED,
    ReturnMask:        PAGE_IS_PRESENT | PAGE_IS_SWAPPED | PAGE_IS_WRITTEN,
}
```

이 scan은 (제외 필터에 추가로) `PAGE_IS_WRITTEN`이 설정되어 있을 것을 요구하고, 일치하는 영역을 category 플래그와 함께 반환하며, 원자적으로 write protection을 재설정한다.

#### 4.2.5 Verification Scan

uffd-wp 설정 후, 프로파일러는 `PAGE_IS_WPALLOWED`가 있는 페이지를 스캔하여 write-protection이 활성 상태인지 검증한다:

```go
arg := pmScanArg{
    Start:             0,
    End:               0x7fffffffffff,
    MaxPages:          1,
    CategoryMask:      PAGE_IS_WPALLOWED,
    CategoryAnyofMask: PAGE_IS_PRESENT | PAGE_IS_SWAPPED,
    ReturnMask:        PAGE_IS_WPALLOWED,
}
```

`PAGE_IS_WPALLOWED`가 설정된 페이지가 없으면, uffd-wp 설정이 실패한 것이며 프로파일러는 중단된다.

### 4.3 VMA Management

#### 4.3.1 VMA Parsing

프로파일러는 `/proc/pid/maps`를 파싱하여 현재 VMA 레이아웃을 얻는다. 각 줄은 시작 주소, 끝 주소, 권한 문자열, 경로명, 분류된 타입을 포함하는 `VMAInfo` 구조체로 파싱된다.

#### 4.3.2 VMA Classification

VMA는 다음과 같은 타입으로 분류된다:

| Type | Pathname Pattern | Description |
|------|-----------------|-------------|
| `VMAHeap` | `[heap]` | Process heap (brk/sbrk) |
| `VMAStack` | `[stack]` | Main thread stack |
| `VMAAnonymous` | (empty) | Anonymous mmap regions |
| `VMACode` | `/path/...` with `x` perm | Executable file mappings |
| `VMAData` | `/path/...` without `x` perm | Non-executable file mappings |
| `VMAVDSO` | `[vdso]`, `[vvar]`, `[vsyscall]` | Kernel-provided virtual DSOs |

**쓰기 가능하고, private이며, vdso가 아닌** VMA만 추적된다. 이 필터는 `IsWritableAnonymous()`로 구현된다:

```go
func (v *VMAInfo) IsWritableAnonymous() bool {
    return v.IsWritable() && v.IsPrivate() && v.Type != VMAVDSO
}
```

이는 heap, stack, anonymous VMA를 포함하면서 code 섹션, file-backed 데이터, 커널 virtual DSO를 제외한다.

#### 4.3.3 Dynamic VMA Tracking

프로세스 메모리 레이아웃은 `mmap`, `munmap`, `brk` 등의 연산으로 인해 동적으로 변한다. 프로파일러는 각 scan 간격마다 `/proc/pid/maps`를 다시 파싱하고 이전에 등록된 VMA와 비교하여 이를 처리한다:

```go
func diffVMAs(current []VMAInfo, registered []AddrRange) []VMAInfo
```

`diffVMAs` 함수는 현재 맵에는 존재하지만 등록된 집합에는 없는 VMA를 반환한다 (시작 및 끝 주소로 매칭). 새로운 VMA는 tracker 측 uffd fd의 `UFFDIO_REGISTER`를 통해 등록된다 -- 이는 ptrace 재injection이 필요 없는데, `pidfd_getfd`를 통해 얻은 기존 uffd fd에 대한 ioctl 연산이 프로세스 경계를 넘어 동작하기 때문이다.

### 4.4 Heat Classification

#### 4.4.1 Sliding Window

heat classifier는 VMA별 쓰기 비율의 sliding window를 유지한다. 각 VMA는 시작 주소로 키가 지정되며 `vmaHeatState` 구조체에서 추적된다:

```go
type vmaHeatState struct {
    Start          uint64
    End            uint64
    Ratios         [10]float64  // Circular buffer (windowSize = 10)
    Head           int          // Next write position
    Count          int          // Number of samples collected
    ConsecutiveHot int          // Consecutive intervals above threshold
    IsHot          bool
}
```

#### 4.4.2 Written Ratio Calculation

각 scan 간격에서 VMA의 쓰기 비율은 다음과 같다:

```
written_ratio = dirty_pages / total_pages
```

여기서 `dirty_pages`는 `PAGE_IS_WRITTEN`이 설정된 페이지 수 (`PAGEMAP_SCAN`에 의해 반환됨)이고, `total_pages`는 VMA 크기를 4096으로 나눈 값이다.

#### 4.4.3 Hot Classification

VMA는 다음 조건을 만족할 때 **hot**으로 분류된다:

```
written_ratio > hot_threshold    for   hot_consecutive   consecutive intervals
```

기본 파라미터:
- `hot_threshold = 0.3` (간격당 페이지의 30%에 쓰기 발생)
- `hot_consecutive = 3` (3회 연속 간격)

VMA의 쓰기 비율이 임계값 아래로 떨어지면, `ConsecutiveHot`이 0으로 리셋되고 VMA는 hot에서 cold로 전환된다.

#### 4.4.4 VMA Lifecycle

VMA가 (`munmap`으로 인해) `/proc/pid/maps`에서 사라지면, 해당 heat 상태가 classifier에서 제거된다. 이전에 추적되던 주소에 새로운 VMA가 나타나면, 새로운 상태가 생성된다. 이를 통해 classifier가 해제된 메모리 영역의 오래된 상태를 유지하지 않도록 보장한다.

## 5. CRIU Integration

프로파일러는 세 단계를 통해 CRIU의 체크포인트 라이프사이클과 통합된다: dump 전 정리, dump 자체, dump 후 재초기화.

### 5.1 Cleanup Before CRIU Dump

CRIU가 pre-dump 또는 final dump를 수행하기 전에, 프로파일러는 모든 VMA에서 `VM_UFFD_WP` 플래그를 제거해야 한다. CRIU는 dump 중 VMA 플래그를 검사하며, 예상치 못한 플래그를 만나면 실패한다.

```go
func (p *Profiler) CleanupBeforeCRIU() error {
    p.Stop()  // Stop the periodic scan loop
    for _, vma := range p.registeredVMAs {
        uffdioUnregister(p.trackerUffdFd, vma.Start, vma.End)
    }
    p.registeredVMAs = nil
    p.heat.reset()
    return nil
}
```

핵심 사항:
- `UFFDIO_UNREGISTER`만 수행되며, uffd fd는 열린 상태로 유지된다.
- 해제는 tracker 측 fd의 ioctl을 통해 수행된다 (ptrace 불필요).
- 이 연산은 대상의 VMA에서 `VM_UFFD_WP`를 제거하여, CRIU가 정상적으로 dump할 수 있게 한다.

### 5.2 Reinitialization After CRIU Dump

CRIU가 dump를 완료한 후 (pre-dump의 경우 `--leave-running` 사용), 프로파일러가 VMA를 재등록하고 추적을 재개한다:

```go
func (p *Profiler) ReinitAfterCRIU() error {
    vmas, _ := parseVMAs(p.pid)               // Re-read current VMAs
    writable := writableAnonymousVMAs(vmas)
    for _, vma := range writable {
        uffdioRegister(p.trackerUffdFd, ...)   // Re-register with uffd
    }
    wpAllWritablePages(pagemapFd, writable)    // Re-establish WP baseline
    // Reset cumulative counters and restart scan loop
}
```

핵심 사항:
- CRIU가 메모리 레이아웃을 수정했을 수 있으므로, `/proc/pid/maps`에서 VMA를 다시 읽는다.
- ptrace injection이 필요 없다: uffd fd가 여전히 유효하며 `UFFDIO_REGISTER`가 tracker 측 fd를 통해 동작한다.
- 누적 dirty 바이트 카운터가 리셋되어 다음 pre-dump 주기에 대한 새로운 측정을 시작한다.

### 5.3 Exclude Args Generation

agent는 hot 영역을 CRIU 명령줄 인자로 변환한다:

#### 5.3.1 Exclude Range

Hot VMA는 콜론으로 구분된 16진수 형식의 `--exclude-range` 인자로 전달된다:

```
--exclude-range 7f1234000000:7f1234100000
```

hot 범위가 10개를 초과하면, agent가 파일에 기록하고 대신 `--exclude-file`을 사용한다:

```
--exclude-file /path/to/exclude-ranges.txt
```

파일 형식은 공백으로 구분된 16진수 주소를 사용하며, 한 줄에 하나의 범위를 나타낸다:

```
7f1234000000 7f1234100000
7f5678000000 7f5678200000
```

#### 5.3.2 No-Parent Range

VMA가 pre-dump 사이에 hot에서 cold로 전환될 때, 특별한 처리가 필요하다. 이전 pre-dump는 이 VMA를 제외했으므로 (해당 영역의 페이지가 덤프되지 않음), 현재는 더 이상 hot이 아니므로 포함되어야 한다. 그러나 CRIU의 incremental 로직은 이 페이지들의 parent 이미지를 찾으려 하지만, 찾을 수 없다 (parent dump에서 제외되었으므로).

`--no-parent-range` 인자는 CRIU에게 지정된 범위를 parent가 없는 것으로 취급하도록 하여, 해당 페이지의 전체 dump를 강제한다:

```
--no-parent-range 7f1234000000:7f1234100000
```

agent는 이전 exclude 집합 (`prevExcludeSet`)을 추적하고 전환을 감지한다:

```go
// Detect hot -> cold transitions
for start, end := range a.prevExcludeSet {
    if _, exists := currentSet[start]; !exists {
        excludeArgs.NoParentRanges = append(excludeArgs.NoParentRanges,
            profiler.AddrRange{Start: start, End: end})
    }
}
```

### 5.4 Integration Flow with Pre-Dump

pre-dump 주기 동안의 전체 흐름:

```
1. Agent receives PreCheckpoint gRPC call
2. profiler.CleanupBeforeCRIU()
   - Stop scan loop
   - UFFDIO_UNREGISTER all VMAs
3. profiler.GetHotRegions() -> current hot set
4. Compute exclude ranges (current hot) and no-parent ranges (hot->cold transitions)
5. checkpointMgr.PreCheckpoint(pid, parentDumpID, excludeArgs)
   - appendExcludeArgs builds --exclude-range/--exclude-file/--no-parent-range args
   - CRIU pre-dump executes, skipping excluded ranges
6. profiler.ReinitAfterCRIU()
   - Re-read VMAs, UFFDIO_REGISTER, WP baseline
   - Restart scan loop
7. Update prevExcludeSet = currentSet
```

## 6. gRPC API

Write Profiler는 `CRIUAgent` 서비스의 네 가지 gRPC endpoint를 통해 노출된다:

### 6.1 StartProfiling

```protobuf
rpc StartProfiling(StartProfilingRequest) returns (StartProfilingResponse);

message StartProfilingRequest {
    int32 interval_ms = 1;      // Scan interval (default: 1000ms)
    double hot_threshold = 2;   // Written ratio threshold (default: 0.3)
    int32 hot_consecutive = 3;  // Consecutive hot intervals (default: 3)
}

message StartProfilingResponse {
    bool success = 1;
    string error = 2;
    int32 pid = 3;       // Target process PID
    int32 vma_count = 4; // Number of registered writable VMAs
}
```

ptrace injection을 통해 uffd-wp를 초기화하고 주기적 scan 루프를 시작한다. 대상 PID와 초기 VMA 수를 반환한다. 프로파일링이 이미 활성 상태이거나 대상 프로세스를 찾을 수 없으면 실패한다.

### 6.2 StopProfiling

```protobuf
rpc StopProfiling(StopProfilingRequest) returns (StopProfilingResponse);

message StopProfilingResponse {
    bool success = 1;
}
```

scan 루프를 중지하고 모든 리소스 (uffd fd, pagemap fd)를 해제한다. 멱등적 -- 프로파일링이 활성 상태가 아니면 성공을 반환한다.

### 6.3 GetHotRegions

```protobuf
rpc GetHotRegions(GetHotRegionsRequest) returns (GetHotRegionsResponse);

message GetHotRegionsResponse {
    repeated HotRegionProto regions = 1;
    int64 timestamp_ms = 2;
    int32 total_vmas = 3;
    int32 hot_vmas = 4;
}

message HotRegionProto {
    uint64 start_addr = 1;
    uint64 end_addr = 2;
    double written_ratio = 3;
    int32 consecutive_hot = 4;
}
```

현재 식별된 hot 영역의 스냅샷을, 전체 및 hot VMA 수와 함께 반환한다. `sync.RWMutex`를 통해 스레드 안전하다.

### 6.4 GetDirtyVolume

```protobuf
rpc GetDirtyVolume(GetDirtyVolumeRequest) returns (GetDirtyVolumeResponse);

message GetDirtyVolumeResponse {
    int64 dirty_pages = 1;               // Pages dirty in last interval
    int64 dirty_bytes = 2;               // Bytes dirty in last interval
    double dirty_rate_pages_per_sec = 3;  // Instantaneous dirty rate
    int64 cumulative_dirty_bytes = 4;     // Total dirty bytes since start
    double avg_dirty_rate = 5;            // Average dirty rate since start
    int64 timestamp_ms = 6;
}
```

dirty 페이지 통계를 반환한다. 순간 dirty rate는 `dirty_bytes / interval_seconds`로 계산된다. 평균 dirty rate는 `cumulative_dirty_bytes / elapsed_seconds`이다.

## 7. Experimental Validation

Write Profiler는 서로 다른 메모리 쓰기 패턴을 가진 세 가지 워크로드에 대해 검증되었다.

### 7.1 Test Environment

- **Kernel**: Linux 6.8+ (`PAGEMAP_SCAN` 및 `UFFD_FEATURE_WP_ASYNC` 지원)
- **Architecture**: x86_64
- **Profiler configuration**: interval=1000ms, hot_threshold=0.3, hot_consecutive=3

### 7.2 Results

| Workload | Description | Total VMAs | Hot VMAs | Written Ratio | Dirty Rate |
|----------|-------------|-----------|----------|---------------|------------|
| `memwrite` | Continuous 64MB memset loop | 23 | 3 | ~1.0 | 67.5 MB/s |
| `memory-alloc` | 32MB allocation every 3 seconds | 22 | 1 | variable | 188 KB/s |
| `matmul` | 1024x1024 matrix multiplication | 78 | 3 | variable | 500 KB/s |

### 7.3 Analysis

**memwrite (64 MB continuous write)**: 프로파일러가 3개의 hot VMA (heap과 쓰기 버퍼에 사용되는 2개의 anonymous 영역)를 정확히 식별했으며, 쓰기 비율은 약 1.0으로, 거의 모든 페이지가 매 간격마다 다시 쓰여졌음을 나타낸다. 67.5 MB/s의 dirty rate는 높은 쓰기 대역폭을 반영한다. 이 워크로드는 주요 사용 사례를 보여준다: 이 3개의 VMA는 pre-dump에서 제외되어야 하는데, 다음 pre-dump 전에 완전히 다시 dirty 상태가 되기 때문이다.

**memory-alloc (32 MB periodic allocation)**: 가장 최근 할당된 영역에 해당하는 1개의 VMA만 hot으로 분류되었다. 낮은 dirty rate (188 KB/s)는 워크로드의 간헐적 특성을 반영한다. 가변적인 쓰기 비율은 할당이 활성화될 때만 VMA가 hot 임계값을 넘는 것을 나타내며, 프로파일러의 시간적 패턴 추적 능력을 보여준다.

**matmul (1024x1024 matrix multiplication)**: 78개의 전체 VMA (공유 라이브러리 매핑과 스레드 스택 포함)가 있음에도, 입력 행렬과 출력 행렬에 해당하는 3개만 hot이었다. 500 KB/s의 dirty rate는 워크로드의 계산적 특성을 반영하며, 쓰기가 행렬 저장소에 집중된다. 이는 프로파일러의 선택성을 보여준다: 코드, 라이브러리 데이터, 기타 변경되지 않는 콘텐츠를 보유하는 대다수의 VMA를 정확히 무시한다.

## 8. Source File Reference

| File | Purpose |
|------|---------|
| `pkg/profiler/types.go` | Constants (ioctl numbers, page flags, syscall numbers), struct definitions (`pmScanArg`, `pageRegion`, uffd structs, `VMAInfo`, `HotRegion`, `DirtyVolume`), compile-time size assertions |
| `pkg/profiler/vma.go` | `/proc/pid/maps` parsing, VMA classification (heap/stack/anon/code/data/vdso), writable anonymous VMA filtering, dynamic VMA diffing |
| `pkg/profiler/ptrace.go` | `ptraceInjector` for syscall injection, `setupUffdWP` (complete uffd-wp setup via ptrace), `PTRACE_SEIZE`/`PTRACE_INTERRUPT` via `RawSyscall6`, `UFFDIO_REGISTER`/`UFFDIO_UNREGISTER` helpers |
| `pkg/profiler/scan.go` | `openPagemap`, `wpAllWritablePages` (baseline WP scan), `scanDirtyPages` (dirty page scan with `PM_SCAN_WP_MATCHING`), `verifyWPActive` (WP verification scan) |
| `pkg/profiler/heat.go` | `heatClassifier` with per-VMA sliding window, hot/cold classification logic, VMA lifecycle management |
| `pkg/profiler/profiler.go` | `Profiler` orchestrator: `Start` (init + ptrace setup), `loop` (periodic scan goroutine), `collectSample` (VMA re-parse + register + scan + classify), `CleanupBeforeCRIU` / `ReinitAfterCRIU`, thread-safe result accessors |
| `pkg/agent/server.go` | gRPC handlers (`StartProfiling`, `StopProfiling`, `GetHotRegions`, `GetDirtyVolume`), `prevExcludeSet` for hot-to-cold transition tracking, profiler integration with `PreCheckpoint` flow |
| `pkg/agent/checkpoint.go` | `appendExcludeArgs` (builds `--exclude-range`/`--exclude-file`/`--no-parent-range` args), `writeExcludeFile` (writes space-separated hex ranges), `CRIUExcludeArgs` struct |
| `pkg/proto/agent.proto` | Protobuf definitions for `StartProfiling`, `StopProfiling`, `GetHotRegions`, `GetDirtyVolume` request/response messages |

## 9. Requirements and Limitations

### 9.1 Kernel Requirements

- `PAGEMAP_SCAN` ioctl 및 `UFFD_FEATURE_WP_ASYNC`를 위해 Linux >= 6.7 필요.
- 커널 설정에서 `CONFIG_USERFAULTFD=y` 필요.
- x86_64 아키텍처 (syscall injection이 아키텍처 특화적).

### 9.2 Capability Requirements

- ptrace attachment 및 `/proc/pid/pagemap` 접근을 위해 `SYS_PTRACE` capability 필요.
- agent와 대상 컨테이너 간 공유 PID namespace 필요.

### 9.3 Known Limitations

- **아키텍처 특화**: ptrace injection 시퀀스 (레지스터 레이아웃, syscall 명령어 인코딩 `0x0F 0x05`, syscall 번호)는 x86_64에 특화되어 있다. 다른 아키텍처를 지원하려면 별도의 injection 구현이 필요하다.
- **단일 대상**: 각 `Profiler` 인스턴스는 하나의 프로세스를 추적한다. 멀티 프로세스 워크로드는 여러 프로파일러 인스턴스가 필요하다.
- **VMA 단위 세분성**: heat classification은 VMA 단위로 동작한다. 더 큰 cold 영역 내에 국소적 hot 페이지를 가진 VMA는 전체가 hot 또는 cold로 분류된다. Sub-VMA 추적은 `PAGEMAP_SCAN` 출력 영역의 페이지 수준 분석이 필요하다.

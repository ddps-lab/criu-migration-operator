package profiler

import (
	"fmt"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ptraceInjector handles ptrace-based syscall injection into a target process.
// All methods must be called from a goroutine locked to an OS thread.
// Translated from main.c inject_syscall(), write_to_target(), setup_userfaultfd_wp_for_process().
type ptraceInjector struct {
	pid       int
	savedRegs unix.PtraceRegs
	savedCode [8]byte // saved instruction bytes at RIP
	scratch   uint64  // scratch page address in target
}

// injectSyscall executes a single syscall in the stopped target process.
// Assumes 'syscall' instruction (0x0F 0x05) has been poked at savedRegs.Rip.
// Translated from main.c inject_syscall() (lines 593-625).
func (p *ptraceInjector) injectSyscall(nr, a1, a2, a3, a4, a5, a6 uint64) (uint64, error) {
	var regs unix.PtraceRegs
	if err := unix.PtraceGetRegs(p.pid, &regs); err != nil {
		return 0, fmt.Errorf("GETREGS: %w", err)
	}

	regs.Rip = p.savedRegs.Rip
	regs.Rax = nr
	regs.Rdi = a1
	regs.Rsi = a2
	regs.Rdx = a3
	regs.R10 = a4
	regs.R8 = a5
	regs.R9 = a6

	if err := unix.PtraceSetRegs(p.pid, &regs); err != nil {
		return 0, fmt.Errorf("SETREGS: %w", err)
	}
	if err := unix.PtraceSingleStep(p.pid); err != nil {
		return 0, fmt.Errorf("SINGLESTEP: %w", err)
	}

	var ws unix.WaitStatus
	if _, err := unix.Wait4(p.pid, &ws, 0, nil); err != nil {
		return 0, fmt.Errorf("waitpid: %w", err)
	}
	if !ws.Stopped() || ws.StopSignal() != unix.SIGTRAP {
		return 0, fmt.Errorf("unexpected stop: status=0x%x", uint32(ws))
	}

	if err := unix.PtraceGetRegs(p.pid, &regs); err != nil {
		return 0, fmt.Errorf("GETREGS after step: %w", err)
	}
	return regs.Rax, nil
}

// writeToTarget writes data to the target process's memory via PTRACE_POKEDATA.
// Translated from main.c write_to_target() (lines 631-652).
func (p *ptraceInjector) writeToTarget(addr uint64, data []byte) error {
	wordSize := 8 // x86_64
	i := 0

	// Write full words
	for i+wordSize <= len(data) {
		if _, err := unix.PtracePokeData(p.pid, uintptr(addr+uint64(i)), data[i:i+wordSize]); err != nil {
			return fmt.Errorf("POKEDATA at 0x%x: %w", addr+uint64(i), err)
		}
		i += wordSize
	}

	// Handle trailing bytes (partial word)
	if i < len(data) {
		existing := make([]byte, wordSize)
		if _, err := unix.PtracePeekData(p.pid, uintptr(addr+uint64(i)), existing); err != nil {
			return fmt.Errorf("PEEKDATA at 0x%x: %w", addr+uint64(i), err)
		}
		copy(existing, data[i:])
		if _, err := unix.PtracePokeData(p.pid, uintptr(addr+uint64(i)), existing); err != nil {
			return fmt.Errorf("POKEDATA trailing at 0x%x: %w", addr+uint64(i), err)
		}
	}

	return nil
}

// setupUffdWP sets up userfaultfd write-protection on the target process via ptrace injection.
// Returns the tracker-side uffd fd, the target-side uffd fd number, and the list of registered VMAs.
// Translated from main.c setup_userfaultfd_wp_for_process() (lines 666-901).
//
// The caller MUST call runtime.LockOSThread() before calling this function.
func setupUffdWP(pid int, vmas []VMAInfo) (trackerFd int, targetFd int64, registeredVMAs []AddrRange, err error) {
	inj := &ptraceInjector{pid: pid}
	trackerFd = -1
	targetFd = -1

	// 1. Seize and interrupt the target process
	if err = ptraceSeize(pid); err != nil {
		return -1, -1, nil, fmt.Errorf("SEIZE pid=%d: %w", pid, err)
	}

	if err = ptraceInterrupt(pid); err != nil {
		ptraceDetach(pid)
		return -1, -1, nil, fmt.Errorf("INTERRUPT: %w", err)
	}

	var ws unix.WaitStatus
	if _, err = unix.Wait4(pid, &ws, 0, nil); err != nil {
		ptraceDetach(pid)
		return -1, -1, nil, fmt.Errorf("waitpid: %w", err)
	}

	// 2. Save original state
	if err = unix.PtraceGetRegs(pid, &inj.savedRegs); err != nil {
		ptraceDetach(pid)
		return -1, -1, nil, fmt.Errorf("GETREGS: %w", err)
	}

	savedWord := make([]byte, 8)
	if _, err = unix.PtracePeekData(pid, uintptr(inj.savedRegs.Rip), savedWord); err != nil {
		ptraceDetach(pid)
		return -1, -1, nil, fmt.Errorf("PEEKDATA: %w", err)
	}
	copy(inj.savedCode[:], savedWord)

	// 3. Poke 'syscall' instruction (0x0F 0x05) at current RIP
	codeWithSyscall := make([]byte, 8)
	copy(codeWithSyscall, savedWord)
	codeWithSyscall[0] = 0x0F
	codeWithSyscall[1] = 0x05
	if _, err = unix.PtracePokeData(pid, uintptr(inj.savedRegs.Rip), codeWithSyscall); err != nil {
		ptraceDetach(pid)
		return -1, -1, nil, fmt.Errorf("POKEDATA syscall: %w", err)
	}

	// Cleanup function to restore state and detach
	restore := func() {
		unix.PtracePokeData(pid, uintptr(inj.savedRegs.Rip), inj.savedCode[:])
		unix.PtraceSetRegs(pid, &inj.savedRegs)
		ptraceDetach(pid)
	}

	// 4. Inject mmap() to get a scratch page in target's address space
	result, injErr := inj.injectSyscall(sysMmap, 0, pageSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS,
		^uint64(0), // -1 for fd
		0)
	if injErr != nil || int64(result) < 0 {
		restore()
		return -1, -1, nil, fmt.Errorf("inject mmap: result=0x%x err=%v", result, injErr)
	}
	inj.scratch = result

	// Cleanup scratch page on exit
	cleanupMmap := func() {
		inj.injectSyscall(sysMunmap, inj.scratch, pageSize, 0, 0, 0, 0)
		restore()
	}

	// 5. Inject userfaultfd() syscall with UFFD_USER_MODE_ONLY
	result, injErr = inj.injectSyscall(sysUserfaultfd,
		uint64(syscall.O_CLOEXEC|syscall.O_NONBLOCK|uffdUserModeOnly),
		0, 0, 0, 0, 0)
	if injErr != nil || int64(result) < 0 {
		// Fallback without UFFD_USER_MODE_ONLY
		result, injErr = inj.injectSyscall(sysUserfaultfd,
			uint64(syscall.O_CLOEXEC|syscall.O_NONBLOCK),
			0, 0, 0, 0, 0)
		if injErr != nil || int64(result) < 0 {
			cleanupMmap()
			return -1, -1, nil, fmt.Errorf("inject userfaultfd: result=%d err=%v", int64(result), injErr)
		}
	}
	uffd := int64(result)

	// Cleanup uffd on failure
	cleanupUffd := func() {
		inj.injectSyscall(sysClose, uint64(uffd), 0, 0, 0, 0, 0)
		cleanupMmap()
	}

	// 6. UFFDIO_API: enable WP_ASYNC + WP_UNPOPULATED features
	api := uffdioAPIStruct{
		API:      uffdAPI,
		Features: uffdFeatureWPAsync | uffdFeatureWPUnpopulated,
	}
	apiBytes := (*[unsafe.Sizeof(api)]byte)(unsafe.Pointer(&api))[:]
	if err = inj.writeToTarget(inj.scratch, apiBytes); err != nil {
		cleanupUffd()
		return -1, -1, nil, fmt.Errorf("write uffdio_api: %w", err)
	}
	result, injErr = inj.injectSyscall(sysIoctl, uint64(uffd), uffdioAPIIoctl, inj.scratch, 0, 0, 0)
	if injErr != nil || int64(result) < 0 {
		cleanupUffd()
		return -1, -1, nil, fmt.Errorf("UFFDIO_API: result=%d err=%v", int64(result), injErr)
	}

	// 7. Register each writable anonymous VMA with UFFDIO_REGISTER_MODE_WP
	var registered int
	for i := range vmas {
		if !vmas[i].IsWritableAnonymous() {
			continue
		}

		reg := uffdioRegisterStruct{
			Range: uffdioRange{
				Start: vmas[i].Start,
				Len:   vmas[i].End - vmas[i].Start,
			},
			Mode: uffdioRegisterModeWP,
		}
		regBytes := (*[unsafe.Sizeof(reg)]byte)(unsafe.Pointer(&reg))[:]
		if writeErr := inj.writeToTarget(inj.scratch, regBytes); writeErr != nil {
			continue
		}

		result, injErr = inj.injectSyscall(sysIoctl, uint64(uffd), uffdioRegisterIoctl, inj.scratch, 0, 0, 0)
		if injErr != nil || int64(result) < 0 {
			continue
		}
		registered++
		registeredVMAs = append(registeredVMAs, AddrRange{Start: vmas[i].Start, End: vmas[i].End})
	}

	if registered == 0 {
		cleanupUffd()
		return -1, -1, nil, fmt.Errorf("no VMAs registered for WP (pid=%d)", pid)
	}

	targetFd = uffd

	// 8. Copy uffd fd to tracker via pidfd_getfd
	pidfd, pidfdErr := unix.PidfdOpen(pid, 0)
	if pidfdErr != nil {
		cleanupUffd()
		return -1, -1, nil, fmt.Errorf("pidfd_open: %w", pidfdErr)
	}
	defer unix.Close(pidfd)

	tfd, getfdErr := unix.PidfdGetfd(pidfd, int(uffd), 0)
	if getfdErr != nil {
		cleanupUffd()
		return -1, -1, nil, fmt.Errorf("pidfd_getfd: %w", getfdErr)
	}
	trackerFd = tfd

	// Success: cleanup scratch page and restore, but keep uffd open in target
	inj.injectSyscall(sysMunmap, inj.scratch, pageSize, 0, 0, 0, 0)
	restore()

	return trackerFd, targetFd, registeredVMAs, nil
}

// uffdioUnregister unregisters a VMA range from uffd using the tracker's uffd fd.
// This is done via ioctl on the tracker-side fd — no ptrace needed.
func uffdioUnregister(trackerFd int, start, end uint64) error {
	r := uffdioRange{
		Start: start,
		Len:   end - start,
	}
	_, _, errno := syscall.Syscall(unix.SYS_IOCTL,
		uintptr(trackerFd),
		uintptr(uffdioUnregisterIoctl),
		uintptr(unsafe.Pointer(&r)))
	if errno != 0 {
		return fmt.Errorf("UFFDIO_UNREGISTER 0x%x-0x%x: %w", start, end, errno)
	}
	return nil
}

// uffdioRegister registers a VMA range with uffd in WP mode using the tracker's uffd fd.
// This is done via ioctl on the tracker-side fd — no ptrace needed.
func uffdioRegister(trackerFd int, start, end uint64) error {
	reg := uffdioRegisterStruct{
		Range: uffdioRange{
			Start: start,
			Len:   end - start,
		},
		Mode: uffdioRegisterModeWP,
	}
	_, _, errno := syscall.Syscall(unix.SYS_IOCTL,
		uintptr(trackerFd),
		uintptr(uffdioRegisterIoctl),
		uintptr(unsafe.Pointer(&reg)))
	if errno != 0 {
		return fmt.Errorf("UFFDIO_REGISTER 0x%x-0x%x: %w", start, end, errno)
	}
	return nil
}

// ptraceSeize attaches to a process with PTRACE_SEIZE (no-stop attach).
func ptraceSeize(pid int) error {
	_, _, errno := syscall.RawSyscall6(syscall.SYS_PTRACE,
		uintptr(unix.PTRACE_SEIZE), uintptr(pid), 0, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// ptraceInterrupt sends PTRACE_INTERRUPT to a seized process.
func ptraceInterrupt(pid int) error {
	_, _, errno := syscall.RawSyscall6(syscall.SYS_PTRACE,
		uintptr(unix.PTRACE_INTERRUPT), uintptr(pid), 0, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// ptraceDetach detaches from a ptraced process.
func ptraceDetach(pid int) error {
	return unix.PtraceDetach(pid)
}

// lockOSThread is a helper that locks the current goroutine to its OS thread.
// Must be called before any ptrace operations.
func lockOSThread() {
	runtime.LockOSThread()
}

// closeTargetUffd closes the userfaultfd file descriptor inside the target
// process via ptrace syscall injection. This is required before CRIU dump
// because CRIU cannot checkpoint userfaultfd descriptors.
func closeTargetUffd(pid int, targetFd int64) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Seize and interrupt
	if err := ptraceSeize(pid); err != nil {
		return fmt.Errorf("ptrace seize: %w", err)
	}
	defer ptraceDetach(pid)

	if err := ptraceInterrupt(pid); err != nil {
		return fmt.Errorf("ptrace interrupt: %w", err)
	}

	var ws syscall.WaitStatus
	if _, err := syscall.Wait4(pid, &ws, 0, nil); err != nil {
		return fmt.Errorf("wait4: %w", err)
	}

	// Save registers and instruction
	var regs unix.PtraceRegs
	if err := unix.PtraceGetRegs(pid, &regs); err != nil {
		return fmt.Errorf("getregs: %w", err)
	}

	var savedCode [8]byte
	n, err := unix.PtracePeekText(pid, uintptr(regs.Rip), savedCode[:])
	if err != nil || n != 8 {
		return fmt.Errorf("peektext: %w", err)
	}

	// Poke syscall instruction (0x0F 0x05)
	var code [8]byte
	copy(code[:], savedCode[:])
	code[0] = 0x0F
	code[1] = 0x05
	if _, err := unix.PtracePokeText(pid, uintptr(regs.Rip), code[:]); err != nil {
		return fmt.Errorf("poketext: %w", err)
	}

	// Inject close(targetFd) syscall
	inj := &ptraceInjector{pid: pid, savedRegs: regs}
	copy(inj.savedCode[:], savedCode[:])
	result, err := inj.injectSyscall(uint64(unix.SYS_CLOSE), uint64(targetFd), 0, 0, 0, 0, 0)
	if err != nil {
		// Restore original code before returning
		unix.PtracePokeText(pid, uintptr(regs.Rip), savedCode[:])
		unix.PtraceSetRegs(pid, &regs)
		return fmt.Errorf("inject close: %w", err)
	}
	if int64(result) < 0 {
		unix.PtracePokeText(pid, uintptr(regs.Rip), savedCode[:])
		unix.PtraceSetRegs(pid, &regs)
		return fmt.Errorf("close(%d) returned %d", targetFd, int64(result))
	}

	// Restore original instruction and registers
	unix.PtracePokeText(pid, uintptr(regs.Rip), savedCode[:])
	unix.PtraceSetRegs(pid, &regs)

	return nil
}

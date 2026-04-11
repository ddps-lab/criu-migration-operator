package profiler

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// Config holds profiler configuration parameters.
type Config struct {
	IntervalMs     int     // scan interval in milliseconds (default: 1000)
	HotThreshold   float64 // written ratio threshold for hot classification (default: 0.3)
	HotConsecutive int     // consecutive hot intervals to mark VMA as hot (default: 3)
}

// DefaultConfig returns the default profiler configuration.
func DefaultConfig() Config {
	return Config{
		IntervalMs:     5000, // 5-second scan interval (paper default)
		HotThreshold:   0.3,  // θ = 0.3 dirty ratio threshold
		HotConsecutive: 3,    // N = 3 consecutive intervals above θ
	}
}

// Profiler tracks memory write patterns of a target process using uffd-wp async.
// It identifies hot VMAs (frequently written regions) and tracks dirty volume.
type Profiler struct {
	pid    int
	config Config

	// uffd state
	trackerUffdFd int   // uffd fd in this process (via pidfd_getfd)
	targetUffdFd  int64 // uffd fd number in target process
	pagemapFile   *os.File
	registeredVMAs []AddrRange

	// heat classification
	heat *heatClassifier

	// thread-safe results
	mu          sync.RWMutex
	hotRegions  []HotRegion
	dirtyVolume DirtyVolume
	totalVMAs   int
	hotVMACount int

	// cumulative tracking
	sampleCount          int64
	cumulativeDirtyBytes int64
	startTime            time.Time

	// lifecycle
	stopCh chan struct{}
	doneCh chan struct{}
}

// New creates a new Profiler for the given process.
func New(pid int, config Config) *Profiler {
	if config.IntervalMs <= 0 {
		config.IntervalMs = 1000
	}
	if config.HotThreshold <= 0 {
		config.HotThreshold = 0.3
	}
	if config.HotConsecutive <= 0 {
		config.HotConsecutive = 3
	}
	return &Profiler{
		pid:           pid,
		config:        config,
		trackerUffdFd: -1,
		targetUffdFd:  -1,
		heat:          newHeatClassifier(config.HotThreshold, config.HotConsecutive),
	}
}

// Start initializes uffd-wp via ptrace injection and begins the profiling loop.
func (p *Profiler) Start() error {
	// Parse VMAs
	vmas, err := parseVMAs(p.pid)
	if err != nil {
		return fmt.Errorf("parse VMAs: %w", err)
	}

	// Open pagemap for scanning
	p.pagemapFile, err = openPagemap(p.pid)
	if err != nil {
		return fmt.Errorf("open pagemap: %w", err)
	}

	// Setup uffd-wp via ptrace injection (needs locked OS thread)
	var trackerFd int
	var targetFd int64
	var registered []AddrRange

	setupDone := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		var setupErr error
		trackerFd, targetFd, registered, setupErr = setupUffdWP(p.pid, vmas)
		setupDone <- setupErr
	}()

	if err = <-setupDone; err != nil {
		p.pagemapFile.Close()
		return fmt.Errorf("setup uffd-wp: %w", err)
	}

	p.trackerUffdFd = trackerFd
	p.targetUffdFd = targetFd
	p.registeredVMAs = registered

	log.Printf("profiler: uffd-wp setup complete (pid=%d, registered=%d VMAs, trackerFd=%d)",
		p.pid, len(registered), trackerFd)

	// Verify WP is active
	wpActive, err := verifyWPActive(int(p.pagemapFile.Fd()))
	if err != nil || !wpActive {
		p.Close()
		return fmt.Errorf("WP not active after setup (pid=%d): %v", p.pid, err)
	}

	// WP all writable pages for baseline
	writableVMAs := writableAnonymousVMAs(vmas)
	if err = wpAllWritablePages(int(p.pagemapFile.Fd()), writableVMAs); err != nil {
		p.Close()
		return fmt.Errorf("WP baseline: %w", err)
	}

	p.startTime = time.Now()
	p.stopCh = make(chan struct{})
	p.doneCh = make(chan struct{})

	go p.loop()

	log.Printf("profiler: started (pid=%d, interval=%dms, threshold=%.2f, consecutive=%d)",
		p.pid, p.config.IntervalMs, p.config.HotThreshold, p.config.HotConsecutive)

	return nil
}

// loop is the main profiling goroutine.
func (p *Profiler) loop() {
	defer close(p.doneCh)

	interval := time.Duration(p.config.IntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.collectSample()
		}
	}
}

// collectSample performs one profiling cycle: re-parse VMAs, register new ones, scan dirty pages, update heat.
func (p *Profiler) collectSample() {
	pagemapFd := int(p.pagemapFile.Fd())

	// Re-parse VMAs (may have changed due to mmap/munmap/brk)
	vmas, err := parseVMAs(p.pid)
	if err != nil {
		log.Printf("profiler: parse VMAs failed: %v", err)
		return
	}

	// Register any new VMAs that appeared since last scan
	newVMAs := diffVMAs(vmas, p.registeredVMAs)
	for i := range newVMAs {
		if err := uffdioRegister(p.trackerUffdFd, newVMAs[i].Start, newVMAs[i].End); err != nil {
			continue
		}
		p.registeredVMAs = append(p.registeredVMAs, AddrRange{
			Start: newVMAs[i].Start,
			End:   newVMAs[i].End,
		})
		// WP new VMA pages for baseline
		wpAllWritablePages(pagemapFd, []VMAInfo{newVMAs[i]})
	}

	// Scan dirty pages
	writableVMAs := writableAnonymousVMAs(vmas)
	results, totalDirty, err := scanDirtyPages(pagemapFd, writableVMAs)
	if err != nil {
		log.Printf("profiler: scan dirty pages failed: %v", err)
		return
	}

	// Update heat classifier
	hotRegions := p.heat.update(results)

	// Calculate dirty volume
	dirtyBytes := totalDirty * pageSize
	p.sampleCount++
	p.cumulativeDirtyBytes += dirtyBytes
	elapsed := time.Since(p.startTime).Seconds()
	intervalSec := float64(p.config.IntervalMs) / 1000.0

	var avgDirtyRate float64
	if elapsed > 0 {
		avgDirtyRate = float64(p.cumulativeDirtyBytes) / elapsed
	}

	// Update thread-safe results
	p.mu.Lock()
	p.hotRegions = hotRegions
	p.dirtyVolume = DirtyVolume{
		DirtyPages:           totalDirty,
		DirtyBytes:           dirtyBytes,
		DirtyRatePerSec:      float64(dirtyBytes) / intervalSec,
		CumulativeDirtyBytes: p.cumulativeDirtyBytes,
		AvgDirtyRate:         avgDirtyRate,
		TimestampMs:          time.Now().UnixMilli(),
	}
	p.totalVMAs = p.heat.totalVMAs()
	p.hotVMACount = p.heat.hotVMAs()
	p.mu.Unlock()
}

// Stop stops the profiling loop. Does not close the uffd fd.
func (p *Profiler) Stop() {
	if p.stopCh != nil {
		select {
		case <-p.stopCh:
			// Already stopped
		default:
			close(p.stopCh)
			<-p.doneCh
		}
	}
}

// Close releases all resources. Call Stop() first if the loop is running.
func (p *Profiler) Close() {
	p.Stop()
	if p.pagemapFile != nil {
		p.pagemapFile.Close()
		p.pagemapFile = nil
	}
	if p.trackerUffdFd >= 0 {
		_ = syscallClose(p.trackerUffdFd)
		p.trackerUffdFd = -1
	}
}

// CleanupBeforeCRIU stops the profiling loop and unregisters all VMAs from uffd.
// The uffd fd is kept open so ReinitAfterCRIU can re-register without ptrace.
//
// Multi-process safety: uffd-wp registrations are per-address-space in the kernel.
// All threads within the same process share the address space and thus the same
// uffd registrations. Unregistering from the main PID covers all threads.
// Child processes created via fork() do NOT inherit uffd registrations (the uffd fd
// is inherited but registrations are per-vm_area_struct). Therefore, cleanup of the
// main profiled PID is sufficient for the expected single-process container case.
//
// The uffd fd itself remains open in the target process. CRIU can checkpoint/restore
// it because we use O_CLOEXEC. After CRIU restore, ReinitAfterCRIU re-registers VMAs
// using the tracker-side fd copy (obtained via pidfd_getfd during setup).
func (p *Profiler) CleanupBeforeCRIU() error {
	p.Stop()

	// Unregister all VMAs from uffd
	for _, vma := range p.registeredVMAs {
		if err := uffdioUnregister(p.trackerUffdFd, vma.Start, vma.End); err != nil {
			log.Printf("profiler: unregister VMA 0x%x-0x%x: %v", vma.Start, vma.End, err)
		}
	}
	p.registeredVMAs = nil
	p.heat.reset()

	// Close uffd fd in target process via ptrace injection.
	// CRIU cannot dump userfaultfd file descriptors, so this fd must be
	// removed before dump. After restore, setupUffdWP must be called again.
	if p.targetUffdFd >= 0 {
		if err := closeTargetUffd(p.pid, p.targetUffdFd); err != nil {
			log.Printf("profiler: failed to close target uffd fd %d: %v", p.targetUffdFd, err)
		} else {
			log.Printf("profiler: closed target uffd fd %d in pid %d", p.targetUffdFd, p.pid)
		}
		p.targetUffdFd = -1
	}

	// Close tracker-side uffd fd
	if p.trackerUffdFd >= 0 {
		syscall.Close(p.trackerUffdFd)
		p.trackerUffdFd = -1
	}

	log.Printf("profiler: cleanup before CRIU complete (pid=%d)", p.pid)
	return nil
}

// ReinitAfterCRIU re-creates uffd via ptrace injection and restarts profiling.
// CleanupBeforeCRIU closes both tracker and target uffd fds, so a full
// re-setup is needed (same as initial Start, but reuses existing Profiler).
func (p *Profiler) ReinitAfterCRIU() error {
	vmas, err := parseVMAs(p.pid)
	if err != nil {
		return fmt.Errorf("parse VMAs: %w", err)
	}

	writable := writableAnonymousVMAs(vmas)

	// Full re-setup: ptrace inject new uffd into target
	trackerFd, targetFd, registered, err := setupUffdWP(p.pid, writable)
	if err != nil {
		return fmt.Errorf("reinit uffd-wp setup: %w", err)
	}
	p.trackerUffdFd = trackerFd
	p.targetUffdFd = targetFd
	p.registeredVMAs = registered

	// Reopen pagemap
	if p.pagemapFile != nil {
		p.pagemapFile.Close()
	}
	pmPath := fmt.Sprintf("/proc/%d/pagemap", p.pid)
	p.pagemapFile, err = os.Open(pmPath)
	if err != nil {
		return fmt.Errorf("reopen pagemap: %w", err)
	}

	// WP all pages for fresh baseline
	if err = wpAllWritablePages(int(p.pagemapFile.Fd()), writable); err != nil {
		return fmt.Errorf("WP baseline after reinit: %w", err)
	}

	// Reset counters and cached dirty volume
	p.cumulativeDirtyBytes = 0
	p.sampleCount = 0
	p.startTime = time.Now()
	p.mu.Lock()
	p.dirtyVolume = DirtyVolume{}
	p.mu.Unlock()

	// Restart loop
	p.stopCh = make(chan struct{})
	p.doneCh = make(chan struct{})
	go p.loop()

	log.Printf("profiler: reinit after CRIU complete (pid=%d, registered=%d VMAs)",
		p.pid, len(p.registeredVMAs))
	return nil
}

// GetHotRegions returns a snapshot of current hot regions.
func (p *Profiler) GetHotRegions() []HotRegion {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]HotRegion, len(p.hotRegions))
	copy(result, p.hotRegions)
	return result
}

// GetVMADetails returns all tracked VMAs with hot/cold classification.
func (p *Profiler) GetVMADetails() []VMAHotDetail {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.heat == nil {
		return nil
	}
	return p.heat.getAllVMAs()
}

// GetDirtyVolume returns a snapshot of current dirty volume statistics.
func (p *Profiler) GetDirtyVolume() DirtyVolume {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dirtyVolume
}

// GetVMACounts returns total and hot VMA counts.
func (p *Profiler) GetVMACounts() (total, hot int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalVMAs, p.hotVMACount
}

// syscallClose closes a file descriptor via syscall.
func syscallClose(fd int) error {
	return os.NewFile(uintptr(fd), "").Close()
}

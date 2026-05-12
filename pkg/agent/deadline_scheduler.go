package agent

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/ddps-lab/criu-migration-operator/pkg/profiler"
)

const pageSize = 4096

// InvariantSchedulerConfig holds configuration for the dirty volume invariant scheduler.
type InvariantSchedulerConfig struct {
	DeadlineSeconds int     // Cloud provider termination deadline (e.g., 120 for AWS, 30 for Azure)
	BandwidthMBps   float64 // Upload bandwidth in MB/s (0 = auto-detect)
	ScanIntervalMs  int     // Invariant evaluation interval in milliseconds (default 5000)
	TFreezeMs       int     // Estimated process freeze + final dump time in ms (default 50)
	TMarginMs       int     // Safety margin in ms (default 5000)
	DryRun          bool    // Log decisions without triggering actual pre-dumps
}

// InvariantState holds the current invariant evaluation state.
type InvariantState struct {
	TRemainMs       float64 // Estimated time to complete final dump + upload (ms)
	DeadlineMs      float64 // Deadline in ms
	DCurrentPages   int64   // Dirty pages since last pre-dump
	DCurrentBytes   int64   // Dirty bytes since last pre-dump
	BUploadBytesMs  float64 // Upload bandwidth (bytes/ms)
	Violated        bool    // T_remain >= Deadline
}

// FeasibilityScore holds the F_op evaluation (read-only, for warnings).
type FeasibilityScore struct {
	Fop          float64 // Operational feasibility score (≥1 = feasible)
	Alpha        float64 // Re-dirty ratio (dirty_rate / bandwidth)
	DMinBytes    int64   // Minimum data to transfer at steady state
	DHotBytes    int64   // Hot VMA data size
	DColdSSBytes int64   // Steady-state cold residual
	Warning      string  // Non-empty if F_op < 1
}

// InvariantScheduler maintains the dirty volume invariant:
//
//	T_remain = T_freeze + (D_current × P_size) / B_upload + T_margin
//
// When T_remain >= Deadline, a pre-dump is triggered to reduce D_current.
// F_op is computed separately as a read-only feasibility score for warnings.
type InvariantScheduler struct {
	config   InvariantSchedulerConfig
	profiler *profiler.Profiler
	agent    *Agent

	// State
	initialDumpDone bool
	lastPreDumpTime time.Time
	preDumpCount    int
	lastFopCheck    time.Time
	lastFop         FeasibilityScore
	stopCh          chan struct{}
}

// NewInvariantScheduler creates a new invariant-based scheduler.
func NewInvariantScheduler(config InvariantSchedulerConfig, prof *profiler.Profiler, agent *Agent) *InvariantScheduler {
	if config.ScanIntervalMs <= 0 {
		config.ScanIntervalMs = 5000
	}
	if config.TFreezeMs <= 0 {
		config.TFreezeMs = 50
	}
	if config.TMarginMs <= 0 {
		config.TMarginMs = 5000
	}
	if config.DeadlineSeconds <= 0 {
		config.DeadlineSeconds = 120
	}
	return &InvariantScheduler{
		config:   config,
		profiler: prof,
		agent:    agent,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the invariant scheduling loop.
// Step 1: Trigger initial pre-dump (full writable memory).
// Step 2: Periodically evaluate T_remain, trigger pre-dump when invariant is violated.
func (s *InvariantScheduler) Start(ctx context.Context) {
	log.Printf("[INVARIANT-SCHEDULER] Started (deadline=%ds, bandwidth=%.0fMB/s, scan=%dms, T_freeze=%dms, T_margin=%dms)",
		s.config.DeadlineSeconds, s.config.BandwidthMBps,
		s.config.ScanIntervalMs, s.config.TFreezeMs, s.config.TMarginMs)

	// Step 1: Initial pre-dump (transfer entire writable memory to object storage)
	if !s.config.DryRun {
		s.performInitialPreDump(ctx)
	} else {
		log.Printf("[INVARIANT-SCHEDULER] DryRun: skipping initial pre-dump")
		s.initialDumpDone = true
	}

	// Step 2: Periodic invariant evaluation
	interval := time.Duration(s.config.ScanIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[INVARIANT-SCHEDULER] Stopped (context done)")
			return
		case <-s.stopCh:
			log.Printf("[INVARIANT-SCHEDULER] Stopped (explicit stop)")
			return
		case <-ticker.C:
			if !s.initialDumpDone {
				continue
			}

			// Evaluate invariant
			state := s.evaluateInvariant()
			if state.Violated {
				log.Printf("[INVARIANT-SCHEDULER] INVARIANT VIOLATED: T_remain=%.1fms >= Deadline=%.0fms (D_current=%d pages, %.1f MB)",
					state.TRemainMs, state.DeadlineMs,
					state.DCurrentPages, float64(state.DCurrentBytes)/1024/1024)
				if !s.config.DryRun {
					s.triggerPreDump(ctx)
				}
			}

			// Evaluate F_op periodically (every 60 seconds) for warnings
			if time.Since(s.lastFopCheck) >= 60*time.Second {
				fop := s.evaluateFeasibility()
				s.lastFop = fop
				s.lastFopCheck = time.Now()
				if fop.Warning != "" {
					log.Printf("[INVARIANT-SCHEDULER] WARNING: %s", fop.Warning)
				}
			}
		}
	}
}

// Stop stops the scheduler.
func (s *InvariantScheduler) Stop() {
	select {
	case s.stopCh <- struct{}{}:
	default:
	}
}

// evaluateInvariant computes T_remain and checks the dirty volume invariant.
//
//	T_remain = T_freeze + (D_current × P_size) / B_upload + T_margin
//	Invariant: T_remain < Deadline
func (s *InvariantScheduler) evaluateInvariant() InvariantState {
	if s.profiler == nil {
		return InvariantState{}
	}

	dv := s.profiler.GetDirtyVolume()

	deadlineMs := float64(s.config.DeadlineSeconds) * 1000
	bUpload := s.config.BandwidthMBps * 1024 * 1024 / 1000 // bytes/ms
	if bUpload <= 0 {
		bUpload = 100 * 1024 * 1024 / 1000 // fallback 100 MB/s
	}

	// D_current: dirty pages since last pre-dump
	// profiler resets CumulativeDirtyBytes on ReinitAfterCRIU,
	// but DirtyPages is the last-scan snapshot. Use CumulativeDirtyBytes for total since pre-dump.
	dCurrentBytes := dv.CumulativeDirtyBytes
	dCurrentPages := dCurrentBytes / pageSize

	// T_remain = T_freeze + (D_current * P_size) / B_upload + T_margin
	// Note: D_current is already in bytes (CumulativeDirtyBytes), no need for × P_size
	tRemainMs := float64(s.config.TFreezeMs) + float64(dCurrentBytes)/bUpload + float64(s.config.TMarginMs)

	state := InvariantState{
		TRemainMs:      tRemainMs,
		DeadlineMs:     deadlineMs,
		DCurrentPages:  dCurrentPages,
		DCurrentBytes:  dCurrentBytes,
		BUploadBytesMs: bUpload,
		Violated:       tRemainMs >= deadlineMs,
	}

	log.Printf("[INVARIANT-SCHEDULER] T_remain=%.1fms (D_current=%d pages, %.1fMB, rate=%.0f B/s) deadline=%.0fms violated=%v",
		tRemainMs, dCurrentPages, float64(dCurrentBytes)/1024/1024,
		dv.DirtyRatePerSec, deadlineMs, state.Violated)

	return state
}

// evaluateFeasibility computes the F_op score (Eq.6 from paper).
// This is read-only — used for warnings, not for triggering pre-dumps.
//
//	F_op = (Deadline - T_freeze - T_margin) / (D_min / B_eff)
//	D_min = D_hot + D_cold_ss
//	D_cold_ss = R_cold × I × α/(1-α)
//	α = R_cold × P_size / B_eff  (simplified: dirty_rate_bytes / B_eff)
func (s *InvariantScheduler) evaluateFeasibility() FeasibilityScore {
	if s.profiler == nil {
		return FeasibilityScore{Fop: 999, Warning: ""}
	}

	dv := s.profiler.GetDirtyVolume()
	hotRegions := s.profiler.GetHotRegions()

	bEff := s.config.BandwidthMBps * 1024 * 1024 / 1000 // bytes/ms
	if bEff <= 0 {
		return FeasibilityScore{Fop: 999}
	}

	availableMs := float64(s.config.DeadlineSeconds)*1000 - float64(s.config.TFreezeMs) - float64(s.config.TMarginMs)
	if availableMs <= 0 {
		return FeasibilityScore{Fop: 0, Warning: "no available time (deadline <= T_freeze + T_margin)"}
	}

	// D_hot: sum of hot VMA sizes
	var dHotBytes int64
	for _, hr := range hotRegions {
		dHotBytes += int64(hr.EndAddr - hr.StartAddr)
	}

	// Dirty rate in bytes/ms (DirtyRatePerSec is bytes/sec from profiler)
	dirtyRateBytesMs := dv.DirtyRatePerSec / 1000

	// α = dirty_rate / bandwidth
	alpha := dirtyRateBytesMs / bEff
	if alpha >= 1 {
		return FeasibilityScore{
			Fop:       0,
			Alpha:     alpha,
			DHotBytes: dHotBytes,
			Warning:   fmt.Sprintf("F_op=0: alpha=%.2f >= 1 (dirty rate exceeds bandwidth)", alpha),
		}
	}

	// Scan interval for steady-state calculation
	intervalMs := float64(s.config.ScanIntervalMs)

	// D_cold_ss = R_cold × I × α/(1-α)
	dColdSS := dirtyRateBytesMs * intervalMs * alpha / (1 - alpha)
	if dColdSS < 0 {
		dColdSS = 0
	}

	// D_min = D_hot + D_cold_ss
	dMinBytes := dHotBytes + int64(math.Ceil(dColdSS))

	// F_op = available / (D_min / B_eff)
	var fop float64
	if dMinBytes > 0 {
		tRequiredMs := float64(dMinBytes) / bEff
		fop = availableMs / tRequiredMs
	} else {
		fop = 999
	}

	var warning string
	if fop < 1.0 {
		warning = fmt.Sprintf("F_op=%.2f < 1: migration not guaranteed (alpha=%.3f, D_min=%.1fMB, D_hot=%.1fMB)",
			fop, alpha, float64(dMinBytes)/1024/1024, float64(dHotBytes)/1024/1024)
	}

	return FeasibilityScore{
		Fop:          fop,
		Alpha:        alpha,
		DMinBytes:    dMinBytes,
		DHotBytes:    dHotBytes,
		DColdSSBytes: int64(math.Ceil(dColdSS)),
		Warning:      warning,
	}
}

// GetState returns the current invariant state and feasibility score.
func (s *InvariantScheduler) GetState() (InvariantState, FeasibilityScore) {
	state := s.evaluateInvariant()
	return state, s.lastFop
}

// performInitialPreDump triggers the first pre-dump to transfer entire writable memory.
func (s *InvariantScheduler) performInitialPreDump(ctx context.Context) {
	log.Printf("[INVARIANT-SCHEDULER] Performing initial pre-dump (full writable memory)")

	pid := s.agent.mainPID
	if pid <= 0 {
		log.Printf("[INVARIANT-SCHEDULER] No main PID, deferring initial pre-dump")
		s.initialDumpDone = true // Don't block the loop
		return
	}

	// Initial pre-dump: no profiler running yet, no exclude args needed
	result, err := s.agent.checkpointMgr.PreCheckpoint(ctx, pid, "", nil)
	if err != nil {
		log.Printf("[INVARIANT-SCHEDULER] Initial pre-dump failed: %v", err)
		s.initialDumpDone = true
		return
	}

	s.initialDumpDone = true
	s.lastPreDumpTime = time.Now()
	s.preDumpCount++

	log.Printf("[INVARIANT-SCHEDULER] Initial pre-dump completed: %s (size=%d bytes)",
		result.DumpID, result.SizeBytes)

	// CRIU's pre-dump exits with --leave-running, but the kernel needs a
	// brief moment to fully unfreeze the cgroup and let the task reach
	// TASK_RUNNING. Without this wait the next ptrace_attach issued by
	// the profiler's setupUffdWP frequently races and observes "no such
	// process" or "operation not permitted". Polling /proc/<pid>/status
	// for TracerPid: 0 is the cheap correct check.
	if s.agent.mainPID > 0 {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if tpid, err := readTracerPid(s.agent.mainPID); err == nil && tpid == 0 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Start profiler AFTER initial pre-dump — D_current starts fresh from 0
	if s.profiler == nil && s.agent.mainPID > 0 {
		p := profiler.New(s.agent.mainPID, profiler.DefaultConfig())
		if err := p.Start(); err != nil {
			log.Printf("[INVARIANT-SCHEDULER] Failed to start profiler after initial dump: %v", err)
		} else {
			s.profiler = p
			s.agent.profilerInst = p
			totalVMAs, hotVMAs := p.GetVMACounts()
			log.Printf("[INVARIANT-SCHEDULER] Profiler started after initial dump (pid=%d, vmas=%d, hot=%d)",
				s.agent.mainPID, totalVMAs, hotVMAs)
		}
	}
}

// triggerPreDump triggers an incremental pre-dump to reduce D_current.
func (s *InvariantScheduler) triggerPreDump(ctx context.Context) {
	if s.agent == nil {
		return
	}

	pid := s.agent.mainPID
	if pid <= 0 {
		log.Printf("[INVARIANT-SCHEDULER] No main PID, skipping pre-dump")
		return
	}

	parentID := ""
	if s.agent.checkpointMgr != nil {
		parentID = s.agent.checkpointMgr.lastCheckpointID
	}

	// Build exclude args from hot regions and cleanup profiler BEFORE CRIU dump
	var excludeArgs *CRIUExcludeArgs
	if s.profiler != nil {
		hotRegions := s.profiler.GetHotRegions()
		if len(hotRegions) > 0 {
			excludeArgs = &CRIUExcludeArgs{}
			for _, hr := range hotRegions {
				excludeArgs.ExcludeRanges = append(excludeArgs.ExcludeRanges, profiler.AddrRange{
					Start: hr.StartAddr,
					End:   hr.EndAddr,
				})
			}
		}
		// Must cleanup uffd before CRIU dump (same as server.go gRPC handler)
		s.profiler.CleanupBeforeCRIU()
	}

	result, err := s.agent.checkpointMgr.PreCheckpoint(ctx, pid, parentID, excludeArgs)
	if err != nil {
		log.Printf("[INVARIANT-SCHEDULER] Pre-dump failed: %v", err)
		return
	}

	s.lastPreDumpTime = time.Now()
	s.preDumpCount++

	// Reinit profiler (resets CumulativeDirtyBytes → D_current back to 0)
	if s.profiler != nil {
		if err := s.profiler.ReinitAfterCRIU(); err != nil {
			log.Printf("[INVARIANT-SCHEDULER] Profiler reinit failed: %v", err)
		}
	}

	log.Printf("[INVARIANT-SCHEDULER] Pre-dump #%d completed: %s (size=%d bytes)",
		s.preDumpCount, result.DumpID, result.SizeBytes)
}

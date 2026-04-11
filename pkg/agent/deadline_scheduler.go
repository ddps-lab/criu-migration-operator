package agent

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/ddps-lab/criu-migration-operator/pkg/profiler"
)

// DeadlineSchedulerConfig holds configuration for deadline-driven checkpoint scheduling.
type DeadlineSchedulerConfig struct {
	Enabled         bool
	DryRun          bool
	DeadlineSeconds int     // Cloud provider termination deadline (e.g., 120 for AWS)
	BandwidthMBps   float64 // Estimated upload bandwidth in MB/s
	ScanIntervalMs  int     // Evaluation interval in milliseconds
	TFreezeMs       int     // Estimated process freeze time in ms
	TMarginMs       int     // Safety margin in ms
}

// DeadlineScheduler evaluates whether pre-dumps should be triggered
// based on the paper's F_op (Operational Feasibility Score) model.
//
// F_op = Available / T_required
// Where:
//   Available = Deadline - T_freeze - T_margin
//   T_required = D_min / B_eff
//   D_min = D_hot + D_cold_ss
//   D_cold_ss = R_cold × I × α/(1-α)  (steady-state residual via geometric series)
//   α = R_cold × P_size / B_eff
type DeadlineScheduler struct {
	config   DeadlineSchedulerConfig
	profiler *profiler.Profiler
	agent    *Agent

	// State
	lastPreDumpTime time.Time
	preDumpCount    int
	stopCh          chan struct{}
}

// NewDeadlineScheduler creates a new deadline scheduler.
func NewDeadlineScheduler(config DeadlineSchedulerConfig, prof *profiler.Profiler, agent *Agent) *DeadlineScheduler {
	return &DeadlineScheduler{
		config:   config,
		profiler: prof,
		agent:    agent,
		stopCh:   make(chan struct{}),
	}
}

// FeasibilityResult holds the result of a feasibility evaluation.
type FeasibilityResult struct {
	Fop            float64 // Operational feasibility score (≥1 = feasible)
	AvailableMs    float64 // Available time for migration (ms)
	TRequiredMs    float64 // Estimated required time (ms)
	DMinBytes      int64   // Minimum data to transfer (bytes)
	DHotBytes      int64   // Hot VMA data (bytes)
	DColdSSBytes   int64   // Steady-state cold residual (bytes)
	Alpha          float64 // Re-dirty ratio
	DirtyRateMBps  float64 // Current dirty rate (MB/s)
	ShouldPreDump  bool    // Whether to trigger pre-dump now
	ShouldFullDump bool    // Whether to trigger final dump (deadline approaching)
	Reason         string
}

// Start begins the deadline scheduler evaluation loop.
func (ds *DeadlineScheduler) Start(ctx context.Context) {
	if !ds.config.Enabled {
		return
	}

	interval := time.Duration(ds.config.ScanIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 2 * time.Second
	}

	log.Printf("[DEADLINE-SCHEDULER] Started (deadline=%ds, bandwidth=%.0fMB/s, interval=%dms)",
		ds.config.DeadlineSeconds, ds.config.BandwidthMBps, ds.config.ScanIntervalMs)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[DEADLINE-SCHEDULER] Stopped (context done)")
			return
		case <-ds.stopCh:
			log.Printf("[DEADLINE-SCHEDULER] Stopped (explicit stop)")
			return
		case <-ticker.C:
			result := ds.evaluate()
			if result.ShouldPreDump {
				log.Printf("[DEADLINE-SCHEDULER] Triggering pre-dump: %s (F_op=%.2f, D_min=%.1fMB)",
					result.Reason, result.Fop, float64(result.DMinBytes)/1024/1024)
				if !ds.config.DryRun {
					ds.triggerPreDump(ctx)
				}
			}
			if result.ShouldFullDump {
				log.Printf("[DEADLINE-SCHEDULER] ALERT: Final dump needed! (F_op=%.2f, available=%.0fms, required=%.0fms)",
					result.Fop, result.AvailableMs, result.TRequiredMs)
				// Final dump is triggered by migration flow, not scheduler
				// Scheduler only sets a flag/annotation for controller to pick up
			}
		}
	}
}

// Stop stops the scheduler.
func (ds *DeadlineScheduler) Stop() {
	select {
	case ds.stopCh <- struct{}{}:
	default:
	}
}

// evaluate computes the current feasibility score.
func (ds *DeadlineScheduler) evaluate() FeasibilityResult {
	if ds.profiler == nil {
		return FeasibilityResult{Reason: "no profiler"}
	}

	dv := ds.profiler.GetDirtyVolume()
	hotRegions := ds.profiler.GetHotRegions()

	// Available time
	deadlineMs := float64(ds.config.DeadlineSeconds) * 1000
	availableMs := deadlineMs - float64(ds.config.TFreezeMs) - float64(ds.config.TMarginMs)
	if availableMs <= 0 {
		return FeasibilityResult{
			AvailableMs:    availableMs,
			ShouldFullDump: true,
			Reason:         "no_time_left",
		}
	}

	// Bandwidth in bytes/ms
	bEff := ds.config.BandwidthMBps * 1024 * 1024 / 1000 // bytes per ms

	// Hot VMA size (excluded from pre-dump, must transfer at final dump)
	var dHotBytes int64
	for _, hr := range hotRegions {
		dHotBytes += int64(hr.EndAddr - hr.StartAddr)
	}

	// Cold dirty rate: total dirty rate minus hot contribution
	// Simplified: use total dirty rate as cold approximation
	// (hot pages are excluded from pre-dump, so they re-dirty freely)
	dirtyRateBytesPerMs := dv.DirtyRatePerSec * 4096 / 1000 // pages/s → bytes/ms

	// Total process size estimate (from dirty volume cumulative)
	// Use cumulative as proxy, or a better estimate from process RSS
	pSize := float64(dv.CumulativeDirtyBytes)
	if pSize < 1024*1024 {
		pSize = 100 * 1024 * 1024 // minimum 100MB estimate
	}

	// α = R_cold × P_size / B_eff
	rCold := dirtyRateBytesPerMs / pSize
	if rCold < 0 {
		rCold = 0
	}
	alpha := rCold * pSize / bEff
	if alpha >= 1 {
		// α ≥ 1 means dirty rate exceeds bandwidth — infeasible
		return FeasibilityResult{
			Fop:            0,
			AvailableMs:    availableMs,
			Alpha:          alpha,
			DirtyRateMBps:  dv.DirtyRatePerSec * 4096 / 1024 / 1024,
			DHotBytes:      dHotBytes,
			ShouldFullDump: true,
			Reason:         fmt.Sprintf("alpha>=1(%.2f)", alpha),
		}
	}

	// Checkpoint interval estimate
	intervalMs := float64(ds.config.ScanIntervalMs)
	if ds.lastPreDumpTime.IsZero() {
		intervalMs = 30000 // default 30s if no pre-dump yet
	} else {
		intervalMs = float64(time.Since(ds.lastPreDumpTime).Milliseconds())
	}

	// D_cold_ss = R_cold × I × α/(1-α)  (geometric series steady-state)
	dColdSS := dirtyRateBytesPerMs * intervalMs * alpha / (1 - alpha)
	if dColdSS < 0 {
		dColdSS = 0
	}

	// D_min = D_hot + D_cold_ss
	dMinBytes := dHotBytes + int64(math.Ceil(dColdSS))

	// T_required = D_min / B_eff
	tRequiredMs := float64(dMinBytes) / bEff

	// F_op = Available / T_required
	fop := 0.0
	if tRequiredMs > 0 {
		fop = availableMs / tRequiredMs
	} else {
		fop = 999 // no data to transfer = always feasible
	}

	result := FeasibilityResult{
		Fop:           fop,
		AvailableMs:   availableMs,
		TRequiredMs:   tRequiredMs,
		DMinBytes:     dMinBytes,
		DHotBytes:     dHotBytes,
		DColdSSBytes:  int64(math.Ceil(dColdSS)),
		Alpha:         alpha,
		DirtyRateMBps: dv.DirtyRatePerSec * 4096 / 1024 / 1024,
	}

	// Decision logic
	if fop < 1.0 {
		// Infeasible — need more pre-dumps or alert
		result.ShouldPreDump = true
		result.Reason = fmt.Sprintf("fop<1(%.2f)", fop)
	} else if fop < 2.0 {
		// Marginal — pre-dump to improve
		result.ShouldPreDump = true
		result.Reason = fmt.Sprintf("fop_marginal(%.2f)", fop)
	}
	// fop ≥ 2.0: comfortable, no action needed

	return result
}

// triggerPreDump triggers a pre-dump via the agent.
func (ds *DeadlineScheduler) triggerPreDump(ctx context.Context) {
	if ds.agent == nil {
		return
	}

	parentID := ""
	if ds.agent.checkpointMgr != nil {
		parentID = ds.agent.checkpointMgr.lastCheckpointID
	}

	// Get hot regions for exclude
	var excludeArgs *CRIUExcludeArgs
	if ds.profiler != nil {
		hotRegions := ds.profiler.GetHotRegions()
		if len(hotRegions) > 0 {
			excludeArgs = &CRIUExcludeArgs{}
			for _, hr := range hotRegions {
				excludeArgs.ExcludeRanges = append(excludeArgs.ExcludeRanges, profiler.AddrRange{
					Start: hr.StartAddr,
					End:   hr.EndAddr,
				})
			}
		}
	}

	pid := ds.agent.mainPID
	if pid <= 0 {
		log.Printf("[DEADLINE-SCHEDULER] No main PID, skipping pre-dump")
		return
	}

	result, err := ds.agent.checkpointMgr.PreCheckpoint(ctx, pid, parentID, excludeArgs)
	if err != nil {
		log.Printf("[DEADLINE-SCHEDULER] Pre-dump failed: %v", err)
		return
	}

	ds.lastPreDumpTime = time.Now()
	ds.preDumpCount++

	// Reinit profiler after CRIU dump
	if ds.profiler != nil {
		if err := ds.profiler.ReinitAfterCRIU(); err != nil {
			log.Printf("[DEADLINE-SCHEDULER] Profiler reinit failed: %v", err)
		}
	}

	log.Printf("[DEADLINE-SCHEDULER] Pre-dump #%d completed: %s (size=%d, pages=%d)",
		ds.preDumpCount, result.DumpID, result.SizeBytes, result.PagesDumped)
}

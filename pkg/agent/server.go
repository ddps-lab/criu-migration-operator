package agent

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ddps-lab/criu-migration-operator/pkg/profiler"
	pb "github.com/ddps-lab/criu-migration-operator/pkg/proto"
	"google.golang.org/grpc"
)

// Agent represents the CRIU agent
type Agent struct {
	pb.UnimplementedCRIUAgentServer

	checkpointMgr *CheckpointManager
	restoreMgr    *RestoreManager
	s3Client      *S3Client

	mode      string // "normal", "restore", or "idle"
	mainPID   int
	startTime time.Time
	podName   string
	nodeName  string

	// Page-server process management (for dump/source side)
	pageServerCmd     *exec.Cmd // Running page-server process
	pageServerCtx     context.Context
	pageServerCancel  context.CancelFunc
	pageServerDumpID  string // Which dump this page-server is serving

	// Restore process management (for restore/target side)
	restoreLazyPagesCmd *exec.Cmd // Running lazy-pages daemon
	restoreCmd          *exec.Cmd // Running criu restore process
	restoreCtx          context.Context
	restoreCancel       context.CancelFunc
	restoreDumpID       string   // Which dump is being restored
	restorePidNsFd      *os.File // PID namespace fd — must stay open for restored process lifetime
	lazyPagesActive     bool     // true while lazy-pages is still transferring pages

	// Write profiler
	profilerInst   *profiler.Profiler       // nil when not profiling
	prevExcludeSet map[uint64]uint64        // previous pre-dump's exclude ranges (start→end)
}

// NewAgent creates a new CRIU agent
func NewAgent(workDir, s3Bucket, s3Endpoint, s3Region, mode, podName, nodeName string) (*Agent, error) {
	// Read additional S3 options from environment
	downloadEndpoint := os.Getenv("DOWNLOAD_ENDPOINT")
	expressOneZone := os.Getenv("EXPRESS_ONE_ZONE") == "true"
	storageType := os.Getenv("STORAGE_TYPE")

	// Create S3 client with advanced options
	s3Client, err := NewS3ClientWithOptions(s3Bucket, s3Endpoint, downloadEndpoint, s3Region, storageType, expressOneZone)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	checkpointMgr := NewCheckpointManager(workDir, s3Client, podName, nodeName)
	restoreMgr := NewRestoreManager(workDir, s3Client, podName, nodeName)

	agent := &Agent{
		checkpointMgr: checkpointMgr,
		restoreMgr:    restoreMgr,
		s3Client:      s3Client,
		mode:          mode,
		startTime:     time.Now(),
		podName:       podName,
		nodeName:      nodeName,
	}

	return agent, nil
}

// Start starts the gRPC server and performs initial setup
func (a *Agent) Start(ctx context.Context, port int) error {
	// If in normal mode, start the user application process from annotations
	if a.mode == "normal" {
		if err := a.startUserProcess(); err != nil {
			log.Printf("Warning: failed to start user process: %v", err)
			// Continue anyway - user can manually investigate
		}
	}

	// If in restore mode, perform restore first
	if a.mode == "restore" {
		if err := a.performRestoreOnStartup(ctx); err != nil {
			log.Printf("Warning: restore on startup failed: %v", err)
			// Don't fail completely; continue as normal mode
			a.mode = "idle"
		}
	}

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterCRIUAgentServer(grpcServer, a)

	log.Printf("CRIU Agent gRPC server listening on port %d (mode: %s)", port, a.mode)

	// Start server in goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	grpcServer.GracefulStop()

	return nil
}

// PreCheckpoint implements the gRPC PreCheckpoint method
func (a *Agent) PreCheckpoint(ctx context.Context, req *pb.PreCheckpointRequest) (*pb.PreCheckpointResponse, error) {
	log.Printf("Received PreCheckpoint request (parent: %s)", req.ParentDumpId)

	// Block pre-dump while lazy-pages is still transferring pages.
	// CRIU pre-dump freezes the process, which prevents lazy-pages from delivering
	// page faults, causing a deadlock.
	if a.lazyPagesActive {
		return nil, fmt.Errorf("pre-checkpoint blocked: lazy-pages is still transferring pages")
	}

	// Find main process PID if not already set
	if a.mainPID == 0 {
		pid, err := a.checkpointMgr.FindMainProcessPID()
		if err != nil {
			return nil, fmt.Errorf("failed to find main process: %w", err)
		}
		a.mainPID = pid
		log.Printf("Found main process PID: %d", pid)
	}

	// Build exclude args from profiler if active
	var excludeArgs *CRIUExcludeArgs
	if a.profilerInst != nil {
		a.profilerInst.CleanupBeforeCRIU()

		currentHot := a.profilerInst.GetHotRegions()
		excludeArgs = &CRIUExcludeArgs{}
		currentSet := make(map[uint64]uint64, len(currentHot))

		for _, r := range currentHot {
			excludeArgs.ExcludeRanges = append(excludeArgs.ExcludeRanges,
				profiler.AddrRange{Start: r.StartAddr, End: r.EndAddr})
			currentSet[r.StartAddr] = r.EndAddr
		}

		// Detect hot→cold transitions
		for start, end := range a.prevExcludeSet {
			if _, exists := currentSet[start]; !exists {
				excludeArgs.NoParentRanges = append(excludeArgs.NoParentRanges,
					profiler.AddrRange{Start: start, End: end})
			}
		}

		a.prevExcludeSet = currentSet
		log.Printf("Profiler: %d exclude ranges, %d no-parent ranges",
			len(excludeArgs.ExcludeRanges), len(excludeArgs.NoParentRanges))
	}

	// Perform pre-checkpoint
	result, err := a.checkpointMgr.PreCheckpoint(ctx, a.mainPID, req.ParentDumpId, excludeArgs)
	if err != nil {
		if a.profilerInst != nil {
			a.profilerInst.ReinitAfterCRIU()
		}
		return nil, fmt.Errorf("pre-checkpoint failed: %w", err)
	}

	// Reinit profiler after CRIU dump
	if a.profilerInst != nil {
		if err := a.profilerInst.ReinitAfterCRIU(); err != nil {
			log.Printf("Warning: profiler reinit failed: %v", err)
		}
	}

	log.Printf("Pre-checkpoint completed: %s (size: %d bytes, pages: %d)",
		result.DumpID, result.SizeBytes, result.PagesDumped)

	return &pb.PreCheckpointResponse{
		DumpId:      result.DumpID,
		Timestamp:   result.Timestamp.Unix(),
		SizeBytes:   result.SizeBytes,
		PagesDumped: result.PagesDumped,
	}, nil
}

// FinalDump implements the gRPC FinalDump method
func (a *Agent) FinalDump(ctx context.Context, req *pb.FinalDumpRequest) (*pb.FinalDumpResponse, error) {
	log.Printf("Received FinalDump request (page-server: %s:%d, parent: %s)",
		req.PageServerAddr, req.PageServerPort, req.ParentDumpId)

	if a.lazyPagesActive {
		return nil, fmt.Errorf("final dump blocked: lazy-pages is still transferring pages")
	}

	// Find main process PID if not already set
	if a.mainPID == 0 {
		pid, err := a.checkpointMgr.FindMainProcessPID()
		if err != nil {
			return nil, fmt.Errorf("failed to find main process: %w", err)
		}
		a.mainPID = pid
	}

	// Kill any existing page-server before starting a new one
	if a.pageServerCmd != nil && a.pageServerCmd.Process != nil {
		log.Printf("Killing previous page-server process (PID %d)", a.pageServerCmd.Process.Pid)
		a.pageServerCmd.Process.Kill()
		if a.pageServerCancel != nil {
			a.pageServerCancel()
		}
		a.pageServerCmd = nil
	}

	// Create a long-lived context for page-server (independent of RPC context)
	// This ensures page-server survives beyond this RPC call
	pageServerCtx, pageServerCancel := context.WithCancel(context.Background())
	a.pageServerCtx = pageServerCtx
	a.pageServerCancel = pageServerCancel

	// Build exclude args from profiler if active
	var excludeArgs *CRIUExcludeArgs
	if a.profilerInst != nil {
		currentHot := a.profilerInst.GetHotRegions()
		a.profilerInst.CleanupBeforeCRIU()

		// Final dump: --exclude-range only (triggers has_parent=false for full dump)
		// No --no-parent-range needed (final dump dumps everything)
		excludeArgs = &CRIUExcludeArgs{}
		for _, r := range currentHot {
			excludeArgs.ExcludeRanges = append(excludeArgs.ExcludeRanges,
				profiler.AddrRange{Start: r.StartAddr, End: r.EndAddr})
		}
		log.Printf("Profiler: %d exclude ranges for final dump", len(excludeArgs.ExcludeRanges))
		// No reinit needed - process will be frozen after final dump
	}

	// Determine migration strategy
	strategy := req.MigrationStrategy
	if strategy == "" {
		strategy = "lazy-hybrid" // default for backward compatibility
	}
	log.Printf("FinalDump with strategy: %s", strategy)

	// Perform final dump with long-lived context
	result, cmd, err := a.checkpointMgr.FinalDump(
		pageServerCtx,
		a.mainPID,
		req.PageServerAddr,
		int(req.PageServerPort),
		req.ParentDumpId,
		excludeArgs,
		strategy,
	)
	if err != nil {
		pageServerCancel()
		return nil, fmt.Errorf("final dump failed: %w", err)
	}

	// Store page-server process in Agent to keep it alive
	a.pageServerCmd = cmd
	a.pageServerDumpID = result.DumpID

	// Reap the page-server process in background to prevent zombie
	go func() {
		if cmd != nil && cmd.Process != nil {
			cmd.Wait()
			log.Printf("[SOURCE-AGENT] Page-server process (PID %d) has exited", result.PageServerPID)
		}
	}()

	log.Printf("[%s] [SOURCE-AGENT] ✓ FinalDump completed - Page-server started",
		time.Now().Format("15:04:05.000"))
	log.Printf("[%s] [SOURCE-AGENT] Page-server PID: %d",
		time.Now().Format("15:04:05.000"), result.PageServerPID)
	log.Printf("[%s] [SOURCE-AGENT] Dump ID: %s",
		time.Now().Format("15:04:05.000"), result.DumpID)
	log.Printf("[%s] [SOURCE-AGENT] Target: %s:%d",
		time.Now().Format("15:04:05.000"), req.PageServerAddr, req.PageServerPort)
	log.Printf("[%s] [SOURCE-AGENT] Page-server will remain alive until lazy-pages completes",
		time.Now().Format("15:04:05.000"))

	log.Printf("Final dump completed: %s (metadata size: %d bytes, page-server PID: %d)",
		result.DumpID, result.SizeBytes, result.PageServerPID)

	return &pb.FinalDumpResponse{
		DumpId:            result.DumpID,
		Timestamp:         result.Timestamp.Unix(),
		MetadataSizeBytes: result.SizeBytes,
		ExternalMounts:    result.ExternalMounts,
		PageServerPid:     int32(result.PageServerPID),
		PipeInodes:        result.PipeInodes,
	}, nil
}

// Restore implements the gRPC Restore method
func (a *Agent) Restore(ctx context.Context, req *pb.RestoreRequest) (*pb.RestoreResponse, error) {
	log.Printf("Received Restore request (dump: %s, lazy-pages: %t)",
		req.DumpId, req.UseLazyPages)

	// Kill any existing restore processes before starting new ones
	if a.restoreLazyPagesCmd != nil && a.restoreLazyPagesCmd.Process != nil {
		log.Printf("Killing previous lazy-pages daemon (PID %d)", a.restoreLazyPagesCmd.Process.Pid)
		a.restoreLazyPagesCmd.Process.Kill()
	}
	if a.restoreCmd != nil && a.restoreCmd.Process != nil {
		log.Printf("Killing previous restore process (PID %d)", a.restoreCmd.Process.Pid)
		a.restoreCmd.Process.Kill()
	}
	if a.restorePidNsFd != nil {
		a.restorePidNsFd.Close()
		a.restorePidNsFd = nil
	}
	if a.restoreCancel != nil {
		a.restoreCancel()
	}

	// Create a long-lived context for restore processes (independent of RPC context)
	// This ensures lazy-pages daemon and restore process survive beyond this RPC call
	restoreCtx, restoreCancel := context.WithCancel(context.Background())
	a.restoreCtx = restoreCtx
	a.restoreCancel = restoreCancel

	// Determine migration strategy
	restoreStrategy := req.MigrationStrategy
	if restoreStrategy == "" {
		if req.UseLazyPages {
			restoreStrategy = "lazy-hybrid"
		} else {
			restoreStrategy = "full"
		}
	}
	log.Printf("Restore with strategy: %s", restoreStrategy)

	// Perform restore with long-lived context
	result, lazyPagesCmd, restoreCmd, err := a.restoreMgr.Restore(
		restoreCtx,
		req.DumpId,
		req.S3Prefix,
		req.UseLazyPages,
		int(req.PageServerPort),
		req.SourceAddr,
		req.ExternalMounts,
		restoreStrategy,
		req.PipeInodes,
	)
	if err != nil {
		restoreCancel()
		return nil, fmt.Errorf("restore failed: %w", err)
	}

	// Store restore processes and PID namespace fd in Agent to keep them alive.
	// The PID namespace fd MUST stay open for the lifetime of the restored process.
	a.restoreLazyPagesCmd = lazyPagesCmd
	a.restoreCmd = restoreCmd
	a.restoreDumpID = req.DumpId
	a.restorePidNsFd = result.PidNsFd

	needsLazy := restoreStrategy != "full"
	if needsLazy {
		a.lazyPagesActive = true
	}

	// Reap the CRIU restore process and clean up PID namespace fd after it exits.
	go func() {
		if restoreCmd != nil && restoreCmd.Process != nil {
			restoreCmd.Wait()
			log.Printf("[TARGET-AGENT] CRIU restore process exited")
		}
		if a.restorePidNsFd != nil {
			a.restorePidNsFd.Close()
			a.restorePidNsFd = nil
			log.Printf("[TARGET-AGENT] PID namespace fd closed")
		}
	}()

	log.Printf("[%s] [TARGET-AGENT] ✓ Restore initiated successfully (strategy: %s)",
		time.Now().Format("15:04:05.000"), restoreStrategy)
	log.Printf("[%s] [TARGET-AGENT] Dump ID: %s",
		time.Now().Format("15:04:05.000"), req.DumpId)
	if lazyPagesCmd != nil && lazyPagesCmd.Process != nil {
		log.Printf("[%s] [TARGET-AGENT] Lazy-pages daemon PID: %d",
			time.Now().Format("15:04:05.000"), lazyPagesCmd.Process.Pid)
	}
	if restoreCmd != nil && restoreCmd.Process != nil {
		log.Printf("[%s] [TARGET-AGENT] Restore process PID: %d",
			time.Now().Format("15:04:05.000"), restoreCmd.Process.Pid)
	}

	// Set restored process PID: prefer pidfile (set by RestoreManager), fallback to pgrep
	var pid int
	if result.NewPID > 0 {
		pid = int(result.NewPID)
		a.mainPID = pid
		a.checkpointMgr.SetMainPID(pid)
		log.Printf("Restored process PID (from pidfile): %d", pid)
	} else {
		// Fallback: search by process name
		time.Sleep(500 * time.Millisecond)
		foundPID, err := a.checkpointMgr.FindMainProcessPID()
		if err != nil {
			log.Printf("Warning: could not find restored process PID: %v", err)
		} else {
			pid = foundPID
			a.mainPID = pid
			result.NewPID = int32(pid)
			log.Printf("Restored process PID (from fallback): %d", pid)
		}
	}

	// Wait for lazy-pages completion in background, then allow checkpoints again
	if needsLazy {
		go func() {
			if lazyPagesCmd != nil && lazyPagesCmd.Process != nil {
				lazyPagesCmd.Wait()
			}
			a.lazyPagesActive = false
			log.Printf("[TARGET-AGENT] Lazy-pages completed, checkpoints are now allowed")
		}()
	}

	log.Printf("Restore completed successfully (duration: %dms)", result.DurationMs)

	return &pb.RestoreResponse{
		Success:    result.Success,
		NewPid:     result.NewPID,
		Timestamp:  result.Timestamp.Unix(),
		DurationMs: result.DurationMs,
	}, nil
}

// GetStatus implements the gRPC GetStatus method
func (a *Agent) GetStatus(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	// Try to find main process if not already set
	if a.mainPID == 0 {
		pid, _ := a.checkpointMgr.FindMainProcessPID()
		a.mainPID = pid
	}

	uptime := time.Since(a.startTime).Seconds()

	return &pb.StatusResponse{
		Mode:                 a.mode,
		MainProcessPid:       int32(a.mainPID),
		LastCheckpointId:     a.checkpointMgr.GetLastCheckpointID(),
		CheckpointChainDepth: int32(a.checkpointMgr.GetChainDepth()),
		NodeName:             a.nodeName,
		PodName:              a.podName,
		UptimeSeconds:        int64(uptime),
		LazyPagesActive:      a.lazyPagesActive,
	}, nil
}

// StartPageServer implements the gRPC StartPageServer method
// Note: This is deprecated - lazy-pages is started automatically during Restore
func (a *Agent) StartPageServer(ctx context.Context, req *pb.PageServerRequest) (*pb.PageServerResponse, error) {
	log.Printf("Received StartPageServer request (port: %d, dir: %s) - DEPRECATED", req.Port, req.CheckpointDir)

	// StartPageServer now requires sourceAddr parameter
	// This RPC method is deprecated and should not be used directly
	// lazy-pages is automatically started during Restore
	return &pb.PageServerResponse{
		Success: false,
		Pid:     0,
		Port:    req.Port,
	}, fmt.Errorf("StartPageServer is deprecated - lazy-pages is started automatically during Restore")
}

// CheckPageServerStatus implements the gRPC CheckPageServerStatus method
func (a *Agent) CheckPageServerStatus(ctx context.Context, req *pb.PageServerStatusRequest) (*pb.PageServerStatusResponse, error) {
	pid := int(req.Pid)

	// Check if process exists using /proc filesystem
	procPath := fmt.Sprintf("/proc/%d", pid)
	if _, err := os.Stat(procPath); os.IsNotExist(err) {
		// Process doesn't exist - try to get exit code
		return &pb.PageServerStatusResponse{
			IsAlive:       false,
			ExitCode:      0, // We can't determine exit code after process ends
			StatusMessage: fmt.Sprintf("Page-server process %d has terminated", pid),
		}, nil
	}

	// Process still exists - check if it's the page-server we're looking for
	cmdlinePath := fmt.Sprintf("/proc/%d/cmdline", pid)
	cmdlineData, err := os.ReadFile(cmdlinePath)
	if err != nil {
		// Process disappeared between stat and read
		log.Printf("[%s] [SOURCE-AGENT] Page-server PID %d has terminated",
			time.Now().Format("15:04:05.000"), pid)
		return &pb.PageServerStatusResponse{
			IsAlive:       false,
			ExitCode:      0,
			StatusMessage: fmt.Sprintf("Page-server process %d has terminated", pid),
		}, nil
	}

	cmdline := string(cmdlineData)
	// In page-server mode, CRIU may fork and the cmdline could contain "dump" or "page-server"
	// Also check for empty cmdline which indicates a zombie process
	if cmdline == "" {
		log.Printf("[%s] [SOURCE-AGENT] PID %d has empty cmdline (zombie/terminated)",
			time.Now().Format("15:04:05.000"), pid)
		return &pb.PageServerStatusResponse{
			IsAlive:       false,
			ExitCode:      0,
			StatusMessage: fmt.Sprintf("Page-server process %d has terminated (zombie)", pid),
		}, nil
	}
	if !strings.Contains(cmdline, "criu") {
		// Not the CRIU process we're looking for
		log.Printf("[%s] [SOURCE-AGENT] PID %d is not a CRIU process (cmdline: %s)",
			time.Now().Format("15:04:05.000"), pid, cmdline)
		return &pb.PageServerStatusResponse{
			IsAlive:       false,
			ExitCode:      0,
			StatusMessage: fmt.Sprintf("Process %d is not a CRIU process", pid),
		}, nil
	}

	// Process is still running
	log.Printf("[%s] [SOURCE-AGENT] Page-server PID %d is still alive and serving pages",
		time.Now().Format("15:04:05.000"), pid)
	return &pb.PageServerStatusResponse{
		IsAlive:       true,
		ExitCode:      0,
		StatusMessage: fmt.Sprintf("Page-server process %d is still running", pid),
	}, nil
}

// StartProfiling implements the gRPC StartProfiling method
func (a *Agent) StartProfiling(ctx context.Context, req *pb.StartProfilingRequest) (*pb.StartProfilingResponse, error) {
	log.Printf("Received StartProfiling request (interval=%dms, threshold=%.2f, consecutive=%d)",
		req.IntervalMs, req.HotThreshold, req.HotConsecutive)

	if a.profilerInst != nil {
		return &pb.StartProfilingResponse{
			Success: false,
			Error:   "profiler already running",
		}, nil
	}

	// Find main process PID if not already set
	if a.mainPID == 0 {
		pid, err := a.checkpointMgr.FindMainProcessPID()
		if err != nil {
			return &pb.StartProfilingResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to find main process: %v", err),
			}, nil
		}
		a.mainPID = pid
	}

	cfg := profiler.DefaultConfig()
	if req.IntervalMs > 0 {
		cfg.IntervalMs = int(req.IntervalMs)
	}
	if req.HotThreshold > 0 {
		cfg.HotThreshold = req.HotThreshold
	}
	if req.HotConsecutive > 0 {
		cfg.HotConsecutive = int(req.HotConsecutive)
	}

	p := profiler.New(a.mainPID, cfg)
	if err := p.Start(); err != nil {
		return &pb.StartProfilingResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to start profiler: %v", err),
		}, nil
	}

	a.profilerInst = p
	totalVMAs, _ := p.GetVMACounts()

	log.Printf("Profiler started (pid=%d, vmas=%d)", a.mainPID, totalVMAs)

	return &pb.StartProfilingResponse{
		Success:  true,
		Pid:      int32(a.mainPID),
		VmaCount: int32(totalVMAs),
	}, nil
}

// StopProfiling implements the gRPC StopProfiling method
func (a *Agent) StopProfiling(ctx context.Context, req *pb.StopProfilingRequest) (*pb.StopProfilingResponse, error) {
	log.Printf("Received StopProfiling request")

	if a.profilerInst == nil {
		return &pb.StopProfilingResponse{Success: true}, nil
	}

	a.profilerInst.Close()
	a.profilerInst = nil
	a.prevExcludeSet = nil

	log.Printf("Profiler stopped")
	return &pb.StopProfilingResponse{Success: true}, nil
}

// GetHotRegions implements the gRPC GetHotRegions method
func (a *Agent) GetHotRegions(ctx context.Context, req *pb.GetHotRegionsRequest) (*pb.GetHotRegionsResponse, error) {
	if a.profilerInst == nil {
		return &pb.GetHotRegionsResponse{}, nil
	}

	regions := a.profilerInst.GetHotRegions()
	totalVMAs, hotVMAs := a.profilerInst.GetVMACounts()

	protoRegions := make([]*pb.HotRegionProto, len(regions))
	for i, r := range regions {
		protoRegions[i] = &pb.HotRegionProto{
			StartAddr:      r.StartAddr,
			EndAddr:        r.EndAddr,
			WrittenRatio:   r.WrittenRatio,
			ConsecutiveHot: int32(r.ConsecutiveHot),
		}
	}

	return &pb.GetHotRegionsResponse{
		Regions:     protoRegions,
		TimestampMs: time.Now().UnixMilli(),
		TotalVmas:   int32(totalVMAs),
		HotVmas:     int32(hotVMAs),
	}, nil
}

// GetDirtyVolume implements the gRPC GetDirtyVolume method
func (a *Agent) GetDirtyVolume(ctx context.Context, req *pb.GetDirtyVolumeRequest) (*pb.GetDirtyVolumeResponse, error) {
	if a.profilerInst == nil {
		return &pb.GetDirtyVolumeResponse{}, nil
	}

	dv := a.profilerInst.GetDirtyVolume()

	return &pb.GetDirtyVolumeResponse{
		DirtyPages:           dv.DirtyPages,
		DirtyBytes:           dv.DirtyBytes,
		DirtyRatePagesPerSec: dv.DirtyRatePerSec,
		CumulativeDirtyBytes: dv.CumulativeDirtyBytes,
		AvgDirtyRate:         dv.AvgDirtyRate,
		TimestampMs:          dv.TimestampMs,
	}, nil
}

// performRestoreOnStartup performs restore operation on agent startup
func (a *Agent) performRestoreOnStartup(ctx context.Context) error {
	// Get restore configuration from environment
	checkpointID := os.Getenv("CHECKPOINT_ID")
	s3Prefix := os.Getenv("S3_PREFIX")
	sourceAddr := os.Getenv("SOURCE_POD_IP")

	if checkpointID == "" || s3Prefix == "" {
		return fmt.Errorf("CHECKPOINT_ID or S3_PREFIX not set")
	}

	if sourceAddr == "" {
		return fmt.Errorf("SOURCE_POD_IP environment variable not set - required for lazy-pages connection to source pod (should be set via Downward API from annotation)")
	}

	log.Printf("Performing restore on startup (checkpoint: %s, source: %s)", checkpointID, sourceAddr)

	// Wait for sleep process to appear
	sleepPID, err := a.checkpointMgr.WaitForSleepProcess(10 * time.Second)
	if err != nil {
		return fmt.Errorf("failed to find sleep process: %w", err)
	}

	log.Printf("Found sleep process (PID: %d), starting restore...", sleepPID)

	// Create a long-lived context for restore processes (independent of startup context)
	restoreCtx, restoreCancel := context.WithCancel(context.Background())
	a.restoreCtx = restoreCtx
	a.restoreCancel = restoreCancel

	// Perform restore with lazy pages
	// For startup restore, use empty external mounts (will use defaults)
	result, lazyPagesCmd, restoreCmd, err := a.restoreMgr.Restore(restoreCtx, checkpointID, s3Prefix, true, 9999, sourceAddr, nil, "lazy-hybrid", nil)
	if err != nil {
		restoreCancel()
		return fmt.Errorf("restore failed: %w", err)
	}

	// Store restore processes to keep them alive
	a.restoreLazyPagesCmd = lazyPagesCmd
	a.restoreCmd = restoreCmd

	log.Printf("Restore completed successfully (duration: %dms)", result.DurationMs)

	// Find restored process
	time.Sleep(1 * time.Second)
	pid, err := a.checkpointMgr.FindMainProcessPID()
	if err != nil {
		return fmt.Errorf("failed to find restored process: %w", err)
	}

	a.mainPID = pid
	a.mode = "normal"

	log.Printf("Restored process found (PID: %d), creating baseline checkpoint...", pid)

	// Create baseline checkpoint
	if err := a.restoreMgr.CreateBaselineCheckpoint(ctx, pid, a.checkpointMgr); err != nil {
		log.Printf("Warning: failed to create baseline checkpoint: %v", err)
	}

	return nil
}

// startUserProcess starts the user application process based on pod annotations
func (a *Agent) startUserProcess() error {
	log.Printf("Starting user process from pod annotations...")

	// Read pod annotations from Downward API volume
	annotationsData, err := os.ReadFile("/etc/podinfo/annotations")
	if err != nil {
		return fmt.Errorf("failed to read pod annotations: %w", err)
	}

	// Parse annotations (Downward API format is key="value"\n)
	annotations := string(annotationsData)

	// Extract original command, args, and workdir
	cmdAnnotation := extractAnnotation(annotations, "migration.io/original-command")
	argsAnnotation := extractAnnotation(annotations, "migration.io/original-args")
	workdirAnnotation := extractAnnotation(annotations, "migration.io/original-workdir")

	if cmdAnnotation == "" {
		log.Printf("No original command found in annotations, skipping user process start")
		return nil
	}

	// Parse command (comma-separated)
	command := strings.Split(cmdAnnotation, ",")

	// Parse args (|||-separated to handle commas in args)
	var args []string
	if argsAnnotation != "" {
		args = strings.Split(argsAnnotation, "|||")
	}

	log.Printf("Starting user process: command=%v, args=%v", command, args)

	// Find main container PID to enter its namespace
	mainPID, err := FindMainContainerPID()
	if err != nil {
		return fmt.Errorf("failed to find main container PID: %w", err)
	}

	// Start process in main container's namespace using nsenter
	// This ensures it runs in the app container's environment, not agent container
	// For commands with multiline args (like python -c), we need to use a shell
	nsenterArgs := []string{
		"-t", fmt.Sprintf("%d", mainPID),
		"-m", "-u", "-i", "-n", "-p", // All namespaces
		"--",
		"sh", "-c",
	}

	// Build the full command string for the shell
	fullCmd := ""
	// Prepend cd to original workdir if specified
	if workdirAnnotation != "" {
		if filepath.IsAbs(workdirAnnotation) {
			fullCmd = fmt.Sprintf("cd '%s' && ", workdirAnnotation)
		} else {
			// Relative path: resolve against container's CWD (= image WORKDIR)
			cwd, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", mainPID))
			if err == nil {
				fullCmd = fmt.Sprintf("cd '%s' && ", filepath.Join(cwd, workdirAnnotation))
			} else {
				log.Printf("Warning: relative workdir '%s' but cannot determine container CWD: %v", workdirAnnotation, err)
				fullCmd = fmt.Sprintf("cd '%s' && ", workdirAnnotation)
			}
		}
	}
	for i, part := range command {
		if i > 0 {
			fullCmd += " "
		}
		// Quote parts that might contain spaces
		if strings.Contains(part, " ") {
			fullCmd += fmt.Sprintf("\"%s\"", strings.ReplaceAll(part, "\"", "\\\""))
		} else {
			fullCmd += part
		}
	}
	for _, arg := range args {
		fullCmd += " "
		// For multiline args, use single quotes to preserve newlines
		if strings.Contains(arg, "\n") || strings.Contains(arg, "'") {
			// Escape single quotes by ending quote, adding escaped quote, starting quote again
			escaped := strings.ReplaceAll(arg, "'", "'\"'\"'")
			fullCmd += fmt.Sprintf("'%s'", escaped)
		} else if strings.Contains(arg, " ") {
			fullCmd += fmt.Sprintf("\"%s\"", strings.ReplaceAll(arg, "\"", "\\\""))
		} else {
			fullCmd += arg
		}
	}
	// Prepend exec so the outer sh replaces itself with the actual command.
	// This makes nsenter's direct child the workload process (not sh).
	fullCmd = "exec " + fullCmd
	nsenterArgs = append(nsenterArgs, fullCmd)

	log.Printf("Executing command via nsenter: %s", fullCmd)

	userCmd := exec.Command("nsenter", nsenterArgs...)
	userCmd.Stdout = os.Stdout
	userCmd.Stderr = os.Stderr
	// Open /dev/null from main container's namespace for stdin
	// This ensures fd 0 has the correct mount ID from main container
	devNull, err := os.Open(fmt.Sprintf("/proc/%d/root/dev/null", mainPID))
	if err != nil {
		return fmt.Errorf("failed to open /dev/null from main container: %w", err)
	}
	defer devNull.Close()
	userCmd.Stdin = devNull

	if err := userCmd.Start(); err != nil {
		return fmt.Errorf("failed to start user process: %w", err)
	}

	log.Printf("nsenter process started with PID: %d", userCmd.Process.Pid)

	// Wait a moment for the user process to start in the main container's namespace
	time.Sleep(500 * time.Millisecond)

	// Find the actual user process PID by tracing nsenter's child process tree.
	// With `exec` in the command, nsenter's direct child is the workload process.
	nsenterPID := userCmd.Process.Pid
	realPID, err := findChildPID(nsenterPID)
	if err != nil {
		log.Printf("Warning: failed to find child of nsenter PID %d: %v", nsenterPID, err)
		// Fallback: try finding any non-sleep process
		realPID, err = findNonSleepPID()
		if err != nil {
			return fmt.Errorf("failed to find user process: %w", err)
		}
	}

	// Store the real user process PID
	a.mainPID = realPID
	a.checkpointMgr.SetMainPID(realPID)
	log.Printf("User process found with PID: %d (child of nsenter PID %d)", a.mainPID, nsenterPID)

	// Don't wait for the nsenter process - let the user process run independently
	// CRIU will checkpoint the user process later

	return nil
}

// findChildPID finds a direct child process of the given parent PID.
// Uses pgrep -P (parent PID based) so no process name hardcoding is needed.
func findChildPID(parentPID int) (int, error) {
	cmd := exec.Command("pgrep", "-P", fmt.Sprintf("%d", parentPID))
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("no child of PID %d: %w", parentPID, err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	pid, err := strconv.Atoi(lines[0])
	if err != nil {
		return 0, fmt.Errorf("failed to parse child PID: %w", err)
	}

	return pid, nil
}

// findNonSleepPID finds the first non-sleep process in the main container
func findNonSleepPID() (int, error) {
	mainPID, err := FindMainContainerPID()
	if err != nil {
		return 0, err
	}

	// List all processes in the main container's PID namespace
	// Exclude sleep (PID of sleep infinity) and look for user process
	cmd := exec.Command("nsenter", "-t", fmt.Sprintf("%d", mainPID), "-p", "--", "ps", "-e", "-o", "pid,comm", "--no-headers")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to list processes: %w", err)
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] != "sleep" && fields[1] != "ps" {
			pid, err := strconv.Atoi(fields[0])
			if err == nil && pid != mainPID {
				return pid, nil
			}
		}
	}

	return 0, fmt.Errorf("no non-sleep process found")
}

// extractAnnotation extracts annotation value from Downward API format
func extractAnnotation(annotations, key string) string {
	// Downward API format: key="value"\n where value can contain escaped quotes \"
	keyPattern := fmt.Sprintf("%s=\"", key)
	startIdx := strings.Index(annotations, keyPattern)
	if startIdx == -1 {
		return ""
	}
	startIdx += len(keyPattern)

	// Find the closing quote, skipping escaped quotes
	endIdx := -1
	for i := startIdx; i < len(annotations); i++ {
		if annotations[i] == '"' {
			// Check if it's escaped by looking at preceding backslashes
			numBackslashes := 0
			for j := i - 1; j >= startIdx && annotations[j] == '\\'; j-- {
				numBackslashes++
			}
			// If odd number of backslashes, the quote is escaped
			if numBackslashes%2 == 0 {
				endIdx = i
				break
			}
		}
	}

	if endIdx == -1 {
		return ""
	}

	value := annotations[startIdx:endIdx]

	// Unescape characters that Kubernetes escapes in Downward API
	value = strings.ReplaceAll(value, "\\n", "\n")
	value = strings.ReplaceAll(value, "\\t", "\t")
	value = strings.ReplaceAll(value, "\\\"", "\"")
	value = strings.ReplaceAll(value, "\\\\", "\\")

	return value
}

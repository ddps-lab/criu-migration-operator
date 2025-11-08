package agent

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

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
}

// NewAgent creates a new CRIU agent
func NewAgent(workDir, s3Bucket, s3Endpoint, s3Region, mode, podName, nodeName string) (*Agent, error) {
	// Read additional S3 options from environment
	downloadEndpoint := os.Getenv("DOWNLOAD_ENDPOINT")
	expressOneZone := os.Getenv("EXPRESS_ONE_ZONE") == "true"

	// Create S3 client with advanced options
	s3Client, err := NewS3ClientWithOptions(s3Bucket, s3Endpoint, downloadEndpoint, s3Region, expressOneZone)
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

	// Find main process PID if not already set
	if a.mainPID == 0 {
		pid, err := a.checkpointMgr.FindMainProcessPID()
		if err != nil {
			return nil, fmt.Errorf("failed to find main process: %w", err)
		}
		a.mainPID = pid
		log.Printf("Found main process PID: %d", pid)
	}

	// Perform pre-checkpoint
	result, err := a.checkpointMgr.PreCheckpoint(ctx, a.mainPID, req.ParentDumpId)
	if err != nil {
		return nil, fmt.Errorf("pre-checkpoint failed: %w", err)
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

	// Find main process PID if not already set
	if a.mainPID == 0 {
		pid, err := a.checkpointMgr.FindMainProcessPID()
		if err != nil {
			return nil, fmt.Errorf("failed to find main process: %w", err)
		}
		a.mainPID = pid
	}

	// Perform final dump
	result, err := a.checkpointMgr.FinalDump(
		ctx,
		a.mainPID,
		req.PageServerAddr,
		int(req.PageServerPort),
		req.ParentDumpId,
	)
	if err != nil {
		return nil, fmt.Errorf("final dump failed: %w", err)
	}

	log.Printf("Final dump completed: %s (metadata size: %d bytes)",
		result.DumpID, result.SizeBytes)

	return &pb.FinalDumpResponse{
		DumpId:            result.DumpID,
		Timestamp:         result.Timestamp.Unix(),
		MetadataSizeBytes: result.SizeBytes,
	}, nil
}

// Restore implements the gRPC Restore method
func (a *Agent) Restore(ctx context.Context, req *pb.RestoreRequest) (*pb.RestoreResponse, error) {
	log.Printf("Received Restore request (dump: %s, lazy-pages: %t)",
		req.DumpId, req.UseLazyPages)

	// Perform restore
	result, err := a.restoreMgr.Restore(
		ctx,
		req.DumpId,
		req.S3Prefix,
		req.UseLazyPages,
		int(req.PageServerPort),
		req.SourceAddr, // Source pod IP for lazy-pages connection
	)
	if err != nil {
		return nil, fmt.Errorf("restore failed: %w", err)
	}

	// Find restored process PID
	time.Sleep(500 * time.Millisecond)
	pid, err := a.checkpointMgr.FindMainProcessPID()
	if err != nil {
		log.Printf("Warning: could not find restored process PID: %v", err)
	} else {
		a.mainPID = pid
		result.NewPID = int32(pid)
		log.Printf("Restored process PID: %d", pid)
	}

	// Create baseline checkpoint for new chain
	if pid > 0 {
		go func() {
			ctx := context.Background()
			if err := a.restoreMgr.CreateBaselineCheckpoint(ctx, pid, a.checkpointMgr); err != nil {
				log.Printf("Warning: failed to create baseline checkpoint: %v", err)
			} else {
				log.Printf("Baseline checkpoint created successfully")
			}
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
		return fmt.Errorf("SOURCE_POD_IP not set for lazy-pages connection")
	}

	log.Printf("Performing restore on startup (checkpoint: %s, source: %s)", checkpointID, sourceAddr)

	// Wait for sleep process to appear
	sleepPID, err := a.checkpointMgr.WaitForSleepProcess(10 * time.Second)
	if err != nil {
		return fmt.Errorf("failed to find sleep process: %w", err)
	}

	log.Printf("Found sleep process (PID: %d), starting restore...", sleepPID)

	// Perform restore with lazy pages
	result, err := a.restoreMgr.Restore(ctx, checkpointID, s3Prefix, true, 9999, sourceAddr)
	if err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}

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

package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// RestoreManager handles CRIU restore operations
type RestoreManager struct {
	workDir  string
	s3Client *S3Client
	podName  string
	nodeName string
}

// NewRestoreManager creates a new restore manager
func NewRestoreManager(workDir string, s3Client *S3Client, podName, nodeName string) *RestoreManager {
	return &RestoreManager{
		workDir:  workDir,
		s3Client: s3Client,
		podName:  podName,
		nodeName: nodeName,
	}
}

// Restore restores a process from a checkpoint
func (m *RestoreManager) Restore(ctx context.Context, dumpID, s3Prefix string, useLazyPages bool, pageServerPort int) (*RestoreResult, error) {
	startTime := time.Now()

	// Download checkpoint from S3
	dumpDir := filepath.Join(m.workDir, dumpID)
	if err := m.s3Client.DownloadCheckpoint(ctx, s3Prefix, dumpDir); err != nil {
		return nil, fmt.Errorf("failed to download checkpoint: %w", err)
	}

	var pageServerPID int
	if useLazyPages {
		// Start page-server for lazy restore
		pid, err := m.StartPageServer(ctx, pageServerPort, dumpDir)
		if err != nil {
			return nil, fmt.Errorf("failed to start page-server: %w", err)
		}
		pageServerPID = pid

		// Wait for page-server to be ready
		time.Sleep(1 * time.Second)
	}

	// Build CRIU restore command
	args := []string{
		"restore",
		"-D", dumpDir,
		"--tcp-established",
		"--shell-job",
		"-v4",
		"--log-file", filepath.Join(dumpDir, "restore.log"),
	}

	if useLazyPages {
		args = append(args, "--lazy-pages")
	}

	// Execute CRIU restore
	cmd := exec.CommandContext(ctx, "criu", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("criu restore failed: %w\nOutput: %s", err, string(output))
	}

	duration := time.Since(startTime)

	// Find the restored process PID
	// Note: This is a simplified approach; in production, you'd parse CRIU's output
	time.Sleep(500 * time.Millisecond)

	result := &RestoreResult{
		Success:       true,
		NewPID:        0, // Will be set by caller
		Timestamp:     time.Now(),
		DurationMs:    duration.Milliseconds(),
		PageServerPID: pageServerPID,
	}

	return result, nil
}

// StartPageServer starts the CRIU page-server for lazy restore
func (m *RestoreManager) StartPageServer(ctx context.Context, port int, checkpointDir string) (int, error) {
	args := []string{
		"lazy-pages",
		"--port", strconv.Itoa(port),
		"--page-read",
		"--daemon",
		"--address", "0.0.0.0",
		"-D", checkpointDir,
		"-v4",
		"--log-file", filepath.Join(checkpointDir, "page-server.log"),
	}

	cmd := exec.CommandContext(ctx, "criu", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed to start page-server: %w\nOutput: %s", err, string(output))
	}

	// Parse page-server PID from output or find it via pgrep
	time.Sleep(500 * time.Millisecond)
	pid, err := m.findPageServerPID()
	if err != nil {
		return 0, fmt.Errorf("failed to find page-server PID: %w", err)
	}

	return pid, nil
}

// StopPageServer stops the page-server
func (m *RestoreManager) StopPageServer(pid int) error {
	if pid <= 0 {
		return nil
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process: %w", err)
	}

	if err := process.Kill(); err != nil {
		return fmt.Errorf("failed to kill page-server: %w", err)
	}

	return nil
}

// findPageServerPID finds the PID of the running page-server
func (m *RestoreManager) findPageServerPID() (int, error) {
	cmd := exec.Command("pgrep", "-f", "criu lazy-pages")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("page-server process not found")
	}

	pidStr := string(output)
	pid, err := strconv.Atoi(pidStr[:len(pidStr)-1])
	if err != nil {
		return 0, fmt.Errorf("failed to parse PID: %w", err)
	}

	return pid, nil
}

// CreateBaselineCheckpoint creates a baseline checkpoint after restore
func (m *RestoreManager) CreateBaselineCheckpoint(ctx context.Context, pid int, checkpointMgr *CheckpointManager) error {
	// Wait for process to stabilize
	time.Sleep(2 * time.Second)

	// Create a new baseline checkpoint (no parent)
	result, err := checkpointMgr.PreCheckpoint(ctx, pid, "")
	if err != nil {
		return fmt.Errorf("failed to create baseline checkpoint: %w", err)
	}

	// Reset chain with new baseline
	checkpointMgr.ResetChain()
	checkpointMgr.chainRoot = result.DumpID
	checkpointMgr.lastCheckpointID = result.DumpID
	checkpointMgr.chainDepth = 1

	return nil
}

// RestoreResult contains the result of a restore operation
type RestoreResult struct {
	Success       bool
	NewPID        int32
	Timestamp     time.Time
	DurationMs    int64
	PageServerPID int
}

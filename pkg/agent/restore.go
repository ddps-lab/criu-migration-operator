package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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
func (m *RestoreManager) Restore(ctx context.Context, dumpID, s3Prefix string, useLazyPages bool, pageServerPort int, sourceAddr string) (*RestoreResult, error) {
	startTime := time.Now()

	// Download checkpoint chain metadata from S3 (pages come from page-server)
	// This downloads ALL checkpoints in the chain (all parent dumps' metadata)
	// to preserve directory structure needed for --prev-images-dir references
	if err := m.s3Client.DownloadMetadataOnly(ctx, s3Prefix, m.workDir); err != nil {
		return nil, fmt.Errorf("failed to download checkpoint metadata: %w", err)
	}

	// The final dump directory where restore will run
	dumpDir := filepath.Join(m.workDir, dumpID)

	// Use fixed list of Kubernetes external mounts for restore
	// These must match what was used during dump
	// Note: We cannot read from /proc/PID/mountinfo here because the app container
	// doesn't exist yet at restore time. The agent container has different mounts.
	externalMounts := map[string]string{
		"/etc/hosts":           "etc-hosts",
		"/etc/hostname":        "etc-hostname",
		"/etc/resolv.conf":     "etc-resolv.conf",
		"/dev/termination-log": "dev-termination-log",
		"/dev/shm":             "dev-shm",
		"/run/secrets/kubernetes.io/serviceaccount": "run-secrets-kubernetes.io-serviceaccount",
	}
	fmt.Printf("Using %d external mounts for restore\n", len(externalMounts))

	var pageServerPID int
	if useLazyPages {
		// Start lazy-pages daemon that connects to source pod's page server
		pid, err := m.StartPageServer(ctx, pageServerPort, dumpDir, sourceAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to start lazy-pages: %w", err)
		}
		pageServerPID = pid

		// Wait for lazy-pages to be ready
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
		"--manage-cgroups=ignore", // Use 'ignore' mode: don't deal with cgroups and pretend they don't exist
		"--mntns-compat-mode",
	}

	// Add external mount options for restore
	// Format: --external mnt[identifier]:mountpoint
	// The identifier comes from dump, mountpoint is current location in this pod
	for mountPoint, identifier := range externalMounts {
		args = append(args, "--external", fmt.Sprintf("mnt[%s]:%s", identifier, mountPoint))
	}

	if useLazyPages {
		args = append(args, "--lazy-pages")
	}

	// Add S3/object storage options (use download endpoint for restore)
	if m.s3Client != nil {
		args = append(args,
			"--enable-object-storage",
			"--object-storage-endpoint-url", m.s3Client.getDownloadEndpoint(),
		)

		// Add bucket only if needed (CloudFront doesn't need bucket)
		if m.s3Client.needsBucketOption() {
			args = append(args,
				"--object-storage-bucket", m.s3Client.bucket,
			)
		}

		args = append(args,
			"--object-storage-object-prefix", s3Prefix+"/",
		)

		// Add AWS credentials if available
		awsAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
		awsSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if awsAccessKey != "" && awsSecretKey != "" {
			args = append(args,
				"--aws-access-key", awsAccessKey,
				"--aws-secret-key", awsSecretKey,
			)
		}

		// Add express-one-zone flag if enabled
		if m.s3Client.isExpressOneZone() {
			args = append(args, "--express-one-zone")
		}
	}

	// Execute CRIU restore
	// Note: We use cmd.Start() instead of cmd.Run() because the restored process
	// continues running. We just need to verify that CRIU restore initiates successfully.
	cmd := exec.CommandContext(ctx, "criu", args...)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start criu restore: %w", err)
	}

	// Check if restore process fails immediately
	// Since lazy-pages is already ready, restore should start quickly
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- cmd.Wait()
	}()

	select {
	case err := <-doneCh:
		// Process exited - check if it's an error
		if err != nil {
			// Read restore.log and lazy-pages.log for debugging
			restoreLog := m.readLogFile(filepath.Join(dumpDir, "restore.log"), 100)
			lazyPagesLog := m.readLogFile(filepath.Join(dumpDir, "lazy-pages.log"), 50)

			errorMsg := fmt.Sprintf("criu restore failed: %v\n\n=== CRIU Args ===\n%v\n\n=== Restore Log (last 100 lines) ===\n%s\n\n=== Lazy-Pages Log (last 50 lines) ===\n%s",
				err, args, restoreLog, lazyPagesLog)
			return nil, fmt.Errorf("%s", errorMsg)
		}
		// Process exited with success - restore completed
	case <-time.After(500 * time.Millisecond):
		// Process is still running after 500ms - restore is in progress
		// The restored process will continue running, which is expected
	}

	duration := time.Since(startTime)

	result := &RestoreResult{
		Success:       true,
		NewPID:        0, // Will be set by caller
		Timestamp:     time.Now(),
		DurationMs:    duration.Milliseconds(),
		PageServerPID: pageServerPID,
	}

	return result, nil
}

// StartPageServer starts the CRIU lazy-pages daemon that connects to source pod's page server
func (m *RestoreManager) StartPageServer(ctx context.Context, port int, checkpointDir, sourceAddr string) (int, error) {
	args := []string{
		"lazy-pages",
		"--images-dir", checkpointDir,
		"--page-server",
		"--address", sourceAddr, // Connect to source pod's page server
		"--port", strconv.Itoa(port),
		"-v4",
		"--log-file", filepath.Join(checkpointDir, "lazy-pages.log"),
	}

	// Add async-prefetch if enabled
	if os.Getenv("ASYNC_PREFETCH") == "true" {
		args = append(args, "--async-prefetch")
	}

	// Add S3/object storage options for lazy-pages (use download endpoint)
	if m.s3Client != nil {
		args = append(args,
			"--enable-object-storage",
			"--object-storage-endpoint-url", m.s3Client.getDownloadEndpoint(),
		)

		// Add bucket only if needed (CloudFront doesn't need bucket)
		if m.s3Client.needsBucketOption() {
			args = append(args,
				"--object-storage-bucket", m.s3Client.bucket,
			)
		}

		// Add AWS credentials if available
		awsAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
		awsSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if awsAccessKey != "" && awsSecretKey != "" {
			args = append(args,
				"--aws-access-key", awsAccessKey,
				"--aws-secret-key", awsSecretKey,
			)
		}

		// Add express-one-zone flag if enabled
		if m.s3Client.isExpressOneZone() {
			args = append(args, "--express-one-zone")
		}
	}

	// Start lazy-pages daemon in background
	cmd := exec.CommandContext(ctx, "criu", args...)
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("failed to start lazy-pages daemon: %w", err)
	}

	// Wait for lazy-pages to be ready by monitoring the log file
	logPath := filepath.Join(checkpointDir, "lazy-pages.log")
	if err := m.waitForLazyPagesReady(logPath, 10*time.Second); err != nil {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return 0, fmt.Errorf("lazy-pages failed to become ready: %w", err)
	}

	// Find the lazy-pages daemon PID
	pid, err := m.findPageServerPID()
	if err != nil {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return 0, fmt.Errorf("failed to find lazy-pages PID: %w", err)
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

// waitForLazyPagesReady waits for lazy-pages daemon to be ready by monitoring its log file
// Looks for "uffd: Waiting for incoming connections on lazy-pages.socket" message
func (m *RestoreManager) waitForLazyPagesReady(logPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	// Wait for log file to be created
	for time.Now().Before(deadline) {
		if _, err := os.Stat(logPath); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Monitor log file for ready indicator
	for time.Now().Before(deadline) {
		content, err := os.ReadFile(logPath)
		if err == nil {
			logContent := string(content)
			// CRIU lazy-pages is ready when it's waiting for connections
			if strings.Contains(logContent, "Waiting for incoming connections on lazy-pages.socket") {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	return fmt.Errorf("lazy-pages did not become ready within timeout")
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

// readLogFile reads the last N lines of a log file for debugging
func (m *RestoreManager) readLogFile(path string, maxLines int) string {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("(failed to read %s: %v)", path, err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}

	return strings.Join(lines, "\n")
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

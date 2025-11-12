package agent

import (
	"context"
	"fmt"
	"net"
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
// Returns: result, lazyPagesCmd, restoreCmd, error
// The caller MUST store the returned cmds to keep processes alive
func (m *RestoreManager) Restore(ctx context.Context, dumpID, s3Prefix string, useLazyPages bool, pageServerPort int, sourceAddr string, externalMounts map[string]string) (*RestoreResult, *exec.Cmd, *exec.Cmd, error) {
	startTime := time.Now()

	// Download checkpoint chain metadata from S3 (pages come from page-server)
	// This downloads ALL checkpoints in the chain (all parent dumps' metadata)
	// to preserve directory structure needed for --prev-images-dir references
	if err := m.s3Client.DownloadMetadataOnly(ctx, s3Prefix, m.workDir); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to download checkpoint metadata: %w", err)
	}

	// The final dump directory where restore will run
	dumpDir := filepath.Join(m.workDir, dumpID)

	// Use external mounts from dump (passed via gRPC from controller)
	// If not provided, use default list as fallback
	if len(externalMounts) == 0 {
		fmt.Printf("Warning: no external mounts provided, using defaults\n")
		externalMounts = map[string]string{
			"/etc/hosts":           "etc-hosts",
			"/etc/hostname":        "etc-hostname",
			"/etc/resolv.conf":     "etc-resolv.conf",
			"/dev/termination-log": "dev-termination-log",
			"/dev/shm":             "dev-shm",
			"/run/secrets/kubernetes.io/serviceaccount": "run-secrets-kubernetes.io-serviceaccount",
		}
	}
	fmt.Printf("Using %d external mounts for restore\n", len(externalMounts))
	for mountPoint, identifier := range externalMounts {
		fmt.Printf("  %s -> %s\n", mountPoint, identifier)
	}

	var pageServerPID int
	var lazyPagesCmd *exec.Cmd
	if useLazyPages {
		fmt.Printf("[DEBUG] Starting lazy-pages daemon to connect to source: %s:%d\n", sourceAddr, pageServerPort)

		// Controller already verified page-server is ready, start lazy-pages immediately
		// Start lazy-pages daemon that connects to source pod's page server
		pid, cmd, err := m.StartPageServer(ctx, pageServerPort, dumpDir, sourceAddr, s3Prefix)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to start lazy-pages: %w", err)
		}
		pageServerPID = pid
		lazyPagesCmd = cmd

		fmt.Printf("[DEBUG] Lazy-pages daemon started with PID: %d\n", pageServerPID)
		fmt.Printf("[DEBUG] Waiting 1 second for lazy-pages to be ready...\n")

		// Wait for lazy-pages to be ready
		time.Sleep(1 * time.Second)

		fmt.Printf("[DEBUG] Lazy-pages should be ready now\n")
	}

	// Find main container PID to join its namespaces
	mainPID, err := FindMainContainerPID()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to find main container PID: %w", err)
	}
	fmt.Printf("Using main container PID %d for namespace joining\n", mainPID)

	// Open file descriptor for PID namespace (required for --inherit-fd)
	pidNsPath := fmt.Sprintf("/proc/%d/ns/pid", mainPID)
	pidNsFd, err := os.Open(pidNsPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open PID namespace: %w", err)
	}
	defer pidNsFd.Close()

	// Build CRIU restore command using CRIU's native namespace joining
	// This approach:
	// 1. CRIU runs from agent container (has all dependencies - libcurl, libssl, etc.)
	// 2. Uses --inherit-fd to inject main container's PID and mount namespaces
	// 3. Uses --join-ns to join main container's OTHER namespaces (uts, ipc, net)
	// 4. Works cross-distribution (agent=Ubuntu, main=Alpine/Ubuntu/etc.)
	// 5. No mount manipulation needed - mount namespace is inherited, not restored
	args := []string{
		"restore",
		"-D", dumpDir,
		"--tcp-established",
		"--shell-job",
		"-v4",
		"--log-file", filepath.Join(dumpDir, "restore.log"),
		// NOTE: Do NOT use --mntns-compat-mode with --join-ns mnt
		// It causes CRIU to try umounting mounts, which fails with "Device busy"
	}

	// Inject PID namespace via file descriptor (matches --external from dump)
	// We use fd 3 because stdin=0, stdout=1, stderr=2, so first ExtraFiles fd is 3
	args = append(args,
		"--inherit-fd", "fd[3]:main_pidns",
	)

	// Join all other namespaces using --join-ns (including mount namespace!)
	// We fixed CRIU to properly support --join-ns mnt by clearing root_ns_mask
	args = append(args,
		"--join-ns", fmt.Sprintf("mnt:/proc/%d/ns/mnt", mainPID), // Mount namespace (FIXED in CRIU!)
		"--join-ns", fmt.Sprintf("uts:/proc/%d/ns/uts", mainPID), // UTS namespace (hostname)
		"--join-ns", fmt.Sprintf("ipc:/proc/%d/ns/ipc", mainPID), // IPC namespace
		"--join-ns", fmt.Sprintf("net:/proc/%d/ns/net", mainPID), // Network namespace
	)

	// External mount mappings for CRIU validation
	// Even with --join-ns mnt, CRIU reads the mount namespace image to validate it.
	// We need to provide mappings for all external mounts marked during dump.
	// The actual paths don't matter since we're not restoring mounts (due to --join-ns mnt).
	//
	// Note: Format reverses from dump: dump uses mnt[path]:id, restore uses mnt[id]:path
	// We DON'T include /dev here - let CRIU handle it normally (won't umount due to --join-ns mnt)
	args = append(args, "--external", "mnt[dev-termination-log]:/dev/termination-log")
	args = append(args, "--external", "mnt[etc-hosts]:/etc/hosts")
	args = append(args, "--external", "mnt[etc-hostname]:/etc/hostname")
	args = append(args, "--external", "mnt[etc-resolv-conf]:/etc/resolv.conf")

	// Auto-detect other external mounts (shared/slave mounts)
	args = append(args, "--ext-mount-map", "auto")

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

		// Add AWS credentials ONLY for express-one-zone
		// Regular S3 and CloudFront use IAM roles or public access
		if m.s3Client.isExpressOneZone() {
			awsAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
			awsSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
			if awsAccessKey != "" && awsSecretKey != "" {
				args = append(args,
					"--aws-access-key", awsAccessKey,
					"--aws-secret-key", awsSecretKey,
				)
			}
			args = append(args, "--express-one-zone")
		}
	}

	fmt.Printf("Executing CRIU restore with namespace joining: criu %s\n", strings.Join(args, " "))

	// Execute CRIU restore
	// IMPORTANT: criu restore becomes the parent process of the restored application
	// We MUST NOT call cmd.Wait() because that would terminate the restore process
	// and make the restored application an orphan
	// DO NOT use exec.CommandContext - process must survive independently
	cmd := exec.Command("criu", args...)
	cmd.ExtraFiles = []*os.File{pidNsFd} // Pass PID namespace fd at fd 3
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to start criu restore: %w", err)
	}

	fmt.Printf("[%s] [TARGET-AGENT] CRIU restore process started with PID %d\n",
		time.Now().Format("15:04:05.000"), cmd.Process.Pid)

	// Wait a bit to check if restore process fails immediately
	// If it's still running after 500ms, we assume restore started successfully
	time.Sleep(500 * time.Millisecond)

	// Check if process is still alive
	procPath := fmt.Sprintf("/proc/%d", cmd.Process.Pid)
	if _, err := os.Stat(procPath); os.IsNotExist(err) {
		// Process already terminated - read logs to see what went wrong
		restoreLog := m.readLogFile(filepath.Join(dumpDir, "restore.log"), 100)
		lazyPagesLog := m.readLogFile(filepath.Join(dumpDir, "lazy-pages.log"), 50)

		errorMsg := fmt.Sprintf("criu restore process terminated immediately\n\n=== CRIU Args ===\n%v\n\n=== Restore Log (last 100 lines) ===\n%s\n\n=== Lazy-Pages Log (last 50 lines) ===\n%s",
			args, restoreLog, lazyPagesLog)
		return nil, nil, nil, fmt.Errorf("%s", errorMsg)
	}

	fmt.Printf("[%s] [TARGET-AGENT] CRIU restore process is still running (PID %d) - restore successful\n",
		time.Now().Format("15:04:05.000"), cmd.Process.Pid)

	duration := time.Since(startTime)

	result := &RestoreResult{
		Success:       true,
		NewPID:        0, // Will be set by caller
		Timestamp:     time.Now(),
		DurationMs:    duration.Milliseconds(),
		PageServerPID: pageServerPID,
	}

	// Return both lazy-pages cmd and restore cmd - caller MUST store them to keep processes alive
	return result, lazyPagesCmd, cmd, nil
}

// StartPageServer starts the CRIU lazy-pages daemon that connects to source pod's page server
// Returns: PID, cmd, error
// The caller MUST store the returned cmd to keep the lazy-pages daemon alive
func (m *RestoreManager) StartPageServer(ctx context.Context, port int, checkpointDir, sourceAddr, s3Prefix string) (int, *exec.Cmd, error) {
	// Build CRIU lazy-pages command
	// NOTE: lazy-pages is a simple daemon that fetches pages from remote page-server and serves them locally
	// It runs in agent container's namespace and doesn't need to join main container's namespaces
	// Only the restore process needs --inherit-fd and --join-ns
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

		// Add object storage prefix (same as restore)
		if s3Prefix != "" {
			args = append(args,
				"--object-storage-object-prefix", s3Prefix+"/",
			)
		}

		// Add AWS credentials ONLY for express-one-zone
		// Regular S3 and CloudFront use IAM roles or public access
		if m.s3Client.isExpressOneZone() {
			awsAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
			awsSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
			if awsAccessKey != "" && awsSecretKey != "" {
				args = append(args,
					"--aws-access-key", awsAccessKey,
					"--aws-secret-key", awsSecretKey,
				)
			}
			args = append(args, "--express-one-zone")
		}
	}

	fmt.Printf("======================================\n")
	fmt.Printf("EXECUTING LAZY-PAGES COMMAND:\n")
	fmt.Printf("criu %s\n", strings.Join(args, " "))
	fmt.Printf("======================================\n")

	// Start lazy-pages daemon directly from agent container
	// CRIU will join main container's namespaces using --join-ns and --inherit-fd
	// DO NOT use exec.CommandContext - daemon must survive independently
	cmd := exec.Command("criu", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("[%s] [TARGET-AGENT] Starting lazy-pages daemon (will connect to source page-server %s:%d)...\n",
		time.Now().Format("15:04:05.000"), sourceAddr, port)
	if err := cmd.Start(); err != nil {
		return 0, nil, fmt.Errorf("failed to start lazy-pages daemon: %w", err)
	}
	fmt.Printf("[%s] [TARGET-AGENT] Lazy-pages daemon process started with PID: %d\n",
		time.Now().Format("15:04:05.000"), cmd.Process.Pid)

	// Wait for lazy-pages to be ready by monitoring the log file
	logPath := filepath.Join(checkpointDir, "lazy-pages.log")
	fmt.Printf("[%s] [TARGET-AGENT] Waiting for lazy-pages to connect to page-server...\n",
		time.Now().Format("15:04:05.000"))
	if err := m.waitForLazyPagesReady(logPath, 10*time.Second); err != nil {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return 0, nil, fmt.Errorf("lazy-pages failed to become ready: %w", err)
	}
	fmt.Printf("[%s] [TARGET-AGENT] ✓ Lazy-pages is ready and connected!\n",
		time.Now().Format("15:04:05.000"))

	// Find the lazy-pages daemon PID
	pid, err := m.findPageServerPID()
	if err != nil {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return 0, nil, fmt.Errorf("failed to find lazy-pages PID: %w", err)
	}

	// Return both PID and cmd - caller MUST store cmd to keep lazy-pages daemon alive
	return pid, cmd, nil
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
	// Use -n to get only the newest process, in case multiple matches exist
	cmd := exec.Command("pgrep", "-n", "-f", "criu lazy-pages")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("page-server process not found")
	}

	pidStr := strings.TrimSpace(string(output))
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse PID from '%s': %w", pidStr, err)
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

// verifyPageServerConnection checks if the source page-server is reachable
func (m *RestoreManager) verifyPageServerConnection(sourceAddr string, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	retryInterval := 500 * time.Millisecond
	connectTimeout := 2 * time.Second

	fmt.Printf("[DEBUG] Verifying page-server connection to %s:%d (timeout: %v)\n", sourceAddr, port, timeout)

	attempt := 0
	for time.Now().Before(deadline) {
		attempt++

		// Try to establish TCP connection
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", sourceAddr, port), connectTimeout)
		if err == nil {
			conn.Close()
			fmt.Printf("[DEBUG] Page-server connection verified (attempt %d)\n", attempt)
			return nil
		}

		fmt.Printf("[DEBUG] Page-server connection attempt %d failed: %v\n", attempt, err)

		// Wait before retry
		time.Sleep(retryInterval)
	}

	return fmt.Errorf("could not connect to page-server at %s:%d after %d attempts (timeout: %v)", sourceAddr, port, attempt, timeout)
}

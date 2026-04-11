package agent

import (
	"context"
	"fmt"
	"io"
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
func (m *RestoreManager) Restore(ctx context.Context, dumpID, s3Prefix string, useLazyPages bool, pageServerPort int, sourceAddr string, externalMounts map[string]string, strategy string, pipeInodes map[string]string) (*RestoreResult, *exec.Cmd, *exec.Cmd, error) {
	startTime := time.Now()

	// Download checkpoint data from storage
	if strategy == "full" {
		// full: download everything (metadata + pages) before restore
		fmt.Printf("[%s] [TARGET-AGENT] Downloading full checkpoint from storage (strategy: full, prefix: %s)\n",
			time.Now().Format("15:04:05.000"), s3Prefix)
		if err := m.s3Client.DownloadFullCheckpoint(ctx, s3Prefix, m.workDir); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to download full checkpoint: %w", err)
		}
		fmt.Printf("[%s] [TARGET-AGENT] Full checkpoint download completed\n",
			time.Now().Format("15:04:05.000"))
	} else {
		// lazy-storage/lazy-direct/lazy-hybrid: download metadata only
		if err := m.s3Client.DownloadMetadataOnly(ctx, s3Prefix, m.workDir); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to download checkpoint metadata: %w", err)
		}
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

	// Determine if lazy-pages is needed
	needsLazyPages := strategy == "lazy-storage" || strategy == "lazy-direct" || strategy == "lazy-hybrid"

	var pageServerPID int
	var lazyPagesCmd *exec.Cmd
	if needsLazyPages {
		// For lazy-storage: sourceAddr is empty (fetch from storage only)
		// For lazy-direct/lazy-hybrid: sourceAddr points to source page-server
		lazySourceAddr := sourceAddr
		if strategy == "lazy-storage" {
			lazySourceAddr = "" // no page-server, storage only
		}

		fmt.Printf("[DEBUG] Starting lazy-pages daemon (strategy: %s, source: %s)\n", strategy, lazySourceAddr)

		pid, cmd, err := m.StartPageServer(ctx, pageServerPort, dumpDir, lazySourceAddr, s3Prefix)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to start lazy-pages: %w", err)
		}
		pageServerPID = pid
		lazyPagesCmd = cmd

		fmt.Printf("[DEBUG] Lazy-pages daemon started with PID: %d\n", pageServerPID)

		// Wait for lazy-pages socket (fast polling instead of fixed sleep)
		socketPath := filepath.Join(dumpDir, "lazy-pages.socket")
		for i := 0; i < 50; i++ {
			if info, err := os.Stat(socketPath); err == nil && info.Mode()&os.ModeSocket != 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
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
	// NOTE: Do NOT defer Close() here. The fd must stay open for the lifetime
	// of the restored process. Caller is responsible for closing via RestoreResult.PidNsFd.

	// Build CRIU restore command using CRIU's native namespace joining
	// This approach:
	// 1. CRIU runs from agent container (has all dependencies - libcurl, libssl, etc.)
	// 2. Uses --inherit-fd to inject main container's PID and mount namespaces
	// 3. Uses --join-ns to join main container's OTHER namespaces (uts, ipc, net)
	// 4. Works cross-distribution (agent=Ubuntu, main=Alpine/Ubuntu/etc.)
	// 5. No mount manipulation needed - mount namespace is inherited, not restored
	pidFile := filepath.Join(dumpDir, "restored.pid")
	args := []string{
		"restore",
		"-D", dumpDir,
		"--pidfile", pidFile,
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

	// External mount mappings for CRIU validation.
	// Format reverses from dump: dump uses mnt[path]:label, restore uses mnt[label]:path.
	// externalMounts map is {mountPoint -> label} from dump, so we reverse for restore.
	for mountPoint, label := range externalMounts {
		args = append(args, "--external", fmt.Sprintf("mnt[%s]:%s", label, mountPoint))
	}

	// Auto-detect other external mounts (shared/slave mounts)
	args = append(args, "--ext-mount-map", "auto")

	if needsLazyPages {
		args = append(args, "--lazy-pages")
	}

	// Add S3/object storage options (use download endpoint for restore)
	// For "full" strategy, all files are already local — skip object-storage args.
	if strategy != "full" {
		args = append(args, m.s3Client.BuildCRIUObjectStorageArgs(s3Prefix)...)
	}

	// Replace dumped pipes with new pipe pairs to prevent SIGPIPE.
	// During dump, stdout/stderr pipes were marked external (pipe[inode]:stdout/stderr).
	// We create new pipe pairs and map them via --inherit-fd.
	// Agent holds the read-end to drain output and prevent SIGPIPE.
	extraFiles := []*os.File{pidNsFd} // fd3=pidns
	fdIndex := 4                       // next available fd for ExtraFiles

	// Replace dumped pipes with new pipe pairs using --inherit-fd fd[N]:pipe:[inode]
	// Format: pipe:[inode] (with colon before bracket, matching /proc/pid/fd symlink format)
	if stdoutInode, ok := pipeInodes["stdout"]; ok {
		stdoutR, stdoutW, err := os.Pipe()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
		}
		extraFiles = append(extraFiles, stdoutW)
		args = append(args, "--inherit-fd", fmt.Sprintf("fd[%d]:pipe:[%s]", fdIndex, stdoutInode))
		fmt.Printf("[TARGET-AGENT] Replacing stdout pipe:[%s] with new pipe at fd %d\n", stdoutInode, fdIndex)
		fdIndex++
		go func() {
			io.Copy(os.Stdout, stdoutR)
			stdoutR.Close()
		}()
	}
	if stderrInode, ok := pipeInodes["stderr"]; ok {
		stderrR, stderrW, err := os.Pipe()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create stderr pipe: %w", err)
		}
		extraFiles = append(extraFiles, stderrW)
		args = append(args, "--inherit-fd", fmt.Sprintf("fd[%d]:pipe:[%s]", fdIndex, stderrInode))
		fmt.Printf("[TARGET-AGENT] Replacing stderr pipe:[%s] with new pipe at fd %d\n", stderrInode, fdIndex)
		fdIndex++
		go func() {
			io.Copy(os.Stderr, stderrR)
			stderrR.Close()
		}()
	}

	fmt.Printf("Executing CRIU restore with namespace joining: criu %s\n", strings.Join(args, " "))

	// Execute CRIU restore
	// DO NOT use exec.CommandContext - process must survive independently of RPC context.
	// The caller MUST call cmd.Wait() in a goroutine to reap the process and prevent zombies.
	// Wait() does NOT kill the restored application — it only collects the exit status.
	cmd := exec.Command("criu", args...)
	cmd.ExtraFiles = extraFiles
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

	// Read restored process PID from pidfile (written by CRIU --pidfile)
	var restoredPID int32
	if pidData, err := os.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(pidData))); err == nil && pid > 0 {
			restoredPID = int32(pid)
			fmt.Printf("[%s] [TARGET-AGENT] Restored process PID (from pidfile): %d\n",
				time.Now().Format("15:04:05.000"), pid)
		}
	}

	result := &RestoreResult{
		Success:       true,
		NewPID:        restoredPID,
		Timestamp:     time.Now(),
		DurationMs:    duration.Milliseconds(),
		PageServerPID: pageServerPID,
		PidNsFd:       pidNsFd,
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
		"-v4",
		"--log-file", filepath.Join(checkpointDir, "lazy-pages.log"),
	}

	// Connect to source page-server only if sourceAddr is provided
	// For lazy-storage strategy, sourceAddr is empty — pages come from object storage only
	if sourceAddr != "" {
		args = append(args, "--page-server", "--address", sourceAddr, "--port", strconv.Itoa(port))
	}

	// Async prefetch configuration
	if os.Getenv("ASYNC_PREFETCH") == "true" {
		args = append(args, "--async-prefetch")

		// Prefetch workers (default: CRIU's built-in default of 4)
		if w := os.Getenv("PREFETCH_WORKERS"); w != "" {
			args = append(args, "--prefetch-workers", w)
		}

		// Hot VMA seeding (default: enabled when async-prefetch is on)
		if os.Getenv("NO_HOT_VMA_SEED") == "true" {
			args = append(args, "--no-hot-vma-seed")
		}
	}

	// Semi-sync IOV (independent of async prefetch — works with object storage)
	if os.Getenv("NO_SEMI_SYNC_IOV") == "true" {
		args = append(args, "--no-semi-sync-iov")
	}

	// Add S3/object storage options for lazy-pages
	args = append(args, m.s3Client.BuildCRIUObjectStorageArgs(s3Prefix)...)

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
	result, err := checkpointMgr.PreCheckpoint(ctx, pid, "", nil)
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
	PidNsFd       *os.File // PID namespace fd — caller must keep open until process exits
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

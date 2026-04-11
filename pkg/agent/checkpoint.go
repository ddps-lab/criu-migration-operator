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

	"github.com/ddps-lab/criu-migration-operator/pkg/profiler"
	"github.com/google/uuid"
)

// CRIUExcludeArgs holds address ranges to pass to CRIU for hot page skipping.
type CRIUExcludeArgs struct {
	ExcludeRanges  []profiler.AddrRange // --exclude-range (currently hot VMAs)
	NoParentRanges []profiler.AddrRange // --no-parent-range (hot→cold transition VMAs)
}

// CheckpointManager handles CRIU checkpoint operations
type CheckpointManager struct {
	workDir  string
	s3Client *S3Client
	podName  string
	appName  string // MigratableApp name (from migration.io/app label)
	nodeName string

	// Current checkpoint state
	lastCheckpointID string
	chainRoot        string
	chainDepth       int
	generation       int

	// Known main process PID (set by agent after startUserProcess)
	knownMainPID int
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(workDir string, s3Client *S3Client, podName, nodeName string) *CheckpointManager {
	// Read generation from environment
	generation := 0
	if genStr := os.Getenv("POD_GENERATION"); genStr != "" {
		if gen, err := strconv.Atoi(genStr); err == nil {
			generation = gen
		}
	}

	// Read MigratableApp name from pod annotations via Downward API
	// This should match the MigratableApp resource name for consistent S3 paths
	appName := podName // Default to pod name if label not found
	annotationsData, err := os.ReadFile("/etc/podinfo/annotations")
	if err == nil {
		// Parse annotations file (key="value" format, one per line)
		lines := strings.Split(string(annotationsData), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "migration.io/app=") {
				appName = strings.Trim(strings.TrimPrefix(line, "migration.io/app="), "\"")
				break
			}
		}
	}

	return &CheckpointManager{
		workDir:    workDir,
		s3Client:   s3Client,
		podName:    podName,
		appName:    appName,
		nodeName:   nodeName,
		generation: generation,
	}
}

// PreCheckpoint performs an incremental pre-checkpoint
func (m *CheckpointManager) PreCheckpoint(ctx context.Context, pid int, parentDumpID string, excludeArgs *CRIUExcludeArgs) (*CheckpointResult, error) {
	dumpID := m.generateDumpID()
	dumpDir := filepath.Join(m.workDir, dumpID)

	// Create dump directory
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create dump directory: %w", err)
	}

	// Build CRIU command
	args := []string{
		"pre-dump",
		"-t", strconv.Itoa(pid),
		"-D", dumpDir,
		"--root", fmt.Sprintf("/proc/%d/root", pid), // Use main container's filesystem root
		"--track-mem",
		"--leave-running",
		"-v4",
		"--log-file", filepath.Join(dumpDir, "criu.log"),
	}

	// Add parent reference for incremental dump
	if parentDumpID != "" {
		parentDir := filepath.Join(m.workDir, parentDumpID)
		if _, err := os.Stat(parentDir); err == nil {
			args = append(args, "--prev-images-dir", parentDir)
		}
	}

	// Add exclude args for hot page skipping
	args = appendExcludeArgs(args, excludeArgs, dumpDir)

	// Add S3 direct upload if enabled (CRIU uploads pages directly to S3)
	directUpload := os.Getenv("DIRECT_UPLOAD") == "true"
	if directUpload {
		s3Prefix := m.getS3Prefix(dumpID)
		args = append(args, m.s3Client.BuildCRIUUploadArgs(s3Prefix)...)
	}

	// Execute CRIU dump directly from agent container
	// We DON'T need nsenter for dump because:
	// 1. Shared PID namespace lets us see main container's processes
	// 2. SYS_PTRACE capability allows dumping processes
	// 3. Dump only reads process state, doesn't need main container's mount/network namespaces
	fmt.Printf("Executing pre-dump: criu %s\n", strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "criu", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("criu pre-dump failed: %w\nOutput: %s", err, string(output))
	}

	// Get checkpoint size
	size, err := m.getDirectorySize(dumpDir)
	if err != nil {
		size = 0
	}

	// Count pages
	pageCount, err := m.countPages(dumpDir)
	if err != nil {
		pageCount = 0
	}

	// Update state
	m.lastCheckpointID = dumpID
	if m.chainRoot == "" {
		m.chainRoot = dumpID
	}
	m.chainDepth++

	result := &CheckpointResult{
		DumpID:      dumpID,
		Timestamp:   time.Now(),
		SizeBytes:   size,
		PagesDumped: pageCount,
	}

	// Upload to S3
	if directUpload {
		// CRIU already uploaded — only upload agent-generated metadata
		s3Prefix := m.getS3Prefix(dumpID)
		m.uploadAgentMetadata(ctx, dumpDir, s3Prefix)
		fmt.Printf("Direct upload mode: CRIU uploaded checkpoint to S3: %s\n", s3Prefix)
	} else {
		// Go-side upload asynchronously
		go func() {
			uploadCtx := context.Background()
			s3Prefix := m.getS3Prefix(dumpID)
			if err := m.s3Client.UploadCheckpoint(uploadCtx, dumpDir, s3Prefix); err != nil {
				fmt.Printf("Failed to upload checkpoint to S3: %v\n", err)
				return
			}
			fmt.Printf("Successfully uploaded checkpoint to S3: %s\n", s3Prefix)
		}()
	}

	return result, nil
}

// FinalDump performs the final dump with page-server
// This returns immediately after starting the dump; CRIU runs in background as page-server
// The cmd parameter should be stored by the caller to keep the process alive
func (m *CheckpointManager) FinalDump(ctx context.Context, pid int, pageServerAddr string, pageServerPort int, parentDumpID string, excludeArgs *CRIUExcludeArgs, strategy string) (*CheckpointResult, *exec.Cmd, error) {
	dumpID := m.generateDumpID()
	dumpDir := filepath.Join(m.workDir, dumpID)

	// Create dump directory
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create dump directory: %w", err)
	}

	// Get external mounts dynamically from /proc/PID/mountinfo
	externalMounts, err := getExternalMounts(pid)
	if err != nil {
		fmt.Printf("Warning: failed to get external mounts: %v\n", err)
		externalMounts = make(map[string]string) // Continue with empty map
	}

	// Get PID namespace inode for external namespace marking
	pidNsPath := fmt.Sprintf("/proc/%d/ns/pid", pid)
	pidNsLink, err := os.Readlink(pidNsPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read PID namespace: %w", err)
	}
	// Extract inode from format "pid:[4026532508]"
	// Use strings.TrimPrefix and TrimSuffix for simple parsing
	pidNsInode := strings.TrimPrefix(pidNsLink, "pid:[")
	pidNsInode = strings.TrimSuffix(pidNsInode, "]")
	fmt.Printf("Detected PID namespace inode: %s (from %s)\n", pidNsInode, pidNsLink)

	// Determine if this strategy uses page-server (lazy-direct, lazy-hybrid)
	usePageServer := strategy == "lazy-direct" || strategy == "lazy-hybrid"

	// Direct upload: CRIU dumps directly to S3 (zero local disk I/O for pages)
	// Only for full/lazy-storage — page-server strategies need local pages
	directUpload := os.Getenv("DIRECT_UPLOAD") == "true" && !usePageServer

	// Build CRIU dump command
	args := []string{
		"dump",
		"-t", strconv.Itoa(pid),
		"-D", dumpDir,
		"--root", fmt.Sprintf("/proc/%d/root", pid),
		"--tcp-established",
		"--shell-job",
		"-v4",
		"--log-file", filepath.Join(dumpDir, "criu.log"),
		"--evasive-devices",
	}

	// Add page-server options only for lazy-direct/lazy-hybrid strategies
	var pidFile string
	if usePageServer {
		pidFile = filepath.Join(dumpDir, "page-server.pid")
		args = append(args,
			"--pidfile", pidFile,
			"--lazy-pages",
			"--address", "0.0.0.0",
			"--port", strconv.Itoa(pageServerPort),
		)
	}

	// Add S3 direct upload args — CRIU streams pages to S3 during dump
	if directUpload {
		s3Prefix := m.getS3Prefix(dumpID)
		args = append(args, m.s3Client.BuildCRIUUploadArgs(s3Prefix)...)
	}

	// Mark PID namespace as external (will be injected via inherit-fd during restore)
	args = append(args, "--external", fmt.Sprintf("pid[%s]:main_pidns", pidNsInode))

	// Mark K8s-injected bind mounts as external (dynamically detected from mountinfo).
	// These mounts exist in both source and target pods, so CRIU should not try to
	// save/restore them — instead they are mapped via --external on restore.
	for mountPoint, label := range externalMounts {
		args = append(args, "--external", fmt.Sprintf("mnt[%s]:%s", mountPoint, label))
	}

	// Auto-detect shared/slave mounts that getExternalMounts might miss
	args = append(args, "--external", "mnt[]:ms")

	// Record stdout/stderr pipe inodes for restore-time replacement.
	// In K8s, these are containerd log-collection pipes. After restore in a different pod,
	// the pipe read-end doesn't exist → SIGPIPE kills the process.
	// We record the inodes here and use --inherit-fd pipe:[inode] on restore to replace them.
	// Note: We do NOT mark them as --external during dump. CRIU dumps them normally,
	// and --inherit-fd on restore replaces the pipe with a new one.
	pipeInodes := make(map[string]string) // "stdout" -> "232671"
	fdLabels := map[int]string{1: "stdout", 2: "stderr"}
	for fd, label := range fdLabels {
		linkPath := fmt.Sprintf("/proc/%d/fd/%d", pid, fd)
		link, err := os.Readlink(linkPath)
		if err == nil && strings.HasPrefix(link, "pipe:[") {
			inode := strings.TrimPrefix(link, "pipe:[")
			inode = strings.TrimSuffix(inode, "]")
			pipeInodes[label] = inode
			fmt.Printf("[SOURCE-AGENT] Recorded pipe fd %d inode %s as %s\n", fd, inode, label)
		}
	}

	// Add parent reference for incremental dump
	if parentDumpID != "" {
		parentDir := filepath.Join(m.workDir, parentDumpID)
		if _, err := os.Stat(parentDir); err == nil {
			args = append(args, "--prev-images-dir", parentDir)
		}
	}

	// Add exclude args for hot page skipping (final dump: has_parent=false for hot VMAs)
	args = appendExcludeArgs(args, excludeArgs, dumpDir)

	fmt.Printf("CRIU dump args: %v\n", args)

	// Execute CRIU dump directly from agent container
	// We DON'T need nsenter for dump - only for restore
	fmt.Printf("======================================\n")
	fmt.Printf("EXECUTING FINAL DUMP COMMAND:\n")
	fmt.Printf("criu %s\n", strings.Join(args, " "))
	fmt.Printf("======================================\n")

	cmd := exec.Command("criu", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	var pageServerPID int
	var pageServerCmd *exec.Cmd

	if usePageServer {
		// lazy-direct / lazy-hybrid: Start dump in background, CRIU becomes page-server
		if err := cmd.Start(); err != nil {
			return nil, nil, fmt.Errorf("failed to start criu dump: %w", err)
		}
		criuPID := cmd.Process.Pid
		fmt.Printf("[%s] [SOURCE-AGENT] Started CRIU dump (page-server mode) with PID: %d\n",
			time.Now().Format("15:04:05.000"), criuPID)

		if err := m.waitForCheckpointFiles(dumpDir, 30*time.Second); err != nil {
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
			criuLog := m.readLogFile(filepath.Join(dumpDir, "criu.log"), 100)
			return nil, nil, fmt.Errorf("failed to wait for checkpoint files: %w\n\nCRIU Log:\n%s", err, criuLog)
		}

		// Read page-server PID from pidfile
		pageServerPID = criuPID
		if pidFile != "" {
			if pidData, err := os.ReadFile(pidFile); err == nil {
				if pid, err := strconv.Atoi(strings.TrimSpace(string(pidData))); err == nil && pid > 0 {
					pageServerPID = pid
				}
			}
		}
		fmt.Printf("[%s] [SOURCE-AGENT] Page-server PID: %d\n",
			time.Now().Format("15:04:05.000"), pageServerPID)
		pageServerCmd = cmd
	} else {
		// full / lazy-storage: Run dump synchronously, no page-server
		fmt.Printf("[%s] [SOURCE-AGENT] Running CRIU dump (strategy: %s, no page-server)\n",
			time.Now().Format("15:04:05.000"), strategy)
		if err := cmd.Run(); err != nil {
			criuLog := m.readLogFile(filepath.Join(dumpDir, "criu.log"), 100)
			return nil, nil, fmt.Errorf("criu dump failed: %w\nCRIU Log:\n%s", err, criuLog)
		}
		fmt.Printf("[%s] [SOURCE-AGENT] CRIU dump completed successfully\n",
			time.Now().Format("15:04:05.000"))
	}

	// Get checkpoint size
	size, err := m.getDirectorySize(dumpDir)
	if err != nil {
		size = 0
	}

	result := &CheckpointResult{
		DumpID:          dumpID,
		Timestamp:       time.Now(),
		SizeBytes:       size,
		ExternalMounts:  externalMounts,
		PipeInodes:      pipeInodes,
		PageServerPID:   pageServerPID,
		PageServerAlive: pageServerPID > 0,
	}

	// Storage upload: sync for full/lazy-storage, async for lazy-hybrid, skip for lazy-direct
	s3Prefix := m.getS3Prefix(dumpID)
	switch strategy {
	case "full", "lazy-storage":
		if directUpload {
			// CRIU already uploaded pages + metadata to S3.
			// Only upload agent-generated files (hot-vmas.json, hot_vma_metadata.json)
			// that CRIU doesn't know about.
			fmt.Printf("[%s] [SOURCE-AGENT] Direct upload mode — CRIU uploaded to S3, uploading agent metadata...\n",
				time.Now().Format("15:04:05.000"))
			m.uploadAgentMetadata(ctx, dumpDir, s3Prefix)
		} else {
			// Synchronous upload — must complete before returning
			fmt.Printf("[%s] [SOURCE-AGENT] Uploading checkpoint to storage (synchronous)...\n",
				time.Now().Format("15:04:05.000"))
			if err := m.s3Client.UploadCheckpoint(ctx, dumpDir, s3Prefix); err != nil {
				return nil, nil, fmt.Errorf("failed to upload checkpoint to storage: %w", err)
			}
			fmt.Printf("[%s] [SOURCE-AGENT] Storage upload completed: %s\n",
				time.Now().Format("15:04:05.000"), s3Prefix)
		}
	case "lazy-hybrid":
		// Async upload — page-server serves pages, storage is fallback
		go func() {
			uploadCtx := context.Background()
			if err := m.s3Client.UploadCheckpoint(uploadCtx, dumpDir, s3Prefix); err != nil {
				fmt.Printf("Failed to upload checkpoint to storage: %v\n", err)
				return
			}
			fmt.Printf("Successfully uploaded checkpoint to storage: %s\n", s3Prefix)
		}()
	case "lazy-direct":
		// Upload checkpoint metadata (pages served via page-server, but target needs
		// metadata files to initialize lazy-pages daemon).
		// With --lazy-pages dump, pages-*.img files are minimal (lazy stubs).
		fmt.Printf("[%s] [SOURCE-AGENT] Uploading checkpoint metadata to storage (lazy-direct)\n",
			time.Now().Format("15:04:05.000"))
		go func() {
			uploadCtx := context.Background()
			if err := m.s3Client.UploadCheckpoint(uploadCtx, dumpDir, s3Prefix); err != nil {
				fmt.Printf("Failed to upload checkpoint to storage: %v\n", err)
				return
			}
			fmt.Printf("Successfully uploaded checkpoint to storage: %s\n", s3Prefix)
		}()
	}

	fmt.Printf("[DEBUG] FinalDump returning (strategy: %s, pageServerPID: %d)\n", strategy, pageServerPID)
	return result, pageServerCmd, nil
}

// uploadAgentMetadata uploads agent-generated files (hot-vmas.json, etc.)
// that CRIU's direct upload doesn't handle.
func (m *CheckpointManager) uploadAgentMetadata(ctx context.Context, dumpDir, s3Prefix string) {
	agentFiles := []string{"hot-vmas.json", "hot_vma_metadata.json"}
	for _, name := range agentFiles {
		path := filepath.Join(dumpDir, name)
		if _, err := os.Stat(path); err != nil {
			continue
		}
		s3Key := s3Prefix + "/" + name
		if err := m.s3Client.UploadFile(ctx, path, s3Key); err != nil {
			fmt.Printf("Warning: failed to upload %s: %v\n", name, err)
		} else {
			fmt.Printf("Uploaded agent metadata: %s\n", s3Key)
		}
	}
}

// waitForCheckpointFiles waits for essential checkpoint files to be created
func (m *CheckpointManager) waitForCheckpointFiles(dumpDir string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	// Essential file patterns that must exist (except pages-*.img which goes to page-server)
	// CRIU creates files like core-PID.img, so we use pattern matching
	// Note: stats-dump is not created in lazy-pages mode
	requiredPatterns := []string{"core-*.img", "inventory.img"}

	for time.Now().Before(deadline) {
		allExist := true
		for _, pattern := range requiredPatterns {
			searchPath := filepath.Join(dumpDir, pattern)

			matches, err := filepath.Glob(searchPath)
			if err != nil || len(matches) == 0 {
				allExist = false
				break
			}
		}

		if allExist {
			// Give CRIU a bit more time to stabilize
			time.Sleep(500 * time.Millisecond)
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for checkpoint files to be created")
}

// SetMainPID stores a known-good PID for the main user process.
func (m *CheckpointManager) SetMainPID(pid int) {
	m.knownMainPID = pid
}

// FindMainProcessPID finds the PID of the main application process
func (m *CheckpointManager) FindMainProcessPID() (int, error) {
	// First: check if we have a known PID and it's still alive
	if m.knownMainPID > 0 {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d/status", m.knownMainPID)); err == nil {
			return m.knownMainPID, nil
		}
		// PID died, clear it
		m.knownMainPID = 0
	}

	// Fallback: common process names to look for
	processNames := []string{
		"python",
		"python3",
		"node",
		"java",
		"ruby",
		"go",
		"bash",
		"sh",
	}

	for _, procName := range processNames {
		cmd := exec.Command("pgrep", "-f", procName)
		output, err := cmd.Output()
		if err == nil && len(output) > 0 {
			pidStr := strings.TrimSpace(string(output))
			lines := strings.Split(pidStr, "\n")
			if len(lines) > 0 {
				pid, err := strconv.Atoi(lines[0])
				if err == nil {
					return pid, nil
				}
			}
		}
	}

	return 0, fmt.Errorf("main process not found")
}

// WaitForSleepProcess waits for a sleep process to appear (for restore mode)
func (m *CheckpointManager) WaitForSleepProcess(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		cmd := exec.Command("pgrep", "sleep")
		output, err := cmd.Output()
		if err == nil && len(output) > 0 {
			pidStr := strings.TrimSpace(string(output))
			lines := strings.Split(pidStr, "\n")
			if len(lines) > 0 {
				pid, err := strconv.Atoi(lines[0])
				if err == nil {
					return pid, nil
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return 0, fmt.Errorf("sleep process not found within timeout")
}

// CheckpointExists checks if a checkpoint exists locally
func (m *CheckpointManager) CheckpointExists(dumpID string) bool {
	dumpDir := filepath.Join(m.workDir, dumpID)
	_, err := os.Stat(dumpDir)
	return err == nil
}

// CleanupOldCheckpoints removes old checkpoint directories
func (m *CheckpointManager) CleanupOldCheckpoints(keepRecent int) error {
	entries, err := os.ReadDir(m.workDir)
	if err != nil {
		return fmt.Errorf("failed to read work directory: %w", err)
	}

	// Sort by modification time (newest first)
	var checkpoints []os.DirEntry
	for _, entry := range entries {
		if entry.IsDir() {
			checkpoints = append(checkpoints, entry)
		}
	}

	// Keep only recent checkpoints
	if len(checkpoints) > keepRecent {
		for i := keepRecent; i < len(checkpoints); i++ {
			dirPath := filepath.Join(m.workDir, checkpoints[i].Name())
			if err := os.RemoveAll(dirPath); err != nil {
				fmt.Printf("Failed to remove old checkpoint %s: %v\n", dirPath, err)
			}
		}
	}

	return nil
}

// generateDumpID generates a unique checkpoint ID
func (m *CheckpointManager) generateDumpID() string {
	return fmt.Sprintf("%s-%d", uuid.New().String()[:8], time.Now().Unix())
}

// getS3Prefix returns the S3 prefix for a checkpoint
// Format: checkpoints/{app-name}/{generation}/{node-name}/{checkpoint-id}
func (m *CheckpointManager) getS3Prefix(dumpID string) string {
	// Use appName (MigratableApp name) instead of podName for consistent S3 paths
	// This ensures gen0, gen1, gen2... all use the same base path: checkpoints/my-web-app/...
	return fmt.Sprintf("checkpoints/%s/%d/%s/%s", m.appName, m.generation, m.nodeName, dumpID)
}

// getDirectorySize calculates the total size of a directory
func (m *CheckpointManager) getDirectorySize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// countPages counts the number of pages in a checkpoint
func (m *CheckpointManager) countPages(dumpDir string) (int64, error) {
	// Look for pages-*.img files
	pattern := filepath.Join(dumpDir, "pages-*.img")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, err
	}

	var totalPages int64
	for _, file := range matches {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		// Approximate: 4KB per page
		totalPages += info.Size() / 4096
	}

	return totalPages, nil
}

// GetLastCheckpointID returns the last checkpoint ID
func (m *CheckpointManager) GetLastCheckpointID() string {
	return m.lastCheckpointID
}

// GetChainDepth returns the current checkpoint chain depth
func (m *CheckpointManager) GetChainDepth() int {
	return m.chainDepth
}

// GetChainRoot returns the root checkpoint ID
func (m *CheckpointManager) GetChainRoot() string {
	return m.chainRoot
}

// ResetChain resets the checkpoint chain
// Cleanup will be handled after the first checkpoint of the new chain is created
func (m *CheckpointManager) ResetChain() {
	// Save the old chain root for cleanup after new checkpoint is created
	oldChainRoot := m.chainRoot

	// Reset chain state immediately
	m.chainRoot = ""
	m.chainDepth = 0

	// Note: We don't delete old checkpoints here to avoid race conditions.
	// The cleanup should happen AFTER the new checkpoint is created successfully.
	// For now, we rely on:
	// 1. MigratableApp deletion to clean up all S3 checkpoints (via finalizer)
	// 2. Periodic cleanup if needed (could be added later)

	fmt.Printf("Reset checkpoint chain (old root: %s)\n", oldChainRoot)
}

// readLogFile reads the last N lines of a log file for debugging
func (m *CheckpointManager) readLogFile(path string, maxLines int) string {
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

// CheckpointResult contains the result of a checkpoint operation
type CheckpointResult struct {
	DumpID          string
	Timestamp       time.Time
	SizeBytes       int64
	PagesDumped     int64
	ExternalMounts  map[string]string // mountpoint -> identifier
	PipeInodes      map[string]string // fd label -> inode (e.g., "stdout" -> "232671")
	PageServerPID   int               // PID of page-server process (for final dump)
	PageServerAlive bool              // Whether page-server is still running
}

// appendExcludeArgs appends --exclude-range/--exclude-file and --no-parent-range args to CRIU command.
func appendExcludeArgs(args []string, excludeArgs *CRIUExcludeArgs, dumpDir string) []string {
	if excludeArgs == nil {
		return args
	}

	// Exclude ranges: use --exclude-file for large sets, --exclude-range for small sets
	if len(excludeArgs.ExcludeRanges) > 10 {
		excludePath := filepath.Join(dumpDir, "exclude-ranges.txt")
		if err := writeExcludeFile(excludePath, excludeArgs.ExcludeRanges); err != nil {
			fmt.Printf("Warning: failed to write exclude file: %v\n", err)
		} else {
			args = append(args, "--exclude-file", excludePath)
		}
	} else {
		for _, r := range excludeArgs.ExcludeRanges {
			args = append(args, "--exclude-range", fmt.Sprintf("%x:%x", r.Start, r.End))
		}
	}

	// No-parent ranges (hot→cold transition)
	for _, r := range excludeArgs.NoParentRanges {
		args = append(args, "--no-parent-range", fmt.Sprintf("%x:%x", r.Start, r.End))
	}

	return args
}

// writeExcludeFile writes address ranges to a file in space-separated format (for --exclude-file).
// Format: "start_hex end_hex" per line (CRIU reads via fscanf "%lx %lx").
func writeExcludeFile(path string, ranges []profiler.AddrRange) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, r := range ranges {
		if _, err := fmt.Fprintf(f, "%x %x\n", r.Start, r.End); err != nil {
			return err
		}
	}
	return nil
}

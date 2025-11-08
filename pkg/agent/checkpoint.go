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

	"github.com/google/uuid"
)

// CheckpointManager handles CRIU checkpoint operations
type CheckpointManager struct {
	workDir  string
	s3Client *S3Client
	podName  string
	nodeName string

	// Current checkpoint state
	lastCheckpointID string
	chainRoot        string
	chainDepth       int
	generation       int
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

	return &CheckpointManager{
		workDir:    workDir,
		s3Client:   s3Client,
		podName:    podName,
		nodeName:   nodeName,
		generation: generation,
	}
}

// PreCheckpoint performs an incremental pre-checkpoint
func (m *CheckpointManager) PreCheckpoint(ctx context.Context, pid int, parentDumpID string) (*CheckpointResult, error) {
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

	// Execute CRIU
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

	// Upload to S3 asynchronously (keep all checkpoints in chain)
	go func() {
		uploadCtx := context.Background()
		s3Prefix := m.getS3Prefix(dumpID)
		if err := m.s3Client.UploadCheckpoint(uploadCtx, dumpDir, s3Prefix); err != nil {
			fmt.Printf("Failed to upload checkpoint to S3: %v\n", err)
			return
		}
		fmt.Printf("Successfully uploaded checkpoint to S3: %s\n", s3Prefix)
	}()

	return result, nil
}

// FinalDump performs the final dump with page-server
// This returns immediately after starting the dump; CRIU runs in background as page-server
func (m *CheckpointManager) FinalDump(ctx context.Context, pid int, pageServerAddr string, pageServerPort int, parentDumpID string) (*CheckpointResult, error) {
	dumpID := m.generateDumpID()
	dumpDir := filepath.Join(m.workDir, dumpID)

	// Create dump directory
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create dump directory: %w", err)
	}

	// Get external mounts dynamically from /proc/PID/mountinfo
	externalMounts, err := getExternalMounts(pid)
	if err != nil {
		fmt.Printf("Warning: failed to get external mounts: %v\n", err)
		externalMounts = make(map[string]string) // Continue with empty map
	}

	// Build CRIU command with lazy-pages (this pod acts as page server)
	args := []string{
		"dump",
		"-t", strconv.Itoa(pid),
		"-D", dumpDir,
		"--lazy-pages",
		"--address", "0.0.0.0", // Listen on all interfaces
		"--port", strconv.Itoa(pageServerPort),
		"--tcp-established",
		"--shell-job",
		"-v4",
		"--log-file", filepath.Join(dumpDir, "criu.log"),
		"--manage-cgroups=ignore", // Use 'ignore' mode: don't deal with cgroups and pretend they don't exist
		"--evasive-devices",
	}

	// Add external mount options dynamically
	// Format: --external mnt[/mountpoint]:identifier
	for mountPoint, identifier := range externalMounts {
		args = append(args, "--external", fmt.Sprintf("mnt[%s]:%s", mountPoint, identifier))
	}

	// Add parent reference for incremental dump
	if parentDumpID != "" {
		parentDir := filepath.Join(m.workDir, parentDumpID)
		if _, err := os.Stat(parentDir); err == nil {
			args = append(args, "--prev-images-dir", parentDir)
		}
	}

	fmt.Printf("CRIU dump args: %v\n", args)

	// Start CRIU dump in background (it will act as page-server)
	cmd := exec.CommandContext(ctx, "criu", args...)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start criu dump: %w", err)
	}

	// Wait for checkpoint metadata files to be created (not for process to complete)
	// This indicates CRIU has started and created the checkpoint structure
	if err := m.waitForCheckpointFiles(dumpDir, 30*time.Second); err != nil {
		// Try to kill the CRIU process if it failed
		if cmd.Process != nil {
			cmd.Process.Kill()
		}

		// Read CRIU log for debugging
		criuLog := m.readLogFile(filepath.Join(dumpDir, "criu.log"), 100)
		return nil, fmt.Errorf("failed to wait for checkpoint files: %w\n\nCRIU Log:\n%s", err, criuLog)
	}

	// Get metadata size (pages sent via page-server)
	size, err := m.getDirectorySize(dumpDir)
	if err != nil {
		size = 0
	}

	result := &CheckpointResult{
		DumpID:    dumpID,
		Timestamp: time.Now(),
		SizeBytes: size,
	}

	// Upload metadata only to S3 asynchronously (keep all checkpoints in chain)
	go func() {
		uploadCtx := context.Background()
		s3Prefix := m.getS3Prefix(dumpID)
		if err := m.s3Client.UploadMetadataOnly(uploadCtx, dumpDir, s3Prefix); err != nil {
			fmt.Printf("Failed to upload metadata to S3: %v\n", err)
			return
		}
		fmt.Printf("Successfully uploaded metadata to S3: %s\n", s3Prefix)
	}()

	return result, nil
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

// FindMainProcessPID finds the PID of the main application process
func (m *CheckpointManager) FindMainProcessPID() (int, error) {
	// Common process names to look for
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
	return fmt.Sprintf("checkpoints/%s/%d/%s/%s", m.podName, m.generation, m.nodeName, dumpID)
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
	DumpID      string
	Timestamp   time.Time
	SizeBytes   int64
	PagesDumped int64
}

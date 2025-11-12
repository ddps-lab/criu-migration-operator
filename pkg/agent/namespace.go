package agent

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// FindMainContainerPID finds the PID of the main container's init process
// in a shared PID namespace environment.
// It returns the PID of the first non-agent container process.
func FindMainContainerPID() (int, error) {
	logrus.Info("Searching for main container PID in shared namespace...")

	// Get current process PID to exclude agent processes
	agentPID := os.Getpid()
	logrus.Debugf("Agent PID: %d", agentPID)

	// Read /proc directory
	entries, err := ioutil.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("failed to read /proc: %w", err)
	}

	var candidates []struct {
		pid     int
		cmdline string
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Parse PID from directory name
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		// Skip PID 1 (pause container), agent itself, and kernel threads
		if pid <= 1 {
			continue
		}

		// Read cmdline to identify the process
		cmdlinePath := fmt.Sprintf("/proc/%d/cmdline", pid)
		cmdlineBytes, err := ioutil.ReadFile(cmdlinePath)
		if err != nil {
			// Process might have exited
			continue
		}

		cmdline := string(cmdlineBytes)
		// cmdline uses null bytes as separators
		cmdline = strings.ReplaceAll(cmdline, "\x00", " ")
		cmdline = strings.TrimSpace(cmdline)

		if cmdline == "" {
			// Kernel thread
			continue
		}

		logrus.Debugf("Found process PID %d: %s", pid, cmdline)

		// Skip agent processes
		if strings.Contains(cmdline, "criu-agent") ||
			strings.Contains(cmdline, "/app/agent") {
			logrus.Debugf("Skipping agent process: %d", pid)
			continue
		}

		// Skip CRIU processes
		if strings.Contains(cmdline, "criu") {
			logrus.Debugf("Skipping criu process: %d", pid)
			continue
		}

		candidates = append(candidates, struct {
			pid     int
			cmdline string
		}{pid: pid, cmdline: cmdline})
	}

	if len(candidates) == 0 {
		return 0, fmt.Errorf("no main container process found (agent PID: %d)", agentPID)
	}

	// Prefer "sleep infinity" if found (target pod placeholder)
	// Otherwise return the first non-agent process
	for _, candidate := range candidates {
		if strings.Contains(candidate.cmdline, "sleep") {
			logrus.Infof("Found main container PID: %d (%s)", candidate.pid, candidate.cmdline)
			return candidate.pid, nil
		}
	}

	// Fallback: return first candidate
	mainPID := candidates[0].pid
	mainCmdline := candidates[0].cmdline

	logrus.Infof("Found main container PID: %d (%s)", mainPID, mainCmdline)

	return mainPID, nil
}

// VerifyMainContainerPID verifies that a PID exists and is not the agent
func VerifyMainContainerPID(pid int) error {
	if pid <= 1 {
		return fmt.Errorf("invalid PID: %d", pid)
	}

	// Check if process exists
	cmdlinePath := fmt.Sprintf("/proc/%d/cmdline", pid)
	cmdlineBytes, err := ioutil.ReadFile(cmdlinePath)
	if err != nil {
		return fmt.Errorf("process %d does not exist: %w", pid, err)
	}

	cmdline := string(cmdlineBytes)
	cmdline = strings.ReplaceAll(cmdline, "\x00", " ")

	// Verify it's not an agent process
	if strings.Contains(cmdline, "criu-agent") || strings.Contains(cmdline, "/app/agent") {
		return fmt.Errorf("PID %d is an agent process, not main container", pid)
	}

	logrus.Infof("Verified main container PID %d: %s", pid, strings.TrimSpace(cmdline))
	return nil
}

// GetMainContainerRoot returns the root directory path for the main container
// This is used by CRIU's --root option to specify the target root directory
func GetMainContainerRoot(pid int) string {
	return fmt.Sprintf("/proc/%d/root", pid)
}

package agent

import (
	"bufio"
	"fmt"
	"os"
)

// getExternalMounts reads /proc/PID/mountinfo and returns external mount options for CRIU
// Returns a map of mountpoint -> identifier for use in --external mnt[...] options
func getExternalMounts(pid int) (map[string]string, error) {
	mountInfoPath := fmt.Sprintf("/proc/%d/mountinfo", pid)
	file, err := os.Open(mountInfoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open mountinfo: %w", err)
	}
	defer file.Close()

	externalMounts := make(map[string]string)
	scanner := bufio.NewScanner(file)

	// TEMPORARY TEST: Disable ALL external mount detection
	// When using --join-ns mnt, the joined namespace already has all mounts
	// so marking them as external might cause conflicts
	for scanner.Scan() {
		// Just consume the scanner, don't add any mounts
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading mountinfo: %w", err)
	}

	fmt.Printf("Total external mounts detected for PID %d: %d\n", pid, len(externalMounts))
	return externalMounts, nil
}

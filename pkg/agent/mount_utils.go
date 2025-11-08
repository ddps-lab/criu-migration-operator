package agent

import (
	"bufio"
	"fmt"
	"os"
	"strings"
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

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		mountPoint := fields[4]

		// Kubernetes-specific external mounts that need to be preserved
		// These are bind mounts from host into container
		if strings.HasPrefix(mountPoint, "/etc/") ||
			strings.HasPrefix(mountPoint, "/dev/termination-log") ||
			strings.HasPrefix(mountPoint, "/run/secrets/kubernetes.io/serviceaccount") ||
			strings.HasPrefix(mountPoint, "/dev/shm") {

			// Create a simple identifier from the mountpoint path
			// e.g., "/etc/resolv.conf" -> "etc-resolv-conf"
			identifier := strings.ReplaceAll(strings.Trim(mountPoint, "/"), "/", "-")
			externalMounts[mountPoint] = identifier
			fmt.Printf("Detected external mount: %s -> %s\n", mountPoint, identifier)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading mountinfo: %w", err)
	}

	fmt.Printf("Total external mounts detected for PID %d: %d\n", pid, len(externalMounts))
	return externalMounts, nil
}

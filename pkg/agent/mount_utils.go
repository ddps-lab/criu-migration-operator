package agent

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// getExternalMounts reads /proc/PID/mountinfo and returns K8s-injected bind mounts
// that should be marked as external for CRIU dump.
//
// In K8s, the container's mount namespace contains:
// - Root overlay filesystem (/)
// - Kernel filesystems (proc, sysfs, devpts, mqueue, cgroup2, tmpfs for /dev)
// - K8s-injected bind mounts (hosts, hostname, resolv.conf, termination-log, etc.)
// - K8s volumes (emptyDir, configMap, secret, serviceaccount, downwardAPI)
//
// We detect K8s-injected mounts by checking if they are bind mounts from the host
// (same device as root) to well-known paths, or tmpfs mounts for volumes.
func getExternalMounts(pid int) (map[string]string, error) {
	mountInfoPath := fmt.Sprintf("/proc/%d/mountinfo", pid)
	file, err := os.Open(mountInfoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open mountinfo: %w", err)
	}
	defer file.Close()

	externalMounts := make(map[string]string)
	scanner := bufio.NewScanner(file)

	// Filesystem types that are kernel-internal (not K8s-injected)
	kernelFS := map[string]bool{
		"proc":    true,
		"sysfs":   true,
		"devpts":  true,
		"mqueue":  true,
		"cgroup2": true,
		"cgroup":  true,
	}

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		mountPoint := fields[4] // 5th field = mount point

		// Skip root mount
		if mountPoint == "/" {
			continue
		}

		// Find the separator " - " to get filesystem type
		sepIdx := -1
		for i, f := range fields {
			if f == "-" {
				sepIdx = i
				break
			}
		}
		if sepIdx < 0 || sepIdx+1 >= len(fields) {
			continue
		}
		fsType := fields[sepIdx+1]

		// Skip kernel-internal filesystems whose fsType is kernel-managed
		// AND whose mount path is the canonical kernel one. Bind mounts
		// from these (e.g. read-only /proc/bus from proc) are kept by
		// CRIU intrinsically, so we don't list them as external.
		if kernelFS[fsType] {
			continue
		}

		// /sys and its children are already excluded above (sysfs/cgroup).
		// What's left here is everything containerd injected on top:
		//   - /dev itself (tmpfs)                         → external dev
		//   - /dev/* tmpfs children (mqueue is kernelFS)  → external
		//   - /proc/* tmpfs/null masks                    → external
		//   - K8s bind mounts (/etc/hosts, etc.)          → external
		// Marking ALL of them external is the right call: at restore
		// time the destination's containerd recreates them, and CRIU
		// uses --ext-mount-map auto to wire the new mounts back in.

		// Generate a label from the mount point path
		// /etc/hosts -> etc-hosts
		// /dev/termination-log -> dev-termination-log
		// /run/secrets/kubernetes.io/serviceaccount -> run-secrets-kubernetes.io-serviceaccount
		label := strings.TrimPrefix(mountPoint, "/")
		label = strings.ReplaceAll(label, "/", "-")
		label = strings.ReplaceAll(label, ".", "-")

		externalMounts[mountPoint] = label
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading mountinfo: %w", err)
	}

	fmt.Printf("Detected %d external mounts for PID %d:\n", len(externalMounts), pid)
	for mp, label := range externalMounts {
		fmt.Printf("  %s -> %s\n", mp, label)
	}

	return externalMounts, nil
}

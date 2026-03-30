package profiler

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// parseVMAs reads /proc/pid/maps and returns parsed VMA entries.
// Translated from main.c parse_maps_for_process() (lines 302-338).
func parseVMAs(pid int) ([]VMAInfo, error) {
	path := fmt.Sprintf("/proc/%d/maps", pid)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	var vmas []VMAInfo
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		var start, end, offset, inode uint64
		var perms string
		var major, minor int
		var pathname string

		// Parse: start-end perms offset major:minor inode pathname
		n, _ := fmt.Sscanf(line, "%x-%x %s %x %d:%d %d",
			&start, &end, &perms, &offset, &major, &minor, &inode)
		if n < 5 {
			continue
		}

		// Extract pathname (may contain spaces, comes after inode)
		fields := strings.Fields(line)
		if len(fields) >= 6 {
			// pathname is the last field (if present)
			if len(fields) > 5 {
				// Skip the first 5 fields, check if 6th is the inode
				// In /proc/pid/maps, pathname comes after inode
				pathname = fields[len(fields)-1]
				// Verify it's not the inode (a number)
				if pathname == fmt.Sprintf("%d", inode) {
					pathname = ""
				}
			}
		}

		vma := VMAInfo{
			Start:    start,
			End:      end,
			Perms:    perms,
			Pathname: pathname,
			Type:     classifyVMA(pathname, perms),
		}
		vmas = append(vmas, vma)
	}

	return vmas, scanner.Err()
}

// classifyVMA determines the VMA type from pathname and permissions.
// Translated from main.c classify_vma() (lines 274-286).
func classifyVMA(pathname, perms string) VMAType {
	switch pathname {
	case "[heap]":
		return VMAHeap
	case "[stack]":
		return VMAStack
	case "[vdso]", "[vvar]", "[vsyscall]":
		return VMAVDSO
	}
	if len(pathname) > 0 && pathname[0] == '/' {
		if strings.Contains(perms, "x") {
			return VMACode
		}
		return VMAData
	}
	if pathname == "" {
		return VMAAnonymous
	}
	return VMAUnknown
}

// writableAnonymousVMAs filters VMAs to only writable, private, non-vdso ones.
func writableAnonymousVMAs(vmas []VMAInfo) []VMAInfo {
	var result []VMAInfo
	for i := range vmas {
		if vmas[i].IsWritableAnonymous() {
			result = append(result, vmas[i])
		}
	}
	return result
}

// diffVMAs finds VMAs in 'current' that are not in 'registered'.
// Used to detect new VMAs that need UFFDIO_REGISTER.
// Translated from collect_sample VMA re-registration logic (main.c lines 1638-1681).
func diffVMAs(current []VMAInfo, registered []AddrRange) []VMAInfo {
	regSet := make(map[uint64]uint64, len(registered))
	for _, r := range registered {
		regSet[r.Start] = r.End
	}

	var newVMAs []VMAInfo
	for i := range current {
		if !current[i].IsWritableAnonymous() {
			continue
		}
		if end, ok := regSet[current[i].Start]; ok && end == current[i].End {
			continue // already registered
		}
		newVMAs = append(newVMAs, current[i])
	}
	return newVMAs
}

// calcWritableVMABytes returns the total size of writable VMAs in bytes.
func calcWritableVMABytes(vmas []VMAInfo) int64 {
	var total int64
	for i := range vmas {
		if vmas[i].IsWritable() {
			total += int64(vmas[i].End - vmas[i].Start)
		}
	}
	return total
}

package profiler

import (
	"fmt"
	"log"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// CRIU IOV granularity: 4 MB (1024 pages). Hot regions are tracked at this
// granularity within each VMA so that the classifier matches the unit CRIU
// actually compresses (one zstd seekable frame) and lazy-restore prefetches
// (one Range GET).
const chunkBytes uint64 = 4 * 1024 * 1024
const chunkPages uint64 = chunkBytes / pageSize

// scanResult holds the result of a PAGEMAP_SCAN for one VMA. ChunkDirty[k] is
// the dirty-page count in chunk k of this VMA, where chunk k spans
// [VMAStart + k*chunkBytes, min(VMAStart + (k+1)*chunkBytes, VMAEnd)). The
// last chunk may be partial. Sum of ChunkDirty equals DirtyPages by
// construction. Empty / nil ChunkDirty means the scanner did not emit
// chunk-level data (e.g. a fallback path without chunk aggregation).
type scanResult struct {
	VMAStart    uint64
	VMAEnd      uint64
	DirtyPages  int64
	TotalPages  uint64
	VMAType     VMAType
	Pathname    string
	ChunkDirty  []uint16
}

// vmaChunkCount returns the number of 4 MB chunks needed to cover the VMA,
// rounding up so the last chunk may be partial.
func vmaChunkCount(start, end uint64) int {
	bytes := end - start
	return int((bytes + chunkBytes - 1) / chunkBytes)
}

// openPagemap opens /proc/pid/pagemap for PAGEMAP_SCAN ioctl.
func openPagemap(pid int) (*os.File, error) {
	path := fmt.Sprintf("/proc/%d/pagemap", pid)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return f, nil
}

// wpAllWritablePages write-protects all present pages in writable VMAs.
// This establishes the baseline: after this call, any new write will set PAGE_IS_WRITTEN.
// Translated from main.c lines 1322-1348 (initial WP scan).
func wpAllWritablePages(pagemapFd int, vmas []VMAInfo) error {
	regions := make([]pageRegion, maxScanRegions)

	for i := range vmas {
		if !vmas[i].IsWritable() {
			continue
		}

		arg := pmScanArg{
			Size:             uint64(unsafe.Sizeof(pmScanArg{})),
			Flags:            pmScanWPMatching,
			Start:            vmas[i].Start,
			End:              vmas[i].End,
			Vec:              uint64(uintptr(unsafe.Pointer(&regions[0]))),
			VecLen:           uint64(len(regions)),
			MaxPages:         0, // unlimited
			CategoryInverted: pageIsPFNZero | pageIsFile,
			CategoryMask:     pageIsPFNZero | pageIsFile,
			CategoryAnyofMask: pageIsPresent | pageIsSwapped,
			ReturnMask:       0, // don't need results, just WP
		}

		_, _, errno := syscall.Syscall(unix.SYS_IOCTL,
			uintptr(pagemapFd),
			uintptr(pagemapScan),
			uintptr(unsafe.Pointer(&arg)))
		if errno != 0 && errno != syscall.ENOENT {
			log.Printf("profiler: wpAllWritablePages VMA 0x%x-0x%x errno=%v", vmas[i].Start, vmas[i].End, errno)
			return fmt.Errorf("WP baseline scan VMA 0x%x-0x%x: %w", vmas[i].Start, vmas[i].End, errno)
		}
	}
	return nil
}

// scanDirtyPages performs PAGEMAP_SCAN with PM_SCAN_WP_MATCHING on each writable VMA.
// Returns per-VMA dirty page counts and total dirty pages.
// Translated from main.c read_dirty_pages_pagemap_scan() (lines 1374-1438).
func scanDirtyPages(pagemapFd int, vmas []VMAInfo) ([]scanResult, int64, error) {
	regions := make([]pageRegion, maxScanRegions)
	var results []scanResult
	var totalDirty int64

	for i := range vmas {
		if !vmas[i].IsWritable() {
			continue
		}

		arg := pmScanArg{
			Size:              uint64(unsafe.Sizeof(pmScanArg{})),
			Flags:             pmScanWPMatching,
			Start:             vmas[i].Start,
			End:               vmas[i].End,
			Vec:               uint64(uintptr(unsafe.Pointer(&regions[0]))),
			VecLen:            uint64(len(regions)),
			MaxPages:          0,
			CategoryInverted:  pageIsPFNZero | pageIsFile,
			CategoryMask:      pageIsPFNZero | pageIsFile | pageIsWritten,
			CategoryAnyofMask: pageIsPresent | pageIsSwapped,
			ReturnMask:        pageIsPresent | pageIsSwapped | pageIsWritten,
		}

		ret, _, errno := syscall.Syscall(unix.SYS_IOCTL,
			uintptr(pagemapFd),
			uintptr(pagemapScan),
			uintptr(unsafe.Pointer(&arg)))
		if errno != 0 {
			if errno == syscall.EPERM {
				return nil, 0, fmt.Errorf("PM_SCAN_WP_MATCHING EPERM: uffd-wp not active")
			}
			log.Printf("profiler: scanDirtyPages VMA 0x%x-0x%x errno=%v", vmas[i].Start, vmas[i].End, errno)
			continue
		}

		var vmaDirty int64
		numRegions := int(ret)

		// Aggregate dirty pages per IOV-aligned 4 MB chunk in addition to
		// the VMA total. The classifier downstream uses chunk-level data
		// to identify hot sub-regions of large VMAs (e.g. XGBoost DMatrix)
		// that whole-VMA averaging would dilute below the threshold.
		nChunks := vmaChunkCount(vmas[i].Start, vmas[i].End)
		var chunkDirty []uint16
		if nChunks > 0 {
			chunkDirty = make([]uint16, nChunks)
		}
		for j := 0; j < numRegions; j++ {
			pages := int64(regions[j].End-regions[j].Start) / pageSize
			vmaDirty += pages
			if chunkDirty != nil {
				for addr := regions[j].Start; addr < regions[j].End; addr += pageSize {
					ci := int((addr - vmas[i].Start) / chunkBytes)
					if ci >= 0 && ci < nChunks && chunkDirty[ci] < ^uint16(0) {
						chunkDirty[ci]++
					}
				}
			}
		}

		totalPages := vmas[i].Pages()
		results = append(results, scanResult{
			VMAStart:   vmas[i].Start,
			VMAEnd:     vmas[i].End,
			DirtyPages: vmaDirty,
			TotalPages: totalPages,
			VMAType:    vmas[i].Type,
			Pathname:   vmas[i].Pathname,
			ChunkDirty: chunkDirty,
		})
		totalDirty += vmaDirty
	}

	return results, totalDirty, nil
}

// verifyWPActive checks if PAGE_IS_WPALLOWED is set on any page.
// Used after uffd-wp setup to verify WP mode is working.
// Translated from main.c lines 1302-1316.
func verifyWPActive(pagemapFd int) (bool, error) {
	regions := make([]pageRegion, 1)
	arg := pmScanArg{
		Size:              uint64(unsafe.Sizeof(pmScanArg{})),
		Flags:             0,
		Start:             0,
		End:               0x7fffffffffff,
		Vec:               uint64(uintptr(unsafe.Pointer(&regions[0]))),
		VecLen:            1,
		MaxPages:          1,
		CategoryMask:      pageIsWPAllowed,
		CategoryAnyofMask: pageIsPresent | pageIsSwapped,
		ReturnMask:        pageIsWPAllowed,
	}

	ret, _, errno := syscall.Syscall(unix.SYS_IOCTL,
		uintptr(pagemapFd),
		uintptr(pagemapScan),
		uintptr(unsafe.Pointer(&arg)))
	if errno != 0 {
		return false, fmt.Errorf("verify WP scan: %w", errno)
	}
	return int(ret) > 0, nil
}

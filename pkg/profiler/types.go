package profiler

import "unsafe"

// Page size constant
const pageSize = 4096

// PAGEMAP_SCAN ioctl number: _IOWR('f', 16, 96)
// pm_scan_arg is 12 × uint64 = 96 bytes
const pagemapScan = 0xC0606610

// PAGEMAP_SCAN flags (main.c lines 47-48)
const (
	pmScanWPMatching   = 1 << 0
	pmScanCheckWPAsync = 1 << 1
)

// Page category flags (main.c lines 51-58)
const (
	pageIsWPAllowed  = 1 << 0
	pageIsWritten    = 1 << 1
	pageIsFile       = 1 << 2
	pageIsPresent    = 1 << 3
	pageIsSwapped    = 1 << 4
	pageIsPFNZero    = 1 << 5
	pageIsHuge       = 1 << 6
	pageIsSoftDirty  = 1 << 7
)

// UFFD constants (from /usr/include/linux/userfaultfd.h)
const (
	uffdUserModeOnly         = 1
	uffdFeatureWPAsync       = 1 << 15
	uffdFeatureWPUnpopulated = 1 << 13
)

// UFFD API version
const uffdAPI = 0xAA

// UFFDIO ioctl numbers (computed from _IOWR/_IOW macros)
const (
	uffdioAPIIoctl          = 0xC018AA3F // _IOWR(0xAA, 0x3F, 24)
	uffdioRegisterIoctl     = 0xC020AA00 // _IOWR(0xAA, 0x00, 32)
	uffdioUnregisterIoctl   = 0x4010AA01 // _IOW(0xAA, 0x01, 16)
	uffdioWriteprotectIoctl = 0x4018AA06 // _IOW(0xAA, 0x06, 24)
)

// UFFDIO register modes
const (
	uffdioRegisterModeWP = 1 << 1
)

// Syscall numbers (x86_64)
const (
	sysUserfaultfd = 323
	sysMmap        = 9
	sysMunmap      = 11
	sysIoctl       = 16
	sysClose       = 3
	sysPidfdOpen   = 434
	sysPidfdGetfd  = 438
)

// syscall instruction bytes (x86_64): 0x0F 0x05
const syscallInsn = 0x050F

// Maximum regions per scan
const maxScanRegions = 65536

// Sliding window size for heat classification
const windowSize = 10

// pmScanArg matches struct pm_scan_arg (main.c lines 66-79)
// 12 × uint64 = 96 bytes
type pmScanArg struct {
	Size             uint64
	Flags            uint64
	Start            uint64
	End              uint64
	WalkEnd          uint64
	Vec              uint64 // pointer to pageRegion array
	VecLen           uint64
	MaxPages         uint64
	CategoryInverted uint64
	CategoryMask     uint64
	CategoryAnyofMask uint64
	ReturnMask       uint64
}

// Verify pmScanArg is 96 bytes at compile time
var _ [96]byte = [unsafe.Sizeof(pmScanArg{})]byte{}

// pageRegion matches struct page_region (main.c lines 60-64)
type pageRegion struct {
	Start      uint64
	End        uint64
	Categories uint64
}

// uffdioAPIStruct matches struct uffdio_api
type uffdioAPIStruct struct {
	API      uint64
	Features uint64
	Ioctls   uint64
}

// uffdioRange matches struct uffdio_range
type uffdioRange struct {
	Start uint64
	Len   uint64
}

// uffdioRegisterStruct matches struct uffdio_register
type uffdioRegisterStruct struct {
	Range  uffdioRange
	Mode   uint64
	Ioctls uint64
}

// uffdioWriteprotectStruct matches struct uffdio_writeprotect
type uffdioWriteprotectStruct struct {
	Range uffdioRange
	Mode  uint64
}

// VMAType classifies the type of a VMA
type VMAType int

const (
	VMAHeap      VMAType = iota
	VMAStack
	VMAAnonymous
	VMACode
	VMAData
	VMAVDSO
	VMAUnknown
)

// String returns the string representation of a VMAType.
func (t VMAType) String() string {
	switch t {
	case VMAHeap:
		return "heap"
	case VMAStack:
		return "stack"
	case VMAAnonymous:
		return "anonymous"
	case VMACode:
		return "code"
	case VMAData:
		return "data"
	case VMAVDSO:
		return "vdso"
	default:
		return "unknown"
	}
}

// VMAInfo represents a parsed VMA entry from /proc/pid/maps
type VMAInfo struct {
	Start    uint64
	End      uint64
	Perms    string
	Pathname string
	Type     VMAType
}

// IsWritable returns true if this VMA has write permission
func (v *VMAInfo) IsWritable() bool {
	for _, c := range v.Perms {
		if c == 'w' {
			return true
		}
	}
	return false
}

// IsPrivate returns true if this VMA is private (not shared)
func (v *VMAInfo) IsPrivate() bool {
	for _, c := range v.Perms {
		if c == 'p' {
			return true
		}
	}
	return false
}

// IsWritableAnonymous returns true if this VMA is writable, private, and not vdso/vvar
func (v *VMAInfo) IsWritableAnonymous() bool {
	return v.IsWritable() && v.IsPrivate() && v.Type != VMAVDSO
}

// Pages returns the number of pages in this VMA
func (v *VMAInfo) Pages() uint64 {
	return (v.End - v.Start) / pageSize
}

// HotRegion represents a hot memory region identified by the profiler
type HotRegion struct {
	StartAddr      uint64
	EndAddr        uint64
	WrittenRatio   float64
	ConsecutiveHot int
}

// DirtyVolume represents the current dirty page statistics
type DirtyVolume struct {
	DirtyPages           int64   // dirty pages in last scan interval
	DirtyBytes           int64   // dirty bytes in last scan interval (DirtyPages * 4096)
	DirtyRatePerSec      float64 // bytes per second (not pages) in last scan interval
	CumulativeDirtyBytes int64   // total dirty bytes since last ReinitAfterCRIU
	AvgDirtyRate         float64 // bytes per second average since profiler start
	TimestampMs          int64   // unix milliseconds of last update
}

// AddrRange represents an address range for CRIU exclude args
type AddrRange struct {
	Start uint64
	End   uint64
}

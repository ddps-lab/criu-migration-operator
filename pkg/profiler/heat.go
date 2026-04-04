package profiler

// heatClassifier tracks per-VMA write heat using a sliding window.
// Each VMA gets a circular buffer of recent written ratios.
// A VMA is "hot" if its ratio exceeds hotThreshold for hotConsecutive consecutive intervals.
type heatClassifier struct {
	hotThreshold   float64
	hotConsecutive int
	states         map[uint64]*vmaHeatState // keyed by VMA start address
}

// vmaHeatState holds sliding-window state for one VMA.
type vmaHeatState struct {
	Start          uint64
	End            uint64
	Ratios         [windowSize]float64 // circular buffer
	Head           int                 // next write position
	Count          int                 // number of samples added
	ConsecutiveHot int                 // consecutive intervals above threshold
	IsHot          bool
	LastDirty      int64   // dirty pages in most recent scan
	LastTotal      uint64  // total pages in most recent scan
	VMAType        VMAType // VMA type (heap, anonymous, etc.)
	Pathname       string  // VMA pathname (e.g., "[heap]", "/lib/x.so")
}

func newHeatClassifier(hotThreshold float64, hotConsecutive int) *heatClassifier {
	return &heatClassifier{
		hotThreshold:   hotThreshold,
		hotConsecutive: hotConsecutive,
		states:         make(map[uint64]*vmaHeatState),
	}
}

// update processes scan results and updates heat state for each VMA.
// Returns the current list of hot regions.
func (h *heatClassifier) update(results []scanResult) []HotRegion {
	// Mark all existing states as not-seen this round
	seen := make(map[uint64]bool, len(results))

	for _, r := range results {
		seen[r.VMAStart] = true

		state, ok := h.states[r.VMAStart]
		if !ok {
			state = &vmaHeatState{
				Start: r.VMAStart,
				End:   r.VMAEnd,
			}
			h.states[r.VMAStart] = state
		}
		// Update address range in case VMA was resized
		state.End = r.VMAEnd

		// Store dirty/total pages and VMA info for this interval
		state.LastDirty = r.DirtyPages
		state.LastTotal = r.TotalPages
		state.VMAType = r.VMAType
		state.Pathname = r.Pathname

		// Calculate written ratio for this interval
		var ratio float64
		if r.TotalPages > 0 {
			ratio = float64(r.DirtyPages) / float64(r.TotalPages)
		}

		// Add to sliding window
		state.Ratios[state.Head] = ratio
		state.Head = (state.Head + 1) % windowSize
		if state.Count < windowSize {
			state.Count++
		}

		// Check consecutive hot
		if ratio > h.hotThreshold {
			state.ConsecutiveHot++
		} else {
			state.ConsecutiveHot = 0
		}

		state.IsHot = state.ConsecutiveHot >= h.hotConsecutive
	}

	// Remove states for VMAs that no longer exist
	for addr := range h.states {
		if !seen[addr] {
			delete(h.states, addr)
		}
	}

	// Collect hot regions
	var hot []HotRegion
	for _, state := range h.states {
		if state.IsHot {
			// Get most recent ratio
			idx := (state.Head - 1 + windowSize) % windowSize
			hot = append(hot, HotRegion{
				StartAddr:      state.Start,
				EndAddr:        state.End,
				WrittenRatio:   state.Ratios[idx],
				ConsecutiveHot: state.ConsecutiveHot,
			})
		}
	}
	return hot
}

// reset clears all heat state. Called when profiling is restarted.
func (h *heatClassifier) reset() {
	h.states = make(map[uint64]*vmaHeatState)
}

// totalVMAs returns the number of tracked VMAs.
func (h *heatClassifier) totalVMAs() int {
	return len(h.states)
}

// VMAHotDetail contains per-VMA hot/cold classification with dirty stats.
type VMAHotDetail struct {
	Start          uint64
	End            uint64
	Type           string // "heap", "anonymous", "data", "stack", etc.
	Pathname       string
	IsHot          bool
	DirtyPages     int64
	TotalPages     uint64
	DirtyRatio     float64
	ConsecutiveHot int
}

// getAllVMAs returns all tracked VMAs with their current hot/cold classification.
func (h *heatClassifier) getAllVMAs() []VMAHotDetail {
	result := make([]VMAHotDetail, 0, len(h.states))
	for _, state := range h.states {
		var ratio float64
		if state.Count > 0 {
			idx := (state.Head - 1 + windowSize) % windowSize
			ratio = state.Ratios[idx]
		}
		result = append(result, VMAHotDetail{
			Start:          state.Start,
			End:            state.End,
			Type:           state.VMAType.String(),
			Pathname:       state.Pathname,
			IsHot:          state.IsHot,
			DirtyPages:     state.LastDirty,
			TotalPages:     state.LastTotal,
			DirtyRatio:     ratio,
			ConsecutiveHot: state.ConsecutiveHot,
		})
	}
	return result
}

// hotVMAs returns the number of hot VMAs.
func (h *heatClassifier) hotVMAs() int {
	count := 0
	for _, s := range h.states {
		if s.IsHot {
			count++
		}
	}
	return count
}

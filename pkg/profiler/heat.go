package profiler

// heatClassifier tracks per-chunk write heat using a sliding window. Each VMA
// is split into IOV-aligned 4 MB chunks (matching CRIU's compress/lazy-fetch
// granularity); each chunk is independently classified as hot / cold from its
// own write history. A chunk is "hot" if its dirty/total ratio exceeds
// hotThreshold for hotConsecutive consecutive intervals. VMAs <= 4 MB form a
// single chunk, preserving the previous per-VMA semantics for small VMAs.
type heatClassifier struct {
	hotThreshold   float64
	hotConsecutive int
	states         map[chunkKey]*chunkHeatState
}

// chunkKey uniquely identifies one 4 MB chunk within a VMA.
type chunkKey struct {
	VMAStart uint64
	ChunkIdx int
}

// chunkHeatState holds sliding-window state for one 4 MB chunk.
type chunkHeatState struct {
	VMAStart       uint64
	VMAEnd         uint64
	ChunkIdx       int
	ChunkStart     uint64
	ChunkEnd       uint64
	Ratios         [windowSize]float64
	Head           int
	Count          int
	ConsecutiveHot int
	IsHot          bool
	LastDirty      int64
	LastTotal      uint64 // pages in this chunk
	VMAType        VMAType
	Pathname       string
}

func newHeatClassifier(hotThreshold float64, hotConsecutive int) *heatClassifier {
	return &heatClassifier{
		hotThreshold:   hotThreshold,
		hotConsecutive: hotConsecutive,
		states:         make(map[chunkKey]*chunkHeatState),
	}
}

// update processes scan results and updates heat state for each chunk.
// Returns the current list of hot chunks as HotRegion entries.
func (h *heatClassifier) update(results []scanResult) []HotRegion {
	seen := make(map[chunkKey]bool, len(results))

	for _, r := range results {
		nChunks := vmaChunkCount(r.VMAStart, r.VMAEnd)
		if nChunks == 0 {
			continue
		}
		for ci := 0; ci < nChunks; ci++ {
			chunkStart := r.VMAStart + uint64(ci)*chunkBytes
			chunkEnd := chunkStart + chunkBytes
			if chunkEnd > r.VMAEnd {
				chunkEnd = r.VMAEnd
			}
			chunkPages := (chunkEnd - chunkStart) / pageSize

			var dirty int64
			if r.ChunkDirty != nil && ci < len(r.ChunkDirty) {
				dirty = int64(r.ChunkDirty[ci])
			}

			key := chunkKey{r.VMAStart, ci}
			seen[key] = true

			state, ok := h.states[key]
			if !ok {
				state = &chunkHeatState{
					VMAStart: r.VMAStart,
					ChunkIdx: ci,
				}
				h.states[key] = state
			}
			state.VMAEnd = r.VMAEnd
			state.ChunkStart = chunkStart
			state.ChunkEnd = chunkEnd
			state.LastDirty = dirty
			state.LastTotal = chunkPages
			state.VMAType = r.VMAType
			state.Pathname = r.Pathname

			var ratio float64
			if chunkPages > 0 {
				ratio = float64(dirty) / float64(chunkPages)
			}

			state.Ratios[state.Head] = ratio
			state.Head = (state.Head + 1) % windowSize
			if state.Count < windowSize {
				state.Count++
			}

			if ratio > h.hotThreshold {
				state.ConsecutiveHot++
			} else {
				state.ConsecutiveHot = 0
			}

			state.IsHot = state.ConsecutiveHot >= h.hotConsecutive
		}
	}

	for k := range h.states {
		if !seen[k] {
			delete(h.states, k)
		}
	}

	var hot []HotRegion
	for _, state := range h.states {
		if state.IsHot {
			idx := (state.Head - 1 + windowSize) % windowSize
			hot = append(hot, HotRegion{
				StartAddr:      state.ChunkStart,
				EndAddr:        state.ChunkEnd,
				WrittenRatio:   state.Ratios[idx],
				ConsecutiveHot: state.ConsecutiveHot,
			})
		}
	}
	return hot
}

func (h *heatClassifier) reset() {
	h.states = make(map[chunkKey]*chunkHeatState)
}

// totalChunks returns the number of tracked 4 MB chunks.
func (h *heatClassifier) totalChunks() int {
	return len(h.states)
}

// totalVMAs returns the number of distinct VMAs covered by the tracked chunks.
func (h *heatClassifier) totalVMAs() int {
	vmaSet := make(map[uint64]struct{}, len(h.states))
	for _, s := range h.states {
		vmaSet[s.VMAStart] = struct{}{}
	}
	return len(vmaSet)
}

// VMAHotDetail contains per-chunk hot/cold classification with dirty stats.
// One entry per tracked 4 MB chunk; consumers that want per-VMA aggregates
// can group by Start (the chunk start) modulo chunkBytes.
type VMAHotDetail struct {
	Start          uint64 // chunk start address
	End            uint64 // chunk end address (chunk-bound, possibly < VMA end for last chunk)
	Type           string
	Pathname       string
	IsHot          bool
	DirtyPages     int64
	TotalPages     uint64
	DirtyRatio     float64
	ConsecutiveHot int
}

// getAllVMAs returns all tracked chunks with their current classification.
// (Name retained for protobuf/RPC compatibility; entries are now chunks.)
func (h *heatClassifier) getAllVMAs() []VMAHotDetail {
	result := make([]VMAHotDetail, 0, len(h.states))
	for _, state := range h.states {
		var ratio float64
		if state.Count > 0 {
			idx := (state.Head - 1 + windowSize) % windowSize
			ratio = state.Ratios[idx]
		}
		result = append(result, VMAHotDetail{
			Start:          state.ChunkStart,
			End:            state.ChunkEnd,
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

// hotVMAs returns the number of hot chunks (preserves the old method name to
// avoid touching callers; semantically this is now the count of hot chunks).
func (h *heatClassifier) hotVMAs() int {
	count := 0
	for _, s := range h.states {
		if s.IsHot {
			count++
		}
	}
	return count
}

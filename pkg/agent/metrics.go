package agent

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// LazyPagesMetrics holds parsed metrics from criu-lazy-pages.log.
type LazyPagesMetrics struct {
	// Fault counts
	TotalFaults int
	S3Faults    int
	CacheFaults int

	// Stall time (ms)
	StallAvg float64
	StallP50 float64
	StallMax float64

	// S3 vs cache stall breakdown
	S3StallAvg    float64
	S3StallMax    float64
	CacheStallAvg float64
	CacheStallMax float64

	// Pages per fault
	PagesPerFaultAvg float64
	PagesPerFaultMax int

	// UFFD transfer
	TotalPagesTransferred int
	TotalPagesExpected    int

	// Prefetch
	CacheHitRate      float64
	PrefetchCompleted int

	// Daemon
	DaemonDurationS float64
}

// faultEvent represents a single page fault event.
type faultEvent struct {
	stallMs float64
	source  string // "S3" or "CACHE"
	pages   int
}

var (
	tsRe         = regexp.MustCompile(`^\((\d+\.\d+)\)`)
	faultStartRe = regexp.MustCompile(`uffd: (\d+)-+\d+: === PAGE FAULT at (0x[0-9a-f]+)`)
	faultEndRe   = regexp.MustCompile(`uffd: (\d+)-+\d+: === PAGE FAULT SERVED from (\S+)`)
	uffdCopyRe   = regexp.MustCompile(`uffd: (\d+)-+\d+: uffd_copy: 0x[0-9a-f]+/(\d+)`)
	transferRe   = regexp.MustCompile(`UFFD transferred pages: \((\d+)/(\d+)\)`)
	cacheStatsRe = regexp.MustCompile(`Cache stats: lookups=(\d+) hits=(\d+) misses=(\d+) hit_rate=([\d.]+)%`)
	prefetchRe   = regexp.MustCompile(`STATS requests=(\d+) completed=(\d+) failed=(\d+)`)
)

// ParseLazyPagesLog parses a criu-lazy-pages.log file and extracts metrics.
func ParseLazyPagesLog(logPath string) (*LazyPagesMetrics, error) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, fmt.Errorf("read lazy-pages log: %w", err)
	}

	content := string(data)
	lines := strings.Split(content, "\n")

	var (
		events       []faultEvent
		pendingFault = map[string]float64{} // pid → timestamp
		pendingBytes = map[string]int{}     // pid → bytes
		maxTs        float64
	)

	metrics := &LazyPagesMetrics{}

	for _, line := range lines {
		tsMatch := tsRe.FindStringSubmatch(line)
		var ts float64
		if len(tsMatch) > 1 {
			ts, _ = strconv.ParseFloat(tsMatch[1], 64)
			if ts > maxTs {
				maxTs = ts
			}
		}

		// Fault start
		if m := faultStartRe.FindStringSubmatch(line); len(m) > 0 {
			pid := m[1]
			pendingFault[pid] = ts
			pendingBytes[pid] = 0
		}

		// uffd_copy with size
		if m := uffdCopyRe.FindStringSubmatch(line); len(m) > 0 {
			pid := m[1]
			sz, _ := strconv.Atoi(m[2])
			pendingBytes[pid] += sz
		}

		// Fault served
		if m := faultEndRe.FindStringSubmatch(line); len(m) > 0 {
			pid := m[1]
			source := m[2] // "S3" or "CACHE"
			if startTs, ok := pendingFault[pid]; ok {
				stallMs := (ts - startTs) * 1000
				bytes := pendingBytes[pid]
				events = append(events, faultEvent{
					stallMs: stallMs,
					source:  source,
					pages:   bytes / 4096,
				})
				delete(pendingFault, pid)
				delete(pendingBytes, pid)
			}
		}

		// UFFD transferred
		if m := transferRe.FindStringSubmatch(line); len(m) > 0 {
			transferred, _ := strconv.Atoi(m[1])
			expected, _ := strconv.Atoi(m[2])
			metrics.TotalPagesTransferred += transferred
			metrics.TotalPagesExpected += expected
		}

		// Cache stats
		if m := cacheStatsRe.FindStringSubmatch(line); len(m) > 0 {
			metrics.CacheHitRate, _ = strconv.ParseFloat(m[4], 64)
		}

		// Prefetch stats
		if m := prefetchRe.FindStringSubmatch(line); len(m) > 0 {
			metrics.PrefetchCompleted, _ = strconv.Atoi(m[2])
		}
	}

	// If no per-fault events parsed, count PAGE FAULT keywords as fallback
	if len(events) == 0 {
		metrics.TotalFaults = strings.Count(content, "PAGE FAULT at")
	} else {
		metrics.TotalFaults = len(events)

		var allStalls, s3Stalls, cacheStalls []float64
		var allPages []int

		for _, e := range events {
			allStalls = append(allStalls, e.stallMs)
			if e.pages > 0 {
				allPages = append(allPages, e.pages)
			}
			switch e.source {
			case "S3":
				metrics.S3Faults++
				s3Stalls = append(s3Stalls, e.stallMs)
			case "CACHE":
				metrics.CacheFaults++
				cacheStalls = append(cacheStalls, e.stallMs)
			}
		}

		metrics.StallAvg = avg(allStalls)
		metrics.StallP50 = percentile(allStalls, 50)
		metrics.StallMax = max(allStalls)

		metrics.S3StallAvg = avg(s3Stalls)
		metrics.S3StallMax = max(s3Stalls)
		metrics.CacheStallAvg = avg(cacheStalls)
		metrics.CacheStallMax = max(cacheStalls)

		if len(allPages) > 0 {
			sum := 0
			for _, p := range allPages {
				sum += p
				if p > metrics.PagesPerFaultMax {
					metrics.PagesPerFaultMax = p
				}
			}
			metrics.PagesPerFaultAvg = float64(sum) / float64(len(allPages))
		}
	}

	metrics.DaemonDurationS = maxTs

	return metrics, nil
}

func avg(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return math.Round(sum/float64(len(vals))*1000) / 1000
}

func max(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return math.Round(m*1000) / 1000
}

func percentile(vals []float64, pct int) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	idx := len(sorted) * pct / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return math.Round(sorted[idx]*1000) / 1000
}

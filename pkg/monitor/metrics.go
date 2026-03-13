package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for spot instance interruption detection
var (
	// SpotDetectionLatency measures the time from poll start to annotation set
	SpotDetectionLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "spot_interrupt_detection_latency_seconds",
			Help:    "Latency of spot interrupt detection in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// SpotDetectionTotal counts spot interruption detections by method and result
	SpotDetectionTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spot_interrupt_detection_total",
			Help: "Total number of spot interruption detections",
		},
		[]string{"method", "result"},
	)

	// PodDrainDuration measures the time to drain a pod from spot interruption detection to new pod running
	PodDrainDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pod_drain_duration_seconds",
			Help:    "Time to drain pod from detection to new pod running in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"detection_method"},
	)

	// SpotDetectionAvailability tracks availability of detection methods
	SpotDetectionAvailability = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spot_detection_availability",
			Help: "Availability of spot detection method (1=available, 0=unavailable)",
		},
		[]string{"method"},
	)
)

// RecordSpotDetectionSuccess records a successful spot interruption detection
func RecordSpotDetectionSuccess(method string) {
	SpotDetectionTotal.WithLabelValues(method, "success").Inc()
}

// RecordSpotDetectionFailure records a failed spot interruption detection
func RecordSpotDetectionFailure(method string) {
	SpotDetectionTotal.WithLabelValues(method, "failure").Inc()
}

// RecordDetectionLatency records the detection latency
func RecordDetectionLatency(method string, latencySeconds float64) {
	SpotDetectionLatency.WithLabelValues(method).Observe(latencySeconds)
}

// RecordPodDrainDuration records the pod drain duration
func RecordPodDrainDuration(method string, durationSeconds float64) {
	PodDrainDuration.WithLabelValues(method).Observe(durationSeconds)
}

// SetDetectionMethodAvailability sets the availability status of a detection method
func SetDetectionMethodAvailability(method string, available bool) {
	value := 0.0
	if available {
		value = 1.0
	}
	SpotDetectionAvailability.WithLabelValues(method).Set(value)
}

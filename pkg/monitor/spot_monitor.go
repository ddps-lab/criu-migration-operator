package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// AWS EC2 metadata endpoint for spot instance termination notice
	spotTerminationEndpoint = "http://169.254.169.254/latest/meta-data/spot/instance-action"

	// GCP preemptible instance metadata endpoint
	gcpPreemptibleEndpoint = "http://metadata.google.internal/computeMetadata/v1/instance/preempted"

	// Azure spot instance metadata endpoint
	azureSpotEndpoint = "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01"
)

// SpotMonitor monitors for spot instance interruptions
type SpotMonitor struct {
	nodeName      string
	clientset     *kubernetes.Clientset
	cloudType     string // "aws", "gcp", or "azure"
	checkInterval time.Duration
	Enabled       bool   // Feature flag for IMDS detection
}

// NewSpotMonitor creates a new spot monitor
func NewSpotMonitor(nodeName string, clientset *kubernetes.Clientset, cloudType string) *SpotMonitor {
	return &SpotMonitor{
		nodeName:      nodeName,
		clientset:     clientset,
		cloudType:     cloudType,
		checkInterval: 5 * time.Second,
		Enabled:       true, // Default to enabled
	}
}

// NewSpotMonitorWithConfig creates a new spot monitor with custom configuration
func NewSpotMonitorWithConfig(nodeName string, clientset *kubernetes.Clientset, cloudType string, enabled bool) *SpotMonitor {
	return &SpotMonitor{
		nodeName:      nodeName,
		clientset:     clientset,
		cloudType:     cloudType,
		checkInterval: 5 * time.Second,
		Enabled:       enabled,
	}
}

// Start starts the spot monitor
func (m *SpotMonitor) Start(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.Info("Starting Spot Monitor", "node", m.nodeName, "cloudType", m.cloudType, "enabled", m.Enabled)

	// Early return if disabled
	if !m.Enabled {
		logger.Info("Spot Monitor is disabled, exiting")
		return nil
	}

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Spot Monitor stopped")
			return nil
		case <-ticker.C:
			interrupted, err := m.checkSpotInterruption(ctx)
			if err != nil {
				logger.Error(err, "Failed to check spot interruption")
				continue
			}

			if interrupted {
				logger.Info("Spot interruption detected!")
				if err := m.handleSpotInterruption(ctx); err != nil {
					logger.Error(err, "Failed to handle spot interruption")
				} else {
					logger.Info("Successfully handled spot interruption")
					// Exit after handling interruption
					return nil
				}
			}
		}
	}
}

// checkSpotInterruption checks if spot interruption is detected
func (m *SpotMonitor) checkSpotInterruption(ctx context.Context) (bool, error) {
	switch m.cloudType {
	case "aws":
		return m.checkAWSSpotInterruption(ctx)
	case "gcp":
		return m.checkGCPPreemption(ctx)
	case "azure":
		return m.checkAzureSpotInterruption(ctx)
	default:
		// If cloud type is unknown, check all
		if interrupted, _ := m.checkAWSSpotInterruption(ctx); interrupted {
			return true, nil
		}
		if interrupted, _ := m.checkGCPPreemption(ctx); interrupted {
			return true, nil
		}
		if interrupted, _ := m.checkAzureSpotInterruption(ctx); interrupted {
			return true, nil
		}
		return false, nil
	}
}

// getIMDSv2Token retrieves an IMDSv2 token for secure metadata access
func (m *SpotMonitor) getIMDSv2Token(ctx context.Context) (string, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "PUT",
		"http://169.254.169.254/latest/api/token", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	token, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(token), nil
}

// checkAWSSpotInterruption checks AWS spot instance termination using IMDSv2 with fallback to IMDSv1
func (m *SpotMonitor) checkAWSSpotInterruption(ctx context.Context) (bool, error) {
	client := &http.Client{Timeout: 2 * time.Second}

	// Try IMDSv2 first
	token, err := m.getIMDSv2Token(ctx)
	if err != nil {
		// IMDSv2 unavailable, will try IMDSv1
		token = ""
	}

	req, err := http.NewRequestWithContext(ctx, "GET", spotTerminationEndpoint, nil)
	if err != nil {
		return false, err
	}

	// Use IMDSv2 token if available
	if token != "" {
		req.Header.Set("X-aws-ec2-metadata-token", token)
	}

	resp, err := client.Do(req)
	if err != nil {
		// Connection error is normal if not on AWS or no termination
		return false, nil
	}
	defer resp.Body.Close()

	// 200 status means termination is scheduled
	return resp.StatusCode == http.StatusOK, nil
}

// checkGCPPreemption checks GCP preemptible instance termination
func (m *SpotMonitor) checkGCPPreemption(ctx context.Context) (bool, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", gcpPreemptibleEndpoint, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := client.Do(req)
	if err != nil {
		return false, nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	// "TRUE" means preemption is scheduled
	return string(body) == "TRUE", nil
}

// checkAzureSpotInterruption checks Azure spot instance termination
func (m *SpotMonitor) checkAzureSpotInterruption(ctx context.Context) (bool, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", azureSpotEndpoint, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Metadata", "true")

	resp, err := client.Do(req)
	if err != nil {
		return false, nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	// Parse Azure scheduled events response
	return m.parseAzureScheduledEvents(body)
}

// AzureScheduledEvents represents Azure scheduled events response
type AzureScheduledEvents struct {
	Events []AzureEvent `json:"Events"`
}

// AzureEvent represents a single Azure scheduled event
type AzureEvent struct {
	EventType string `json:"EventType"`
	EventID   string `json:"EventId"`
}

// parseAzureScheduledEvents parses Azure scheduled events JSON and checks for termination
func (m *SpotMonitor) parseAzureScheduledEvents(body []byte) (bool, error) {
	if len(body) == 0 {
		return false, nil
	}

	var events AzureScheduledEvents
	if err := json.Unmarshal(body, &events); err != nil {
		// If parsing fails, consider it as no interruption
		return false, nil
	}

	// Check for Terminate or Preempt events
	for _, event := range events.Events {
		if event.EventType == "Terminate" || event.EventType == "Preempt" {
			return true, nil
		}
	}

	return false, nil
}

// handleSpotInterruption handles the spot interruption
func (m *SpotMonitor) handleSpotInterruption(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.Info("Handling spot interruption", "node", m.nodeName)

	// 1. Mark node as unschedulable (idempotency check)
	node, err := m.clientset.CoreV1().Nodes().Get(ctx, m.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node: %w", err)
	}

	if !node.Spec.Unschedulable {
		node.Spec.Unschedulable = true
		_, err = m.clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to mark node unschedulable: %w", err)
		}
		logger.Info("Marked node as unschedulable", "node", m.nodeName)
	} else {
		logger.Info("Node is already unschedulable, skipping cordon", "node", m.nodeName)
	}

	// 2. Find all migratable pods on this node
	pods, err := m.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + m.nodeName,
		LabelSelector: "app.kubernetes.io/managed-by=migration-controller",
	})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	logger.Info("Found migratable pods", "count", len(pods.Items))

	// 3. Add migration trigger annotation to each pod
	timestamp := time.Now().UTC().Format(time.RFC3339)
	for i := range pods.Items {
		pod := &pods.Items[i]

		// Skip pods that are already terminating
		if pod.DeletionTimestamp != nil {
			continue
		}

		// Skip pods that already have migration trigger to avoid re-triggering
		if pod.Annotations != nil && pod.Annotations["migration.io/trigger"] == "requested" {
			logger.Info("Pod already has migration trigger, skipping",
				"pod", pod.Name,
				"namespace", pod.Namespace)
			continue
		}

		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}

		pod.Annotations["migration.io/trigger"] = "requested"
		pod.Annotations["migration.io/reason"] = "spot-interrupt"
		pod.Annotations["migration.io/detection-method"] = "imds"
		pod.Annotations["migration.io/detection-timestamp"] = timestamp

		_, err := m.clientset.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
		if err != nil {
			logger.Error(err, "Failed to trigger migration for pod",
				"pod", pod.Name,
				"namespace", pod.Namespace)
		} else {
			logger.Info("Triggered migration for pod",
				"pod", pod.Name,
				"namespace", pod.Namespace)
		}
	}

	return nil
}

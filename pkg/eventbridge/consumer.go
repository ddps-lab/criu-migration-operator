package eventbridge

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// EventBridgeConsumer receives EventBridge spot interruption events and triggers pod migrations
type EventBridgeConsumer struct {
	clientset *kubernetes.Clientset
	mapper    *InstanceMapper
	port      int
}

// NewConsumer creates a new EventBridgeConsumer
func NewConsumer(clientset kubernetes.Interface, port int) (*EventBridgeConsumer, error) {
	// Cast to concrete type
	cs, ok := clientset.(*kubernetes.Clientset)
	if !ok {
		return nil, fmt.Errorf("clientset is not of type *kubernetes.Clientset")
	}

	return &EventBridgeConsumer{
		clientset: cs,
		mapper:    NewInstanceMapper(clientset),
		port:      port,
	}, nil
}

// Start starts the EventBridge webhook server
func (ec *EventBridgeConsumer) Start(ctx context.Context) error {
	http.HandleFunc("/webhook", ec.handleWebhook)
	http.HandleFunc("/health", ec.handleHealth)

	addr := fmt.Sprintf(":%d", ec.port)
	server := &http.Server{
		Addr:    addr,
		Handler: http.DefaultServeMux,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("[INFO] Starting EventBridge webhook server at %s\n", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[ERROR] Server error: %v\n", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	log.Println("[INFO] Shutting down EventBridge webhook server")
	return server.Shutdown(context.Background())
}

// handleWebhook processes incoming EventBridge events
func (ec *EventBridgeConsumer) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[ERROR] Failed to read request body: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse event
	event, err := ParseEvent(body)
	if err != nil {
		log.Printf("[ERROR] Failed to parse event: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Validate event
	if err := ValidateEvent(event); err != nil {
		log.Printf("[ERROR] Invalid event: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Printf("[INFO] Received EventBridge event - instanceID=%s, action=%s\n", event.Detail.InstanceID, event.Detail.InstanceAction)

	// Process event
	if err := ec.processEvent(r.Context(), event); err != nil {
		log.Printf("[ERROR] Failed to process event (instanceID=%s): %v\n", event.Detail.InstanceID, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// processEvent processes the EventBridge event by finding affected pods and adding migration trigger annotations
func (ec *EventBridgeConsumer) processEvent(ctx context.Context, event *EventBridgeEvent) error {
	// Map instance to node
	nodeName, err := ec.mapper.MapInstanceToNode(ctx, event.Detail.InstanceID)
	if err != nil {
		return fmt.Errorf("failed to map instance to node: %w", err)
	}

	log.Printf("[INFO] Mapped instance %s to node %s\n", event.Detail.InstanceID, nodeName)

	// Find pods on the node
	pods, err := ec.mapper.FindPodsOnNode(ctx, nodeName)
	if err != nil {
		return fmt.Errorf("failed to find pods on node: %w", err)
	}

	log.Printf("[INFO] Found %d pods on node %s\n", len(pods), nodeName)

	// Add migration trigger annotations to each pod
	timestamp := time.Now().UTC().Format(time.RFC3339)
	for _, pod := range pods {
		if err := ec.annotatePodsForMigration(ctx, &pod, event.Detail.InstanceID, timestamp); err != nil {
			log.Printf("[ERROR] Failed to annotate pod %s/%s: %v\n", pod.Namespace, pod.Name, err)
			continue
		}
	}

	return nil
}

// annotatePodsForMigration adds migration trigger annotations to a pod
func (ec *EventBridgeConsumer) annotatePodsForMigration(ctx context.Context, pod *corev1.Pod, instanceID, timestamp string) error {
	// Initialize annotations map if nil
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	// Add migration annotations
	pod.Annotations["migration.io/trigger"] = "requested"
	pod.Annotations["migration.io/reason"] = "eventbridge-spot-interrupt"
	pod.Annotations["migration.io/source-instance"] = instanceID
	pod.Annotations["migration.io/detection-method"] = "eventbridge"
	pod.Annotations["migration.io/detection-timestamp"] = timestamp

	// Update pod with new annotations
	_, err := ec.clientset.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update pod annotations: %w", err)
	}

	log.Printf("[INFO] Annotated pod %s/%s for migration\n", pod.Namespace, pod.Name)
	return nil
}

// handleHealth handles health check requests
func (ec *EventBridgeConsumer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}

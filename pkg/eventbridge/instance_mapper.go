package eventbridge

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// InstanceMapper maps AWS EC2 instance IDs to Kubernetes node names
type InstanceMapper struct {
	clientset kubernetes.Interface
}

// NewInstanceMapper creates a new InstanceMapper
func NewInstanceMapper(clientset kubernetes.Interface) *InstanceMapper {
	return &InstanceMapper{
		clientset: clientset,
	}
}

// MapInstanceToNode maps an AWS instance ID to a Kubernetes node name
// Uses the node.spec.providerID which has format: "aws:///region/instance-id"
func (m *InstanceMapper) MapInstanceToNode(ctx context.Context, instanceID string) (string, error) {
	nodes, err := m.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}

	for _, node := range nodes.Items {
		if instanceIDFromProviderID(node.Spec.ProviderID) == instanceID {
			return node.Name, nil
		}
	}

	return "", fmt.Errorf("no node found for instance-id: %s", instanceID)
}

// instanceIDFromProviderID extracts instance ID from node ProviderID
// ProviderID format: "aws:///region/instance-id" or "aws://region/instance-id"
func instanceIDFromProviderID(providerID string) string {
	if providerID == "" {
		return ""
	}

	// Handle different AWS ProviderID formats
	if strings.HasPrefix(providerID, "aws:///") {
		// Format: aws:///region/instance-id
		parts := strings.Split(providerID, "/")
		if len(parts) >= 4 {
			return parts[3]
		}
	} else if strings.HasPrefix(providerID, "aws://") {
		// Format: aws://region/instance-id
		parts := strings.Split(providerID, "/")
		if len(parts) >= 3 {
			return parts[2]
		}
	}

	return ""
}

// FindPodsOnNode finds all managed pods on a given node
func (m *InstanceMapper) FindPodsOnNode(ctx context.Context, nodeName string) ([]corev1.Pod, error) {
	pods, err := m.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods on node %s: %w", nodeName, err)
	}

	// Filter pods that are managed by migration controller
	var managedPods []corev1.Pod
	for _, pod := range pods.Items {
		if pod.Labels["app.kubernetes.io/managed-by"] == "migration-controller" {
			managedPods = append(managedPods, pod)
		}
	}

	return managedPods, nil
}

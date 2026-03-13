package eventbridge

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestParseEvent(t *testing.T) {
	tests := []struct {
		name    string
		body    []byte
		wantErr bool
	}{
		{
			name: "Valid event",
			body: []byte(`{
				"detail-type": "EC2 Spot Instance Interruption Warning",
				"detail": {
					"instance-id": "i-1234567890abcdef0",
					"instance-action": "terminate"
				},
				"time": "2024-11-15T10:30:45Z"
			}`),
			wantErr: false,
		},
		{
			name:    "Invalid JSON",
			body:    []byte(`{invalid json}`),
			wantErr: true,
		},
		{
			name:    "Empty body",
			body:    []byte(``),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := ParseEvent(tt.body)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseEvent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && event == nil {
				t.Errorf("ParseEvent() returned nil event")
			}
		})
	}
}

func TestValidateEvent(t *testing.T) {
	tests := []struct {
		name    string
		event   *EventBridgeEvent
		wantErr bool
	}{
		{
			name: "Valid event",
			event: &EventBridgeEvent{
				DetailType: "EC2 Spot Instance Interruption Warning",
				Detail: EventDetail{
					InstanceID:     "i-1234567890abcdef0",
					InstanceAction: "terminate",
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid detail type",
			event: &EventBridgeEvent{
				DetailType: "Invalid",
				Detail: EventDetail{
					InstanceID:     "i-1234567890abcdef0",
					InstanceAction: "terminate",
				},
			},
			wantErr: true,
		},
		{
			name: "Missing instance ID",
			event: &EventBridgeEvent{
				DetailType: "EC2 Spot Instance Interruption Warning",
				Detail: EventDetail{
					InstanceAction: "terminate",
				},
			},
			wantErr: true,
		},
		{
			name: "Missing instance action",
			event: &EventBridgeEvent{
				DetailType: "EC2 Spot Instance Interruption Warning",
				Detail: EventDetail{
					InstanceID: "i-1234567890abcdef0",
				},
			},
			wantErr: true,
		},
		{
			name:    "Nil event",
			event:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateEvent(tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInstanceMapperMapInstanceToNode(t *testing.T) {
	// Create fake clientset with test nodes
	fakeClientset := fake.NewSimpleClientset()

	// Create test nodes with different provider IDs
	nodes := []*corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
			},
			Spec: corev1.NodeSpec{
				ProviderID: "aws:///us-east-1/i-1234567890abcdef0",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-2",
			},
			Spec: corev1.NodeSpec{
				ProviderID: "aws://us-east-1/i-0987654321fedcba1",
			},
		},
	}

	for _, node := range nodes {
		_, err := fakeClientset.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create test node: %v", err)
		}
	}

	mapper := NewInstanceMapper(fakeClientset)

	tests := []struct {
		name       string
		instanceID string
		wantNode   string
		wantErr    bool
	}{
		{
			name:       "Found instance with format aws:///",
			instanceID: "i-1234567890abcdef0",
			wantNode:   "node-1",
			wantErr:    false,
		},
		{
			name:       "Found instance with format aws://",
			instanceID: "i-0987654321fedcba1",
			wantNode:   "node-2",
			wantErr:    false,
		},
		{
			name:       "Instance not found",
			instanceID: "i-nonexistent",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := mapper.MapInstanceToNode(context.Background(), tt.instanceID)
			if (err != nil) != tt.wantErr {
				t.Errorf("MapInstanceToNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && node != tt.wantNode {
				t.Errorf("MapInstanceToNode() = %v, want %v", node, tt.wantNode)
			}
		})
	}
}

func TestInstanceMapperFindPodsOnNode(t *testing.T) {
	fakeClientset := fake.NewSimpleClientset()

	// Create test pods
	pods := []*corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "managed-pod-1",
				Namespace: "default",
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "migration-controller",
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Containers: []corev1.Container{
					{
						Name:  "app",
						Image: "app:latest",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "unmanaged-pod-1",
				Namespace: "default",
				Labels: map[string]string{
					"app": "other",
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Containers: []corev1.Container{
					{
						Name:  "app",
						Image: "app:latest",
					},
				},
			},
		},
	}

	for _, pod := range pods {
		_, err := fakeClientset.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create test pod: %v", err)
		}
	}

	mapper := NewInstanceMapper(fakeClientset)

	managedPods, err := mapper.FindPodsOnNode(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("FindPodsOnNode() error = %v", err)
	}

	if len(managedPods) != 1 {
		t.Errorf("FindPodsOnNode() returned %d pods, want 1", len(managedPods))
	}

	if len(managedPods) > 0 && managedPods[0].Name != "managed-pod-1" {
		t.Errorf("FindPodsOnNode() returned pod %s, want managed-pod-1", managedPods[0].Name)
	}
}

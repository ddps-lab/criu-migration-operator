package eventbridge

import (
	"encoding/json"
	"fmt"
)

// EventBridgeEvent represents an AWS EventBridge event for spot instance interruption
type EventBridgeEvent struct {
	DetailType string            `json:"detail-type"`
	Detail     EventDetail       `json:"detail"`
	Time       string            `json:"time"`
	Source     string            `json:"source"`
	Resources  []string          `json:"resources"`
	Version    string            `json:"version"`
	ID         string            `json:"id"`
	Account    string            `json:"account"`
	Region     string            `json:"region"`
}

// EventDetail contains the details of the spot interruption event
type EventDetail struct {
	InstanceID     string `json:"instance-id"`
	InstanceAction string `json:"instance-action"`
}

// ParseEvent parses the JSON body into an EventBridgeEvent
func ParseEvent(body []byte) (*EventBridgeEvent, error) {
	var event EventBridgeEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return nil, fmt.Errorf("failed to parse event: %w", err)
	}
	return &event, nil
}

// ValidateEvent validates the EventBridgeEvent for spot interruption
func ValidateEvent(event *EventBridgeEvent) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	if event.DetailType != "EC2 Spot Instance Interruption Warning" {
		return fmt.Errorf("invalid detail-type: expected 'EC2 Spot Instance Interruption Warning', got '%s'", event.DetailType)
	}

	if event.Detail.InstanceID == "" {
		return fmt.Errorf("missing instance-id in event detail")
	}

	if event.Detail.InstanceAction == "" {
		return fmt.Errorf("missing instance-action in event detail")
	}

	return nil
}

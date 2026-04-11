package webhook

import (
	"context"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultsConfigMapName      = "migration-defaults"
	DefaultsConfigMapNamespace = "migration-system"
)

// WebhookConfig holds default configuration for sidecar injection.
type WebhookConfig struct {
	StorageType        string
	Bucket             string
	Region             string
	Endpoint           string
	DownloadEndpoint   string
	CredentialsSecret  string
	ExpressOneZone     bool
	AsyncPrefetch      bool
	PrefetchWorkers    int
	DirectUpload       bool
	LogUpload          bool
	Strategy           string
	CheckpointInterval string
	AutoAdjust         bool
	MemoryThresholdMB  int
	MaxChainDepth      int
	AgentImage         string
}

// DefaultWebhookConfig returns sensible defaults.
func DefaultWebhookConfig() *WebhookConfig {
	return &WebhookConfig{
		StorageType:        "s3",
		Bucket:             "checkpoints",
		Region:             "us-east-1",
		CredentialsSecret:  "migration-credentials",
		Strategy:           "lazy-storage",
		CheckpointInterval: "30s",
		PrefetchWorkers:    4,
		MemoryThresholdMB:  100,
		MaxChainDepth:      10,
	}
}

// LoadConfigFromConfigMap reads the migration-defaults ConfigMap.
func LoadConfigFromConfigMap(ctx context.Context, c client.Client) (*WebhookConfig, error) {
	return LoadConfigFromNamedConfigMap(ctx, c, DefaultsConfigMapName, DefaultsConfigMapNamespace)
}

// LoadConfigFromNamedConfigMap reads a named ConfigMap from a specific namespace.
func LoadConfigFromNamedConfigMap(ctx context.Context, c client.Client, name, namespace string) (*WebhookConfig, error) {
	cfg := DefaultWebhookConfig()

	cm := &corev1.ConfigMap{}
	err := c.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, cm)
	if err != nil {
		return cfg, err
	}

	d := cm.Data
	if v, ok := d["storageType"]; ok {
		cfg.StorageType = v
	}
	if v, ok := d["bucket"]; ok {
		cfg.Bucket = v
	}
	if v, ok := d["region"]; ok {
		cfg.Region = v
	}
	if v, ok := d["endpoint"]; ok {
		cfg.Endpoint = v
	}
	if v, ok := d["downloadEndpoint"]; ok {
		cfg.DownloadEndpoint = v
	}
	if v, ok := d["credentialsSecret"]; ok {
		cfg.CredentialsSecret = v
	}
	if v, ok := d["strategy"]; ok {
		cfg.Strategy = v
	}
	if v, ok := d["checkpointInterval"]; ok {
		cfg.CheckpointInterval = v
	}
	if v, ok := d["expressOneZone"]; ok {
		cfg.ExpressOneZone = v == "true"
	}
	if v, ok := d["asyncPrefetch"]; ok {
		cfg.AsyncPrefetch = v == "true"
	}
	if v, ok := d["directUpload"]; ok {
		cfg.DirectUpload = v == "true"
	}
	if v, ok := d["logUpload"]; ok {
		cfg.LogUpload = v == "true"
	}
	if v, ok := d["autoAdjust"]; ok {
		cfg.AutoAdjust = v == "true"
	}
	if v, ok := d["prefetchWorkers"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.PrefetchWorkers = n
		}
	}
	if v, ok := d["memoryThresholdMB"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MemoryThresholdMB = n
		}
	}
	if v, ok := d["maxChainDepth"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxChainDepth = n
		}
	}
	if v, ok := d["agentImage"]; ok && v != "" {
		cfg.AgentImage = v
	}

	return cfg, nil
}

// MergeAnnotations returns a copy with pod annotation overrides applied.
// Annotation keys: migration.io/<field-name> (kebab-case).
func (cfg *WebhookConfig) MergeAnnotations(annotations map[string]string) *WebhookConfig {
	merged := *cfg // shallow copy

	if v := annotations["migration.io/bucket"]; v != "" {
		merged.Bucket = v
	}
	if v := annotations["migration.io/region"]; v != "" {
		merged.Region = v
	}
	if v := annotations["migration.io/endpoint"]; v != "" {
		merged.Endpoint = v
	}
	if v := annotations["migration.io/download-endpoint"]; v != "" {
		merged.DownloadEndpoint = v
	}
	if v := annotations["migration.io/storage-type"]; v != "" {
		merged.StorageType = v
	}
	if v := annotations["migration.io/credentials-secret"]; v != "" {
		merged.CredentialsSecret = v
	}
	if v := annotations["migration.io/strategy"]; v != "" {
		merged.Strategy = v
	}
	if v := annotations["migration.io/checkpoint-interval"]; v != "" {
		merged.CheckpointInterval = v
	}
	if v := annotations["migration.io/direct-upload"]; v != "" {
		merged.DirectUpload = strings.EqualFold(v, "true")
	}
	if v := annotations["migration.io/async-prefetch"]; v != "" {
		merged.AsyncPrefetch = strings.EqualFold(v, "true")
	}
	if v := annotations["migration.io/log-upload"]; v != "" {
		merged.LogUpload = strings.EqualFold(v, "true")
	}
	if v := annotations["migration.io/express-one-zone"]; v != "" {
		merged.ExpressOneZone = strings.EqualFold(v, "true")
	}
	if v := annotations["migration.io/prefetch-workers"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			merged.PrefetchWorkers = n
		}
	}
	if v := annotations["migration.io/agent-image"]; v != "" {
		merged.AgentImage = v
	}

	return &merged
}

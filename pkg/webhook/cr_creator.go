package webhook

import (
	"context"
	"log"
	"strings"

	migrationv1alpha1 "github.com/ddps-lab/criu-migration-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EnsureMigratableApp creates a MigratableApp CR if one doesn't already exist.
func EnsureMigratableApp(ctx context.Context, c client.Client, pod *corev1.Pod, cfg *WebhookConfig, appName, namespace string) error {
	existing := &migrationv1alpha1.MigratableApp{}
	err := c.Get(ctx, types.NamespacedName{Name: appName, Namespace: namespace}, existing)
	if err == nil {
		// Already exists
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	mapp := &migrationv1alpha1.MigratableApp{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName,
			Namespace: namespace,
			Labels: map[string]string{
				"migration.io/webhook-managed": "true",
				"migration.io/app":             appName,
			},
			Annotations: map[string]string{
				"migration.io/created-by": "webhook",
			},
		},
		Spec: migrationv1alpha1.MigratableAppSpec{
			Template: corev1.PodTemplateSpec{
				Spec: buildOriginalPodSpec(pod),
			},
			Storage: migrationv1alpha1.StorageConfig{
				Type:             cfg.StorageType,
				Bucket:           cfg.Bucket,
				Region:           cfg.Region,
				Endpoint:         cfg.Endpoint,
				DownloadEndpoint: cfg.DownloadEndpoint,
				CredentialsSecret: cfg.CredentialsSecret,
				ExpressOneZone:   cfg.ExpressOneZone,
				AsyncPrefetch:    cfg.AsyncPrefetch,
				PrefetchWorkers:  cfg.PrefetchWorkers,
				DirectUpload:     cfg.DirectUpload,
				LogUpload:        cfg.LogUpload,
			},
			CheckpointPolicy: migrationv1alpha1.CheckpointPolicy{
				Interval:                cfg.CheckpointInterval,
				AutoAdjust:              cfg.AutoAdjust,
				MemoryThresholdMB:       cfg.MemoryThresholdMB,
				MaxCheckpointChainDepth: cfg.MaxChainDepth,
			},
			MigrationPolicy: migrationv1alpha1.MigrationPolicy{
				Strategy:    cfg.Strategy,
				AutoMigrate: true,
			},
		},
	}

	if err := c.Create(ctx, mapp); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil // Race condition — another webhook call already created it
		}
		return err
	}

	log.Printf("[WEBHOOK] Created MigratableApp %s/%s for pod injection", namespace, appName)
	return nil
}

// buildOriginalPodSpec extracts the original pod spec (before sidecar injection)
// for storage in the MigratableApp template.
func buildOriginalPodSpec(pod *corev1.Pod) corev1.PodSpec {
	spec := corev1.PodSpec{}
	// Copy containers, stripping Kubernetes auto-injected volumeMounts
	for _, c := range pod.Spec.Containers {
		if c.Name == "criu-agent" {
			continue
		}
		clean := c.DeepCopy()
		// Remove auto-injected volumeMounts (kube-api-access-*)
		var filtered []corev1.VolumeMount
		for _, vm := range clean.VolumeMounts {
			if strings.HasPrefix(vm.Name, "kube-api-access-") {
				continue
			}
			filtered = append(filtered, vm)
		}
		clean.VolumeMounts = filtered
		spec.Containers = append(spec.Containers, *clean)
	}
	return spec
}

// EnsureCredentialsSecret mirrors the credentials secret from migration-system
// to the pod's namespace if it doesn't already exist there.
func EnsureCredentialsSecret(ctx context.Context, c client.Client, secretName, targetNamespace string) error {
	if targetNamespace == "migration-system" {
		return nil
	}
	if secretName == "" {
		return nil
	}

	// Check if already exists in target namespace
	existing := &corev1.Secret{}
	err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: targetNamespace}, existing)
	if err == nil {
		return nil // Already exists
	}
	if !errors.IsNotFound(err) {
		return err
	}

	// Fetch from migration-system
	source := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: "migration-system"}, source); err != nil {
		return err
	}

	// Mirror to target namespace
	mirrored := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: targetNamespace,
			Labels: map[string]string{
				"migration.io/mirrored-secret": "true",
			},
		},
		Data: source.Data,
		Type: source.Type,
	}

	if err := c.Create(ctx, mirrored); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	log.Printf("[WEBHOOK] Mirrored credentials secret %s to namespace %s", secretName, targetNamespace)
	return nil
}

// ShouldCreateCR determines whether a MigratableApp CR should be created.
func ShouldCreateCR(pod *corev1.Pod) bool {
	// Explicit override
	if v := pod.Annotations["migration.io/create-cr"]; v == "false" {
		return false
	}
	if v := pod.Annotations["migration.io/create-cr"]; v == "true" {
		return true
	}
	// Default: create CR (single-replica assumed for spot workloads)
	return true
}

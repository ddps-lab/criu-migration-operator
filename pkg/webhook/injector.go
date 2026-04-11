package webhook

import (
	"strings"

	migrationv1alpha1 "github.com/ddps-lab/criu-migration-operator/api/v1alpha1"
	"github.com/ddps-lab/criu-migration-operator/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InjectSidecar mutates a pod to add the CRIU agent sidecar.
// It reuses the existing PodBuilder logic via a synthetic MigratableApp.
func InjectSidecar(pod *corev1.Pod, cfg *WebhookConfig, appName string) {
	// Build a synthetic MigratableApp for PodBuilder
	syntheticMapp := buildSyntheticMapp(cfg, appName, pod.Namespace)
	builder := controller.NewPodBuilder(syntheticMapp)

	// Get agent container from PodBuilder
	agentContainer := builder.BuildAgentContainer("normal")

	// Override agent image if specified
	if cfg.AgentImage != "" {
		agentContainer.Image = cfg.AgentImage
	}

	// Add credentials via envFrom (instead of inline env vars)
	if cfg.CredentialsSecret != "" {
		optional := true
		agentContainer.EnvFrom = append(agentContainer.EnvFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: cfg.CredentialsSecret},
				Optional:             &optional,
			},
		})
	}

	// Mutate app container (first container or named "app")
	appIdx := findAppContainer(pod)
	if appIdx >= 0 {
		c := &pod.Spec.Containers[appIdx]

		// Save original command/args to annotations
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		if len(c.Command) > 0 {
			pod.Annotations["migration.io/original-command"] = strings.Join(c.Command, ",")
		}
		if len(c.Args) > 0 {
			pod.Annotations["migration.io/original-args"] = strings.Join(c.Args, "|||")
		}
		if c.WorkingDir != "" {
			pod.Annotations["migration.io/original-workdir"] = c.WorkingDir
		}

		// Replace with sleep infinity
		c.Command = []string{"sleep", "infinity"}
		c.Args = nil

		// Add SYS_PTRACE capability
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{}
		}
		if c.SecurityContext.Capabilities == nil {
			c.SecurityContext.Capabilities = &corev1.Capabilities{}
		}
		c.SecurityContext.Capabilities.Add = append(c.SecurityContext.Capabilities.Add, "SYS_PTRACE")

		// Mount checkpoints volume
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name:      "checkpoints",
			MountPath: controller.WorkDir,
		})

		// Ensure container is named "app"
		c.Name = "app"
	}

	// Add agent sidecar
	pod.Spec.Containers = append(pod.Spec.Containers, agentContainer)

	// Add volumes
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{
			Name: "checkpoints",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		corev1.Volume{
			Name: "podinfo",
			VolumeSource: corev1.VolumeSource{
				DownwardAPI: &corev1.DownwardAPIVolumeSource{
					Items: []corev1.DownwardAPIVolumeFile{
						{
							Path: "annotations",
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.annotations",
							},
						},
					},
				},
			},
		},
	)

	// Enable shared PID namespace
	shareProcessNamespace := true
	pod.Spec.ShareProcessNamespace = &shareProcessNamespace

	// Set labels
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels["migration.io/injected"] = "true"
	pod.Labels["migration.io/app"] = appName
	pod.Labels["app.kubernetes.io/managed-by"] = "migration-controller"

	// Set annotations
	pod.Annotations["migration.io/app"] = appName
	pod.Annotations["migration.io/generation"] = "0"
	pod.Annotations["migration.io/mode"] = "normal"
}

// findAppContainer finds the index of the "app" container, or 0 for the first container.
func findAppContainer(pod *corev1.Pod) int {
	for i, c := range pod.Spec.Containers {
		if c.Name == "app" {
			return i
		}
	}
	if len(pod.Spec.Containers) > 0 {
		return 0
	}
	return -1
}

// buildSyntheticMapp creates a MigratableApp from webhook config for PodBuilder reuse.
func buildSyntheticMapp(cfg *WebhookConfig, appName, namespace string) *migrationv1alpha1.MigratableApp {
	return &migrationv1alpha1.MigratableApp{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName,
			Namespace: namespace,
		},
		Spec: migrationv1alpha1.MigratableAppSpec{
			Storage: migrationv1alpha1.StorageConfig{
				Type:             cfg.StorageType,
				Bucket:           cfg.Bucket,
				Region:           cfg.Region,
				Endpoint:         cfg.Endpoint,
				DownloadEndpoint: cfg.DownloadEndpoint,
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
				Strategy: cfg.Strategy,
			},
		},
	}
}

// AppNameFromPod derives the logical app name from a pod's annotations or generateName.
func AppNameFromPod(pod *corev1.Pod) string {
	// Explicit annotation takes priority
	if v := pod.Annotations["migration.io/app-name"]; v != "" {
		return v
	}

	// Use generateName prefix (from Deployment)
	if pod.GenerateName != "" {
		name := strings.TrimSuffix(pod.GenerateName, "-")
		// Strip ReplicaSet hash suffix: my-app-6f7b8c9d-
		parts := strings.Split(name, "-")
		if len(parts) >= 3 {
			// Try to detect hash suffix (alphanumeric, typically 8-10 chars)
			last := parts[len(parts)-1]
			if len(last) >= 5 && len(last) <= 10 && isAlphanumeric(last) {
				name = strings.Join(parts[:len(parts)-1], "-")
			}
		}
		return name
	}

	// Bare pod — use pod name
	return pod.Name
}

func isAlphanumeric(s string) bool {
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}


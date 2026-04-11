package controller

import (
	"fmt"
	"os"
	"strconv"

	migrationv1alpha1 "github.com/ddps-lab/criu-migration-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	AgentPort = 8080
	WorkDir   = "/tmp/.criu-checkpoints" // Hidden dir in /tmp to avoid conflicts with user apps
)

var (
	// AgentImage can be overridden via AGENT_IMAGE environment variable
	AgentImage = getAgentImage()
)

func getAgentImage() string {
	if img := os.Getenv("AGENT_IMAGE"); img != "" {
		return img
	}
	return "criu-agent:latest"
}

// PodBuilder builds Pod specs for MigratableApp
type PodBuilder struct {
	mapp         *migrationv1alpha1.MigratableApp
	awsAccessKey string
	awsSecretKey string
}

// NewPodBuilder creates a new PodBuilder
func NewPodBuilder(mapp *migrationv1alpha1.MigratableApp) *PodBuilder {
	return &PodBuilder{mapp: mapp}
}

// NewPodBuilderWithCredentials creates a PodBuilder with AWS credentials
func NewPodBuilderWithCredentials(mapp *migrationv1alpha1.MigratableApp, awsAccessKey, awsSecretKey string) *PodBuilder {
	return &PodBuilder{
		mapp:         mapp,
		awsAccessKey: awsAccessKey,
		awsSecretKey: awsSecretKey,
	}
}

// BuildNormalPod builds a pod spec for normal mode (not restore)
func (b *PodBuilder) BuildNormalPod(generation int) *corev1.Pod {
	pod := b.buildBasePod(generation, "normal")

	// Only for generation 0 (initial pod): Add initContainer to increase PID counter
	// This ensures app container processes start with high PIDs (100+)
	// so CRIU restore can recreate same PIDs without conflicts
	if generation == 0 {
		pidBoosterInit := corev1.Container{
			Name:    "pid-booster",
			Image:   "busybox:latest",
			Command: []string{"/bin/sh", "-c"},
			Args: []string{
				"for i in $(seq 1 150); do /bin/true & done; wait",
			},
		}

		pod.Spec.InitContainers = append(pod.Spec.InitContainers, pidBoosterInit)
	}

	return pod
}

// BuildRestorePod builds a pod spec for restore mode
func (b *PodBuilder) BuildRestorePod(generation int, checkpointID, sourceNode string, s3Prefix string, sourcePodIP string) *corev1.Pod {
	pod := b.buildBasePod(generation, "restore")

	// Add restore-specific annotations
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations["migration.io/checkpoint-id"] = checkpointID
	pod.Annotations["migration.io/source-node"] = sourceNode
	pod.Annotations["migration.io/s3-prefix"] = s3Prefix
	pod.Annotations["migration.io/source-pod-ip"] = sourcePodIP

	// Keep app container spec identical to source pod
	// The app container will run the same initialization (including dummy processes)
	// This ensures consistent PID layout between source and target
	// CRIU will handle process replacement during restore
	// Note: No modification to Command/Args - uses original MigratableApp spec

	// Add anti-affinity to avoid same node
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.PodAntiAffinity == nil {
		pod.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}

	pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
		{
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"migration.io/app": b.mapp.Name,
				},
			},
			TopologyKey: "kubernetes.io/hostname",
		},
	}

	// Prefer on-demand nodes if specified
	if b.mapp.Spec.MigrationPolicy.PreferOnDemand {
		if pod.Spec.Affinity.NodeAffinity == nil {
			pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
		}

		pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.PreferredSchedulingTerm{
			{
				Weight: 100,
				Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "node-lifecycle",
							Operator: corev1.NodeSelectorOpNotIn,
							Values:   []string{"spot"},
						},
					},
				},
			},
		}
	}

	// Add target node selector if specified
	if len(b.mapp.Spec.MigrationPolicy.TargetNodeSelector) > 0 {
		pod.Spec.NodeSelector = b.mapp.Spec.MigrationPolicy.TargetNodeSelector
	}

	return pod
}

// buildBasePod builds the base pod spec
func (b *PodBuilder) buildBasePod(generation int, mode string) *corev1.Pod {
	// Start with template
	template := b.mapp.Spec.Template.DeepCopy()

	// Build labels
	labels := make(map[string]string)
	for k, v := range template.Labels {
		labels[k] = v
	}
	labels["app.kubernetes.io/managed-by"] = "migration-controller"
	labels["migration.io/app"] = b.mapp.Name

	// Build annotations
	annotations := make(map[string]string)
	for k, v := range template.Annotations {
		annotations[k] = v
	}
	annotations["migration.io/generation"] = strconv.Itoa(generation)
	annotations["migration.io/mode"] = mode
	annotations["migration.io/app"] = b.mapp.Name

	// Pod naming: gen0 uses deterministic name, gen1+ uses generateName for collision avoidance
	objMeta := metav1.ObjectMeta{
		Namespace:   b.mapp.Namespace,
		Labels:      labels,
		Annotations: annotations,
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(b.mapp, migrationv1alpha1.GroupVersion.WithKind("MigratableApp")),
		},
	}
	if generation == 0 {
		objMeta.Name = b.mapp.Name
	} else {
		objMeta.GenerateName = fmt.Sprintf("%s-gen%d-", b.mapp.Name, generation)
	}

	pod := &corev1.Pod{
		ObjectMeta: objMeta,
		Spec:       template.Spec,
	}

	// Modify app container (add capabilities and volume mount)
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Name == "app" || i == 0 {
			c.Name = "app" // Ensure name is set

			// Save original command/args to annotations for agent to execute later
			// This avoids mount namespace complexity from application mounts
			if len(c.Command) > 0 || len(c.Args) > 0 {
				// Serialize command and args as JSON-like format
				if len(c.Command) > 0 {
					cmdStr := ""
					for i, cmd := range c.Command {
						if i > 0 {
							cmdStr += ","
						}
						cmdStr += cmd
					}
					pod.Annotations["migration.io/original-command"] = cmdStr
				}
				if len(c.Args) > 0 {
					argsStr := ""
					for i, arg := range c.Args {
						if i > 0 {
							argsStr += "|||" // Use special delimiter for args (can contain commas)
						}
						argsStr += arg
					}
					pod.Annotations["migration.io/original-args"] = argsStr
				}
				if c.WorkingDir != "" {
					pod.Annotations["migration.io/original-workdir"] = c.WorkingDir
				}

				// Replace with sleep infinity to keep mount namespace clean
				c.Command = []string{"sleep", "infinity"}
				c.Args = nil
			}

			if c.SecurityContext == nil {
				c.SecurityContext = &corev1.SecurityContext{}
			}
			if c.SecurityContext.Capabilities == nil {
				c.SecurityContext.Capabilities = &corev1.Capabilities{}
			}
			c.SecurityContext.Capabilities.Add = append(c.SecurityContext.Capabilities.Add, "SYS_PTRACE")

			// Mount checkpoints volume to main container (skip if already present from webhook injection)
			hasCheckpointsMount := false
			for _, vm := range c.VolumeMounts {
				if vm.Name == "checkpoints" {
					hasCheckpointsMount = true
					break
				}
			}
			if !hasCheckpointsMount {
				c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
					Name:      "checkpoints",
					MountPath: "/tmp/.criu-checkpoints",
				})
			}
		}
	}

	// Add CRIU agent sidecar
	agentContainer := b.BuildAgentContainer(mode)
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

	return pod
}

// BuildAgentContainer builds the CRIU agent sidecar container
func (b *PodBuilder) BuildAgentContainer(mode string) corev1.Container {
	agentEnv := []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		{
			Name:  "S3_BUCKET",
			Value: b.mapp.Spec.Storage.Bucket,
		},
		{
			Name:  "S3_ENDPOINT",
			Value: b.mapp.Spec.Storage.Endpoint,
		},
		{
			Name:  "S3_REGION",
			Value: b.mapp.Spec.Storage.Region,
		},
		{
			Name:  "DOWNLOAD_ENDPOINT",
			Value: b.mapp.Spec.Storage.DownloadEndpoint,
		},
		{
			Name:  "EXPRESS_ONE_ZONE",
			Value: strconv.FormatBool(b.mapp.Spec.Storage.ExpressOneZone),
		},
		{
			Name:  "ASYNC_PREFETCH",
			Value: strconv.FormatBool(b.mapp.Spec.Storage.AsyncPrefetch),
		},
		{
			Name:  "DIRECT_UPLOAD",
			Value: strconv.FormatBool(b.mapp.Spec.Storage.DirectUpload),
		},
		{
			Name:  "STORAGE_TYPE",
			Value: b.mapp.Spec.Storage.Type,
		},
		{
			Name:  "LOG_UPLOAD",
			Value: strconv.FormatBool(b.mapp.Spec.Storage.LogUpload),
		},
	}

	// Prefetch tuning (optional)
	if b.mapp.Spec.Storage.PrefetchWorkers > 0 {
		agentEnv = append(agentEnv, corev1.EnvVar{
			Name:  "PREFETCH_WORKERS",
			Value: strconv.Itoa(b.mapp.Spec.Storage.PrefetchWorkers),
		})
	}
	if b.mapp.Spec.Storage.SemiSyncIOV != nil && !*b.mapp.Spec.Storage.SemiSyncIOV {
		agentEnv = append(agentEnv, corev1.EnvVar{
			Name:  "NO_SEMI_SYNC_IOV",
			Value: "true",
		})
	}
	if b.mapp.Spec.Storage.HotVMASeed != nil && !*b.mapp.Spec.Storage.HotVMASeed {
		agentEnv = append(agentEnv, corev1.EnvVar{
			Name:  "NO_HOT_VMA_SEED",
			Value: "true",
		})
	}

	// Add POD_GENERATION from annotation via Downward API (set in buildBasePod)
	agentEnv = append(agentEnv, corev1.EnvVar{
		Name: "POD_GENERATION",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.annotations['migration.io/generation']",
			},
		},
	})

	// Add SOURCE_POD_IP for restore mode (read from annotation via Downward API)
	if mode == "restore" {
		agentEnv = append(agentEnv, corev1.EnvVar{
			Name: "SOURCE_POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.annotations['migration.io/source-pod-ip']",
				},
			},
		})
	}

	// autoAdjust=true → agent-side invariant scheduler handles pre-dumps
	agentEnv = append(agentEnv, corev1.EnvVar{
		Name:  "AUTO_ADJUST",
		Value: strconv.FormatBool(b.mapp.Spec.CheckpointPolicy.AutoAdjust),
	})

	// Deadline/invariant config (used by agent when AUTO_ADJUST=true)
	ds := b.mapp.Spec.CheckpointPolicy.DeadlineScheduler
	if ds.DeadlineSeconds > 0 {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "DEADLINE_SECONDS", Value: strconv.Itoa(ds.DeadlineSeconds)})
	}
	if ds.BandwidthMBps > 0 {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "BANDWIDTH_MBPS", Value: strconv.Itoa(ds.BandwidthMBps)})
	}
	if ds.ScanIntervalMs > 0 {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "DEADLINE_SCAN_INTERVAL_MS", Value: strconv.Itoa(ds.ScanIntervalMs)})
	}
	if ds.TFreezeMs > 0 {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "DEADLINE_TFREEZE_MS", Value: strconv.Itoa(ds.TFreezeMs)})
	}
	if ds.TMarginMs > 0 {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "DEADLINE_TMARGIN_MS", Value: strconv.Itoa(ds.TMarginMs)})
	}
	if ds.DryRun {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "DEADLINE_SCHEDULER_DRY_RUN", Value: "true"})
	}

	// Add AWS credentials if provided
	if b.awsAccessKey != "" && b.awsSecretKey != "" {
		agentEnv = append(agentEnv,
			corev1.EnvVar{
				Name:  "AWS_ACCESS_KEY_ID",
				Value: b.awsAccessKey,
			},
			corev1.EnvVar{
				Name:  "AWS_SECRET_ACCESS_KEY",
				Value: b.awsSecretKey,
			},
		)
	}

	return corev1.Container{
		Name:  "criu-agent",
		Image: AgentImage,
		Args:  []string{"--mode=" + mode},
		Env:   agentEnv,
		Ports: []corev1.ContainerPort{
			{
				Name:          "grpc",
				ContainerPort: AgentPort,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "checkpoints",
				MountPath: WorkDir,
			},
			{
				Name:      "podinfo",
				MountPath: "/etc/podinfo",
				ReadOnly:  true,
			},
		},
		SecurityContext: &corev1.SecurityContext{
			Privileged: boolPtr(true),
		},
	}
}

func boolPtr(b bool) *bool {
	return &b
}

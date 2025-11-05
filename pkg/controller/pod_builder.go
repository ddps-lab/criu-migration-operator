package controller

import (
	"fmt"
	"strconv"

	migrationv1alpha1 "github.com/ddps-lab/criu-migration-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	AgentImage = "criu-agent:latest" // TODO: Make this configurable
	AgentPort  = 8080
	WorkDir    = "/checkpoints"
)

// PodBuilder builds Pod specs for MigratableApp
type PodBuilder struct {
	mapp *migrationv1alpha1.MigratableApp
}

// NewPodBuilder creates a new PodBuilder
func NewPodBuilder(mapp *migrationv1alpha1.MigratableApp) *PodBuilder {
	return &PodBuilder{mapp: mapp}
}

// BuildNormalPod builds a pod spec for normal mode (not restore)
func (b *PodBuilder) BuildNormalPod(generation int) *corev1.Pod {
	pod := b.buildBasePod(generation, "normal")
	return pod
}

// BuildRestorePod builds a pod spec for restore mode
func (b *PodBuilder) BuildRestorePod(generation int, checkpointID, sourceNode string) *corev1.Pod {
	pod := b.buildBasePod(generation, "restore")

	// Add restore-specific annotations
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations["migration.io/checkpoint-id"] = checkpointID
	pod.Annotations["migration.io/source-node"] = sourceNode

	// Modify app container CMD to sleep
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "app" {
			pod.Spec.Containers[i].Command = []string{"sleep"}
			pod.Spec.Containers[i].Args = []string{"infinity"}
			break
		}
	}

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

	// Generate pod name
	podName := b.mapp.Name
	if generation > 0 {
		podName = fmt.Sprintf("%s-gen%d", b.mapp.Name, generation)
	}

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

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   b.mapp.Namespace,
			Labels:      labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(b.mapp, migrationv1alpha1.GroupVersion.WithKind("MigratableApp")),
			},
		},
		Spec: template.Spec,
	}

	// Modify app container (add capabilities)
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Name == "app" || i == 0 {
			c.Name = "app" // Ensure name is set

			if c.SecurityContext == nil {
				c.SecurityContext = &corev1.SecurityContext{}
			}
			if c.SecurityContext.Capabilities == nil {
				c.SecurityContext.Capabilities = &corev1.Capabilities{}
			}
			c.SecurityContext.Capabilities.Add = append(c.SecurityContext.Capabilities.Add, "SYS_PTRACE")
		}
	}

	// Add CRIU agent sidecar
	agentContainer := b.buildAgentContainer(mode)
	pod.Spec.Containers = append(pod.Spec.Containers, agentContainer)

	// Add volumes
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{
			Name: "checkpoints",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	)

	// Enable shared PID namespace
	shareProcessNamespace := true
	pod.Spec.ShareProcessNamespace = &shareProcessNamespace

	return pod
}

// buildAgentContainer builds the CRIU agent sidecar container
func (b *PodBuilder) buildAgentContainer(mode string) corev1.Container {
	return corev1.Container{
		Name:  "criu-agent",
		Image: AgentImage,
		Args:  []string{"--mode=" + mode},
		Env: []corev1.EnvVar{
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
		},
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
		},
		SecurityContext: &corev1.SecurityContext{
			Privileged: boolPtr(true),
		},
	}
}

func boolPtr(b bool) *bool {
	return &b
}

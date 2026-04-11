package webhook

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// PodMutator implements admission.Handler for sidecar injection.
type PodMutator struct {
	Client   client.Client
	Config   *WebhookConfig
	configMu sync.RWMutex
}

// Handle processes pod admission requests.
func (m *PodMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Check opt-in annotation
	if pod.Annotations == nil || pod.Annotations["migration.io/enabled"] != "true" {
		return admission.Allowed("not opted in")
	}

	// Idempotency: skip already-injected pods
	if pod.Labels != nil && pod.Labels["migration.io/injected"] == "true" {
		return admission.Allowed("already injected")
	}

	log.Printf("[WEBHOOK] Intercepted pod %s/%s (generateName: %s)",
		req.Namespace, pod.Name, pod.GenerateName)

	// Load config: migration.io/config annotation → named ConfigMap, else global default
	var effectiveCfg *WebhookConfig
	if configName := pod.Annotations["migration.io/config"]; configName != "" {
		loaded, err := LoadConfigFromNamedConfigMap(ctx, m.Client, configName, req.Namespace)
		if err != nil {
			log.Printf("[WEBHOOK] Warning: ConfigMap %s/%s not found, trying migration-system: %v",
				req.Namespace, configName, err)
			// Fallback: try migration-system namespace
			loaded, err = LoadConfigFromNamedConfigMap(ctx, m.Client, configName, DefaultsConfigMapNamespace)
			if err != nil {
				log.Printf("[WEBHOOK] Error: ConfigMap %s not found in any namespace: %v", configName, err)
				return admission.Allowed("config not found, skipping injection")
			}
		}
		effectiveCfg = loaded.MergeAnnotations(pod.Annotations)
	} else {
		m.configMu.RLock()
		effectiveCfg = m.Config.MergeAnnotations(pod.Annotations)
		m.configMu.RUnlock()
	}

	// Derive logical app name
	appName := AppNameFromPod(pod)
	log.Printf("[WEBHOOK] App name: %s, namespace: %s", appName, req.Namespace)

	// Mirror credentials secret to pod namespace
	if err := EnsureCredentialsSecret(ctx, m.Client, effectiveCfg.CredentialsSecret, req.Namespace); err != nil {
		log.Printf("[WEBHOOK] Warning: failed to mirror credentials secret: %v", err)
	}

	// Create MigratableApp CR BEFORE injection (uses original pod spec)
	if ShouldCreateCR(pod) {
		if err := EnsureMigratableApp(ctx, m.Client, pod, effectiveCfg, appName, req.Namespace); err != nil {
			log.Printf("[WEBHOOK] Warning: failed to create MigratableApp: %v", err)
		}
	}

	// Inject sidecar (AFTER CR creation to keep original pod spec in CR template)
	InjectSidecar(pod, effectiveCfg, appName)

	// Marshal mutated pod and return patch
	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

// UpdateConfig safely updates the cached config.
func (m *PodMutator) UpdateConfig(cfg *WebhookConfig) {
	m.configMu.Lock()
	defer m.configMu.Unlock()
	m.Config = cfg
}

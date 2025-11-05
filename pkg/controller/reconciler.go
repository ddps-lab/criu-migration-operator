package controller

import (
	"context"
	"fmt"
	"time"

	migrationv1alpha1 "github.com/ddps-lab/criu-migration-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MigratableAppReconciler reconciles a MigratableApp object
type MigratableAppReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=migration.io,resources=migratableapps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=migration.io,resources=migratableapps/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=migration.io,resources=migratableapps/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile is the main reconciliation loop
func (r *MigratableAppReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the MigratableApp instance
	var mapp migrationv1alpha1.MigratableApp
	if err := r.Get(ctx, req.NamespacedName, &mapp); err != nil {
		if errors.IsNotFound(err) {
			// Object not found, could have been deleted
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get MigratableApp")
		return ctrl.Result{}, err
	}

	// Get current pod
	pod, err := r.getCurrentPod(ctx, &mapp)
	if err != nil {
		if errors.IsNotFound(err) {
			// No pod exists, create initial pod
			logger.Info("No pod found, creating initial pod")
			return r.createInitialPod(ctx, &mapp)
		}
		logger.Error(err, "Failed to get current pod")
		return ctrl.Result{}, err
	}

	// Check if migration is needed
	needsMigration, reason := r.needsMigration(ctx, &mapp, pod)
	if needsMigration {
		logger.Info("Migration needed", "reason", reason)
		return r.performMigration(ctx, &mapp, pod, reason)
	}

	// Check if pod is in failed state
	if pod.Status.Phase == corev1.PodFailed {
		logger.Info("Pod is in failed state, recreating")
		return r.recreatePod(ctx, &mapp, pod)
	}

	// Update status with current pod info
	if pod.Spec.NodeName != "" && mapp.Status.CurrentNode != pod.Spec.NodeName {
		mapp.Status.CurrentNode = pod.Spec.NodeName
		mapp.Status.CurrentPodName = pod.Name
		if pod.Status.Phase == corev1.PodRunning {
			mapp.Status.Phase = "Running"
		}
		mapp.Status.LastUpdateTime = metav1.Now()
		if err := r.Status().Update(ctx, &mapp); err != nil {
			logger.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}
	}

	// Requeue after some time to check again
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// createInitialPod creates the initial pod for a MigratableApp
func (r *MigratableAppReconciler) createInitialPod(ctx context.Context, mapp *migrationv1alpha1.MigratableApp) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	builder := NewPodBuilder(mapp)
	pod := builder.BuildNormalPod(0)

	logger.Info("Creating initial pod", "pod", pod.Name)

	if err := r.Create(ctx, pod); err != nil {
		logger.Error(err, "Failed to create pod")
		return ctrl.Result{}, err
	}

	// Update status
	mapp.Status.Phase = "Pending"
	mapp.Status.Generation = 0
	mapp.Status.CurrentPodName = pod.Name
	mapp.Status.LastUpdateTime = metav1.Now()

	if err := r.Status().Update(ctx, mapp); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

// recreatePod recreates a failed pod
func (r *MigratableAppReconciler) recreatePod(ctx context.Context, mapp *migrationv1alpha1.MigratableApp, oldPod *corev1.Pod) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Delete old pod
	if err := r.Delete(ctx, oldPod); err != nil {
		logger.Error(err, "Failed to delete failed pod")
		return ctrl.Result{}, err
	}

	// Create new pod with same generation
	builder := NewPodBuilder(mapp)
	newPod := builder.BuildNormalPod(mapp.Status.Generation)

	logger.Info("Recreating pod", "pod", newPod.Name)

	if err := r.Create(ctx, newPod); err != nil {
		logger.Error(err, "Failed to create new pod")
		return ctrl.Result{}, err
	}

	// Update status
	mapp.Status.Phase = "Pending"
	mapp.Status.CurrentPodName = newPod.Name
	mapp.Status.LastUpdateTime = metav1.Now()

	if err := r.Status().Update(ctx, mapp); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

// getCurrentPod gets the current pod for a MigratableApp
func (r *MigratableAppReconciler) getCurrentPod(ctx context.Context, mapp *migrationv1alpha1.MigratableApp) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(mapp.Namespace),
		client.MatchingLabels{"migration.io/app": mapp.Name}); err != nil {
		return nil, err
	}

	// Find the most recent pod that's not terminating
	var currentPod *corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.DeletionTimestamp == nil {
			if currentPod == nil || pod.CreationTimestamp.After(currentPod.CreationTimestamp.Time) {
				currentPod = pod
			}
		}
	}

	if currentPod == nil {
		return nil, errors.NewNotFound(corev1.Resource("pod"), "")
	}

	return currentPod, nil
}

// needsMigration checks if migration is needed
func (r *MigratableAppReconciler) needsMigration(ctx context.Context, mapp *migrationv1alpha1.MigratableApp, pod *corev1.Pod) (bool, string) {
	// Check if auto-migrate is disabled
	if !mapp.Spec.MigrationPolicy.AutoMigrate {
		return false, ""
	}

	// Check annotation for manual trigger
	if pod.Annotations["migration.io/trigger"] == "requested" {
		reason := pod.Annotations["migration.io/reason"]
		if reason == "" {
			reason = "manual"
		}
		return true, reason
	}

	// Check if node is unschedulable (spot interrupt)
	if pod.Spec.NodeName != "" {
		node := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err == nil {
			if node.Spec.Unschedulable {
				return true, "spot-interrupt"
			}
		}
	}

	return false, ""
}

// waitForPodRunning waits for a pod to reach Running state
func (r *MigratableAppReconciler) waitForPodRunning(ctx context.Context, pod *corev1.Pod, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Re-fetch pod
		if err := r.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
			return err
		}

		// Check if pod is running and all containers are ready
		if pod.Status.Phase == corev1.PodRunning {
			allReady := true
			for _, status := range pod.Status.ContainerStatuses {
				if !status.Ready {
					allReady = false
					break
				}
			}
			if allReady {
				return nil
			}
		}

		// Check if pod failed
		if pod.Status.Phase == corev1.PodFailed {
			return fmt.Errorf("pod failed: %s", pod.Status.Message)
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("timeout waiting for pod to be running")
}

// SetupWithManager sets up the controller with the Manager
func (r *MigratableAppReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&migrationv1alpha1.MigratableApp{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

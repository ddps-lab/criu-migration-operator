package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

const (
	finalizerName = "migration.io/cleanup"
)

// Reconcile is the main reconciliation loop
func (r *MigratableAppReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconcile triggered", "name", req.Name, "namespace", req.Namespace)

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

	// Handle deletion with finalizer
	if !mapp.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is being deleted
		if containsString(mapp.ObjectMeta.Finalizers, finalizerName) {
			// Perform cleanup
			if err := r.cleanupS3Checkpoints(ctx, &mapp); err != nil {
				logger.Error(err, "Failed to cleanup S3 checkpoints")
				return ctrl.Result{}, err
			}

			// Remove finalizer
			mapp.ObjectMeta.Finalizers = removeString(mapp.ObjectMeta.Finalizers, finalizerName)
			if err := r.Update(ctx, &mapp); err != nil {
				logger.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !containsString(mapp.ObjectMeta.Finalizers, finalizerName) {
		mapp.ObjectMeta.Finalizers = append(mapp.ObjectMeta.Finalizers, finalizerName)
		if err := r.Update(ctx, &mapp); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
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
	statusChanged := false
	if pod.Spec.NodeName != "" && mapp.Status.CurrentNode != pod.Spec.NodeName {
		mapp.Status.CurrentNode = pod.Spec.NodeName
		mapp.Status.CurrentPodName = pod.Name
		statusChanged = true
	}

	// Update phase based on pod status
	// Preserve "Migrating" phase - don't overwrite it
	if mapp.Status.Phase != "Migrating" {
		newPhase := "Pending"
		if pod.Status.Phase == corev1.PodRunning {
			newPhase = "Running"
		} else if pod.Status.Phase == corev1.PodFailed {
			newPhase = "Failed"
		}
		if mapp.Status.Phase != newPhase {
			mapp.Status.Phase = newPhase
			statusChanged = true
		}
	}

	if statusChanged {
		mapp.Status.LastUpdateTime = metav1.Now()
		if err := r.Status().Update(ctx, &mapp); err != nil {
			if errors.IsConflict(err) {
				// Conflict is expected during rapid reconciles; requeue immediately
				logger.Info("Status update conflict, requeueing")
				return ctrl.Result{Requeue: true}, nil
			}
			logger.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}
	}

	// Perform periodic checkpoints if pod is running and not migrating
	// Skip pre-checkpoints during migration (after final dump has started)
	if pod.Status.Phase == corev1.PodRunning && mapp.Status.Phase != "Migrating" {
		shouldCP := false
		reason := ""

		if mapp.Spec.CheckpointPolicy.AutoAdjust {
			// Adaptive: query dirty volume from agent (with timeout)
			agentClient, err := NewAgentClient(pod)
			if err == nil {
				queryCtx, queryCancel := context.WithTimeout(ctx, 5*time.Second)
				dvResp, dvErr := agentClient.GetDirtyVolume(queryCtx)
				queryCancel()
				agentClient.Close()

				if dvErr == nil {
					shouldCP, reason = shouldPerformCheckpointAdaptive(
						&mapp, dvResp.CumulativeDirtyBytes, dvResp.DirtyRatePagesPerSec)
				} else {
					// Profiler not running or timeout, fallback to time-based
					shouldCP = shouldPerformCheckpoint(&mapp)
					reason = "interval(profiler_unavailable)"
				}
			} else {
				shouldCP = shouldPerformCheckpoint(&mapp)
				reason = "interval(agent_unavailable)"
			}
		} else {
			shouldCP = shouldPerformCheckpoint(&mapp)
			reason = "interval"
		}

		logger.Info("Checkpoint decision",
			"shouldCP", shouldCP,
			"reason", reason,
			"podPhase", pod.Status.Phase,
			"mappPhase", mapp.Status.Phase,
			"lastCheckpoint", mapp.Status.CheckpointStatus.LastCheckpointTime.Time,
			"autoAdjust", mapp.Spec.CheckpointPolicy.AutoAdjust)

		if shouldCP {
			logger.Info("Performing pre-checkpoint", "reason", reason)
			if err := r.performPreCheckpoint(ctx, &mapp, pod); err != nil {
				logger.Error(err, "Failed to perform pre-checkpoint")
			}
		}
	}

	// Requeue after checkpoint interval or 10 seconds
	requeueAfter := 10 * time.Second
	if interval, err := time.ParseDuration(mapp.Spec.CheckpointPolicy.Interval); err == nil && interval > 0 {
		requeueAfter = interval
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// createInitialPod creates the initial pod for a MigratableApp
func (r *MigratableAppReconciler) createInitialPod(ctx context.Context, mapp *migrationv1alpha1.MigratableApp) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get AWS credentials from migration-system namespace
	awsAccessKey, awsSecretKey, err := r.getAWSCredentials(ctx, mapp)
	if err != nil {
		logger.Error(err, "Failed to get AWS credentials")
		// Continue without credentials (agent will fail S3 upload but checkpoint still works)
	}

	var builder *PodBuilder
	if awsAccessKey != "" && awsSecretKey != "" {
		builder = NewPodBuilderWithCredentials(mapp, awsAccessKey, awsSecretKey)
	} else {
		builder = NewPodBuilder(mapp)
	}

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
			// Pod might not be registered in API yet, keep waiting
			time.Sleep(1 * time.Second)
			continue
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

// shouldPerformCheckpoint determines if a checkpoint should be performed now.
// Uses time-based scheduling by default; when autoAdjust is enabled,
// also considers dirty volume from the profiler.
func shouldPerformCheckpoint(mapp *migrationv1alpha1.MigratableApp) bool {
	// Parse checkpoint interval
	interval, err := time.ParseDuration(mapp.Spec.CheckpointPolicy.Interval)
	if err != nil || interval == 0 {
		return false
	}

	// If no checkpoint has been performed yet, perform one
	if mapp.Status.CheckpointStatus.LastCheckpointTime.IsZero() {
		return true
	}

	// Check if enough time has passed since last checkpoint
	elapsed := time.Since(mapp.Status.CheckpointStatus.LastCheckpointTime.Time)
	return elapsed >= interval
}

// shouldPerformCheckpointAdaptive extends shouldPerformCheckpoint with
// dirty volume awareness. Returns (should checkpoint, reason).
//
// Adaptive logic (from paper's Feasibility Score model):
// - If cumulative dirty since last checkpoint > memoryThresholdMB → checkpoint now
// - If dirty rate is low (< 10 pages/s) → extend interval (skip this cycle)
// - If dirty rate is high and approaching threshold → checkpoint early
func shouldPerformCheckpointAdaptive(
	mapp *migrationv1alpha1.MigratableApp,
	dirtyBytes int64,
	dirtyRate float64,
) (bool, string) {
	// Fallback to time-based if autoAdjust not enabled
	if !mapp.Spec.CheckpointPolicy.AutoAdjust {
		if shouldPerformCheckpoint(mapp) {
			return true, "interval"
		}
		return false, ""
	}

	// Always checkpoint if never done before
	if mapp.Status.CheckpointStatus.LastCheckpointTime.IsZero() {
		return true, "initial"
	}

	thresholdBytes := int64(mapp.Spec.CheckpointPolicy.MemoryThresholdMB) * 1024 * 1024
	if thresholdBytes <= 0 {
		thresholdBytes = 50 * 1024 * 1024 // default 50MB
	}

	// Check if dirty volume exceeds threshold
	if dirtyBytes > thresholdBytes {
		return true, fmt.Sprintf("dirty_threshold(%dMB>%dMB)",
			dirtyBytes/1024/1024, thresholdBytes/1024/1024)
	}

	// If dirty rate is very low, skip this cycle (extend interval)
	// This avoids unnecessary checkpoints when workload is idle
	if dirtyRate < 10 { // < 10 pages/sec (~40KB/s)
		return false, ""
	}

	// Time-based fallback: respect max interval even with autoAdjust
	interval, err := time.ParseDuration(mapp.Spec.CheckpointPolicy.Interval)
	if err != nil || interval == 0 {
		interval = 30 * time.Second
	}
	elapsed := time.Since(mapp.Status.CheckpointStatus.LastCheckpointTime.Time)
	if elapsed >= interval {
		return true, "interval"
	}

	return false, ""
}

// performPreCheckpoint performs a pre-checkpoint on the running pod
func (r *MigratableAppReconciler) performPreCheckpoint(ctx context.Context, mapp *migrationv1alpha1.MigratableApp, pod *corev1.Pod) error {
	logger := log.FromContext(ctx)

	// Get parent checkpoint ID (for incremental checkpoints)
	parentID := mapp.Status.CheckpointStatus.LastCheckpointID

	// Check if we need to reset chain based on max depth (before performing checkpoint)
	if mapp.Spec.CheckpointPolicy.MaxCheckpointChainDepth > 0 &&
		mapp.Status.CheckpointStatus.CheckpointChainDepth >= mapp.Spec.CheckpointPolicy.MaxCheckpointChainDepth {
		logger.Info("Checkpoint chain depth limit reached, resetting to start new chain",
			"currentDepth", mapp.Status.CheckpointStatus.CheckpointChainDepth,
			"maxDepth", mapp.Spec.CheckpointPolicy.MaxCheckpointChainDepth)
		parentID = "" // Reset to start a new chain
	}

	// Connect to agent
	agentClient, err := NewAgentClient(pod)
	if err != nil {
		return fmt.Errorf("failed to connect to agent: %w", err)
	}
	defer agentClient.Close()

	// Perform pre-checkpoint
	logger.Info("Calling agent pre-checkpoint", "parentID", parentID)
	resp, err := agentClient.PreCheckpoint(ctx, parentID)
	if err != nil {
		return fmt.Errorf("pre-checkpoint failed: %w", err)
	}

	// Update status with new checkpoint info
	mapp.Status.CheckpointStatus.LastCheckpointID = resp.DumpId
	mapp.Status.CheckpointStatus.LastCheckpointTime = metav1.Now()

	if parentID == "" {
		// This is a new chain (either first checkpoint or reset), set as chain root
		mapp.Status.CheckpointStatus.CheckpointChainRoot = resp.DumpId
		mapp.Status.CheckpointStatus.CheckpointChainDepth = 1
	} else {
		// Continuing existing chain
		mapp.Status.CheckpointStatus.CheckpointChainDepth++
	}

	logger.Info("Pre-checkpoint completed",
		"dumpID", resp.DumpId,
		"chainDepth", mapp.Status.CheckpointStatus.CheckpointChainDepth,
		"chainRoot", mapp.Status.CheckpointStatus.CheckpointChainRoot)

	// Update status with retry on conflict
	for retries := 0; retries < 3; retries++ {
		if err := r.Status().Update(ctx, mapp); err != nil {
			if errors.IsConflict(err) {
				// Re-fetch and re-apply
				if err := r.Get(ctx, client.ObjectKeyFromObject(mapp), mapp); err != nil {
					return fmt.Errorf("failed to re-fetch after conflict: %w", err)
				}
				mapp.Status.CheckpointStatus.LastCheckpointID = resp.DumpId
				mapp.Status.CheckpointStatus.LastCheckpointTime = metav1.Now()
				continue
			}
			return fmt.Errorf("failed to update status: %w", err)
		}
		break
	}

	return nil
}

// getAWSCredentials retrieves AWS credentials from migration-system namespace
func (r *MigratableAppReconciler) getAWSCredentials(ctx context.Context, mapp *migrationv1alpha1.MigratableApp) (string, string, error) {
	logger := log.FromContext(ctx)

	if mapp.Spec.Storage.CredentialsSecret == "" {
		return "", "", fmt.Errorf("no credentials secret specified")
	}

	// Try to get secret from migration-system namespace first
	secret := &corev1.Secret{}
	secretNamespace := "migration-system"
	err := r.Get(ctx, client.ObjectKey{
		Namespace: secretNamespace,
		Name:      mapp.Spec.Storage.CredentialsSecret,
	}, secret)

	if err != nil {
		logger.Info("Failed to get secret from migration-system namespace, will skip credentials",
			"secret", mapp.Spec.Storage.CredentialsSecret,
			"error", err.Error())
		return "", "", err
	}

	accessKey := string(secret.Data["AWS_ACCESS_KEY_ID"])
	secretKey := string(secret.Data["AWS_SECRET_ACCESS_KEY"])

	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("secret missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY")
	}

	logger.Info("Successfully retrieved AWS credentials from migration-system namespace",
		"secret", mapp.Spec.Storage.CredentialsSecret)

	return accessKey, secretKey, nil
}

// cleanupS3Checkpoints deletes all S3 checkpoints for the app
func (r *MigratableAppReconciler) cleanupS3Checkpoints(ctx context.Context, mapp *migrationv1alpha1.MigratableApp) error {
	logger := log.FromContext(ctx)

	// Get AWS credentials
	awsAccessKey, awsSecretKey, err := r.getAWSCredentials(ctx, mapp)
	if err != nil {
		logger.Info("No AWS credentials available, skipping S3 cleanup", "error", err.Error())
		return nil // Don't fail deletion if credentials are missing
	}

	// Create AWS session
	cfg := &aws.Config{
		Region:      aws.String(mapp.Spec.Storage.Region),
		Credentials: credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, ""),
	}

	if mapp.Spec.Storage.Endpoint != "" {
		cfg.Endpoint = aws.String(mapp.Spec.Storage.Endpoint)
		cfg.S3ForcePathStyle = aws.Bool(true) // For MinIO compatibility
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		logger.Error(err, "Failed to create AWS session for cleanup")
		return nil // Don't fail deletion if session creation fails
	}

	svc := s3.New(sess)

	// Delete all checkpoints for this app (all generations)
	// Format: checkpoints/{app-name}/
	s3Prefix := fmt.Sprintf("checkpoints/%s/", mapp.Name)

	logger.Info("Deleting S3 checkpoints for app", "prefix", s3Prefix, "bucket", mapp.Spec.Storage.Bucket)

	// List all objects with the prefix
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(mapp.Spec.Storage.Bucket),
		Prefix: aws.String(s3Prefix),
	}

	var objectsToDelete []*s3.ObjectIdentifier
	err = svc.ListObjectsV2PagesWithContext(ctx, listInput,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{
					Key: obj.Key,
				})
			}
			return true
		})

	if err != nil {
		logger.Error(err, "Failed to list S3 objects for deletion")
		return nil // Don't fail deletion if listing fails
	}

	if len(objectsToDelete) == 0 {
		logger.Info("No S3 objects found to delete", "prefix", s3Prefix)
		return nil
	}

	logger.Info("Deleting S3 objects", "count", len(objectsToDelete), "prefix", s3Prefix)

	// Delete objects in batches (S3 limit is 1000 per request)
	for i := 0; i < len(objectsToDelete); i += 1000 {
		end := i + 1000
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}

		deleteInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(mapp.Spec.Storage.Bucket),
			Delete: &s3.Delete{
				Objects: objectsToDelete[i:end],
				Quiet:   aws.Bool(true),
			},
		}

		_, err := svc.DeleteObjectsWithContext(ctx, deleteInput)
		if err != nil {
			logger.Error(err, "Failed to delete S3 objects batch", "start", i, "end", end)
			// Continue trying to delete remaining batches
		}
	}

	logger.Info("Successfully deleted S3 checkpoints", "prefix", s3Prefix, "count", len(objectsToDelete))
	return nil
}

// Helper functions for finalizer management
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) []string {
	result := []string{}
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}

// SetupWithManager sets up the controller with the Manager
func (r *MigratableAppReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&migrationv1alpha1.MigratableApp{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

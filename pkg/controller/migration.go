package controller

import (
	"context"
	"fmt"
	"time"

	migrationv1alpha1 "github.com/ddps-lab/criu-migration-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// debugPreserveSourcePodAnnotation, when set to "true" on a MigratableApp
// CR, makes the controller skip deleting the source pod after a successful
// migration. The pod is detached from any user Service (workload labels
// stripped) and left Running so that `kubectl logs <source-pod>` and the
// CRIU artifacts under /checkpoints remain accessible for post-migration
// analysis. Intended for measurement / debugging only — production
// deployments should leave this annotation off so resources are cleaned up.
const debugPreserveSourcePodAnnotation = "migration.io/debug-preserve-source-pod"

// performMigration is the top-level entrypoint Reconcile calls when
// needsMigration() returns true. All strategies now flow through the
// FSM in migration_fsm.go; the legacy monolithic path below is kept
// only as a fallback for the (unreachable today) "full" strategy and
// any future strategy that has not been ported to the FSM. Callers
// should not invoke performMigrationLegacy directly.
func (r *MigratableAppReconciler) performMigration(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
	reason string,
) (ctrl.Result, error) {
	return r.startMigration(ctx, mapp, sourcePod, reason)
}

// performMigrationLegacy is the original monolithic migration pipeline.
// Retained as a reference implementation and an emergency fallback;
// not reachable from performMigration today.
//
//nolint:unused
func (r *MigratableAppReconciler) performMigrationLegacy(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
	reason string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	startTime := time.Now()

	logger.Info("Starting migration (legacy path)",
		"sourcePod", sourcePod.Name,
		"sourceNode", sourcePod.Spec.NodeName,
		"reason", reason)

	// Update status to Migrating
	mapp.Status.Phase = "Migrating"
	mapp.Status.LastUpdateTime = metav1.Now()
	if err := r.Status().Update(ctx, mapp); err != nil {
		logger.Error(err, "Failed to update status")
	}

	// Step 1: Get AWS credentials for target pod
	awsAccessKey, awsSecretKey, err := r.getAWSCredentials(ctx, mapp)
	if err != nil {
		logger.Error(err, "Failed to get AWS credentials")
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "CredentialsRetrievalFailed", err.Error())
	}

	// Step 2: Create target pod with credentials
	var builder *PodBuilder
	if awsAccessKey != "" && awsSecretKey != "" {
		builder = NewPodBuilderWithCredentials(mapp, awsAccessKey, awsSecretKey)
	} else {
		builder = NewPodBuilder(mapp)
	}

	newGeneration := mapp.Status.Generation + 1
	lastCheckpointID := mapp.Status.CheckpointStatus.LastCheckpointID

	// Pre-calculate S3 prefix (will be updated after final dump)
	// Note: This is a placeholder; actual dump ID will be determined during final dump
	s3Prefix := fmt.Sprintf("%s/%d/%s/",
		mapp.Name, mapp.Status.Generation, sourcePod.Spec.NodeName)

	targetPod := builder.BuildRestorePod(newGeneration, lastCheckpointID, sourcePod.Spec.NodeName, s3Prefix, sourcePod.Status.PodIP)

	logger.Info("Creating target pod", "generateName", targetPod.GenerateName)
	if err := r.Create(ctx, targetPod); err != nil {
		logger.Error(err, "Failed to create target pod")
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "TargetPodCreationFailed", err.Error())
	}

	// Step 3: Wait for target pod to be running
	logger.Info("Waiting for target pod to be running")
	timeout := time.Duration(mapp.Spec.MigrationPolicy.MigrationTimeoutSeconds) * time.Second
	if err := r.waitForPodRunning(ctx, targetPod, timeout); err != nil {
		logger.Error(err, "Target pod failed to start")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "TargetPodStartFailed", err.Error())
	}

	logger.Info("Target pod is running", "pod", targetPod.Name, "ip", targetPod.Status.PodIP)

	// Step 4: Connect to source agent
	sourceAgent, err := NewAgentClient(sourcePod)
	if err != nil {
		logger.Error(err, "Failed to connect to source agent")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "SourceAgentConnectionFailed", err.Error())
	}
	defer sourceAgent.Close()

	// Step 5: Connect to target agent (lazy-pages will start during restore)
	targetAgent, err := NewAgentClient(targetPod)
	if err != nil {
		logger.Error(err, "Failed to connect to target agent")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "TargetAgentConnectionFailed", err.Error())
	}
	defer targetAgent.Close()

	// Determine migration strategy
	strategy := mapp.Spec.MigrationPolicy.Strategy
	if strategy == "" {
		strategy = "lazy-storage" // default
	}
	logger.Info("Migration strategy", "strategy", strategy)

	// Step 6: Perform final dump on source
	logger.Info("Performing final dump on source", "strategy", strategy)
	dumpResp, err := sourceAgent.FinalDump(ctx, targetPod.Status.PodIP, 9999, lastCheckpointID, strategy)
	if err != nil {
		logger.Error(err, "Final dump failed")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "FinalDumpFailed", err.Error())
	}

	logger.Info("Final dump completed", "dumpID", dumpResp.DumpId, "strategy", strategy)

	// Extract external mounts from dump response
	externalMounts := dumpResp.ExternalMounts
	logger.Info("Received external mounts from source", "count", len(externalMounts), "mounts", externalMounts)

	// Step 7: Update target pod annotations
	actualS3Prefix := fmt.Sprintf("%s/%d/%s/%s",
		mapp.Name, mapp.Status.Generation, sourcePod.Spec.NodeName, dumpResp.DumpId)

	if err := r.Get(ctx, client.ObjectKeyFromObject(targetPod), targetPod); err != nil {
		logger.Error(err, "Failed to refresh target pod")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "TargetPodUpdateFailed", err.Error())
	}

	targetPod.Annotations["migration.io/checkpoint-id"] = dumpResp.DumpId
	targetPod.Annotations["migration.io/s3-prefix"] = actualS3Prefix
	if err := r.Update(ctx, targetPod); err != nil {
		logger.Error(err, "Failed to update target pod annotations")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "TargetPodUpdateFailed", err.Error())
	}

	logger.Info("Updated target pod with checkpoint info", "dumpID", dumpResp.DumpId, "s3Prefix", actualS3Prefix)

	// Step 8: Strategy-specific pre-restore steps
	usePageServer := strategy == "lazy-direct" || strategy == "lazy-hybrid"
	useLazyPages := strategy != "full"

	if strategy == "full" || strategy == "lazy-storage" {
		// Storage-based: FinalDump already uploaded synchronously, no page-server wait needed
		logger.Info("Storage upload completed by agent (synchronous)")
	} else {
		// Page-server strategies: wait for async upload and page-server readiness
		logger.Info("Waiting for metadata upload to storage")
		time.Sleep(5 * time.Second)
	}

	if usePageServer {
		if err := r.waitForPageServerReady(ctx, sourcePod.Status.PodIP, 9999, 30*time.Second); err != nil {
			logger.Error(err, "Page-server not ready")
			r.Delete(ctx, targetPod) // Cleanup
			return r.handleMigrationFailure(ctx, mapp, sourcePod, "PageServerNotReady", err.Error())
		}
	}

	// Step 9: Restore on target
	logger.Info("Performing restore on target", "strategy", strategy)
	restoreResp, err := targetAgent.Restore(ctx, dumpResp.DumpId, mapp.Spec.Storage.Bucket, actualS3Prefix, sourcePod.Status.PodIP, externalMounts, strategy, dumpResp.PipeInodes)
	if err != nil {
		logger.Error(err, "Restore failed")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "RestoreFailed", err.Error())
	}

	logger.Info("Restore completed",
		"newPID", restoreResp.NewPid,
		"duration", restoreResp.DurationMs)

	// Step 10: Strategy-specific post-restore steps
	if useLazyPages {
		if usePageServer {
			logger.Info("Waiting for lazy-pages to connect to page-server", "delay", "5s")
			time.Sleep(5 * time.Second)
		}

		logger.Info("Waiting for lazy-pages to complete on target")
		if err := r.waitForLazyPagesCompletion(ctx, targetAgent, 5*time.Minute); err != nil {
			logger.Error(err, "Lazy-pages did not complete in time (proceeding anyway)")
		} else {
			logger.Info("Lazy-pages completed, all pages transferred")
		}
	}

	// Step 11: Source pod cleanup. Default behavior is to delete the source
	// pod (production). For measurement / debugging that needs post-migration
	// access to source-side logs and CRIU artifacts, set the annotation
	// `migration.io/debug-preserve-source-pod: "true"` on the MigratableApp;
	// the source pod is then detached from any user Service (workload labels
	// stripped) and left Running for manual cleanup later.
	if mapp.Annotations[debugPreserveSourcePodAnnotation] == "true" {
		logger.Info("debug-preserve-source-pod annotation set: detaching source pod from Service and leaving Running for log access")
		sourcePodCopy := sourcePod.DeepCopy()
		if sourcePodCopy.Labels == nil {
			sourcePodCopy.Labels = map[string]string{}
		}
		for k := range sourcePodCopy.Labels {
			if k != "migration.io/app" && k != "migration.io/generation" {
				delete(sourcePodCopy.Labels, k)
			}
		}
		if sourcePodCopy.Annotations == nil {
			sourcePodCopy.Annotations = map[string]string{}
		}
		sourcePodCopy.Annotations["migration.io/post-migration"] = time.Now().UTC().Format(time.RFC3339)
		if err := r.Update(ctx, sourcePodCopy); err != nil {
			logger.Error(err, "Failed to detach source pod (non-fatal)")
		}
	} else {
		logger.Info("Deleting source pod")
		if err := r.Delete(ctx, sourcePod); err != nil {
			logger.Error(err, "Failed to delete source pod (non-fatal)")
		}
	}

	// Step 12: Update status
	duration := time.Since(startTime)
	migrationRecord := migrationv1alpha1.MigrationRecord{
		FromNode:  sourcePod.Spec.NodeName,
		ToNode:    targetPod.Spec.NodeName,
		Timestamp: metav1.Now(),
		Reason:    reason,
		Duration:  duration.String(),
		Success:   true,
		Message:   fmt.Sprintf("Migration completed successfully in %s", duration),
	}

	mapp.Status.Phase = "Running"
	mapp.Status.Generation = newGeneration
	mapp.Status.CurrentNode = targetPod.Spec.NodeName
	mapp.Status.CurrentPodName = targetPod.Name
	mapp.Status.MigrationHistory = append(mapp.Status.MigrationHistory, migrationRecord)
	mapp.Status.CheckpointStatus.CheckpointChainDepth = 1 // Reset chain
	mapp.Status.LastUpdateTime = metav1.Now()

	if err := r.Status().Update(ctx, mapp); err != nil {
		logger.Error(err, "Failed to update status after successful migration")
		return ctrl.Result{}, err
	}

	logger.Info("Migration completed successfully",
		"duration", duration,
		"fromNode", sourcePod.Spec.NodeName,
		"toNode", targetPod.Spec.NodeName)

	// Requeue to resume periodic checkpoints on the new pod
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// handleMigrationFailure handles migration failure
func (r *MigratableAppReconciler) handleMigrationFailure(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
	phase string,
	message string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Error(fmt.Errorf("%s", message), "Migration failed", "phase", phase)

	// Record failed migration
	migrationRecord := migrationv1alpha1.MigrationRecord{
		FromNode:  sourcePod.Spec.NodeName,
		ToNode:    "",
		Timestamp: metav1.Now(),
		Reason:    "migration-failed",
		Success:   false,
		Message:   fmt.Sprintf("%s: %s", phase, message),
	}

	mapp.Status.Phase = "Failed"
	mapp.Status.MigrationHistory = append(mapp.Status.MigrationHistory, migrationRecord)
	mapp.Status.LastUpdateTime = metav1.Now()

	if err := r.Status().Update(ctx, mapp); err != nil {
		logger.Error(err, "Failed to update status after migration failure")
	}

	// Remove migration trigger annotation from source pod
	if sourcePod.Annotations != nil {
		delete(sourcePod.Annotations, "migration.io/trigger")
		delete(sourcePod.Annotations, "migration.io/reason")
		if err := r.Update(ctx, sourcePod); err != nil {
			logger.Error(err, "Failed to remove migration trigger annotation")
		}
	}

	// Requeue after some time to retry
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// waitForPageServerCompletion waits for the page-server process to terminate
func (r *MigratableAppReconciler) waitForPageServerCompletion(
	ctx context.Context,
	sourceAgent *AgentClient,
	pageServerPID int32,
	timeout time.Duration,
) error {
	logger := log.FromContext(ctx)
	deadline := time.Now().Add(timeout)
	pollInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		// Check page-server status
		status, err := sourceAgent.CheckPageServerStatus(ctx, pageServerPID)
		if err != nil {
			logger.Error(err, "Failed to check page-server status", "pid", pageServerPID)
			time.Sleep(pollInterval)
			continue
		}

		if !status.IsAlive {
			logger.Info("Page-server has terminated",
				"pid", pageServerPID,
				"message", status.StatusMessage)
			return nil
		}

		logger.V(1).Info("Page-server still running",
			"pid", pageServerPID,
			"message", status.StatusMessage)

		time.Sleep(pollInterval)
	}

	return fmt.Errorf("page-server did not complete within %v", timeout)
}

// waitForLazyPagesCompletion polls target agent until lazy-pages finishes transferring pages
func (r *MigratableAppReconciler) waitForLazyPagesCompletion(
	ctx context.Context,
	targetAgent *AgentClient,
	timeout time.Duration,
) error {
	logger := log.FromContext(ctx)
	deadline := time.Now().Add(timeout)
	pollInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		status, err := targetAgent.GetStatus(ctx)
		if err != nil {
			logger.Error(err, "Failed to get target agent status")
			time.Sleep(pollInterval)
			continue
		}

		if !status.LazyPagesActive {
			return nil
		}

		logger.V(1).Info("Lazy-pages still active on target, waiting...")
		time.Sleep(pollInterval)
	}

	return fmt.Errorf("lazy-pages did not complete within %v", timeout)
}

// waitForPageServerReady waits for the page-server to be ready to accept connections
func (r *MigratableAppReconciler) waitForPageServerReady(
	ctx context.Context,
	sourceIP string,
	port int,
	timeout time.Duration,
) error {
	logger := log.FromContext(ctx)
	// DISABLED: TCP connection check kills the page-server!
	// CRIU page-server expects the first connection to be from lazy-pages daemon,
	// not a health check that immediately closes the connection.
	// Instead, we trust that FinalDump completed successfully and just add a small delay
	// to ensure page-server has fully started.

	logger.Info("Waiting for page-server to be ready", "address", fmt.Sprintf("%s:%d", sourceIP, port), "timeout", timeout)

	// Give page-server a moment to fully start up
	time.Sleep(1 * time.Second)

	logger.Info("Assuming page-server is ready (no health check to avoid killing it)", "address", fmt.Sprintf("%s:%d", sourceIP, port))
	return nil
}

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

// performMigration performs the actual migration process
func (r *MigratableAppReconciler) performMigration(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
	reason string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	startTime := time.Now()

	logger.Info("Starting migration",
		"sourcePod", sourcePod.Name,
		"sourceNode", sourcePod.Spec.NodeName,
		"reason", reason)

	// Update status to Migrating
	mapp.Status.Phase = "Migrating"
	mapp.Status.LastUpdateTime = metav1.Now()
	if err := r.Status().Update(ctx, mapp); err != nil {
		logger.Error(err, "Failed to update status")
	}

	// Step 1: Create target pod
	builder := NewPodBuilder(mapp)
	newGeneration := mapp.Status.Generation + 1
	lastCheckpointID := mapp.Status.CheckpointStatus.LastCheckpointID

	targetPod := builder.BuildRestorePod(newGeneration, lastCheckpointID, sourcePod.Spec.NodeName)

	logger.Info("Creating target pod", "pod", targetPod.Name)
	if err := r.Create(ctx, targetPod); err != nil {
		logger.Error(err, "Failed to create target pod")
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "TargetPodCreationFailed", err.Error())
	}

	// Step 2: Wait for target pod to be running
	logger.Info("Waiting for target pod to be running")
	timeout := time.Duration(mapp.Spec.MigrationPolicy.MigrationTimeoutSeconds) * time.Second
	if err := r.waitForPodRunning(ctx, targetPod, timeout); err != nil {
		logger.Error(err, "Target pod failed to start")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "TargetPodStartFailed", err.Error())
	}

	logger.Info("Target pod is running", "pod", targetPod.Name, "ip", targetPod.Status.PodIP)

	// Step 3: Connect to source agent
	sourceAgent, err := NewAgentClient(sourcePod)
	if err != nil {
		logger.Error(err, "Failed to connect to source agent")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "SourceAgentConnectionFailed", err.Error())
	}
	defer sourceAgent.Close()

	// Step 4: Start page-server on target
	targetAgent, err := NewAgentClient(targetPod)
	if err != nil {
		logger.Error(err, "Failed to connect to target agent")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "TargetAgentConnectionFailed", err.Error())
	}
	defer targetAgent.Close()

	logger.Info("Starting page-server on target")
	pageServerResp, err := targetAgent.StartPageServer(ctx, 9999, "/checkpoints")
	if err != nil {
		logger.Error(err, "Failed to start page-server")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "PageServerStartFailed", err.Error())
	}

	logger.Info("Page-server started", "pid", pageServerResp.Pid, "port", pageServerResp.Port)

	// Step 5: Perform final dump on source
	logger.Info("Performing final dump on source")
	dumpResp, err := sourceAgent.FinalDump(ctx, targetPod.Status.PodIP, 9999, lastCheckpointID)
	if err != nil {
		logger.Error(err, "Final dump failed")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "FinalDumpFailed", err.Error())
	}

	logger.Info("Final dump completed", "dumpID", dumpResp.DumpId)

	// Step 6: Restore on target
	logger.Info("Performing restore on target")
	s3Prefix := fmt.Sprintf("checkpoints/%s/%s/%s",
		sourcePod.Name, sourcePod.Spec.NodeName, dumpResp.DumpId)

	restoreResp, err := targetAgent.Restore(ctx, dumpResp.DumpId, mapp.Spec.Storage.Bucket, s3Prefix)
	if err != nil {
		logger.Error(err, "Restore failed")
		r.Delete(ctx, targetPod) // Cleanup
		return r.handleMigrationFailure(ctx, mapp, sourcePod, "RestoreFailed", err.Error())
	}

	logger.Info("Restore completed",
		"newPID", restoreResp.NewPid,
		"duration", restoreResp.DurationMs)

	// Step 7: Delete source pod
	logger.Info("Deleting source pod")
	if err := r.Delete(ctx, sourcePod); err != nil {
		logger.Error(err, "Failed to delete source pod (non-fatal)")
	}

	// Step 8: Update status
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

	return ctrl.Result{}, nil
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

	logger.Error(fmt.Errorf(message), "Migration failed", "phase", phase)

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

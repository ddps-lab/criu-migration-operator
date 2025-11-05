package scheduler

import (
	"context"
	"fmt"
	"time"

	migrationv1alpha1 "github.com/ddps-lab/criu-migration-operator/api/v1alpha1"
	"github.com/ddps-lab/criu-migration-operator/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CheckpointScheduler schedules pre-checkpoints for MigratableApps
type CheckpointScheduler struct {
	client client.Client
}

// NewCheckpointScheduler creates a new checkpoint scheduler
func NewCheckpointScheduler(c client.Client) *CheckpointScheduler {
	return &CheckpointScheduler{
		client: c,
	}
}

// Start starts the checkpoint scheduler
func (s *CheckpointScheduler) Start(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.Info("Starting Checkpoint Scheduler")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Checkpoint Scheduler stopped")
			return nil
		case <-ticker.C:
			if err := s.evaluateAndSchedule(ctx); err != nil {
				logger.Error(err, "Failed to evaluate and schedule checkpoints")
			}
		}
	}
}

// evaluateAndSchedule evaluates all MigratableApps and schedules checkpoints
func (s *CheckpointScheduler) evaluateAndSchedule(ctx context.Context) error {
	logger := log.FromContext(ctx)

	// List all MigratableApps
	var mappList migrationv1alpha1.MigratableAppList
	if err := s.client.List(ctx, &mappList); err != nil {
		return fmt.Errorf("failed to list MigratableApps: %w", err)
	}

	for _, mapp := range mappList.Items {
		if s.shouldCheckpoint(ctx, &mapp) {
			if err := s.triggerCheckpoint(ctx, &mapp); err != nil {
				logger.Error(err, "Failed to trigger checkpoint",
					"app", mapp.Name,
					"namespace", mapp.Namespace)
			}
		}
	}

	return nil
}

// shouldCheckpoint determines if a checkpoint should be triggered
func (s *CheckpointScheduler) shouldCheckpoint(ctx context.Context, mapp *migrationv1alpha1.MigratableApp) bool {
	logger := log.FromContext(ctx)

	// Only checkpoint if app is running
	if mapp.Status.Phase != "Running" {
		return false
	}

	// Get checkpoint interval
	interval, err := time.ParseDuration(mapp.Spec.CheckpointPolicy.Interval)
	if err != nil {
		logger.Error(err, "Failed to parse checkpoint interval", "app", mapp.Name)
		interval = 30 * time.Second
	}

	// Check time since last checkpoint
	lastCheckpointTime := mapp.Status.CheckpointStatus.LastCheckpointTime.Time
	if lastCheckpointTime.IsZero() {
		// No checkpoint yet, create one
		return true
	}

	timeSinceLastCheckpoint := time.Since(lastCheckpointTime)
	if timeSinceLastCheckpoint < interval {
		return false
	}

	// Check if chain depth exceeds maximum
	if mapp.Status.CheckpointStatus.CheckpointChainDepth >= mapp.Spec.CheckpointPolicy.MaxCheckpointChainDepth {
		logger.Info("Checkpoint chain depth exceeded, skipping checkpoint",
			"app", mapp.Name,
			"depth", mapp.Status.CheckpointStatus.CheckpointChainDepth)
		// In production, you might want to trigger a full checkpoint here
		return false
	}

	// TODO: If AutoAdjust is enabled, check memory dirty pages
	// For now, just use time-based scheduling

	return true
}

// triggerCheckpoint triggers a checkpoint for a MigratableApp
func (s *CheckpointScheduler) triggerCheckpoint(ctx context.Context, mapp *migrationv1alpha1.MigratableApp) error {
	logger := log.FromContext(ctx)

	// Get current pod
	podList := &corev1.PodList{}
	if err := s.client.List(ctx, podList,
		client.InNamespace(mapp.Namespace),
		client.MatchingLabels{"migration.io/app": mapp.Name}); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	var currentPod *corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning {
			currentPod = pod
			break
		}
	}

	if currentPod == nil {
		return fmt.Errorf("no running pod found for app %s", mapp.Name)
	}

	logger.Info("Triggering checkpoint",
		"app", mapp.Name,
		"pod", currentPod.Name,
		"lastCheckpoint", mapp.Status.CheckpointStatus.LastCheckpointID)

	// Connect to agent
	agentClient, err := controller.NewAgentClient(currentPod)
	if err != nil {
		return fmt.Errorf("failed to connect to agent: %w", err)
	}
	defer agentClient.Close()

	// Trigger pre-checkpoint
	resp, err := agentClient.PreCheckpoint(ctx, mapp.Status.CheckpointStatus.LastCheckpointID)
	if err != nil {
		return fmt.Errorf("pre-checkpoint failed: %w", err)
	}

	logger.Info("Checkpoint completed",
		"app", mapp.Name,
		"dumpID", resp.DumpId,
		"size", resp.SizeBytes,
		"pages", resp.PagesDumped)

	// Update status
	mapp.Status.CheckpointStatus.LastCheckpointID = resp.DumpId
	mapp.Status.CheckpointStatus.LastCheckpointTime = metav1.Now()
	mapp.Status.CheckpointStatus.CheckpointChainDepth++
	if mapp.Status.CheckpointStatus.CheckpointChainRoot == "" {
		mapp.Status.CheckpointStatus.CheckpointChainRoot = resp.DumpId
	}
	mapp.Status.LastUpdateTime = metav1.Now()

	if err := s.client.Status().Update(ctx, mapp); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	return nil
}

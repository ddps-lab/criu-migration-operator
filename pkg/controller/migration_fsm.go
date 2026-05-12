// Package controller — migration FSM (stage-based migration dispatcher).
//
// This file holds the per-stage handlers for the resumable migration
// pipeline. The legacy performMigration in migration.go handled
// (target-pod-create → final-dump → restore) as one transaction. That
// design caused a retry storm on RestoreFailed: every reconcile after
// the failure re-entered the pipeline from the top, re-calling
// FinalDump against a source process that lazy-storage had already
// terminated. The FSM splits the pipeline into stages persisted on
// mapp.Status.Migration.Stage so each reconcile only advances one
// step.
//
// Scope: all migration strategies. lazy-storage destroys the source
// process at dump time, while lazy-direct / lazy-hybrid keep the
// source alive as a page-server; both are handled here. The dispatcher
// in migration.go always routes through the FSM (the legacy monolithic
// path is retired). Strategy-specific differences are confined to
// fsmDumpingHandler, fsmUploadedHandler, fsmRestoringHandler.
//
// See docs/migration_fsm_plan.md for the full design and rollout plan.
package controller

import (
	"context"
	"fmt"
	"time"

	migrationv1alpha1 "github.com/ddps-lab/criu-migration-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Default retry budget. Used by setStage when MaxRetries is unset on
// the mapp. Overridable later if we surface it on the CRD spec.
const defaultMaxRetries int32 = 3

// Backoff between RestoreFailed retries. Sized against the paper's
// 120 s spot-termination deadline and 30 s pre-checkpoint interval —
// migration plus N retries must complete inside the deadline, so the
// per-retry wait stays short.
//
// Sequence (RetryCount=0,1,2): 0 s, 2 s, 5 s. With MaxRetries=3 the
// worst-case retry wait is 7 s, leaving the bulk of the deadline for
// the actual dump/restore work.
const (
	minRestoreBackoff = 0 * time.Second
	maxRestoreBackoff = 5 * time.Second
)

// backoffDuration computes the wait between RestoreFailed retries. The
// first retry fires immediately so a transient restore glitch (e.g. a
// pull-time hiccup creating the target pod) recovers in the same
// reconcile burst as the cordon event.
func backoffDuration(retryCount int32) time.Duration {
	switch {
	case retryCount <= 0:
		return minRestoreBackoff
	case retryCount == 1:
		return 2 * time.Second
	default:
		return maxRestoreBackoff
	}
}

// isTerminalStage returns true for stages the reconciler won't exit
// without a user annotation.
func isTerminalStage(stage migrationv1alpha1.MigrationStage) bool {
	return stage == migrationv1alpha1.StageFinalDumpFailed ||
		stage == migrationv1alpha1.StageFailed
}

// phaseForStage maps an FSM stage to the Status.Phase string the
// scheduler and kubectl printcolumns read.
func phaseForStage(stage migrationv1alpha1.MigrationStage) string {
	switch stage {
	case migrationv1alpha1.StageIdle, migrationv1alpha1.StagePreCheckpointing:
		return "Running"
	case migrationv1alpha1.StageFinalDumpFailed, migrationv1alpha1.StageFailed:
		return "Failed"
	default:
		return "Migrating"
	}
}

// setStage atomically updates Stage + the per-transition fields via
// the mutate callback. Single status-update site so handlers can return
// immediately; if a conflict comes back, the caller requeues without
// treating it as an error.
func (r *MigratableAppReconciler) setStage(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	stage migrationv1alpha1.MigrationStage,
	mutate func(*migrationv1alpha1.MigrationStatusInfo),
) error {
	if mutate != nil {
		mutate(&mapp.Status.Migration)
	}
	mapp.Status.Migration.Stage = stage
	mapp.Status.Migration.LastTransitionTime = metav1.Now()
	if mapp.Status.Migration.MaxRetries == 0 {
		mapp.Status.Migration.MaxRetries = defaultMaxRetries
	}
	mapp.Status.Phase = phaseForStage(stage)
	mapp.Status.LastUpdateTime = metav1.Now()
	return r.Status().Update(ctx, mapp)
}

// resetMigrationFSM clears the FSM in-place. Used by the StageRestoring
// success path and the user-annotation retry path. Caller persists.
func resetMigrationFSM(mapp *migrationv1alpha1.MigratableApp) {
	mapp.Status.Migration = migrationv1alpha1.MigrationStatusInfo{}
}

// startMigration is the kickoff: transitions StageIdle → StageDumping.
// Called from performMigration when the legacy path is bypassed for
// lazy-storage. Captures the migration trigger (manual / spot-interrupt)
// and the source node for the audit trail in MigrationHistory.
func (r *MigratableAppReconciler) startMigration(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
	reason string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("FSM: startMigration",
		"sourcePod", sourcePod.Name,
		"sourceNode", sourcePod.Spec.NodeName,
		"reason", reason)

	// First stage depends on whether the mapp declares a load-generator
	// quiesce step: with preDumpQuiesce set we drain matched pods first,
	// otherwise we go straight to Dumping.
	firstStage := migrationv1alpha1.StageDumping
	if q := mapp.Spec.MigrationPolicy.PreDumpQuiesce; q != nil && len(q.TargetPodSelector) > 0 {
		firstStage = migrationv1alpha1.StageQuiescing
	}

	if err := r.setStage(ctx, mapp, firstStage, func(m *migrationv1alpha1.MigrationStatusInfo) {
		m.PreviousNode = sourcePod.Spec.NodeName
		m.MigrationReason = reason
		m.RetryCount = 0
		m.LastError = ""
		m.UploadedDumpID = ""
		m.UploadedS3Prefix = ""
		m.CurrentTargetPod = ""
		m.PageServerAddr = ""
		m.PageServerPort = 0
	}); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// advanceMigrationFSM dispatches to the per-stage handler. Single
// entrypoint from Reconcile when mapp.Status.Migration.Stage != Idle.
func (r *MigratableAppReconciler) advanceMigrationFSM(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
) (ctrl.Result, error) {
	switch mapp.Status.Migration.Stage {
	case migrationv1alpha1.StageQuiescing:
		return r.fsmQuiescingHandler(ctx, mapp, sourcePod)
	case migrationv1alpha1.StageDumping:
		return r.fsmDumpingHandler(ctx, mapp, sourcePod)
	case migrationv1alpha1.StageUploaded:
		return r.fsmUploadedHandler(ctx, mapp, sourcePod)
	case migrationv1alpha1.StageRestoring:
		return r.fsmRestoringHandler(ctx, mapp, sourcePod)
	case migrationv1alpha1.StageRestoreFailed:
		return r.fsmRestoreFailedHandler(ctx, mapp, sourcePod)
	case migrationv1alpha1.StageFinalDumpFailed, migrationv1alpha1.StageFailed:
		// Terminal: wait for migration.io/retry=requested. The Reconcile
		// entrypoint checks the annotation before dispatching here.
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	case migrationv1alpha1.StagePreCheckpointing:
		// Pre-checkpoints clear themselves; just requeue to re-evaluate.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	default:
		log.FromContext(ctx).Info("FSM: unknown stage, requeueing",
			"stage", mapp.Status.Migration.Stage)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
}

// pageServerPort is the fixed port the source CRIU page-server listens
// on (lazy-direct, lazy-hybrid). Mirrors the constant baked into the
// legacy migration path.
const pageServerPort int32 = 9999

// quiesceActionScaleZero, when set as PreDumpQuiesce.Action, makes the
// quiesce step patch the matched pods' owner Deployment to
// replicas=0 instead of sending SIGTERM. Everything else (the default,
// the empty string, anything unrecognised) is treated as "sigterm".
const quiesceActionScaleZero = "scale-zero"

// fsmQuiescingHandler stops external load-generator pods that match the
// mapp's preDumpQuiesce.targetPodSelector before FinalDump fires. The
// goal is to drain in-flight TCP traffic against the workload (typical
// pattern: a YCSB Deployment hitting redis-svc / memcached-svc) so the
// dump catches a quiescent process tree.
//
// Returns to Dumping after matched pods are gone or DrainSeconds
// elapses, whichever happens first. Any error here is non-fatal — we
// log and proceed to Dumping; better a slightly racy dump than a
// stuck migration.
func (r *MigratableAppReconciler) fsmQuiescingHandler(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	_ *corev1.Pod,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	q := mapp.Spec.MigrationPolicy.PreDumpQuiesce
	if q == nil || len(q.TargetPodSelector) == 0 {
		logger.Info("FSM: Quiescing → no selector, skipping to Dumping")
		return r.fsmAdvanceToDumping(ctx, mapp)
	}

	drainBudget := time.Duration(q.DrainSeconds) * time.Second
	if drainBudget <= 0 {
		drainBudget = 5 * time.Second
	}
	deadlineExpired := time.Since(mapp.Status.Migration.LastTransitionTime.Time) >= drainBudget

	// List matched pods.
	pods, err := r.listQuiesceTargets(ctx, mapp.Namespace, q.TargetPodSelector)
	if err != nil {
		logger.Error(err, "FSM: quiesce list failed; proceeding to Dumping")
		return r.fsmAdvanceToDumping(ctx, mapp)
	}

	// No pods remaining (or never any) → done.
	if len(pods) == 0 {
		logger.Info("FSM: Quiescing done, no remaining pods → Dumping")
		return r.fsmAdvanceToDumping(ctx, mapp)
	}

	// Drain budget elapsed even though pods are still around? Proceed
	// anyway. The dump may have a few stale connections but we cannot
	// hold migration forever on a stuck load generator.
	if deadlineExpired {
		logger.Info("FSM: Quiescing drain budget elapsed, proceeding to Dumping",
			"remaining", len(pods), "budget", drainBudget)
		return r.fsmAdvanceToDumping(ctx, mapp)
	}

	// First entry — apply the quiesce action. After that we just poll.
	// We use LastError as the latch (empty = "haven't acted yet").
	if mapp.Status.Migration.LastError == "" {
		switch q.Action {
		case quiesceActionScaleZero:
			if err := r.quiesceScaleOwnersToZero(ctx, pods); err != nil {
				logger.Error(err, "FSM: quiesce scale-zero failed (non-fatal)")
			}
		default: // "sigterm" + empty + unknown
			for _, p := range pods {
				if err := r.Delete(ctx, &p); err != nil && !errors.IsNotFound(err) {
					logger.Error(err, "FSM: quiesce delete pod failed",
						"pod", p.Name)
				}
			}
		}
		// Stamp a non-empty marker so we don't re-issue Deletes on each
		// reconcile while we wait for terminationGracePeriodSeconds.
		_ = r.setStage(ctx, mapp, migrationv1alpha1.StageQuiescing, func(m *migrationv1alpha1.MigrationStatusInfo) {
			m.LastError = fmt.Sprintf("quiescing %d pods", len(pods))
		})
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	// Poll for matched pods to disappear.
	logger.V(1).Info("FSM: Quiescing waiting for drain",
		"remaining", len(pods),
		"elapsed", time.Since(mapp.Status.Migration.LastTransitionTime.Time).Round(time.Second))
	return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
}

// fsmAdvanceToDumping is the transition from Quiescing → Dumping. Used
// by the quiesce handler in three places (no selector, budget elapsed,
// pods gone) so it gets its own helper.
func (r *MigratableAppReconciler) fsmAdvanceToDumping(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
) (ctrl.Result, error) {
	if err := r.setStage(ctx, mapp, migrationv1alpha1.StageDumping, func(m *migrationv1alpha1.MigrationStatusInfo) {
		m.LastError = ""
	}); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// listQuiesceTargets returns currently-running pods in `namespace` that
// match the label selector. Terminating pods are skipped — they're on
// their way out already.
func (r *MigratableAppReconciler) listQuiesceTargets(
	ctx context.Context,
	namespace string,
	selector map[string]string,
) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return nil, err
	}
	out := make([]corev1.Pod, 0, len(podList.Items))
	for _, p := range podList.Items {
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// quiesceScaleOwnersToZero finds each pod's owner Deployment via
// ReplicaSet → Deployment owner refs and patches replicas=0. Pods
// owned by anything else (StatefulSet, Job, bare pod) are skipped
// with a log line — sigterm action is the right tool for those.
func (r *MigratableAppReconciler) quiesceScaleOwnersToZero(
	ctx context.Context,
	pods []corev1.Pod,
) error {
	logger := log.FromContext(ctx)
	deploys := map[string]bool{} // dedup by namespace/name

	for _, p := range pods {
		var deployName string
		var deployNS string
		for _, owner := range p.OwnerReferences {
			if owner.Kind != "ReplicaSet" {
				continue
			}
			var rs appsv1.ReplicaSet
			if err := r.Get(ctx, client.ObjectKey{Namespace: p.Namespace, Name: owner.Name}, &rs); err != nil {
				continue
			}
			for _, rsOwner := range rs.OwnerReferences {
				if rsOwner.Kind == "Deployment" {
					deployName = rsOwner.Name
					deployNS = p.Namespace
					break
				}
			}
			if deployName != "" {
				break
			}
		}
		if deployName == "" {
			logger.Info("quiesce scale-zero: pod has no Deployment owner, skipping",
				"pod", p.Name)
			continue
		}
		key := deployNS + "/" + deployName
		if deploys[key] {
			continue
		}
		deploys[key] = true

		var d appsv1.Deployment
		if err := r.Get(ctx, client.ObjectKey{Namespace: deployNS, Name: deployName}, &d); err != nil {
			logger.Error(err, "quiesce scale-zero: get Deployment failed",
				"deployment", key)
			continue
		}
		zero := int32(0)
		d.Spec.Replicas = &zero
		if err := r.Update(ctx, &d); err != nil {
			logger.Error(err, "quiesce scale-zero: patch replicas=0 failed",
				"deployment", key)
			continue
		}
		logger.Info("quiesce scale-zero: scaled Deployment to 0",
			"deployment", key)
	}
	return nil
}

// fsmDumpingHandler issues the FinalDump RPC against the source agent
// and records the resulting dump ID + S3 prefix. Success → Uploaded.
// Failure → FinalDumpFailed (terminal — even when the strategy keeps
// the source alive, a failed dump leaves the workload in an
// indeterminate state we don't auto-retry; user annotation resets).
//
// Strategy differences:
//
//   - lazy-storage: pageServerAddr/Port are unused (source process is
//     terminated by the dump; target fetches from S3).
//   - lazy-direct / lazy-hybrid: the source becomes a CRIU page-server
//     after the dump (--lazy-pages --address 0.0.0.0 --port N). We
//     persist the source pod's IP + port on the FSM status so retry
//     attempts (and the restoring handler) reconnect to the same
//     page-server.
//   - full: pageServerAddr/Port unused; target downloads everything.
func (r *MigratableAppReconciler) fsmDumpingHandler(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Re-fetch the latest mapp through the APIReader (uncached) so we
	// short-circuit duplicate fsmDumpingHandler runs caused by the
	// controller-runtime cache lagging behind a successful
	// setStage(Uploaded). Without this guard a stale Dumping snapshot
	// fires a second FinalDump RPC; CRIU has already terminated the
	// workload process from the first dump, so the second call hits
	// "readlink /proc/<pid>/ns/pid: no such file" and the FSM transitions
	// to FinalDumpFailed even though the migration metadata is already
	// safely on S3 from the first attempt.
	fresh := &migrationv1alpha1.MigratableApp{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: mapp.Name, Namespace: mapp.Namespace}, fresh); err == nil {
		if fresh.Status.Migration.Stage != migrationv1alpha1.StageDumping {
			logger.Info("FSM: stage already advanced — skipping duplicate fsmDumpingHandler",
				"observed", fresh.Status.Migration.Stage)
			return ctrl.Result{Requeue: true}, nil
		}
	}

	strategy := mapp.Spec.MigrationPolicy.Strategy
	if strategy == "" {
		strategy = "lazy-storage"
	}
	usePageServer := strategy == "lazy-direct" || strategy == "lazy-hybrid"

	logger.Info("FSM: Dumping",
		"sourcePod", sourcePod.Name,
		"strategy", strategy,
		"usePageServer", usePageServer)

	sourceAgent, err := NewAgentClient(sourcePod)
	if err != nil {
		return r.fsmTransitionToFinalDumpFailed(ctx, mapp,
			fmt.Sprintf("failed to connect to source agent: %v", err))
	}
	defer sourceAgent.Close()

	parentID := mapp.Status.CheckpointStatus.LastCheckpointID

	// page-server arg: the source CRIU listens on 0.0.0.0:N; the
	// address we pass here is informational only on the agent side
	// (the agent invokes CRIU dump with --address 0.0.0.0). What
	// matters for retry is what target uses to dial back, which is
	// sourcePod.Status.PodIP recorded on the FSM status.
	var pageServerAddrArg string
	var pageServerPortArg int32
	if usePageServer {
		pageServerAddrArg = "0.0.0.0"
		pageServerPortArg = pageServerPort
	}

	dumpResp, err := sourceAgent.FinalDump(ctx, pageServerAddrArg, pageServerPortArg, parentID, strategy)
	if err != nil {
		logger.Error(err, "Final dump failed")
		return r.fsmTransitionToFinalDumpFailed(ctx, mapp, err.Error())
	}

	s3Prefix := fmt.Sprintf("%s/%d/%s/%s",
		mapp.Name, mapp.Status.Generation, sourcePod.Spec.NodeName, dumpResp.DumpId)

	// For page-server strategies, record the dial-back address. For
	// lazy-storage / full leave it empty.
	dialBackAddr := ""
	var dialBackPort int32
	if usePageServer {
		dialBackAddr = sourcePod.Status.PodIP
		dialBackPort = pageServerPort
	}

	logger.Info("FSM: dump complete",
		"dumpID", dumpResp.DumpId,
		"s3Prefix", s3Prefix,
		"pageServer", fmt.Sprintf("%s:%d", dialBackAddr, dialBackPort),
		"externalMounts", len(dumpResp.ExternalMounts))

	if err := r.setStage(ctx, mapp, migrationv1alpha1.StageUploaded, func(m *migrationv1alpha1.MigrationStatusInfo) {
		m.UploadedDumpID = dumpResp.DumpId
		m.UploadedS3Prefix = s3Prefix
		m.UploadedPipeInodes = dumpResp.PipeInodes
		m.PageServerAddr = dialBackAddr
		m.PageServerPort = dialBackPort
		m.LastError = ""
	}); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// fsmUploadedHandler creates a target pod and waits for it to reach
// PodRunning. Success → Restoring (with CurrentTargetPod recorded).
// Failure → RestoreFailed (retryable; source's dump is preserved in S3,
// and for page-server strategies the source page-server is still
// listening, so retry just rebuilds the target pod with the same
// checkpoint).
func (r *MigratableAppReconciler) fsmUploadedHandler(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Re-fetch the latest mapp directly from the API to defeat stale cache
	// races. If the cached snapshot still says Uploaded but the canonical
	// state has already advanced to Restoring (by an earlier reconcile in
	// the same controller), short-circuit so we don't create a duplicate
	// target pod and block 5 minutes in waitForPodRunning. We use the
	// APIReader (uncached) here because r.Get goes through the same cache
	// that produced the stale snapshot.
	fresh := &migrationv1alpha1.MigratableApp{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: mapp.Name, Namespace: mapp.Namespace}, fresh); err == nil {
		if fresh.Status.Migration.Stage != migrationv1alpha1.StageUploaded {
			logger.Info("FSM: stage already advanced — skipping duplicate fsmUploadedHandler",
				"observed", fresh.Status.Migration.Stage)
			return ctrl.Result{Requeue: true}, nil
		}
	}

	rs := &mapp.Status.Migration
	logger.Info("FSM: Uploaded → creating target pod",
		"dumpID", rs.UploadedDumpID,
		"retryCount", rs.RetryCount,
		"pageServer", fmt.Sprintf("%s:%d", rs.PageServerAddr, rs.PageServerPort))

	awsAccessKey, awsSecretKey, err := r.getAWSCredentials(ctx, mapp)
	if err != nil {
		logger.Error(err, "Failed to get AWS credentials (continuing without)")
	}

	var builder *PodBuilder
	if awsAccessKey != "" && awsSecretKey != "" {
		builder = NewPodBuilderWithCredentials(mapp, awsAccessKey, awsSecretKey)
	} else {
		builder = NewPodBuilder(mapp)
	}

	newGeneration := mapp.Status.Generation + 1
	// page-server strategies tell the target to dial back to the source
	// page-server. lazy-storage / full leave this empty so CRIU only
	// touches S3.
	targetPod := builder.BuildRestorePod(
		newGeneration,
		rs.UploadedDumpID,
		rs.PreviousNode,
		rs.UploadedS3Prefix,
		rs.PageServerAddr,
	)
	if targetPod.Annotations == nil {
		targetPod.Annotations = map[string]string{}
	}
	targetPod.Annotations["migration.io/checkpoint-id"] = rs.UploadedDumpID
	targetPod.Annotations["migration.io/s3-prefix"] = rs.UploadedS3Prefix

	if err := r.Create(ctx, targetPod); err != nil {
		return r.fsmTransitionToRestoreFailed(ctx, mapp,
			fmt.Sprintf("failed to create target pod: %v", err))
	}

	timeout := time.Duration(mapp.Spec.MigrationPolicy.MigrationTimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	if err := r.waitForPodRunning(ctx, targetPod, timeout); err != nil {
		_ = r.Delete(ctx, targetPod)
		return r.fsmTransitionToRestoreFailed(ctx, mapp,
			fmt.Sprintf("target pod failed to reach Running: %v", err))
	}

	logger.Info("FSM: target pod running",
		"pod", targetPod.Name,
		"ip", targetPod.Status.PodIP,
		"node", targetPod.Spec.NodeName)

	if err := r.setStage(ctx, mapp, migrationv1alpha1.StageRestoring, func(m *migrationv1alpha1.MigrationStatusInfo) {
		m.CurrentTargetPod = targetPod.Name
		m.LastError = ""
	}); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// fsmRestoringHandler issues the Restore RPC to the target agent and,
// on success, finalises the migration (cleans up the source pod,
// records history, transitions back to Idle@gen+1). On failure →
// RestoreFailed (retryable up to MaxRetries).
func (r *MigratableAppReconciler) fsmRestoringHandler(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("FSM: Restoring",
		"targetPod", mapp.Status.Migration.CurrentTargetPod,
		"dumpID", mapp.Status.Migration.UploadedDumpID)

	// Re-fetch target pod.
	targetPod := &corev1.Pod{}
	targetKey := types.NamespacedName{
		Namespace: mapp.Namespace,
		Name:      mapp.Status.Migration.CurrentTargetPod,
	}
	if err := r.Get(ctx, targetKey, targetPod); err != nil {
		return r.fsmTransitionToRestoreFailed(ctx, mapp,
			fmt.Sprintf("target pod %q not found: %v", targetKey.Name, err))
	}

	targetAgent, err := NewAgentClient(targetPod)
	if err != nil {
		return r.fsmTransitionToRestoreFailed(ctx, mapp,
			fmt.Sprintf("failed to connect to target agent: %v", err))
	}
	defer targetAgent.Close()

	strategy := mapp.Spec.MigrationPolicy.Strategy
	if strategy == "" {
		strategy = "lazy-storage"
	}

	// For page-server strategies the target lazy-pages daemon dials
	// back to the source page-server addr we recorded at dump time.
	// lazy-storage and full leave it empty (CRIU reads from S3).
	sourceAddr := mapp.Status.Migration.PageServerAddr

	// Re-fetch the dump's external mounts. We could persist them on the
	// FSM status but at ~16 entries × small key they would balloon the
	// status. Instead, the agent re-reads from the dump bundle on
	// restore — leave externalMounts empty here and the agent applies
	// its own defaults / mountinfo discovery on the target side.
	restoreResp, err := targetAgent.Restore(
		ctx,
		mapp.Status.Migration.UploadedDumpID,
		mapp.Spec.Storage.Bucket,
		mapp.Status.Migration.UploadedS3Prefix,
		sourceAddr,
		nil,
		strategy,
		mapp.Status.Migration.UploadedPipeInodes,
	)
	if err != nil {
		return r.fsmTransitionToRestoreFailed(ctx, mapp,
			fmt.Sprintf("Restore RPC failed: %v", err))
	}

	logger.Info("FSM: restore complete",
		"newPID", restoreResp.NewPid,
		"duration_ms", restoreResp.DurationMs)

	// Wait for lazy-pages to fully drain. 5-minute cap matches legacy.
	if err := r.waitForLazyPagesCompletion(ctx, targetAgent, 5*time.Minute); err != nil {
		logger.Error(err, "Lazy-pages did not complete in time (treating as success and proceeding)")
	}

	// Success: record history, finalize generation, clear FSM, then
	// delete the source pod. Status update FIRST so even if pod
	// deletion's reconcile races, the next reconcile sees Stage=Idle
	// and doesn't re-enter the FSM.
	migrationStart := mapp.Status.Migration.LastTransitionTime
	duration := time.Since(migrationStart.Time)
	historyMsg := fmt.Sprintf("Migration completed in %s (retries=%d)",
		duration, mapp.Status.Migration.RetryCount)
	record := migrationv1alpha1.MigrationRecord{
		FromNode:  mapp.Status.Migration.PreviousNode,
		ToNode:    targetPod.Spec.NodeName,
		Timestamp: metav1.Now(),
		Reason:    mapp.Status.Migration.MigrationReason,
		Duration:  duration.String(),
		Success:   true,
		Message:   historyMsg,
	}
	newGeneration := mapp.Status.Generation + 1
	mapp.Status.Generation = newGeneration
	mapp.Status.CurrentNode = targetPod.Spec.NodeName
	mapp.Status.CurrentPodName = targetPod.Name
	mapp.Status.MigrationHistory = append(mapp.Status.MigrationHistory, record)
	mapp.Status.CheckpointStatus.CheckpointChainDepth = 1
	resetMigrationFSM(mapp)
	mapp.Status.Phase = "Running"
	mapp.Status.LastUpdateTime = metav1.Now()
	if err := r.Status().Update(ctx, mapp); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	// Source pod cleanup (or detach for debug). Best-effort; non-fatal.
	if mapp.Annotations[debugPreserveSourcePodAnnotation] == "true" {
		logger.Info("debug-preserve-source-pod: detaching source pod, leaving Running")
		sourceCopy := sourcePod.DeepCopy()
		for k := range sourceCopy.Labels {
			if k != "migration.io/app" && k != "migration.io/generation" {
				delete(sourceCopy.Labels, k)
			}
		}
		if sourceCopy.Annotations == nil {
			sourceCopy.Annotations = map[string]string{}
		}
		sourceCopy.Annotations["migration.io/post-migration"] = time.Now().UTC().Format(time.RFC3339)
		if err := r.Update(ctx, sourceCopy); err != nil {
			logger.Error(err, "Failed to detach source pod (non-fatal)")
		}
	} else {
		if err := r.Delete(ctx, sourcePod); err != nil {
			if !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete source pod (non-fatal)")
			}
		}
	}

	logger.Info("FSM: migration succeeded",
		"generation", newGeneration,
		"toNode", targetPod.Spec.NodeName,
		"duration", duration)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// fsmRestoreFailedHandler handles the retryable failure case. It
// honours exponential backoff against LastTransitionTime, deletes the
// stale target pod, increments RetryCount, and transitions back to
// Uploaded. Once RetryCount > MaxRetries the FSM becomes terminal
// (Failed).
func (r *MigratableAppReconciler) fsmRestoreFailedHandler(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	sourcePod *corev1.Pod,
) (ctrl.Result, error) {
	_ = sourcePod
	logger := log.FromContext(ctx)
	rs := &mapp.Status.Migration

	// Exhausted retry budget → terminal Failed. Record a single
	// history entry on entry (idempotent guard via LastError).
	if rs.RetryCount >= rs.MaxRetries {
		logger.Info("FSM: RestoreFailed budget exhausted; terminal Failed",
			"retryCount", rs.RetryCount,
			"maxRetries", rs.MaxRetries)

		record := migrationv1alpha1.MigrationRecord{
			FromNode:  rs.PreviousNode,
			ToNode:    "",
			Timestamp: metav1.Now(),
			Reason:    "migration-failed",
			Success:   false,
			Message:   fmt.Sprintf("RestoreFailed after %d retries: %s", rs.RetryCount, rs.LastError),
		}
		mapp.Status.MigrationHistory = append(mapp.Status.MigrationHistory, record)

		if err := r.setStage(ctx, mapp, migrationv1alpha1.StageFailed, nil); err != nil {
			if errors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Backoff: requeue with remaining wait time.
	backoff := backoffDuration(rs.RetryCount)
	elapsed := time.Since(rs.LastTransitionTime.Time)
	if elapsed < backoff {
		remaining := backoff - elapsed
		logger.V(1).Info("FSM: RestoreFailed backoff",
			"retryCount", rs.RetryCount,
			"remaining", remaining)
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	// Cleanup the stale target pod, if it still exists. Block in this
	// reconcile by deleting + requeueing; on next tick the pod is
	// gone and we transition to Uploaded.
	if rs.CurrentTargetPod != "" {
		targetKey := types.NamespacedName{
			Namespace: mapp.Namespace,
			Name:      rs.CurrentTargetPod,
		}
		targetPod := &corev1.Pod{}
		err := r.Get(ctx, targetKey, targetPod)
		switch {
		case err == nil:
			logger.Info("FSM: deleting stale target pod before retry",
				"pod", targetPod.Name)
			if delErr := r.Delete(ctx, targetPod); delErr != nil && !errors.IsNotFound(delErr) {
				logger.Error(delErr, "Failed to delete stale target pod (will retry)")
				return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
			}
			// Wait for pod to actually disappear before transitioning.
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		case errors.IsNotFound(err):
			// Already gone. Fall through to transition.
		default:
			logger.Error(err, "Failed to fetch target pod")
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
	}

	// Move back to Uploaded for the next attempt.
	if err := r.setStage(ctx, mapp, migrationv1alpha1.StageUploaded, func(m *migrationv1alpha1.MigrationStatusInfo) {
		m.RetryCount++
		m.CurrentTargetPod = ""
	}); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	logger.Info("FSM: retrying restore",
		"attempt", mapp.Status.Migration.RetryCount,
		"maxRetries", mapp.Status.Migration.MaxRetries)
	return ctrl.Result{Requeue: true}, nil
}

// fsmTransitionToFinalDumpFailed records the error and moves to the
// terminal FinalDumpFailed stage. Used by the Dumping handler.
func (r *MigratableAppReconciler) fsmTransitionToFinalDumpFailed(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	errMsg string,
) (ctrl.Result, error) {
	record := migrationv1alpha1.MigrationRecord{
		FromNode:  mapp.Status.Migration.PreviousNode,
		ToNode:    "",
		Timestamp: metav1.Now(),
		Reason:    "migration-failed",
		Success:   false,
		Message:   fmt.Sprintf("FinalDumpFailed: %s", errMsg),
	}
	mapp.Status.MigrationHistory = append(mapp.Status.MigrationHistory, record)

	if err := r.setStage(ctx, mapp, migrationv1alpha1.StageFinalDumpFailed, func(m *migrationv1alpha1.MigrationStatusInfo) {
		m.LastError = errMsg
	}); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// fsmTransitionToRestoreFailed records the error and moves the FSM into
// RestoreFailed. The handler for that stage decides between retry and
// terminal-Failed based on RetryCount/MaxRetries.
func (r *MigratableAppReconciler) fsmTransitionToRestoreFailed(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
	errMsg string,
) (ctrl.Result, error) {
	if err := r.setStage(ctx, mapp, migrationv1alpha1.StageRestoreFailed, func(m *migrationv1alpha1.MigrationStatusInfo) {
		m.LastError = errMsg
	}); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	// Requeue immediately so fsmRestoreFailedHandler runs the backoff.
	return ctrl.Result{Requeue: true}, nil
}

// checkRetryAnnotation handles the user-driven recovery from a terminal
// failed migration. If migration.io/retry=requested is set and the FSM
// is in a terminal stage, we clear the FSM, delete the source pod
// (its CRIU process is dead), and remove the annotation. The next
// reconcile recreates a fresh source pod via createInitialPod.
//
// Returns (handled, requeueResult). When handled=true the caller should
// return requeueResult instead of continuing the reconcile.
func (r *MigratableAppReconciler) checkRetryAnnotation(
	ctx context.Context,
	mapp *migrationv1alpha1.MigratableApp,
) (handled bool, res ctrl.Result, err error) {
	if mapp.Annotations["migration.io/retry"] != "requested" {
		return false, ctrl.Result{}, nil
	}
	if !isTerminalStage(mapp.Status.Migration.Stage) {
		// User asked for retry on a non-terminal FSM. Ignore — clearing
		// in-flight state would orphan a target pod we just created.
		return false, ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("FSM: user retry annotation observed, resetting terminal state",
		"prevStage", mapp.Status.Migration.Stage,
		"lastError", mapp.Status.Migration.LastError)

	// Best-effort: delete the source pod so a fresh one is created.
	sourcePod, getErr := r.getCurrentPod(ctx, mapp)
	if getErr == nil && sourcePod != nil {
		if delErr := r.Delete(ctx, sourcePod); delErr != nil && !errors.IsNotFound(delErr) {
			logger.Error(delErr, "Failed to delete zombie source pod (non-fatal)")
		}
	}

	// Reset FSM + Phase.
	resetMigrationFSM(mapp)
	mapp.Status.Phase = "Pending"
	mapp.Status.LastUpdateTime = metav1.Now()
	if updErr := r.Status().Update(ctx, mapp); updErr != nil {
		if errors.IsConflict(updErr) {
			return true, ctrl.Result{Requeue: true}, nil
		}
		return true, ctrl.Result{}, updErr
	}

	// Remove the annotation so we don't re-trigger.
	mappCopy := mapp.DeepCopy()
	delete(mappCopy.Annotations, "migration.io/retry")
	if updErr := r.Update(ctx, mappCopy); updErr != nil {
		if errors.IsConflict(updErr) {
			return true, ctrl.Result{Requeue: true}, nil
		}
		return true, ctrl.Result{}, updErr
	}

	return true, ctrl.Result{Requeue: true}, nil
}

// Compile-time guard so unused import lints catch real removals; client
// is used by fsmRestoringHandler's r.Get/Delete.
var _ = client.ObjectKeyFromObject

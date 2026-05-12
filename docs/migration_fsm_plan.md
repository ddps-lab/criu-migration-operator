# Migration FSM Refactor — Implementation Plan

Stage-based finite state machine to replace `performMigration`'s monolithic
dump+restore transaction. Eliminates the controller's RestoreFailed retry
storm and adds resumable migration with per-attempt retry budget.

## Problem statement

`performMigration` runs (target-pod-create → final-dump → restore) as one
function. On any failure it sets `Status.Phase = "Failed"` and requeues
30 s later. The next reconcile sees the still-cordoned node, re-enters
`performMigration` from the top, calls `FinalDump` again. The source
process has been terminated by the previous successful dump (lazy-storage
strategy is destructive), so the dump RPC errors with `/proc/<pid>/ns/pid:
no such file or directory`. 100+ retries over 10–15 minutes spam logs
and burn target-pod creation cycles.

Root cause: `needsMigration()` only checks `node.Spec.Unschedulable`, and
the migration code is not resumable — there is no record that the dump
already succeeded and only restore needs to retry.

## Solution shape

Introduce `MigrationStatusInfo.Stage` as the source of truth for control
flow. `Status.Phase` becomes a presentational summary derived from
`Stage`.

Stages (already added to CRD types):

| Stage | Meaning | Recovery |
|---|---|---|
| `Idle` | workload Running, no migration in flight | normal pre-checkpoint cycle |
| `PreCheckpointing` | pre-dump in flight | scheduler resumes |
| `Dumping` | final dump RPC issued | success → `Uploaded`, fail → `FinalDumpFailed` |
| `Uploaded` | dump done, S3 upload done | create target + restore |
| `Restoring` | target up, restore RPC issued | success → `Idle@gen+1`, fail → `RestoreFailed` |
| `RestoreFailed` | retryable; new target pod each retry | reuses `UploadedDumpID`, exponential backoff, terminal after MaxRetries |
| `FinalDumpFailed` | terminal | user annotation `migration.io/retry=requested` |
| `Failed` | terminal after retry budget exhausted | same annotation |

## File layout

- **New**: `pkg/controller/migration_fsm.go` — stage handlers, `setStage`,
  backoff, history.
- **Edit**: `pkg/controller/migration.go` — `performMigration` becomes a
  thin dispatcher; existing `waitForX` helpers stay.
- **Edit**: `pkg/controller/reconciler.go` — `needsMigration` gating;
  new FSM dispatch insert before existing migration kickoff; annotation
  reset path.
- **Edit**: `api/v1alpha1/migratableapp_types.go` — already done this
  session (MigrationStage enum + MigrationStatusInfo struct).
- **Regen**: `api/v1alpha1/zz_generated.deepcopy.go` and
  `config/crd/migration.io_migratableapps.yaml` via controller-gen.

## Step-by-step diff sequence

Each step is one commit, independently buildable + testable. Do not
reorder.

### Step 1 — Plumbing (no behavior change)

`migration_fsm.go` (new, ~80 lines):
- `setStage(ctx, mapp, stage, mutate func(*MigrationStatusInfo)) error`
- `backoffDuration(retryCount int32) time.Duration` — `min(60s, 1s<<retryCount)`
- `isTerminalStage(stage MigrationStage) bool`
- Stub `advanceMigrationFSM` that logs `"FSM dispatch not yet wired"` and
  requeues.

Test: builds; deployed, no observable change.

### Step 2 — Wire the FSM gate (still stub)

`reconciler.go`:
- Add `if mapp.Status.Migration.Stage != StageIdle: return advanceMigrationFSM(...)`
  immediately before each `needsMigration` call site (two sites).
- Rewrite `needsMigration`: bail with `false, ""` if
  `mapp.Status.Migration.Stage != StageIdle`.

Behavior impact: zero — `Stage` is always `Idle` because no handler writes
it yet.

Test: confirm normal pre-checkpoint cycle still works; confirm a
manually-triggered migration still completes via the legacy
`performMigration` path.

### Step 3 — Split out `startMigration` and `StageDumping`

`migration.go`:
- Extract AWS-credentials + final-dump portion (current lines 47–122)
  into `dumpHandler` in `migration_fsm.go`.
- `performMigration` becomes:
  ```go
  if mapp.Status.Migration.Stage == StageIdle:
      return startMigration(...)
  return advanceMigrationFSM(...)
  ```
- `startMigration` initialises `MigrationStatusInfo` (PreviousNode,
  MigrationReason, MaxRetries=3 default, LastTransitionTime), persists,
  transitions to `StageDumping`, requeues immediately.
- `advanceMigrationFSM` knows `StageDumping`; other stages fall through
  to `performMigrationLegacy` (rename current monolith).

Test: dump success → mapp status shows `Stage=Uploaded`, `UploadedDumpID`
populated. Legacy path completes restore.

### Step 4 — `StageUploaded` handler (target-pod create) — THE STORM FIX

`migration_fsm.go`:
- `uploadedHandler`: extract current `migration.go` lines 54–87 (build +
  create target pod + waitForRunning).
- Success → `setStage(StageRestoring, ...)` writing `CurrentTargetPod`.
- Failure → `setStage(StageRestoreFailed, ...)` with `LastError`.

**This is where the storm dies**: once dump succeeds we never re-call
dump even if target-pod-create fails. The next reconcile dispatches
through the FSM, not through `performMigration` from the top.

Test: deliberately make target-pod creation fail (bad nodeSelector);
confirm only target-pod-create retries, never dump.

### Step 5 — `StageRestoring` + success-path completion

`migration_fsm.go`:
- `restoringHandler`: extract remaining `migration.go` lines 99–260
  (connect agents, Restore RPC, lazy-pages wait, source-pod cleanup,
  history record).
- Success → `setStage(StageIdle, ...)` + write Generation, CurrentNode,
  CurrentPodName, MigrationHistory entry.
- Failure → `setStage(StageRestoreFailed, ...)`.
- Delete `performMigrationLegacy`; `performMigration` is the thin
  dispatcher.

Test: full happy-path migration on webhook-managed deployment and
CR-managed pod.

### Step 6 — `StageRestoreFailed` handler

`migration_fsm.go`:
- Read `RetryCount`, `MaxRetries`, `LastTransitionTime`.
- `now - LastTransitionTime < backoffDuration(RetryCount)` → requeue
  with remaining backoff.
- After backoff: delete `CurrentTargetPod` if still exists; if pod gone,
  increment RetryCount, `setStage(StageUploaded, ...)`.
- `RetryCount >= MaxRetries` → `setStage(StageFailed, ...)` + history.

Test: deliberately break restore (kill target pod between PodRunning and
Restore RPC). Confirm exactly 3 retries on fresh target pods, no dump
re-execution, then terminal `Failed`.

### Step 7 — Terminal stages + user-annotation reset

`migration_fsm.go`:
- `StageFinalDumpFailed` + `StageFailed`: no-op handlers, requeue 60 s,
  append one history entry on **entry** (idempotent — guard on
  `LastError` and existing history tail).
- `checkRetryAnnotation` at the top of `Reconcile`:
  - If `mapp.Annotations["migration.io/retry"] == "requested"` and
    `isTerminalStage(Stage)`:
    - Clear `mapp.Status.Migration = MigrationStatusInfo{}`.
    - Delete source pod (its CRIU process is dead; next reconcile
      recreates via `createInitialPod`).
    - Remove the annotation.
    - Requeue.

Test: annotate a terminal-failed mapp; confirm status clears, fresh pod
comes up.

### Step 8 (optional follow-up)

Clean up the now-redundant `Status.Phase != "Migrating"` guards (lines
129, 158, 606, 621) and unused imports.

## Pitfalls (read before each step)

- **Status-update conflict**: every handler returns immediately after
  `setStage`. No two updates per reconcile. Conflict → `Requeue: true`,
  not error.
- **No goroutines**: every wait is a `RequeueAfter`. The existing
  `waitForPodRunning` and `waitForLazyPagesCompletion` stay blocking for
  this PR; converting them to poll-requeue is a follow-up.
- **Status-first, then pod-delete on success**: if pod-delete fires
  first, the next reconcile races against the stale FSM state. Update
  status first; even on conflict the next reconcile sees the new
  zero-value Migration and won't re-enter.
- **RestoreFailed cleanup**: must delete `CurrentTargetPod` before
  transitioning back to `Uploaded`. Otherwise fresh `BuildRestorePod`
  collides (no actual name collision via `generateName`, but logs get
  confusing).
- **Never call source agent after `StageDumping`**: source CRIU process
  is dead by then. FSM enforces this by structure — only `StageDumping`
  calls `FinalDump`.
- **Terminal source pod is a zombie**: its CRIU process is dead but the
  pod container is up. User annotation reset must `r.Delete(sourcePod)`
  so `createInitialPod` recreates a fresh one.
- **Scheduler reads `Status.Phase`**: `setStage` writes `"Migrating"`
  for all non-terminal FSM stages, so `scheduler.go`'s `Phase != "Running"`
  guard keeps working unchanged.

## MigrationHistory policy

One record per terminal outcome:
- One success record when `StageRestoring` succeeds.
- One failure record per `StageFinalDumpFailed` or `StageFailed` (on
  entry, with `LastError` and `RetryCount` in the Message).
- **No** record per RestoreFailed → Uploaded transition.

Add `StartTime metav1.Time` to `MigrationStatusInfo` if Step 5 needs it
for the success-record `Duration`.

## Deploy ordering

- Steps 1–2 are safe-to-deploy independently and add no behavioral
  change. Can land while cluster is paused.
- Step 4 is the storm fix. Land + verify on a live cluster before
  starting Step 5.
- Steps 5–7 complete the FSM. Land together if convenient; each is
  independently buildable.

## Not in scope

- Converting blocking waits to requeue-poll.
- `MigrationStartTime` unless step 5 needs it.
- gRPC API changes (none needed).
- Agent changes (none needed).
- Scheduler (`pkg/scheduler/scheduler.go`) changes (none needed).

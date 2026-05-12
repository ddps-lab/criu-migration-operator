package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MigratableAppSpec defines the desired state of MigratableApp
type MigratableAppSpec struct {
	// Template defines the pod template for the application
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:XPreserveUnknownFields
	Template corev1.PodTemplateSpec `json:"template"`

	// CheckpointPolicy defines checkpoint behavior
	CheckpointPolicy CheckpointPolicy `json:"checkpointPolicy,omitempty"`

	// MigrationPolicy defines migration behavior
	MigrationPolicy MigrationPolicy `json:"migrationPolicy,omitempty"`

	// SpotInterruptionHandling defines spot instance interruption detection configuration
	SpotInterruptionHandling SpotInterruptionHandling `json:"spotInterruptionHandling,omitempty"`

	// Storage defines object storage configuration
	Storage StorageConfig `json:"storage"`
}

// CheckpointPolicy defines how checkpoints are performed
type CheckpointPolicy struct {
	// Interval between pre-checkpoints (e.g., "30s", "1m")
	// +kubebuilder:default="30s"
	Interval string `json:"interval,omitempty"`

	// AutoAdjust enables dynamic interval adjustment based on memory changes
	// +kubebuilder:default=true
	AutoAdjust bool `json:"autoAdjust,omitempty"`

	// MemoryThresholdMB is the memory change threshold for triggering checkpoint (in MB)
	// +kubebuilder:default=100
	MemoryThresholdMB int `json:"memoryThresholdMB,omitempty"`

	// MaxCheckpointChainDepth is the maximum chain depth before full checkpoint
	// +kubebuilder:default=10
	MaxCheckpointChainDepth int `json:"maxCheckpointChainDepth,omitempty"`

	// DeadlineScheduler configures the agent-side deadline-driven scheduler.
	// When enabled, the controller's periodic pre-checkpoint is disabled.
	DeadlineScheduler DeadlineSchedulerConfig `json:"deadlineScheduler,omitempty"`
}

// DeadlineSchedulerConfig configures the agent-side deadline scheduler.
// DeadlineSchedulerConfig provides parameters for the dirty volume invariant scheduler.
// Used when autoAdjust=true. The agent evaluates T_remain periodically and triggers
// pre-dumps when the invariant (T_remain < Deadline) is about to be violated.
type DeadlineSchedulerConfig struct {
	// DryRun logs decisions without triggering actual pre-dumps
	// +kubebuilder:default=false
	DryRun bool `json:"dryRun,omitempty"`

	// DeadlineSeconds is the termination deadline in seconds (e.g., 120 for AWS, 30 for Azure)
	// +kubebuilder:default=120
	DeadlineSeconds int `json:"deadlineSeconds,omitempty"`

	// BandwidthMBps is the upload bandwidth in MB/s. 0 = auto-detect from AWS API or NIC speed.
	// +kubebuilder:default=0
	BandwidthMBps int `json:"bandwidthMBps,omitempty"`

	// ScanIntervalMs is the invariant evaluation interval in milliseconds
	// +kubebuilder:default=5000
	ScanIntervalMs int `json:"scanIntervalMs,omitempty"`

	// TFreezeMs is the estimated process freeze + final dump time in milliseconds
	// +kubebuilder:default=50
	TFreezeMs int `json:"tFreezeMs,omitempty"`

	// TMarginMs is the safety margin in milliseconds
	// +kubebuilder:default=5000
	TMarginMs int `json:"tMarginMs,omitempty"`
}

// MigrationPolicy defines migration behavior
type MigrationPolicy struct {
	// Strategy defines the migration data transfer strategy.
	// - full: dump all pages to storage, download all before restore
	// - lazy-storage: dump all pages to storage, lazy restore fetches on-demand from storage
	// - lazy-direct: lazy dump with page-server, lazy restore fetches from source via TCP
	// - lazy-hybrid: lazy dump with page-server + storage upload, lazy restore with TCP + storage fallback
	// +kubebuilder:default="lazy-storage"
	Strategy string `json:"strategy,omitempty"`

	// AutoMigrate enables automatic migration on spot interrupt
	// +kubebuilder:default=true
	AutoMigrate bool `json:"autoMigrate,omitempty"`

	// TargetNodeSelector specifies node selector for migration target
	TargetNodeSelector map[string]string `json:"targetNodeSelector,omitempty"`

	// PreferOnDemand prefers on-demand nodes over spot for migration
	// +kubebuilder:default=true
	PreferOnDemand bool `json:"preferOnDemand,omitempty"`

	// MigrationTimeoutSeconds is the timeout for migration operation
	// +kubebuilder:default=300
	MigrationTimeoutSeconds int `json:"migrationTimeoutSeconds,omitempty"`

	// PreDumpQuiesce, when set, makes the controller stop external
	// load generators before invoking FinalDump on the source. Used to
	// drain in-flight requests against the workload (e.g. a YCSB Java
	// client targeting a redis-server mapp) so the dump captures a
	// quiescent process tree.
	//
	// Without this hook the load generator keeps issuing TCP requests
	// against the soon-to-be-frozen server, sometimes faster than CRIU's
	// freezer can keep up, producing dump-time race failures. With it,
	// the controller sends SIGTERM (or scales the load generator's
	// Deployment to zero), waits DrainSeconds for the pods to exit,
	// then proceeds with FinalDump.
	PreDumpQuiesce *PreDumpQuiesce `json:"preDumpQuiesce,omitempty"`
}

// PreDumpQuiesce describes how to stop external load-generator pods
// before the controller fires FinalDump. The selector matches pods in
// the same namespace as the MigratableApp.
type PreDumpQuiesce struct {
	// TargetPodSelector matches the load-generator pods to drain. The
	// match is by labels in the same namespace; only running pods are
	// considered. Empty selector → no-op.
	TargetPodSelector map[string]string `json:"targetPodSelector,omitempty"`

	// DrainSeconds caps how long the controller waits for matched pods
	// to actually terminate after the quiesce action. After this
	// timeout the controller proceeds with FinalDump anyway so a
	// stubborn load generator doesn't deadlock the migration.
	// +kubebuilder:default=5
	DrainSeconds int32 `json:"drainSeconds,omitempty"`

	// Action selects the quiesce mechanism:
	//   "sigterm" (default) — delete the matched pods via the
	//     Kubernetes API, which sends SIGTERM and respects the pod's
	//     terminationGracePeriodSeconds. Good for stateless YCSB
	//     drivers and other pod-managed-by-Job/Deployment patterns.
	//   "scale-zero" — find the matched pods' owner Deployments and
	//     patch replicas=0. Use when the load generator is owned by a
	//     Deployment and you want it to stay scaled-to-zero across the
	//     migration window (the user / external script restores
	//     replicas later).
	//
	// +kubebuilder:validation:Enum=sigterm;scale-zero
	// +kubebuilder:default=sigterm
	Action string `json:"action,omitempty"`
}

// SpotInterruptionHandling defines spot instance interruption detection configuration
type SpotInterruptionHandling struct {
	// Enabled enables spot interruption handling
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// DetectionMethods defines which detection methods to use
	DetectionMethods []DetectionMethod `json:"detectionMethods,omitempty"`
}

// DetectionMethod defines a spot interruption detection method
type DetectionMethod struct {
	// Type of detection method (imds)
	// +kubebuilder:validation:Enum=imds
	Type string `json:"type"`

	// Enabled enables this detection method
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// CloudType specifies the cloud provider (aws, gcp, azure) - auto-detected if empty
	// +kubebuilder:validation:Enum=aws;gcp;azure;""
	CloudType string `json:"cloudType,omitempty"`

	// PollIntervalSeconds is the poll interval in seconds for IMDS detection
	// +kubebuilder:default=5
	PollIntervalSeconds int `json:"pollIntervalSeconds,omitempty"`
}

// StorageConfig defines object storage configuration
type StorageConfig struct {
	// Type of storage (s3, minio, gcs)
	// +kubebuilder:validation:Enum=s3;minio;gcs
	Type string `json:"type"`

	// Bucket name for storing checkpoints
	Bucket string `json:"bucket"`

	// Endpoint URL (for S3-compatible storage, used for upload)
	Endpoint string `json:"endpoint,omitempty"`

	// DownloadEndpoint URL (for CDN like CloudFront, used for CRIU restore)
	// If not specified, Endpoint will be used for both upload and download
	DownloadEndpoint string `json:"downloadEndpoint,omitempty"`

	// Region for cloud storage
	Region string `json:"region,omitempty"`

	// CredentialsSecret references a secret containing storage credentials
	CredentialsSecret string `json:"credentialsSecret,omitempty"`

	// ExpressOneZone enables S3 Express One Zone optimization
	ExpressOneZone bool `json:"expressOneZone,omitempty"`

	// AsyncPrefetch enables asynchronous prefetching in lazy-pages
	// +kubebuilder:default=false
	AsyncPrefetch bool `json:"asyncPrefetch,omitempty"`

	// PrefetchWorkers sets the number of async prefetch worker threads (default: 4)
	PrefetchWorkers int `json:"prefetchWorkers,omitempty"`

	// DirectUpload enables CRIU's native S3 upload during dump (zero disk I/O)
	DirectUpload bool `json:"directUpload,omitempty"`

	// SemiSyncIOV controls semi-synchronous IOV fetch. Default: enabled when object storage is active.
	// Set to false to disable (ablation: page-by-page fault handling only).
	SemiSyncIOV *bool `json:"semiSyncIOV,omitempty"`

	// HotIOVSeed controls hot VMA priority seeding. Default: enabled when async prefetch is active.
	// Set to false to disable (ablation: sequential prefetch only).
	HotIOVSeed *bool `json:"hotIOVSeed,omitempty"`

	// LogUpload enables uploading all raw CRIU logs and stats to S3 after dump/restore.
	// Uploaded files: criu.log, restore.log, lazy-pages.log, stats-dump, stats-restore,
	// hot_vma_metadata.json, and per-fault metrics. Useful for experiment data collection.
	LogUpload bool `json:"logUpload,omitempty"`

	// Compress enables CRIU's zstd seekable compression for pages-*.img during
	// dump. Restore auto-detects compressed images, so no explicit restore flag.
	// Trade-off (paper Table eval:dump-wall-time): redis/memcached see 40-50%
	// byte savings and 11-16% dump wall reduction; numpy/torch random-data
	// workloads see <5% wall savings because CRIU's CPU is wall-bound.
	Compress bool `json:"compress,omitempty"`

	// CompressLevel is the zstd level (1-22). Default 1 (fastest) when
	// Compress is true. Higher levels trade CPU for more byte savings.
	CompressLevel int `json:"compressLevel,omitempty"`

	// CompressWorkers sets the number of parallel zstd encoder threads
	// CRIU uses during dump. 0 = CRIU auto (min(nproc/4, 8)). Paper §eval
	// uses 8 workers; specifying it explicitly here matches that setting
	// regardless of nproc.
	CompressWorkers int `json:"compressWorkers,omitempty"`
}

// MigratableAppStatus defines the observed state of MigratableApp
type MigratableAppStatus struct {
	// Phase represents the current phase of the application
	// +kubebuilder:validation:Enum=Pending;Running;Migrating;Failed
	Phase string `json:"phase,omitempty"`

	// Generation is the current generation number (increments on each migration)
	Generation int `json:"generation,omitempty"`

	// CurrentNode is the node where the pod is currently running
	CurrentNode string `json:"currentNode,omitempty"`

	// CurrentPodName is the name of the current pod
	CurrentPodName string `json:"currentPodName,omitempty"`

	// MigrationHistory contains the history of migrations
	MigrationHistory []MigrationRecord `json:"migrationHistory,omitempty"`

	// CheckpointStatus contains checkpoint information
	CheckpointStatus CheckpointStatus `json:"checkpointStatus,omitempty"`

	// Migration contains the FSM state of an in-flight or recently-failed
	// migration. The reconciler dispatches per Migration.Stage instead of
	// re-running the full dump+restore pipeline on every reconcile.
	Migration MigrationStatusInfo `json:"migration,omitempty"`

	// Conditions represent the latest available observations of an object's state
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastUpdateTime is the last time the status was updated
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
}

// MigrationStage is the FSM state of a migration in flight.
//
// Idle (empty string) and PreCheckpointing are normal Running states.
// Dumping → Uploaded → Restoring is the success path. RestoreFailed is
// retryable (loops back to Uploaded for a fresh target pod).
// FinalDumpFailed and Failed are terminal — the reconciler stops
// auto-retry and waits for an explicit migration.io/retry=requested
// annotation from the user.
//
// The Idle stage is represented by the empty string and is not listed
// in the enum validation. controller-gen's enum tag does not handle
// empty strings cleanly and Status subresource fields are not
// admission-validated in practice anyway; the only place that compares
// against StageIdle is the reconciler.
//
// +kubebuilder:validation:Enum=PreCheckpointing;Quiescing;Dumping;Uploaded;Restoring;RestoreFailed;FinalDumpFailed;Failed
type MigrationStage string

const (
	// StageIdle: workload Running, no migration in flight.
	StageIdle MigrationStage = ""
	// StagePreCheckpointing: workload Running, scheduled pre-dump in flight.
	StagePreCheckpointing MigrationStage = "PreCheckpointing"
	// StageQuiescing: load-generator pods are being stopped before
	// FinalDump. Only entered when migrationPolicy.preDumpQuiesce is
	// set on the mapp; otherwise Idle goes straight to Dumping.
	StageQuiescing MigrationStage = "Quiescing"
	// StageDumping: final dump RPC issued, waiting for completion.
	StageDumping MigrationStage = "Dumping"
	// StageUploaded: source dumped + S3 upload done. Next: spawn target +
	// call Restore RPC.
	StageUploaded MigrationStage = "Uploaded"
	// StageRestoring: target pod up, Restore RPC issued, waiting for the
	// restored process to be ready.
	StageRestoring MigrationStage = "Restoring"
	// StageRestoreFailed: restore on target failed. Reconciler will, after
	// backoff, delete the failed target pod, increment RetryCount, and
	// transition back to Uploaded so a fresh target pod retries with the
	// existing S3 checkpoint. Terminal when RetryCount > MaxRetries.
	StageRestoreFailed MigrationStage = "RestoreFailed"
	// StageFinalDumpFailed: final dump RPC failed. Source state is
	// indeterminate (lazy-storage kills source after dump); we do not
	// auto-retry. User must annotate migration.io/retry=requested.
	StageFinalDumpFailed MigrationStage = "FinalDumpFailed"
	// StageFailed: terminal failure after exhausting retries. Same
	// recovery path as FinalDumpFailed (user annotation).
	StageFailed MigrationStage = "Failed"
)

// MigrationStatusInfo tracks the FSM of an in-flight or recently-failed
// migration. Populated by the reconciler; do not edit by hand.
type MigrationStatusInfo struct {
	// Stage is the current FSM state.
	Stage MigrationStage `json:"stage,omitempty"`

	// RetryCount counts RestoreFailed retries for the current migration
	// attempt. Reset to 0 when the migration completes or the user
	// requests a fresh retry via annotation.
	RetryCount int32 `json:"retryCount,omitempty"`

	// MaxRetries caps RetryCount. Default 3. When RetryCount > MaxRetries
	// the FSM transitions to Failed.
	// +kubebuilder:default=3
	MaxRetries int32 `json:"maxRetries,omitempty"`

	// LastError is the most recent error message that caused a stage
	// transition (e.g. "lazy-pages did not become ready: timeout").
	LastError string `json:"lastError,omitempty"`

	// LastTransitionTime is when Stage last changed. Used for backoff
	// timing in RestoreFailed.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`

	// UploadedDumpID is the dump that finished and was pushed to S3. The
	// Uploaded → Restoring transition uses this to restore the same
	// checkpoint on a fresh target pod across retries.
	UploadedDumpID string `json:"uploadedDumpID,omitempty"`

	// UploadedS3Prefix is the S3 object prefix where UploadedDumpID's
	// metadata + pages live. Passed to the target agent's Restore RPC.
	UploadedS3Prefix string `json:"uploadedS3Prefix,omitempty"`

	// UploadedPipeInodes carries the source workload's stdout/stderr pipe
	// inodes recorded at dump time. Forwarded to the target agent so it
	// can rewire those fds via --inherit-fd, preventing SIGPIPE when the
	// restored process writes to its now-orphaned containerd log pipe.
	UploadedPipeInodes map[string]string `json:"uploadedPipeInodes,omitempty"`

	// PreviousNode is where the source pod ran. Used to skip scheduling
	// target pods back onto the cordoned node.
	PreviousNode string `json:"previousNode,omitempty"`

	// CurrentTargetPod is the name of the target pod we are currently
	// restoring into. Empty between attempts.
	CurrentTargetPod string `json:"currentTargetPod,omitempty"`

	// MigrationReason is the trigger that initiated the migration
	// (spot-interrupt, manual, etc.). Carried across retries so the
	// success record in MigrationHistory is accurate.
	MigrationReason string `json:"migrationReason,omitempty"`

	// PageServerAddr / PageServerPort are populated for lazy-direct and
	// lazy-hybrid strategies so a Restoring → RestoreFailed → Restoring
	// retry can reconnect to the same source page-server.
	PageServerAddr string `json:"pageServerAddr,omitempty"`
	PageServerPort int32  `json:"pageServerPort,omitempty"`
}

// MigrationRecord represents a single migration event
type MigrationRecord struct {
	// FromNode is the source node
	FromNode string `json:"fromNode"`

	// ToNode is the destination node
	ToNode string `json:"toNode"`

	// Timestamp when migration started
	Timestamp metav1.Time `json:"timestamp"`

	// Reason for migration (e.g., "spot-interrupt", "manual")
	Reason string `json:"reason"`

	// Duration of the migration
	Duration string `json:"duration,omitempty"`

	// Success indicates if migration was successful
	Success bool `json:"success"`

	// Message contains additional information
	Message string `json:"message,omitempty"`
}

// CheckpointStatus contains checkpoint information
type CheckpointStatus struct {
	// LastCheckpointID is the ID of the most recent checkpoint
	LastCheckpointID string `json:"lastCheckpointID,omitempty"`

	// LastCheckpointTime is when the last checkpoint was created
	LastCheckpointTime metav1.Time `json:"lastCheckpointTime,omitempty"`

	// CheckpointChainDepth is the current depth of the checkpoint chain
	CheckpointChainDepth int `json:"checkpointChainDepth,omitempty"`

	// TotalCheckpointSize is the total size of all checkpoints (human readable)
	TotalCheckpointSize string `json:"totalCheckpointSize,omitempty"`

	// CheckpointChainRoot is the root checkpoint ID of the current chain
	CheckpointChainRoot string `json:"checkpointChainRoot,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mapp
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Generation",type=integer,JSONPath=`.status.generation`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.currentNode`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// MigratableApp is the Schema for the migratableapps API
type MigratableApp struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MigratableAppSpec   `json:"spec,omitempty"`
	Status MigratableAppStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MigratableAppList contains a list of MigratableApp
type MigratableAppList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MigratableApp `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MigratableApp{}, &MigratableAppList{})
}

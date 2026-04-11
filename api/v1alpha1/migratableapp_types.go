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

	// HotVMASeed controls hot VMA priority seeding. Default: enabled when async prefetch is active.
	// Set to false to disable (ablation: sequential prefetch only).
	HotVMASeed *bool `json:"hotVMASeed,omitempty"`

	// LogUpload enables uploading all raw CRIU logs and stats to S3 after dump/restore.
	// Uploaded files: criu.log, restore.log, lazy-pages.log, stats-dump, stats-restore,
	// hot_vma_metadata.json, and per-fault metrics. Useful for experiment data collection.
	LogUpload bool `json:"logUpload,omitempty"`
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

	// Conditions represent the latest available observations of an object's state
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastUpdateTime is the last time the status was updated
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
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

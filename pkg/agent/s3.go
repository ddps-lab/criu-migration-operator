package agent

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3Client handles S3 operations for checkpoints
type S3Client struct {
	bucket           string
	endpoint         string
	downloadEndpoint string // Separate endpoint for downloads (e.g., CloudFront)
	region           string
	expressOneZone   bool
	session          *session.Session
	uploader         *s3manager.Uploader
}

// NewS3Client creates a new S3 client
func NewS3Client(bucket, endpoint, region string) (*S3Client, error) {
	return NewS3ClientWithOptions(bucket, endpoint, "", region, false)
}

// NewS3ClientWithOptions creates a new S3 client with advanced options
func NewS3ClientWithOptions(bucket, endpoint, downloadEndpoint, region string, expressOneZone bool) (*S3Client, error) {
	cfg := &aws.Config{
		Region: aws.String(region),
	}

	if endpoint != "" {
		cfg.Endpoint = aws.String(endpoint)
		cfg.S3ForcePathStyle = aws.Bool(true) // For MinIO compatibility
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	return &S3Client{
		bucket:           bucket,
		endpoint:         endpoint,
		downloadEndpoint: downloadEndpoint,
		region:           region,
		expressOneZone:   expressOneZone,
		session:          sess,
		uploader:         s3manager.NewUploader(sess),
	}, nil
}

// UploadCheckpoint uploads a checkpoint directory to S3
func (c *S3Client) UploadCheckpoint(ctx context.Context, localDir, s3Prefix string) error {
	var files []string

	// Walk through the directory to collect all files
	err := filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Only upload regular files (skip directories, symlinks, etc.)
		if info.Mode().IsRegular() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	// Upload files in parallel
	return c.uploadFilesParallel(ctx, localDir, s3Prefix, files)
}

// UploadMetadataOnly uploads only metadata files (excluding pages-*.img)
func (c *S3Client) UploadMetadataOnly(ctx context.Context, localDir, s3Prefix string) error {
	var files []string

	err := filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Only upload regular files (skip directories, symlinks, etc.)
		if info.Mode().IsRegular() {
			// Skip pages-*.img files (sent via page-server)
			if !strings.HasPrefix(info.Name(), "pages-") {
				files = append(files, path)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	return c.uploadFilesParallel(ctx, localDir, s3Prefix, files)
}

// uploadFilesParallel uploads multiple files in parallel
func (c *S3Client) uploadFilesParallel(ctx context.Context, baseDir, s3Prefix string, files []string) error {
	const maxConcurrency = 10

	var wg sync.WaitGroup
	errChan := make(chan error, len(files))
	semaphore := make(chan struct{}, maxConcurrency)

	for _, filePath := range files {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()

			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			if err := c.uploadSingleFile(ctx, baseDir, s3Prefix, path); err != nil {
				errChan <- err
			}
		}(filePath)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// uploadSingleFile uploads a single file to S3
func (c *S3Client) uploadSingleFile(ctx context.Context, baseDir, s3Prefix, filePath string) error {
	relPath, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		return fmt.Errorf("failed to get relative path: %w", err)
	}

	s3Key := filepath.Join(s3Prefix, relPath)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	_, err = c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(s3Key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload %s to S3: %w", s3Key, err)
	}

	return nil
}

// DownloadCheckpoint downloads a checkpoint from S3 to local directory
func (c *S3Client) DownloadCheckpoint(ctx context.Context, s3Prefix, localDir string) error {
	// Create local directory
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return fmt.Errorf("failed to create local directory: %w", err)
	}

	// List all objects with the prefix
	svc := s3.New(c.session)
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(s3Prefix),
	}

	var objects []string
	err := svc.ListObjectsV2PagesWithContext(ctx, listInput,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				objects = append(objects, *obj.Key)
			}
			return true
		})
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	// Download files in parallel
	return c.downloadFilesParallel(ctx, s3Prefix, localDir, objects)
}

// DownloadMetadataOnly downloads checkpoint metadata (excluding pages-*.img)
// Downloads entire checkpoint chain from base path (not just single checkpoint)
// Example: if s3Prefix is "checkpoints/my-app/0/node1/checkpoint-id/",
// it downloads all checkpoints from "checkpoints/my-app/0/node1/"
func (c *S3Client) DownloadMetadataOnly(ctx context.Context, s3Prefix, localDir string) error {
	// Extract base path (remove checkpoint-id at the end)
	// Example: "checkpoints/my-app/0/node1/checkpoint-id/" -> "checkpoints/my-app/0/node1/"
	basePath := s3Prefix
	if strings.HasSuffix(basePath, "/") {
		basePath = strings.TrimSuffix(basePath, "/")
	}
	// Remove the last component (checkpoint ID)
	parts := strings.Split(basePath, "/")
	if len(parts) > 0 {
		basePath = strings.Join(parts[:len(parts)-1], "/")
	}
	if basePath != "" && !strings.HasSuffix(basePath, "/") {
		basePath += "/"
	}

	fmt.Printf("Downloading checkpoint chain metadata from base path: %s\n", basePath)

	// List all objects under the base path (entire checkpoint chain)
	svc := s3.New(c.session)
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(basePath),
	}

	var metadataFiles []string
	err := svc.ListObjectsV2PagesWithContext(ctx, listInput,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				// Filter: exclude pages-*.img files (these come from page-server)
				filename := filepath.Base(*obj.Key)
				if !strings.HasPrefix(filename, "pages-") {
					metadataFiles = append(metadataFiles, *obj.Key)
				}
			}
			return true
		})
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	fmt.Printf("Found %d metadata files in checkpoint chain\n", len(metadataFiles))

	// Download filtered files in parallel, preserving directory structure
	// Use basePath instead of s3Prefix to preserve full checkpoint chain structure
	return c.downloadFilesParallel(ctx, basePath, localDir, metadataFiles)
}

// downloadFilesParallel downloads multiple files in parallel
func (c *S3Client) downloadFilesParallel(ctx context.Context, s3Prefix, localDir string, objects []string) error {
	const maxConcurrency = 10

	var wg sync.WaitGroup
	errChan := make(chan error, len(objects))
	semaphore := make(chan struct{}, maxConcurrency)

	downloader := s3manager.NewDownloader(c.session)

	for _, key := range objects {
		wg.Add(1)
		go func(s3Key string) {
			defer wg.Done()

			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			if err := c.downloadSingleFile(ctx, downloader, s3Prefix, localDir, s3Key); err != nil {
				errChan <- err
			}
		}(key)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// downloadSingleFile downloads a single file from S3
func (c *S3Client) downloadSingleFile(ctx context.Context, downloader *s3manager.Downloader,
	s3Prefix, localDir, s3Key string) error {

	// Calculate local file path
	relPath := strings.TrimPrefix(s3Key, s3Prefix)
	relPath = strings.TrimPrefix(relPath, "/")
	localPath := filepath.Join(localDir, relPath)

	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", localPath, err)
	}
	defer file.Close()

	// Download from S3
	_, err = downloader.DownloadWithContext(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return fmt.Errorf("failed to download %s from S3: %w", s3Key, err)
	}

	return nil
}

// DeleteCheckpoint deletes a checkpoint from S3
func (c *S3Client) DeleteCheckpoint(ctx context.Context, s3Prefix string) error {
	svc := s3.New(c.session)

	// List all objects with the prefix
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(s3Prefix),
	}

	var objectsToDelete []*s3.ObjectIdentifier
	err := svc.ListObjectsV2PagesWithContext(ctx, listInput,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{
					Key: obj.Key,
				})
			}
			return true
		})
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	if len(objectsToDelete) == 0 {
		return nil // Nothing to delete
	}

	// Delete objects in batches (S3 limit: 1000 per request)
	const batchSize = 1000
	for i := 0; i < len(objectsToDelete); i += batchSize {
		end := i + batchSize
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}

		batch := objectsToDelete[i:end]
		_, err := svc.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &s3.Delete{
				Objects: batch,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
	}

	return nil
}

// GetCheckpointSize returns the total size of a checkpoint in S3
func (c *S3Client) GetCheckpointSize(ctx context.Context, s3Prefix string) (int64, error) {
	svc := s3.New(c.session)

	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(s3Prefix),
	}

	var totalSize int64
	err := svc.ListObjectsV2PagesWithContext(ctx, listInput,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				totalSize += *obj.Size
			}
			return true
		})
	if err != nil {
		return 0, fmt.Errorf("failed to list objects: %w", err)
	}

	return totalSize, nil
}

// CheckpointExists checks if a checkpoint exists in S3
func (c *S3Client) CheckpointExists(ctx context.Context, s3Prefix string) (bool, error) {
	svc := s3.New(c.session)

	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(s3Prefix),
		MaxKeys: aws.Int64(1),
	}

	result, err := svc.ListObjectsV2WithContext(ctx, listInput)
	if err != nil {
		return false, fmt.Errorf("failed to check existence: %w", err)
	}

	return len(result.Contents) > 0, nil
}

// WriteMetadataFile writes checkpoint metadata to S3
func (c *S3Client) WriteMetadataFile(ctx context.Context, s3Key string, content io.Reader) error {
	_, err := c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(s3Key),
		Body:   content,
	})
	return err
}

// ReadMetadataFile reads checkpoint metadata from S3
func (c *S3Client) ReadMetadataFile(ctx context.Context, s3Key string) ([]byte, error) {
	svc := s3.New(c.session)

	result, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	return io.ReadAll(result.Body)
}

// getEndpoint returns the S3 endpoint URL for CRIU object storage (uploads)
func (c *S3Client) getEndpoint() string {
	if c.endpoint != "" {
		return c.endpoint
	}
	// Default AWS S3 endpoint format
	return fmt.Sprintf("s3.%s.amazonaws.com", c.region)
}

// getDownloadEndpoint returns the endpoint for downloads (may be CloudFront)
func (c *S3Client) getDownloadEndpoint() string {
	if c.downloadEndpoint != "" {
		return c.downloadEndpoint
	}
	// Fall back to upload endpoint
	return c.getEndpoint()
}

// isExpressOneZone returns whether S3 Express One Zone is enabled
func (c *S3Client) isExpressOneZone() bool {
	return c.expressOneZone
}

// needsBucketOption returns whether CRIU needs --object-storage-bucket option
// CloudFront (CDN) doesn't use bucket concept, so we skip it
func (c *S3Client) needsBucketOption() bool {
	// If download endpoint is set and different from upload endpoint,
	// it's likely a CDN (CloudFront) which doesn't need bucket option
	if c.downloadEndpoint != "" && c.downloadEndpoint != c.endpoint {
		return false
	}
	return true
}

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ddps-lab/criu-migration-operator/pkg/agent"
)

func main() {
	// Command-line flags
	port := flag.Int("port", 8080, "gRPC server port")
	workDir := flag.String("work-dir", "/checkpoints", "Working directory for checkpoints")
	mode := flag.String("mode", "normal", "Agent mode: normal or restore")
	flag.Parse()

	// Get configuration from environment
	s3Bucket := os.Getenv("S3_BUCKET")
	if s3Bucket == "" {
		log.Fatal("S3_BUCKET environment variable is required")
	}

	s3Endpoint := os.Getenv("S3_ENDPOINT")
	s3Region := os.Getenv("S3_REGION")
	if s3Region == "" {
		s3Region = "us-east-1"
	}

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = "unknown-pod"
	}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		nodeName = "unknown-node"
	}

	log.Printf("Starting CRIU Agent")
	log.Printf("  Mode: %s", *mode)
	log.Printf("  Port: %d", *port)
	log.Printf("  Work Dir: %s", *workDir)
	log.Printf("  S3 Bucket: %s", s3Bucket)
	log.Printf("  S3 Endpoint: %s", s3Endpoint)
	log.Printf("  Pod Name: %s", podName)
	log.Printf("  Node Name: %s", nodeName)

	// Create work directory
	if err := os.MkdirAll(*workDir, 0755); err != nil {
		log.Fatalf("Failed to create work directory: %v", err)
	}

	// Create agent
	ag, err := agent.NewAgent(*workDir, s3Bucket, s3Endpoint, s3Region, *mode, podName, nodeName)
	if err != nil {
		log.Fatalf("Failed to create agent: %v", err)
	}

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v, shutting down...", sig)
		cancel()
	}()

	// Start agent
	if err := ag.Start(ctx, *port); err != nil {
		log.Fatalf("Agent failed: %v", err)
	}

	log.Println("CRIU Agent shut down gracefully")
}

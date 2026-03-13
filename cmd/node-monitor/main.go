package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ddps-lab/criu-migration-operator/pkg/monitor"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var kubeconfig string
	var cloudType string
	var enableIMDS bool

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file (optional, for out-of-cluster testing)")
	flag.StringVar(&cloudType, "cloud-type", "", "Cloud provider type: aws, gcp, or azure (auto-detect if empty)")
	flag.BoolVar(&enableIMDS, "enable-imds", true, "Enable IMDS-based spot interruption detection")
	flag.Parse()

	// Get node name from environment
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		log.Fatal("NODE_NAME environment variable is required")
	}

	log.Printf("Starting Node Monitor")
	log.Printf("  Node Name: %s", nodeName)
	log.Printf("  Cloud Type: %s", cloudType)
	log.Printf("  Enable IMDS: %v", enableIMDS)

	// Create Kubernetes client
	var config *rest.Config
	var err error

	if kubeconfig != "" {
		// Use kubeconfig file (for local testing)
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		// Use in-cluster config
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		log.Fatalf("Failed to create Kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	// Create monitor
	spotMonitor := monitor.NewSpotMonitorWithConfig(nodeName, clientset, cloudType, enableIMDS)

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

	// Start monitor
	if err := spotMonitor.Start(ctx); err != nil {
		log.Fatalf("Monitor failed: %v", err)
	}

	log.Println("Node Monitor shut down gracefully")
}

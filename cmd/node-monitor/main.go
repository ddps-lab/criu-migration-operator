package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ddps-lab/criu-migration-operator/pkg/monitor"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	var kubeconfig string
	var cloudType string
	var enableIMDS bool
	var checkInterval time.Duration

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file (optional, for out-of-cluster testing)")
	flag.StringVar(&cloudType, "cloud-type", "", "Cloud provider type: aws, gcp, or azure (auto-detect if empty)")
	flag.BoolVar(&enableIMDS, "enable-imds", true, "Enable IMDS-based spot interruption detection")
	flag.DurationVar(&checkInterval, "check-interval", 1*time.Second, "IMDS polling interval")
	flag.Parse()

	// Initialize controller-runtime logger
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrllog.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Get node name from environment
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		log.Fatal("NODE_NAME environment variable is required")
	}

	log.Printf("Starting Node Monitor")
	log.Printf("  Node Name: %s", nodeName)
	log.Printf("  Cloud Type: %s", cloudType)
	log.Printf("  Enable IMDS: %v", enableIMDS)
	log.Printf("  Check Interval: %v", checkInterval)

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

	// Create monitor (will use HTTP to write to CloudWatch Logs)
	spotMonitor := monitor.NewSpotMonitorWithConfig(nodeName, clientset, cloudType, enableIMDS, checkInterval)

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

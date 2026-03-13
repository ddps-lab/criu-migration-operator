package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/ddps-lab/criu-migration-operator/pkg/eventbridge"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	var webhookPort int

	flag.IntVar(&webhookPort, "webhook-port", 8080, "Port for webhook server")
	flag.Parse()

	// Create Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	// Create EventBridge consumer
	consumer, err := eventbridge.NewConsumer(clientset, webhookPort)
	if err != nil {
		panic(err)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	// Start server
	if err := consumer.Start(ctx); err != nil {
		panic(err)
	}
}

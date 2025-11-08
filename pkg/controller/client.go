package controller

import (
	"context"
	"fmt"

	pb "github.com/ddps-lab/criu-migration-operator/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
)

// AgentClient wraps gRPC client for CRIU agent
type AgentClient struct {
	conn   *grpc.ClientConn
	client pb.CRIUAgentClient
	podIP  string
}

// NewAgentClient creates a new agent client for a pod
func NewAgentClient(pod *corev1.Pod) (*AgentClient, error) {
	if pod.Status.PodIP == "" {
		return nil, fmt.Errorf("pod has no IP address")
	}

	addr := fmt.Sprintf("%s:8080", pod.Status.PodIP)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to agent: %w", err)
	}

	return &AgentClient{
		conn:   conn,
		client: pb.NewCRIUAgentClient(conn),
		podIP:  pod.Status.PodIP,
	}, nil
}

// Close closes the agent client connection
func (c *AgentClient) Close() error {
	return c.conn.Close()
}

// PreCheckpoint calls the agent's PreCheckpoint RPC
func (c *AgentClient) PreCheckpoint(ctx context.Context, parentDumpID string) (*pb.PreCheckpointResponse, error) {
	return c.client.PreCheckpoint(ctx, &pb.PreCheckpointRequest{
		ParentDumpId: parentDumpID,
		UploadToS3:   true,
	})
}

// FinalDump calls the agent's FinalDump RPC
func (c *AgentClient) FinalDump(ctx context.Context, pageServerAddr string, pageServerPort int32, parentDumpID string) (*pb.FinalDumpResponse, error) {
	return c.client.FinalDump(ctx, &pb.FinalDumpRequest{
		PageServerAddr: pageServerAddr,
		PageServerPort: pageServerPort,
		ParentDumpId:   parentDumpID,
		LeaveRunning:   false,
	})
}

// Restore calls the agent's Restore RPC
func (c *AgentClient) Restore(ctx context.Context, dumpID, s3Bucket, s3Prefix, sourceAddr string) (*pb.RestoreResponse, error) {
	return c.client.Restore(ctx, &pb.RestoreRequest{
		DumpId:         dumpID,
		S3Bucket:       s3Bucket,
		S3Prefix:       s3Prefix,
		UseLazyPages:   true,
		PageServerPort: 9999,
		SourceAddr:     sourceAddr, // Source pod IP for lazy-pages connection
	})
}

// GetStatus calls the agent's GetStatus RPC
func (c *AgentClient) GetStatus(ctx context.Context) (*pb.StatusResponse, error) {
	return c.client.GetStatus(ctx, &pb.StatusRequest{})
}

// StartPageServer calls the agent's StartPageServer RPC
func (c *AgentClient) StartPageServer(ctx context.Context, port int32, checkpointDir string) (*pb.PageServerResponse, error) {
	return c.client.StartPageServer(ctx, &pb.PageServerRequest{
		Port:          port,
		CheckpointDir: checkpointDir,
		Daemon:        true,
	})
}

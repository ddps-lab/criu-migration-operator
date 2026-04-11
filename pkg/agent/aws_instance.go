package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

const (
	imdsTokenURL    = "http://169.254.169.254/latest/api/token"
	imdsMetadataURL = "http://169.254.169.254/latest/meta-data"
	imdsTokenTTL    = "60"
)

// AWSInstanceInfo holds discovered instance metadata.
type AWSInstanceInfo struct {
	InstanceType         string
	Region               string
	AvailabilityZone     string
	BaselineBandwidthGbps float64
	PeakBandwidthGbps    float64
}

// DetectAWSInstance queries IMDS for instance type and EC2 API for network bandwidth.
// Returns nil if not running on AWS or if detection fails.
func DetectAWSInstance() *AWSInstanceInfo {
	client := &http.Client{Timeout: 2 * time.Second}

	// Step 1: Get IMDSv2 token
	token, err := getIMDSv2Token(client)
	if err != nil {
		log.Printf("[AWS-DETECT] Not on AWS or IMDS unavailable: %v", err)
		return nil
	}

	// Step 2: Get instance type
	instanceType, err := queryIMDS(client, token, "/instance-type")
	if err != nil {
		log.Printf("[AWS-DETECT] Failed to get instance type: %v", err)
		return nil
	}

	// Step 3: Get region from AZ
	az, err := queryIMDS(client, token, "/placement/availability-zone")
	if err != nil {
		log.Printf("[AWS-DETECT] Failed to get AZ: %v", err)
		return nil
	}
	// Region = AZ minus the last character (e.g., us-east-1a → us-east-1)
	region := az
	if len(az) > 0 {
		region = az[:len(az)-1]
	}

	info := &AWSInstanceInfo{
		InstanceType:     instanceType,
		Region:           region,
		AvailabilityZone: az,
	}

	// Step 4: Query EC2 API for network bandwidth
	baseline, peak, err := queryEC2NetworkBandwidth(instanceType, region)
	if err != nil {
		log.Printf("[AWS-DETECT] Failed to query EC2 API for bandwidth: %v", err)
		log.Printf("[AWS-DETECT] Instance: %s, Region: %s (bandwidth will use default)", instanceType, region)
		return info
	}

	info.BaselineBandwidthGbps = baseline
	info.PeakBandwidthGbps = peak

	log.Printf("[AWS-DETECT] Instance: %s, Region: %s, Bandwidth: baseline=%.2f Gbps, peak=%.2f Gbps",
		instanceType, region, baseline, peak)
	return info
}

// getIMDSv2Token fetches an IMDSv2 session token.
func getIMDSv2Token(client *http.Client) (string, error) {
	req, err := http.NewRequest("PUT", imdsTokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", imdsTokenTTL)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("IMDS token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IMDS token request returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// queryIMDS fetches a metadata value from IMDS.
func queryIMDS(client *http.Client, token, path string) (string, error) {
	req, err := http.NewRequest("GET", imdsMetadataURL+path, nil)
	if err != nil {
		return "", err
	}
	if token != "" {
		req.Header.Set("X-aws-ec2-metadata-token", token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IMDS %s returned %d", path, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// queryEC2NetworkBandwidth calls ec2:DescribeInstanceTypes to get network bandwidth.
// Returns (baselineGbps, peakGbps, error).
func queryEC2NetworkBandwidth(instanceType, region string) (float64, float64, error) {
	// Use agent's AWS credentials if available, otherwise use default chain
	var sess *session.Session
	var err error

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	cfg := &aws.Config{
		Region: aws.String(region),
	}

	if accessKey != "" && secretKey != "" {
		cfg.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}

	sess, err = session.NewSession(cfg)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create AWS session: %w", err)
	}

	svc := ec2.New(sess)

	input := &ec2.DescribeInstanceTypesInput{
		InstanceTypes: []*string{aws.String(instanceType)},
	}

	result, err := svc.DescribeInstanceTypes(input)
	if err != nil {
		return 0, 0, fmt.Errorf("DescribeInstanceTypes failed: %w", err)
	}

	if len(result.InstanceTypes) == 0 {
		return 0, 0, fmt.Errorf("no instance type info returned for %s", instanceType)
	}

	it := result.InstanceTypes[0]
	if it.NetworkInfo == nil {
		return 0, 0, fmt.Errorf("no NetworkInfo for %s", instanceType)
	}

	var baseline, peak float64

	// NetworkCards contains per-card bandwidth info
	if len(it.NetworkInfo.NetworkCards) > 0 {
		card := it.NetworkInfo.NetworkCards[0]
		if card.BaselineBandwidthInGbps != nil {
			baseline = *card.BaselineBandwidthInGbps
		}
		if card.PeakBandwidthInGbps != nil {
			peak = *card.PeakBandwidthInGbps
		}
	}

	// Fallback: parse NetworkPerformance string if structured data unavailable
	if baseline == 0 && it.NetworkInfo.NetworkPerformance != nil {
		perf := *it.NetworkInfo.NetworkPerformance
		baseline = parseNetworkPerformance(perf)
		if peak == 0 {
			peak = baseline
		}
	}

	return baseline, peak, nil
}

// parseNetworkPerformance extracts Gbps from strings like "Up to 10 Gigabit", "25 Gigabit", etc.
func parseNetworkPerformance(perf string) float64 {
	var gbps float64
	// Try "Up to N Gigabit"
	if n, _ := fmt.Sscanf(perf, "Up to %f Gigabit", &gbps); n == 1 {
		return gbps
	}
	// Try "N Gigabit"
	if n, _ := fmt.Sscanf(perf, "%f Gigabit", &gbps); n == 1 {
		return gbps
	}
	// "Low to Moderate" → ~0.3, "Moderate" → ~0.5, "High" → ~1
	switch perf {
	case "Low to Moderate":
		return 0.3
	case "Moderate":
		return 0.5
	case "High":
		return 1.0
	}
	return 0
}

// BaselineBandwidthMBps returns the baseline bandwidth in MB/s (bytes, not bits).
// Converts from Gbps (network bits) to MB/s (storage bytes).
func (info *AWSInstanceInfo) BaselineBandwidthMBps() float64 {
	if info == nil || info.BaselineBandwidthGbps == 0 {
		return 0
	}
	// 1 Gbps = 125 MB/s (1000/8)
	return info.BaselineBandwidthGbps * 125
}

// ToJSON returns a JSON representation for logging.
func (info *AWSInstanceInfo) ToJSON() string {
	if info == nil {
		return "{}"
	}
	b, _ := json.Marshal(info)
	return string(b)
}

// NetworkBandwidthInfo holds detected network bandwidth for any environment.
type NetworkBandwidthInfo struct {
	Source          string  // "aws", "on-premise", "manual"
	InstanceType    string  // AWS instance type or "local"
	BaselineMBps    float64 // Baseline bandwidth in MB/s (bytes, not bits)
	PeakMBps        float64 // Peak bandwidth in MB/s
	InterfaceName   string  // Network interface used (on-premise)
	InterfaceSpeedMbps int  // Raw link speed in Mbps (on-premise)
}

// DetectNetworkBandwidth auto-detects network bandwidth.
// Priority: AWS IMDS → on-premise NIC speed → manual (env var) → default.
func DetectNetworkBandwidth() *NetworkBandwidthInfo {
	// Try AWS first (fast fail if not on AWS — 2s timeout)
	awsInfo := DetectAWSInstance()
	if awsInfo != nil && awsInfo.BaselineBandwidthGbps > 0 {
		return &NetworkBandwidthInfo{
			Source:       "aws",
			InstanceType: awsInfo.InstanceType,
			BaselineMBps: awsInfo.BaselineBandwidthMBps(),
			PeakMBps:     awsInfo.PeakBandwidthGbps * 125,
		}
	}

	// On-premise: read NIC speed from sysfs
	if info := detectOnPremiseNIC(); info != nil {
		return info
	}

	// Manual fallback from env var
	if bw := os.Getenv("BANDWIDTH_MBPS"); bw != "" {
		if v, err := strconv.ParseFloat(bw, 64); err == nil && v > 0 {
			return &NetworkBandwidthInfo{
				Source:       "manual",
				BaselineMBps: v,
				PeakMBps:     v,
			}
		}
	}

	// Default
	return &NetworkBandwidthInfo{
		Source:       "default",
		BaselineMBps: 100,
		PeakMBps:     100,
	}
}

// detectOnPremiseNIC reads network interface speed from /sys/class/net/*/speed.
// Returns the fastest non-loopback, non-virtual interface.
func detectOnPremiseNIC() *NetworkBandwidthInfo {
	entries, err := os.ReadDir("/sys/class/net")
	if err != nil {
		return nil
	}

	var bestName string
	var bestSpeed int

	for _, entry := range entries {
		name := entry.Name()
		// Skip loopback, virtual, and bridge interfaces
		if name == "lo" || strings.HasPrefix(name, "veth") ||
			strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "br-") ||
			strings.HasPrefix(name, "flannel") || strings.HasPrefix(name, "cni") ||
			strings.HasPrefix(name, "vxlan") {
			continue
		}

		// Check if interface is up (operstate)
		opPath := filepath.Join("/sys/class/net", name, "operstate")
		opBytes, err := os.ReadFile(opPath)
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(opBytes)) != "up" {
			continue
		}

		// Read speed (in Mbps)
		speedPath := filepath.Join("/sys/class/net", name, "speed")
		speedBytes, err := os.ReadFile(speedPath)
		if err != nil {
			continue
		}
		speed, err := strconv.Atoi(strings.TrimSpace(string(speedBytes)))
		if err != nil || speed <= 0 {
			continue
		}

		if speed > bestSpeed {
			bestSpeed = speed
			bestName = name
		}
	}

	if bestSpeed <= 0 {
		return nil
	}

	// Convert Mbps (megabits) to MB/s (megabytes)
	baselineMBps := float64(bestSpeed) / 8.0

	log.Printf("[NET-DETECT] On-premise NIC: %s, speed=%d Mbps → baseline=%.0f MB/s",
		bestName, bestSpeed, baselineMBps)

	return &NetworkBandwidthInfo{
		Source:             "on-premise",
		InstanceType:       "local",
		BaselineMBps:       baselineMBps,
		PeakMBps:           baselineMBps,
		InterfaceName:      bestName,
		InterfaceSpeedMbps: bestSpeed,
	}
}

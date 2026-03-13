#!/bin/bash
set -e

# Phase 4 Deployment Script: AWS EKS Spot Instance with Dual-Path
# This script automates the deployment of CRIU Migration Operator on AWS EKS

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== CRIU Migration Operator - Phase 4 Deployment ==="
echo "Project Root: $PROJECT_ROOT"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
PREFIX="jglee"
AWS_REGION="us-east-1"
AWS_PROFILE="default"
TAG="${PREFIX}-phase4"

log_info "Configuration:"
echo "  Prefix: $PREFIX"
echo "  AWS Region: $AWS_REGION"
echo "  AWS Profile: $AWS_PROFILE"
echo "  Tag: $TAG"
echo ""

# Step 1: Verify prerequisites
log_info "Verifying prerequisites..."

commands=("aws" "terraform" "kubectl" "docker")
for cmd in "${commands[@]}"; do
    if ! command -v $cmd &> /dev/null; then
        log_error "$cmd is not installed"
        exit 1
    fi
done

log_info "All prerequisites met"
echo ""

# Step 2: Initialize and deploy Terraform
log_info "Setting up Terraform infrastructure..."

cd "$PROJECT_ROOT/terraform"

terraform init
log_info "Terraform initialized"

terraform validate
log_info "Terraform configuration validated"

terraform plan -out=tfplan
read -p "Review plan above. Continue with terraform apply? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_warn "Terraform apply cancelled"
    exit 0
fi

terraform apply tfplan
log_info "Terraform infrastructure deployed"

# Capture outputs
MIGRATION_ROLE_ARN=$(terraform output -raw migration_controller_role_arn)
AWS_ACCOUNT=$(terraform output -raw aws_account_id)
CLUSTER_NAME="${PREFIX}-criu-migration-test"
REGISTRY="${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"

log_info "Deployment outputs:"
echo "  AWS Account: $AWS_ACCOUNT"
echo "  Cluster Name: $CLUSTER_NAME"
echo "  Registry: $REGISTRY"
echo "  Tag: $TAG"
echo "  Migration Role ARN: $MIGRATION_ROLE_ARN"
echo ""

# Step 3: Update kubeconfig
log_info "Updating kubeconfig..."
aws eks update-kubeconfig --region ${AWS_REGION} --name ${CLUSTER_NAME} --profile ${AWS_PROFILE}
log_info "kubeconfig updated"
echo ""

# Step 4: Build and push Docker images
log_info "Building and pushing Docker images..."

# Create ECR repositories
for repo in criu-migration-controller criu-node-monitor criu-agent criu-eventbridge-consumer; do
    aws ecr create-repository --repository-name $repo --region ${AWS_REGION} --profile ${AWS_PROFILE} 2>/dev/null || true
done
log_info "ECR repositories ready"

# Login to ECR
aws ecr get-login-password --region ${AWS_REGION} --profile ${AWS_PROFILE} | \
    docker login --username AWS --password-stdin ${REGISTRY}
log_info "Logged in to ECR"

# Build all images
cd "$PROJECT_ROOT"

images=(
    "criu-migration-controller:deploy/controller/Dockerfile"
    "criu-node-monitor:deploy/node-monitor/Dockerfile"
    "criu-agent:deploy/agent/Dockerfile"
    "criu-eventbridge-consumer:deploy/eventbridge-consumer/Dockerfile"
)

log_info "Building Docker images for linux/amd64..."
for image_spec in "${images[@]}"; do
    IFS=':' read -r image_name dockerfile <<< "$image_spec"
    log_info "Building ${image_name}..."
    docker build --platform linux/amd64 \
        -t ${REGISTRY}/${image_name}:${TAG} \
        -f ${dockerfile} .
    log_info "${image_name} built"
done

# Push images
log_info "Pushing images to ECR..."
for image_spec in "${images[@]}"; do
    IFS=':' read -r image_name dockerfile <<< "$image_spec"
    docker push ${REGISTRY}/${image_name}:${TAG}
done
log_info "All images pushed"
echo ""

# Step 5: Deploy Kubernetes manifests
log_info "Deploying Kubernetes manifests..."

# Create temporary manifests with proper image references
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Copy and update manifests
cp -r config $TEMP_DIR/

# Update image references and registry
sed -i.bak "s|REGISTRY|${REGISTRY}|g" $TEMP_DIR/config/manager/manager.yaml
sed -i.bak "s|TAG|${TAG}|g" $TEMP_DIR/config/manager/manager.yaml
sed -i.bak "s|CONTROLLER_IMAGE|criu-migration-controller|g" $TEMP_DIR/config/manager/manager.yaml
sed -i.bak "s|MONITOR_IMAGE|criu-node-monitor|g" $TEMP_DIR/config/manager/manager.yaml

sed -i.bak "s|REGISTRY|${REGISTRY}|g" $TEMP_DIR/config/manager/eventbridge-consumer.yaml
sed -i.bak "s|TAG|${TAG}|g" $TEMP_DIR/config/manager/eventbridge-consumer.yaml
sed -i.bak "s|EVENTBRIDGE_CONSUMER_IMAGE|criu-eventbridge-consumer|g" $TEMP_DIR/config/manager/eventbridge-consumer.yaml

# Deploy
kubectl apply -f $TEMP_DIR/config/crd/
log_info "CRDs deployed"

kubectl apply -f $TEMP_DIR/config/rbac/
log_info "RBAC deployed"

kubectl apply -f $TEMP_DIR/config/manager/manager.yaml
log_info "Manager and monitoring components deployed"

kubectl apply -f $TEMP_DIR/config/manager/eventbridge-consumer.yaml
log_info "EventBridge consumer deployed"

echo ""

# Step 6: Configure IRSA
log_info "Configuring IRSA..."
kubectl annotate serviceaccount migration-controller \
    -n migration-system \
    eks.amazonaws.com/role-arn=${MIGRATION_ROLE_ARN} \
    --overwrite=true
log_info "IRSA configured"
echo ""

# Step 7: Wait for deployment
log_info "Waiting for deployment to become ready..."
kubectl wait --for=condition=ready pod \
    -l app=migration-controller \
    -n migration-system \
    --timeout=300s 2>/dev/null || log_warn "Timeout waiting for pod (may still be starting)"

echo ""
log_info "Deployment complete!"
echo ""
echo "Next steps:"
echo "1. Verify all pods are running:"
echo "   kubectl get pods -n migration-system"
echo ""
echo "2. Get EventBridge consumer URL:"
echo "   kubectl get svc eventbridge-consumer -n migration-system"
echo ""
echo "3. Apply sample MigratableApp:"
echo "   kubectl apply -f config/samples/migratableapp_sample.yaml"
echo ""
echo "4. Monitor spot detection:"
echo "   kubectl logs -n migration-system -l app=node-monitor -f"
echo ""

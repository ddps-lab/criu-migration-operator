#!/bin/bash

set -e

# Install protoc and plugins if not already installed
if ! command -v protoc &> /dev/null; then
    echo "protoc not found. Please install protobuf compiler."
    echo "Visit: https://grpc.io/docs/protoc-installation/"
    exit 1
fi

if ! command -v protoc-gen-go &> /dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Installing protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

# Generate Go code from proto files
echo "Generating gRPC code from protobuf..."

protoc \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    pkg/proto/agent.proto

echo "gRPC code generation complete!"

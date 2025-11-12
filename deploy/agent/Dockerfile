# Build stage
FROM golang:1.25.3-alpine AS builder

WORKDIR /workspace

# Install build dependencies
RUN apk add --no-cache git make protobuf-dev

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY api/ api/
COPY pkg/ pkg/
COPY cmd/agent/ cmd/agent/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o agent cmd/agent/main.go

# Runtime stage
FROM ubuntu:24.04

# Install CRIU runtime dependencies and kubectl
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libprotobuf-c1 \
    libnl-3-200 \
    libnl-genl-3-200 \
    libnet1 \
    libcap2 \
    libgnutls30 \
    libnftables1 \
    libbsd0 \
    libdrm2 \
    libcurl4 \
    libssl3 \
    util-linux \
    curl \
    && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/kubectl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /workspace/agent .

# Copy CRIU binary (should be downloaded via Makefile)
COPY criu/criu /usr/local/bin/criu
RUN chmod +x /usr/local/bin/criu

# Create working directory for checkpoints
RUN mkdir -p /tmp/.criu-checkpoints && chmod 755 /tmp/.criu-checkpoints

# Run as root (required for CRIU)
USER root

ENTRYPOINT ["/app/agent"]

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

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
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
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /workspace/agent .

# Download CRIU binary (using pre-built from releases)
RUN curl -fsSL https://github.com/checkpoint-restore/criu/releases/download/v3.18/criu-3.18.linux-amd64 \
    -o /usr/local/bin/criu && \
    chmod +x /usr/local/bin/criu || \
    echo "Warning: CRIU binary download failed, image will still work for basic operations"

# Create working directory for checkpoints
RUN mkdir -p /checkpoints && chmod 755 /checkpoints

# Run as root (required for CRIU)
USER root

ENTRYPOINT ["/app/agent"]

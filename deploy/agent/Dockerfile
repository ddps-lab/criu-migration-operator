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
FROM alpine:3.18

# Install CRIU runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    libnet \
    libnl3 \
    protobuf-c \
    && rm -rf /var/cache/apk/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /workspace/agent .

# Copy CRIU binary (should be downloaded via Makefile)
COPY criu/criu /usr/local/bin/criu
RUN chmod +x /usr/local/bin/criu

# Create working directory for checkpoints
RUN mkdir -p /checkpoints && chmod 755 /checkpoints

# Run as root (required for CRIU)
USER root

ENTRYPOINT ["/app/agent"]

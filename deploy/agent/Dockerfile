# Build stage
FROM golang:1.21-alpine AS builder

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

# Install CRIU and dependencies
RUN apk add --no-cache \
    criu \
    ca-certificates \
    && rm -rf /var/cache/apk/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /workspace/agent .

# Create working directory for checkpoints
RUN mkdir -p /checkpoints && chmod 755 /checkpoints

# Run as root (required for CRIU)
USER root

ENTRYPOINT ["/app/agent"]

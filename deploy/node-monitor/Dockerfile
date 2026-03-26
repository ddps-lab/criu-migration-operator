# Build stage
FROM golang:1.25.3-alpine AS builder

WORKDIR /workspace

# Install build dependencies
RUN apk add --no-cache git make

# Copy go mod files
COPY go.mod ./

# Copy source code
COPY api/ api/
COPY pkg/ pkg/
COPY cmd/node-monitor/ cmd/node-monitor/

# Download dependencies and ensure go.sum is up to date
RUN go mod tidy && go mod download

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o node-monitor cmd/node-monitor/main.go

# Runtime stage
FROM alpine:3.18

# Install ca-certificates for HTTPS requests
RUN apk add --no-cache ca-certificates && rm -rf /var/cache/apk/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /workspace/node-monitor .

USER nobody

ENTRYPOINT ["/app/node-monitor"]

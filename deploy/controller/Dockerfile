# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /workspace

# Install build dependencies
RUN apk add --no-cache git make

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY api/ api/
COPY pkg/ pkg/
COPY cmd/controller/ cmd/controller/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o controller cmd/controller/main.go

# Runtime stage
FROM gcr.io/distroless/static:nonroot

WORKDIR /

# Copy binary from builder
COPY --from=builder /workspace/controller .

USER 65532:65532

ENTRYPOINT ["/controller"]

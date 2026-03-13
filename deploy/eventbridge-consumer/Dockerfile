# Build stage
FROM golang:1.25 AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -a -installsuffix cgo -o eventbridge-consumer ./cmd/eventbridge-consumer/

# Final stage
FROM gcr.io/distroless/static:nonroot

WORKDIR /

# Copy binary from builder
COPY --from=builder /app/eventbridge-consumer /eventbridge-consumer

# Set user to nonroot (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/eventbridge-consumer"]

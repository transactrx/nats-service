# Use the official Golang image based on Alpine Linux
FROM golang:1.24.3-alpine AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the Go app
# Using CGO_ENABLED=0 to build a statically linked binary to ensure it runs in a minimal Alpine image
RUN CGO_ENABLED=0 go build -v -o nats-service-example ./cmd/nats-service-example
RUN CGO_ENABLED=0 go build -v -o requester-example ./cmd/requester-example


# Start a new stage from scratch for a leaner final image
FROM alpine:latest

WORKDIR /root/

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/nats-service-example .
COPY --from=builder /app/requester-example .

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
# This will be overridden by the docker-compose command or run_tests.sh
CMD ["./nats-service-example"]

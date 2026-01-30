# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Go framework for building microservices with NATS messaging. It provides a request-response pattern over NATS with automatic compression, chunking for large messages, and path parameter routing.

## Essential Commands

### Testing
```bash
# Run all tests with Docker Compose (includes NATS server)
./run_tests.sh

# Run tests locally (requires NATS running at localhost:4222)
NATS_URL=nats://localhost:4222 NATS_QUEUE_NAME=testing go test -v ./pkg/nats-service

# Run tests with vendor dependencies
go test -mod=vendor -v ./...

# Start NATS server for local testing
docker-compose up -d nats
```

### Development
```bash
# Format code
gofmt -w .

# Vet code
go vet ./...

# Build all packages
go build ./...

# Run example service
cd cmd/nats-service-example && NATS_URL=nats://localhost:4222 NATS_QUEUE_NAME=example go run main.go

# Run example client
cd cmd/requester-example && NATS_URL=nats://localhost:4222 go run main.go
```

## Architecture

### Core Components

**pkg/nats-service** - Server-side service framework
- `nats-service.go`: Main service implementation with endpoint routing
- `handlers.go`: Example handler functions showing patterns
- `chunks-handling.go`: Automatic chunking for large messages
- `error-types.go`: Typed error responses with status codes
- `endpoint-stats.go`: Endpoint performance tracking

**pkg/nats-service-client** - Client library
- `nats-service-client.go`: Client implementation with automatic decompression/dechunking
- `header.go`: Header management utilities

**pkg/nats-service-common** - Shared utilities
- `common.go`: Compression, chunking utilities, and header constants

### Message Flow

1. **Client Request**: Client compresses large payloads (>2KB) and sends to NATS subject
2. **Service Routing**: Service matches subject against registered endpoints using regex patterns
3. **Handler Execution**: Matched endpoint handler processes request
4. **Response Handling**: Large responses (>300KB) are automatically chunked
5. **Client Reassembly**: Client automatically handles decompression and chunk reassembly

### Key Features

**Path Parameters**: Endpoints support dynamic routing with `:param` syntax
- Pattern: `users/:userId/orders/:orderId`
- Handler accesses via `msg.Parameters["userId"]`

**Wildcards**: NATS wildcard subjects are supported
- `*` matches a single token between periods
- `>` matches everything after (suffix wildcard)

**Message Chunking**: Responses exceeding `maxRespSizeToChunk` are automatically split
- Service stores chunks in TTL cache (3-minute expiry)
- Client retrieves chunks via dedicated subject
- Transparent to handler and client code

**Compression**: Automatic gzip compression for large payloads
- Request compression at client when >2KB (`maxSizeBeforeCompress`)
- Response compression at service when >2KB
- Automatic decompression by recipient

**Endpoint Discovery**: Services automatically register for discovery
- Discovery subject: `_discovery.all`
- Returns service name, base path, and all registered endpoints
- Includes parameter names, descriptions, and wildcard types
- Graceful degradation if subscription fails (service continues normally)

### Important Implementation Details

**Endpoint Registration**: Use `AddEndpoint()` or `AddEndpointWithDoc()` before `Start()`
- Endpoints are matched by regex against incoming NATS subjects
- Path separator can be `.` or `/` (detected automatically)
- Parameters extracted via regex named groups
- Use `AddEndpointWithDoc()` to include descriptions for discovery
- Use `AddEndpointWithDocs()` for batch registration with descriptions

**Error Handling**: Return `*NatsServiceError` from handlers
- Status codes: 400 (validation), 403 (auth), 404 (not found), 500 (server error)
- Special status 302 indicates forwarded message (no response sent)
- Errors automatically marshaled to JSON with status header

**Message Context**: `NatsMessage` provides full request context
- `Body`: Request payload bytes
- `Header`: NATS headers (use NATS header constants from common package)
- `Parameters`: Extracted path parameters
- `Logger`: Pre-configured logger with message ID
- `ResponseBody`: Set this to return response data
- `ResponseHeader`: Optional custom response headers

**Connection Management**: Automatic reconnection with exponential backoff
- 15-minute total reconnection window
- 1-second delay between attempts
- Connection closed handler exits process (-1)

## Environment Variables

Required:
- `NATS_URL`: NATS server URL (e.g., `nats://localhost:4222`)
- `NATS_QUEUE_NAME`: Queue group name for service load balancing

Optional:
- `NATS_JWT`: JWT token for authenticated connections
- `NATS_KEY`: Private key for authenticated connections
- `NATS_DEBUG`: Enable debug logging (`true`/`false`)
- `APPID`: Application identifier for connection naming
- `MAX_SIZE_BEFORE_COMPRESS`: Client compression threshold (bytes)
- `MAX_SIZE_BEFORE_CHUNK`: Client chunking threshold (bytes)

## Code Patterns

### Creating a Service Handler
```go
func MyHandler(msg *natsservice.NatsMessage) *natsservice.NatsServiceError {
    // Access request body
    var requestData MyRequestType
    json.Unmarshal(msg.Body, &requestData)

    // Access path parameters
    userId := msg.Parameters["userId"]

    // Access headers
    traceId := msg.Header.Get("X-Trace-ID")

    // Log with context
    msg.Logger.Printf("Processing request for user %s", userId)

    // Set response (framework handles compression/chunking)
    responseData, _ := json.Marshal(myResponse)
    msg.ResponseBody = responseData

    // Set custom status code if needed
    msg.ResponseHeader = nats.Header{}
    msg.ResponseHeader.Set(nats_service_common.STATUS, "201")

    // Return nil for success or error for failure
    return nil
}
```

### Error Handling Pattern
```go
// Validation error (400)
if invalid {
    err := natsservice.NewValidationError("invalid input", 4001, err)
    return &err
}

// Server error (500)
if err != nil {
    svcErr := natsservice.NewServerError("processing failed", 5001, err)
    return &svcErr
}

// Authorization error (403)
if unauthorized {
    err := natsservice.NewAuthorizationError("access denied", 4031, err)
    return &err
}

// Forwarded message (302) - indicates message was forwarded, no response needed
forwardErr := natsservice.NewForwardedError("forwarded to another service")
return &forwardErr
```

### Endpoint Discovery CLI

The `nats-discover` CLI tool discovers all services running the nats-service framework:

```bash
# Build the CLI
go build -o nats-discover ./cmd/nats-discover

# Discover services (table format)
nats-discover -s nats://localhost:4222

# JSON output
nats-discover -s nats://localhost:4222 --format json

# YAML output
nats-discover -s nats://localhost:4222 --format yaml

# With NATS CLI context (reads from ~/.config/nats/context/)
nats-discover --context mycontext

# Custom timeout
nats-discover -s nats://localhost:4222 --timeout 5s
```

### Registering Endpoints with Documentation
```go
// Single endpoint with description
err := service.AddEndpointWithDoc("users.:userId", "Get user by ID", userHandler)

// Batch registration
endpoints := []nats_service.EndpointRegistration{
    {Path: "ping", Description: "Health check", Handler: pingHandler},
    {Path: "users.:userId", Description: "Get user by ID", Handler: userHandler},
}
err := service.AddEndpointWithDocs(endpoints)
```

## Development Notes

- This codebase uses Go 1.25.2
- Dependencies are vendored in `vendor/` directory
- Tests require a running NATS server (Docker Compose handles this)
- The framework uses `dlclark/regexp2` for advanced regex features (named groups, wildcards)
- TTL cache (`jellydator/ttlcache/v3`) stores message chunks with automatic expiration
- Before submitting PR, ensure: `gofmt -w .`, `go vet ./...`, `go test ./...`, `go build ./...` all succeed
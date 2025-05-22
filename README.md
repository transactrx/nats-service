# NATS Service Framework

A simple yet powerful framework for building microservices with NATS messaging.

## Overview

NATS Service Framework makes it easy to build microservices that communicate over NATS. It provides:

- **Service Creation**: Define endpoints and handlers in a familiar pattern
- **Client Libraries**: Simple clients to call your services
- **Automatic Features**: Compression, chunking for large messages, timeouts
- **Path Parameters**: Support for dynamic routes like `users/:userId`

## Installation

```bash
go get github.com/transactrx/nats-service
```

## Quick Example

### Creating a Service

```go
package main

import (
    "github.com/transactrx/nats-service/pkg/nats-service"
    "log"
    "time"
)

func main() {
    // Create a NATS service
    service, err := natsservice.NewNATSService(&natsservice.NATSServiceOpts{
        NatsURL:    "nats://localhost:4222",
        QueueGroup: "my-service",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Add a handler for a simple endpoint
    service.AddHandler("getTime", func(msg *natsservice.NatsMessage) (interface{}, error) {
        return time.Now().Format(time.RFC3339), nil
    })

    // Start the service
    service.Start()
}
```

### Creating a Client

```go
package main

import (
    "github.com/transactrx/nats-service/pkg/nats-service-client"
    "log"
    "fmt"
)

func main() {
    // Create a NATS client
    client, err := natsclient.NewNATSServiceClient(&natsclient.NATSServiceClientOpts{
        NatsURL: "nats://localhost:4222",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Send a request
    var response string
    err = client.Request("getTime", "", &response)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Current time:", response)
}
```

## Creating Services

### Service Configuration

```go
service, err := natsservice.NewNATSService(&natsservice.NATSServiceOpts{
    NatsURL:              "nats://localhost:4222",  // NATS server URL
    QueueGroup:           "my-service",             // Queue group for load balancing
    ConnectionName:       "my-cool-service",        // Name for monitoring
    ReconnectWait:        1 * time.Second,          // Time between reconnection attempts
    MaxReconnectAttempts: 10,                       // Max reconnection attempts
})
```

### Adding Handlers

```go
// Simple handler
service.AddHandler("ping", func(msg *natsservice.NatsMessage) (interface{}, error) {
    return "pong", nil
})

// Handler with request body
service.AddHandler("echo", func(msg *natsservice.NatsMessage) (interface{}, error) {
    return msg.Body, nil
})

// Handler with path parameters
service.AddHandler("users/:id", func(msg *natsservice.NatsMessage) (interface{}, error) {
    userId := msg.Params["id"]
    return fmt.Sprintf("Hello, user %s", userId), nil
})
```

### Error Handling in Handlers

```go
service.AddHandler("getTimeError", func(msg *natsservice.NatsMessage) (interface{}, error) {
    // Return an application error
    return nil, natsservice.NewAppError("simulated error getting time")
})
```

## Creating Clients

### Client Configuration

```go
client, err := natsclient.NewNATSServiceClient(&natsclient.NATSServiceClientOpts{
    NatsURL:              "nats://localhost:4222",  // NATS server URL
    ConnectionName:       "my-client",              // Name for monitoring
    ReconnectWait:        1 * time.Second,          // Time between reconnection attempts
    MaxReconnectAttempts: 10,                       // Max reconnection attempts
    RequestTimeout:       5 * time.Second,          // Request timeout
})
```

### Making Requests

```go
// Simple request
var response string
err = client.Request("ping", "", &response)

// Request with body
var echoResponse string
err = client.Request("echo", "hello world", &echoResponse)

// Request with path parameter
var userResponse string
err = client.Request("users/123", "", &userResponse)

// Request with header
var response string
err = client.RequestWithHeader("ping", "", map[string]string{
    "X-Trace-ID": "abc123",
}, &response)
```

### Handling Errors

```go
var response string
err = client.Request("getTimeError", "", &response)
if err != nil {
    if appErr, ok := err.(*natsclient.AppError); ok {
        fmt.Printf("Application error: %s (code: %d)\n", appErr.ErrorMessage, appErr.ApiStatusCode)
    } else {
        fmt.Printf("Request error: %s\n", err)
    }
}
```

## Advanced Features

### Large Message Chunking

For messages that exceed NATS size limits, the framework automatically handles chunking:

```go
// Service side - no special handling needed
service.AddHandler("getLargeData", func(msg *natsservice.NatsMessage) (interface{}, error) {
    // Return a large response - chunking happens automatically
    return generateLargeResponse(), nil
})

// Client side - no special handling needed
var largeResponse []byte
err = client.Request("getLargeData", "", &largeResponse)
```

### Automatic Compression

```go
// Service side
service.AddHandler("getCompressedResponse", func(msg *natsservice.NatsMessage) (interface{}, error) {
    data := generateLargeData()
    return natsservice.CompressData(data)
})

// Client side - decompression happens automatically
var response []byte
err = client.Request("getCompressedResponse", "", &response)
```

## Running Tests

Tests run in Docker Compose for consistency:

```bash
# Run all tests
./run_tests.sh
```

## Environment Variables

- `NATS_URL` - NATS server URL (default: nats://localhost:4222)
- `NATS_QUEUE_NAME` - Queue group name for services

## Example Applications

See the `cmd` directory for complete examples:

- `cmd/nats-service-example` - Example service implementation
- `cmd/requester-example` - Example client implementation

## Best Practices

1. **Use Queue Groups**: Ensure all service instances use the same queue group for load balancing
2. **Handle Timeouts**: Configure appropriate timeouts for your services
3. **Implement Health Checks**: Add a health check endpoint for monitoring
4. **Add Tracing**: Use headers to pass trace IDs between services
5. **Handle Errors Properly**: Return typed errors for better client handling
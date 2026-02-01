# NATS Service Framework

A Go framework for building microservices with NATS messaging. It provides request-response patterns over NATS with automatic compression, chunking for large messages, path parameter routing, and endpoint discovery.

## Features

- **Service Creation**: Define endpoints and handlers with path parameter support
- **Client Libraries**: Simple clients to call your services
- **Automatic Compression**: Large payloads (>2KB) are automatically compressed
- **Message Chunking**: Responses exceeding 300KB are automatically chunked
- **Path Parameters**: Support for dynamic routes like `users.:userId`
- **Endpoint Discovery**: Automatic service discovery with documentation
- **Wildcards**: NATS wildcard subjects (`*` and `>`) supported

## Installation

```bash
go get github.com/transactrx/nats-service
```

## Quick Start

### Creating a Service

```go
package main

import (
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    nats_service "github.com/transactrx/nats-service/pkg/nats-service"
)

func main() {
    // Create service with base path (requires NATS_URL and NATS_QUEUE_NAME env vars)
    service, err := nats_service.New("myapp.api")
    if err != nil {
        log.Fatal(err)
    }

    // Register endpoints with documentation
    endpoints := []nats_service.EndpointRegistration{
        {
            Path:        "health",
            Description: "Health check endpoint",
            Handler:     healthHandler,
        },
        {
            Path:        "users.:userId",
            Description: "Get user by ID",
            Parameters: []nats_service.ParameterDoc{
                {Name: "userId", Description: "User identifier", Required: true, Example: "USR-123"},
            },
            Handler: getUserHandler,
        },
    }

    if err := service.AddEndpointWithDocs(endpoints); err != nil {
        log.Fatal(err)
    }

    // Start service
    if err := service.Start(); err != nil {
        log.Fatal(err)
    }

    log.Println("Service started. Press Ctrl+C to stop.")

    // Handle graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    service.Shutdown()
}

func healthHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
    response := map[string]string{
        "status":    "healthy",
        "timestamp": time.Now().UTC().Format(time.RFC3339),
    }
    msg.ResponseBody, _ = json.Marshal(response)
    return nil
}

func getUserHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
    userId := msg.Parameters["userId"]
    if userId == "" {
        err := nats_service.NewValidationError("userId is required", 400, nil)
        return &err
    }

    response := map[string]string{
        "userId": userId,
        "name":   "John Doe",
    }
    msg.ResponseBody, _ = json.Marshal(response)
    return nil
}
```

### Creating a Client

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "time"

    nats_service_client "github.com/transactrx/nats-service/pkg/nats-service-client"
)

func main() {
    // Create client (requires NATS_URL env var)
    client, err := nats_service_client.NewClient()
    if err != nil {
        log.Fatal(err)
    }

    // Make a request
    response, svcErr, err := client.DoRequest(
        "",                        // correlation ID (auto-generated if empty)
        "myapp.api.users.USR-123", // NATS subject
        nil,                       // headers
        nil,                       // request body
        5*time.Second,             // timeout
    )

    if err != nil {
        log.Fatal("Request error:", err)
    }
    if svcErr != nil {
        log.Fatal("Service error:", svcErr.ErrorMessage)
    }

    var user map[string]string
    json.Unmarshal(response.Data, &user)
    fmt.Printf("User: %+v\n", user)
}
```

## Endpoint Registration

### Basic Registration

```go
// Simple endpoint without documentation
service.AddEndpoint("ping", pingHandler)
```

### Single Endpoint with Documentation

```go
// Endpoint with full documentation including response
service.AddEndpointWithDoc(
    "orders.:orderId",
    "Get order by ID",
    []nats_service.HeaderDoc{
        {Name: "Authorization", Description: "Bearer token", Required: true},
    },
    []nats_service.ParameterDoc{
        {Name: "orderId", Description: "Order identifier", Required: true, Example: "ORD-456"},
    },
    &nats_service.ResponseDoc{
        Description: "Order details in JSON format",
        ContentType: "application/json",
        Example:     `{"id": "ORD-456", "status": "pending"}`,
    },
    getOrderHandler,
)
```

### Batch Registration

```go
endpoints := []nats_service.EndpointRegistration{
    {
        Path:        "health",
        Description: "Health check endpoint",
        Response:    &nats_service.ResponseDoc{Description: "OK", ContentType: "text/plain"},
        Handler:     healthHandler,
    },
    {
        Path:        "orders.:orderId",
        Description: "Get order by ID",
        Headers: []nats_service.HeaderDoc{
            {Name: "Authorization", Required: true},
        },
        Parameters: []nats_service.ParameterDoc{
            {Name: "orderId", Description: "Order ID", Required: true, Example: "ORD-123"},
        },
        Response: &nats_service.ResponseDoc{
            Description: "Order details",
            ContentType: "application/json",
        },
        Handler: getOrderHandler,
    },
    {
        Path:        "search.*",
        Description: "Search with wildcard",
        Handler:     searchHandler,
    },
}

service.AddEndpointWithDocs(endpoints)
```

## Handler Functions

Handlers receive a `NatsMessage` and return a `*NatsServiceError` (nil for success):

```go
func myHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
    // Access request body
    var request MyRequestType
    json.Unmarshal(msg.Body, &request)

    // Access path parameters
    userId := msg.Parameters["userId"]

    // Access headers
    authHeader := msg.Header.Get("Authorization")

    // Log with message context
    msg.Logger.Printf("Processing request for user %s", userId)

    // Set response
    response := MyResponse{Status: "ok"}
    msg.ResponseBody, _ = json.Marshal(response)

    // Return nil for success
    return nil
}
```

### Error Handling

```go
func myHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
    // Validation error (400)
    if invalid {
        err := nats_service.NewValidationError("invalid input", 4001, nil)
        return &err
    }

    // Not found error (404)
    if notFound {
        err := nats_service.NewEndpointNotFoundError("resource not found")
        return &err
    }

    // Authorization error (403)
    if unauthorized {
        err := nats_service.NewAuthorizationError("access denied", 4031, nil)
        return &err
    }

    // Server error (500)
    if err != nil {
        svcErr := nats_service.NewServerError("processing failed", 5001, err)
        return &svcErr
    }

    return nil
}
```

## Endpoint Discovery

Services automatically register for discovery. Use the `nats-discover` CLI to find all running services:

```bash
# Build the CLI
go build -o nats-discover ./cmd/nats-discover

# Discover services (table format)
nats-discover -s nats://localhost:4222

# JSON output
nats-discover -s nats://localhost:4222 --format json

# YAML output
nats-discover -s nats://localhost:4222 --format yaml
```

### Example Output

```
SERVICE     SUBJECT PATTERN                  EXAMPLE                         DESCRIPTION
-------     ---------------                  -------                         -----------
myapp.api   myapp.api.health                 -                               Health check endpoint
              Response: text/plain - OK
myapp.api   myapp.api.orders.:orderId        myapp.api.orders.ORD-123        Get order by ID
              Params: orderId* (Order identifier)
              Headers: Authorization*
              Response: application/json - Order details
```

The output shows:
- **SUBJECT PATTERN**: The NATS subject with `:param` placeholders
- **EXAMPLE**: A concrete example showing the actual subject to call
- **Params/Headers**: Documentation for parameters and headers (`*` = required)
- **Response**: Content type and description of what the endpoint returns

See [docs/endpoint-discovery.md](docs/endpoint-discovery.md) for complete documentation.

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `NATS_URL` | NATS server URL (e.g., `nats://localhost:4222`) |
| `NATS_QUEUE_NAME` | Queue group name for load balancing (service only) |

### Optional

| Variable | Description |
|----------|-------------|
| `NATS_JWT` | JWT token for authenticated connections |
| `NATS_KEY` | Private key for authenticated connections |
| `NATS_DEBUG` | Enable debug logging (`true`/`false`) |
| `APPID` | Application identifier for connection naming |
| `MAX_SIZE_BEFORE_COMPRESS` | Client compression threshold (default: 2KB) |
| `MAX_SIZE_BEFORE_CHUNK` | Client chunking threshold (default: 8KB) |

## Message Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  Client Request                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 1. Client compresses payload if > 2KB                    │  │
│  │ 2. Sends to NATS subject (e.g., myapp.api.users.USR-123) │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Service Processing                                             │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 1. Matches subject against registered endpoints          │  │
│  │ 2. Extracts path parameters (userId = "USR-123")         │  │
│  │ 3. Decompresses request if needed                        │  │
│  │ 4. Calls handler function                                │  │
│  │ 5. Compresses response if > 2KB                          │  │
│  │ 6. Chunks response if > 300KB                            │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Client Response                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 1. Reassembles chunks if chunked                         │  │
│  │ 2. Decompresses if compressed                            │  │
│  │ 3. Returns response to caller                            │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Running Tests

```bash
# Run all tests with Docker Compose (includes NATS server)
./run_tests.sh

# Run tests locally (requires NATS at localhost:4222)
NATS_URL=nats://localhost:4222 NATS_QUEUE_NAME=testing go test -v ./pkg/nats-service
```

## Example Applications

| Directory | Description |
|-----------|-------------|
| `cmd/discovery-example` | Service demonstrating endpoint discovery with full documentation |
| `cmd/nats-service-example` | Basic service example |
| `cmd/nats-discover` | CLI tool for discovering services |

### Running the Discovery Example

```bash
# Start NATS
docker-compose up -d nats

# Run example service
NATS_URL=nats://localhost:4222 NATS_QUEUE_NAME=orders go run ./cmd/discovery-example

# In another terminal, discover endpoints
go run ./cmd/nats-discover -s nats://localhost:4222
```

## Best Practices

1. **Use Queue Groups**: All service instances should use the same queue group for load balancing
2. **Document Endpoints**: Use `AddEndpointWithDoc()` or `AddEndpointWithDocs()` for discoverable APIs
3. **Include Examples**: Add `Example` values to `ParameterDoc` so discovery shows concrete subject patterns
4. **Handle Errors Properly**: Return typed errors (`NewValidationError`, `NewServerError`, etc.)
5. **Add Health Checks**: Include a health endpoint for monitoring
6. **Use Correlation IDs**: Pass correlation IDs through headers for distributed tracing

## Coding Agent Prompt for Full Service Documentation

When using a coding agent (like Claude) to document your nats-service microservice, use the following prompt to ensure complete and thorough documentation of all endpoints and the service itself:

---

**Prompt:**

```
Analyze this nats-service microservice and add complete documentation for endpoint discovery. Follow these steps:

## Step 1: Understand the Service
- Read the main.go and all handler files to understand what this service does
- Identify the service's purpose, domain, and responsibilities
- Find the git repository URL for this project

## Step 2: Configure Service Metadata
Add these calls before service.Start():
- SetDescription(): Write a clear 1-2 sentence description of what this service does
- SetRepositoryURL(): Set the git origin URL so users can find the source code

## Step 3: Document Each Endpoint
For EVERY endpoint, ensure AddEndpointWithDoc() or AddEndpointWithDocs() includes:

### Path & Description
- path: The NATS subject pattern (e.g., "orders.:orderId")
- description: Clear explanation of what this endpoint does (1-2 sentences)

### Parameters ([]ParameterDoc)
For each path parameter (e.g., :userId, :orderId):
- Name: Parameter name (must match the :param in path)
- Description: What this parameter represents
- Required: true for path parameters
- Example: A realistic example value (e.g., "USR-123", "ORD-456")

### Headers ([]HeaderDoc)
For each expected request header:
- Name: Header name (e.g., "Authorization", "X-Tenant-ID")
- Description: What this header is for
- Required: true/false
- Example: Example value (e.g., "Bearer eyJ...")

### Response (*ResponseDoc)
- Description: What the response contains
- ContentType: MIME type (e.g., "application/json")
- Example: A realistic JSON example of the response
- Headers: Any custom response headers returned

## Step 4: Verify Completeness
Ensure every endpoint has:
- [ ] Meaningful description (not just the path repeated)
- [ ] All path parameters documented with examples
- [ ] All expected headers documented
- [ ] Response documentation with content type
- [ ] Realistic examples that help users understand the data format

## Output Format
Provide the complete code changes needed to fully document this service.
```

---

### Example of Complete Documentation

```go
func main() {
    service, _ := nats_service.New("orders.api")

    // Service metadata
    service.SetDescription("Order management service - handles order creation, retrieval, updates, and fulfillment workflows")
    service.SetRepositoryURL("https://github.com/mycompany/order-service")

    // Fully documented endpoints
    endpoints := []nats_service.EndpointRegistration{
        {
            Path:        "health",
            Description: "Health check endpoint for monitoring and load balancer probes",
            Response: &nats_service.ResponseDoc{
                Description: "Service health status",
                ContentType: "application/json",
                Example:     `{"status": "healthy", "timestamp": "2024-01-15T10:30:00Z"}`,
            },
            Handler: healthHandler,
        },
        {
            Path:        "orders.:orderId",
            Description: "Retrieve a specific order by its unique identifier",
            Headers: []nats_service.HeaderDoc{
                {
                    Name:        "Authorization",
                    Description: "Bearer token for authentication",
                    Required:    true,
                    Example:     "Bearer eyJhbGciOiJIUzI1NiIs...",
                },
                {
                    Name:        "X-Tenant-ID",
                    Description: "Tenant identifier for multi-tenant isolation",
                    Required:    true,
                    Example:     "tenant-abc-123",
                },
            },
            Parameters: []nats_service.ParameterDoc{
                {
                    Name:        "orderId",
                    Description: "Unique order identifier (UUID format)",
                    Required:    true,
                    Example:     "ord-550e8400-e29b-41d4-a716-446655440000",
                },
            },
            Response: &nats_service.ResponseDoc{
                Description: "Complete order details including line items and status",
                ContentType: "application/json",
                Example:     `{"id": "ord-550e8400-e29b-41d4-a716-446655440000", "status": "pending", "total": 99.99, "items": [{"sku": "PROD-001", "qty": 2}]}`,
            },
            Handler: getOrderHandler,
        },
        {
            Path:        "orders.customer.:customerId",
            Description: "List all orders for a specific customer with optional filtering",
            Headers: []nats_service.HeaderDoc{
                {Name: "Authorization", Description: "Bearer token", Required: true},
            },
            Parameters: []nats_service.ParameterDoc{
                {
                    Name:        "customerId",
                    Description: "Customer's unique identifier",
                    Required:    true,
                    Example:     "cust-12345",
                },
            },
            Response: &nats_service.ResponseDoc{
                Description: "Paginated list of customer orders",
                ContentType: "application/json",
                Example:     `{"orders": [...], "total": 42, "page": 1, "pageSize": 20}`,
            },
            Handler: getCustomerOrdersHandler,
        },
    }

    service.AddEndpointWithDocs(endpoints)
    service.Start()
}
```

### Documentation Quality Checklist

| Field | Bad Example | Good Example |
|-------|-------------|--------------|
| Description | "Get order" | "Retrieve a specific order by its unique identifier including line items, shipping info, and payment status" |
| Parameter Example | "123" | "ord-550e8400-e29b-41d4-a716-446655440000" |
| Header Example | "token" | "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." |
| Response Example | `{}` | `{"id": "ord-123", "status": "pending", "items": [...]}` |

## License

See [LICENSE](LICENSE) file.

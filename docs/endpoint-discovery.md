# Endpoint Discovery

The nats-service framework includes automatic endpoint discovery, allowing you to discover all services and their endpoints running on a NATS network.

## Overview

When a service starts, it automatically registers itself on the `_discovery.all` subject. The `nats-discover` CLI tool broadcasts a discovery request and collects responses from all running services, displaying their endpoints, parameters, headers, and descriptions.

## Features

- **Automatic Registration**: Services automatically register for discovery when `Start()` is called
- **Graceful Degradation**: If discovery subscription fails (e.g., due to permissions), the service continues operating normally
- **Deduplication**: Multiple instances of the same service are deduplicated by endpoint full subject
- **Header Documentation**: Document required and optional headers for each endpoint
- **Parameter Extraction**: Path parameters (`:param` syntax) are automatically extracted and displayed
- **Wildcard Detection**: Single (`*`) and multi (`>`) wildcards are detected and labeled

## Documenting Your Endpoints

### Basic Registration (no documentation)

```go
service.AddEndpoint("health", healthHandler)
```

### Single Endpoint with Description

```go
service.AddEndpointWithDoc("health", "Health check endpoint", healthHandler)
```

### Batch Registration with Full Documentation

```go
endpoints := []nats_service.EndpointRegistration{
    {
        Path:        "health",
        Description: "Health check endpoint - returns service status",
        Handler:     healthHandler,
    },
    {
        Path:        "orders.:orderId",
        Description: "Get order by ID",
        Headers: []nats_service.HeaderDoc{
            {
                Name:        "Authorization",
                Description: "Bearer token for authentication",
                Required:    true,
                Example:     "Bearer eyJhbG...",
            },
            {
                Name:        "X-Request-ID",
                Description: "Unique request identifier for tracing",
                Required:    false,
            },
        },
        Handler: getOrderHandler,
    },
    {
        Path:        "search.*",
        Description: "Search with wildcard matching",
        Handler:     searchHandler,
    },
}

err := service.AddEndpointWithDocs(endpoints)
```

### HeaderDoc Fields

| Field       | Type   | Description                              |
|-------------|--------|------------------------------------------|
| Name        | string | Header name (e.g., "Authorization")      |
| Description | string | What the header is used for              |
| Required    | bool   | Whether the header is required           |
| Example     | string | Example value for documentation          |

## Using the nats-discover CLI

### Installation

```bash
# Build from source
go build -o nats-discover ./cmd/nats-discover

# Or run directly
go run ./cmd/nats-discover [options]
```

### Basic Usage

```bash
# Discover services (table format)
nats-discover -s nats://localhost:4222

# JSON output
nats-discover -s nats://localhost:4222 --format json

# YAML output
nats-discover -s nats://localhost:4222 --format yaml
```

### Connection Options

```bash
# Using NATS URL
nats-discover -s nats://localhost:4222

# Using NATS CLI context (reads from ~/.config/nats/context/)
nats-discover --context mycontext

# Using environment variable
export NATS_URL=nats://localhost:4222
nats-discover

# With credentials file
nats-discover -s nats://localhost:4222 --creds /path/to/user.creds

# With JWT and seed
nats-discover -s nats://localhost:4222 --jwt "eyJ..." --seed "SUAM..."
```

### Options

| Flag        | Description                                      |
|-------------|--------------------------------------------------|
| `-s`        | NATS server URL                                  |
| `--context` | NATS CLI context name                            |
| `--format`  | Output format: table, json, yaml (default: table)|
| `--timeout` | Discovery timeout (default: 2s)                  |
| `--creds`   | Path to credentials file                         |
| `--jwt`     | JWT token for authentication                     |
| `--seed`    | NKey seed for authentication                     |
| `--version` | Show version                                     |

### Output Formats

#### Table (default)

```
SERVICE     ENDPOINT                          PARAMETERS  HEADERS                     DESCRIPTION
-------     --------                          ----------  -------                     -----------
orders.api  orders.api.health                                                         Health check endpoint
orders.api  orders.api.orders.:orderId        orderId     Authorization*, X-Request-ID  Get order by ID
orders.api  orders.api.users.:userId.orders   userId      Authorization*              Get all orders for a user
```

- Required headers are marked with `*`
- Parameters show path parameter names
- Wildcards show `[single wildcard]` or `[multi wildcard]`

#### JSON

```json
[
  {
    "serviceName": "orders.api",
    "basePath": "orders.api",
    "endpoints": [
      {
        "path": "orders.:orderId",
        "fullSubject": "orders.api.orders.:orderId",
        "parameters": ["orderId"],
        "headers": [
          {
            "name": "Authorization",
            "description": "Bearer token for authentication",
            "required": true,
            "example": "Bearer eyJhbG..."
          }
        ],
        "description": "Get order by ID"
      }
    ]
  }
]
```

#### YAML

```yaml
serviceName: orders.api
basePath: orders.api
endpoints:
  - path: orders.:orderId
    fullSubject: orders.api.orders.:orderId
    parameters: [orderId]
    headers:
      - name: Authorization
        description: Bearer token for authentication
        required: true
        example: Bearer eyJhbG...
    description: Get order by ID
```

## Discovery Protocol

### Subject

All discovery requests are sent to: `_discovery.all`

### Response Format

Services respond with a JSON payload:

```json
{
  "serviceName": "orders.api",
  "basePath": "orders.api",
  "endpoints": [
    {
      "path": "health",
      "fullSubject": "orders.api.health",
      "description": "Health check endpoint"
    },
    {
      "path": "orders.:orderId",
      "fullSubject": "orders.api.orders.:orderId",
      "parameters": ["orderId"],
      "headers": [...],
      "description": "Get order by ID"
    }
  ]
}
```

## Example Service

See `cmd/discovery-example/main.go` for a complete example service demonstrating:

- Batch endpoint registration with `AddEndpointWithDocs()`
- Header documentation with required/optional flags
- Path parameters (`:orderId`, `:userId`)
- Wildcard endpoints (`search.*`)

Run the example:

```bash
# Start NATS
docker-compose up -d nats

# Run example service
NATS_URL=nats://localhost:4222 NATS_QUEUE_NAME=orders go run ./cmd/discovery-example

# In another terminal, discover endpoints
go run ./cmd/nats-discover -s nats://localhost:4222
```

## Permissions

If your NATS server uses authorization, ensure the service account has permission to:

- Subscribe to `_discovery.all`
- Publish responses to reply subjects

If permissions are denied, the service logs a warning and continues operating normally without discovery support.

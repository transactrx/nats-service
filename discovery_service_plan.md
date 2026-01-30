# Discovery Service Implementation Plan

## Overview

Add automatic service discovery functionality to the nats-service library. When deployed, all microservices using this library will automatically expose their available endpoints via a standardized discovery subject (`_discovery.all`). This creates a "live document" of all available subjects and how to call them across the entire microservice ecosystem.

## Key Design Principles

1. **Automatic Discovery**: Services automatically report endpoints with parameter information extracted from existing metadata
2. **Backwards Compatible**: Existing services work without changes; documentation improves as developers add descriptions
3. **Graceful Degradation**: If discovery subscription fails (permissions), service operates normally with a warning
4. **Opt-in Documentation**: Developers can gradually add descriptions using new `AddEndpointWithDescription()` method
5. **Network Separation**: Different environments (dev/stage/prod) run on separate NATS superclusters, no code-level separation needed

## Discovery Subject

**Subject**: `_discovery.all`

- Global subject all services subscribe to (not in queue group)
- Each service instance responds independently
- Allows for future expansion (`_discovery.health`, `_discovery.metrics`, etc.)

## Implementation Tasks

### 1. Core Data Structures

#### Add to NatService struct (nats-service.go)
```go
type NatService struct {
    // ... existing fields ...
    discoverySubscription *nats.Subscription
}
```

#### Add to NatsEndpoint struct (nats-service.go)
```go
type NatsEndpoint struct {
    // ... existing fields ...
    description string  // Optional endpoint description
}
```

#### New Discovery Response Structures
```go
type DiscoveryResponse struct {
    ServiceName string        `json:"serviceName"`
    BasePath    string        `json:"basePath"`
    Endpoints   []EndpointDoc `json:"endpoints"`
}

type EndpointDoc struct {
    Path         string   `json:"path"`
    FullSubject  string   `json:"fullSubject"`
    Parameters   []string `json:"parameters,omitempty"`
    Description  string   `json:"description,omitempty"`
    WildcardType string   `json:"wildcardType,omitempty"` // "single" (*) or "multi" (>)
}
```

### 2. API Methods

#### Add New Method to NatService
```go
// AddEndpointWithDescription adds an endpoint with optional documentation
func (ns *NatService) AddEndpointWithDescription(
    path string,
    endPoint NatsEndpointFunc,
    description string,
) error {
    // Call existing AddEndpoint logic
    err := ns.AddEndpoint(path, endPoint)
    if err != nil {
        return err
    }

    // Set description on the last added endpoint
    if len(ns.endPoints) > 0 {
        ns.endPoints[len(ns.endPoints)-1].description = description
    }

    return nil
}
```

### 3. Discovery Registration

#### registerDiscoveryEndpoint Method
```go
func (ns *NatService) registerDiscoveryEndpoint() {
    discoverySubject := "_discovery.all"

    sub, err := ns.nc.Subscribe(discoverySubject, ns.handleDiscoveryRequest)
    if err != nil {
        log.Printf("WARNING: Unable to subscribe to discovery subject '%s': %v. Service will operate normally.", discoverySubject, err)
        return
    }

    if !sub.IsValid() {
        log.Printf("WARNING: Discovery subscription to '%s' is invalid. Service will operate normally.", discoverySubject)
        return
    }

    log.Printf("Registered discovery endpoint on subject: %s", discoverySubject)
    ns.discoverySubscription = sub
}
```

### 4. Discovery Request Handler

#### handleDiscoveryRequest Method
```go
func (ns *NatService) handleDiscoveryRequest(msg *nats.Msg) {
    response := DiscoveryResponse{
        ServiceName: ns.basePath,
        BasePath:    ns.basePath,
        Endpoints:   ns.buildEndpointDocs(),
    }

    jsonData, err := json.Marshal(response)
    if err != nil {
        log.Printf("Error marshaling discovery response: %v", err)
        return
    }

    msg.Respond(jsonData)
}
```

### 5. Automatic Metadata Extraction

#### buildEndpointDocs Method
```go
func (ns *NatService) buildEndpointDocs() []EndpointDoc {
    docs := []EndpointDoc{}

    for _, ep := range ns.endPoints {
        doc := EndpointDoc{
            Path:        ep.path,
            FullSubject: ns.basePath + "." + ep.path,
        }

        // Automatically extract parameter names from regex
        if ep.paramRegex != nil {
            paramNames := ep.paramRegex.GetGroupNames()[1:] // Skip group 0 (full match)
            if len(paramNames) > 0 {
                doc.Parameters = paramNames
            }
        }

        // Detect wildcard type
        if ep.pathHasWildcards {
            if strings.HasSuffix(ep.path, ">") {
                doc.WildcardType = "multi"
            } else if strings.Contains(ep.path, "*") {
                doc.WildcardType = "single"
            }
        }

        // Include description if provided
        if ep.description != "" {
            doc.Description = ep.description
        }

        docs = append(docs, doc)
    }

    return docs
}
```

### 6. Integration into Service Lifecycle

#### Modify Start() Method
```go
func (ns *NatService) Start() error {
    // ... existing endpoint subscription code ...

    ns.subscription = subscribe

    err = ns.startChunkResponder()
    if err != nil {
        return err
    }

    // Register discovery endpoint (new)
    ns.registerDiscoveryEndpoint()

    return nil
}
```

#### Modify Shutdown() Method
```go
func (ns *NatService) Shutdown() error {
    err := ns.subscription.Drain()

    // Drain discovery subscription if it exists
    if ns.discoverySubscription != nil {
        discErr := ns.discoverySubscription.Drain()
        if discErr != nil {
            log.Printf("Error draining discovery subscription: %v", discErr)
        }
    }

    return err
}
```

### 7. Testing

#### Test Cases to Implement

1. **Test Discovery Response Format**
   - Verify JSON structure matches DiscoveryResponse
   - Verify all endpoints are included

2. **Test Automatic Parameter Extraction**
   - Endpoint with `:userId` → parameters: ["userId"]
   - Endpoint with `:userId/:orderId` → parameters: ["userId", "orderId"]
   - Endpoint without params → parameters: omitted

3. **Test Wildcard Detection**
   - Path with `*` → wildcardType: "single"
   - Path with `>` → wildcardType: "multi"
   - Path without wildcards → wildcardType: omitted

4. **Test Description Handling**
   - Endpoint with description → included in response
   - Endpoint without description → description omitted
   - Mixed endpoints → only described ones have description field

5. **Test Graceful Failure**
   - Mock NATS connection that rejects discovery subscription
   - Verify service still starts successfully
   - Verify appropriate warning is logged
   - Verify main endpoints still work

6. **Test Multiple Responses**
   - Start multiple service instances
   - Send discovery request
   - Verify receiving multiple responses (one per instance)

### 8. Documentation Updates

#### CLAUDE.md
Add section:
```markdown
## Service Discovery

All services automatically expose a discovery endpoint on `_discovery.all`.

### Querying Discovery
```bash
# Get all available services and endpoints
nats request _discovery.all ""
```

### Discovery Response Format
```json
{
  "serviceName": "users-service",
  "basePath": "users",
  "endpoints": [
    {
      "path": "getUser/:userId",
      "fullSubject": "users.getUser/:userId",
      "parameters": ["userId"],
      "description": "Retrieves user profile by ID"
    }
  ]
}
```

### Adding Endpoint Documentation
```go
// Without description (backwards compatible)
service.AddEndpoint("getUser/:userId", GetUserHandler)

// With description (recommended)
service.AddEndpointWithDescription(
    "getUser/:userId",
    GetUserHandler,
    "Retrieves user profile information by user ID",
)
```

### Permissions Note
Discovery requires subscription permission to `_discovery.all`. If permissions are not granted, service will log a warning but operate normally without discovery features.
```

#### README.md
Add section after "Advanced Features":
```markdown
## Service Discovery

The framework includes automatic service discovery. All services expose their available endpoints via the `_discovery.all` subject.

### Querying All Services
```bash
nats request _discovery.all ""
```

This returns JSON responses from all running service instances:
```json
{
  "serviceName": "users-service",
  "basePath": "users",
  "endpoints": [
    {
      "path": "getUser/:userId",
      "fullSubject": "users.getUser/:userId",
      "parameters": ["userId"],
      "description": "Optional description added by developer"
    }
  ]
}
```

### Automatic Discovery Features
- Parameter names automatically extracted from path patterns (`:paramName`)
- Wildcard detection (`*` and `>` patterns)
- Optional descriptions via `AddEndpointWithDescription()`

### Gradual Rollout
Discovery subscription fails gracefully if permissions are not granted. Services operate normally and log a warning. As permissions are granted across your infrastructure, services automatically gain discovery capabilities without redeployment.
```

### 9. Example Updates

#### cmd/nats-service-example/main.go
Add examples demonstrating both methods:
```go
// Backwards compatible - no description
service.AddEndpoint("getTime", natsservice.GetTime)

// With description - enhanced documentation
service.AddEndpointWithDescription(
    "getTimeError",
    natsservice.GetTimeError,
    "Example endpoint that demonstrates error handling by returning a simulated error",
)

service.AddEndpointWithDescription(
    "users/:userId",
    UserHandler,
    "Retrieves user information by user ID",
)

service.AddEndpointWithDescription(
    "users/:userId/orders/:orderId",
    OrderHandler,
    "Retrieves specific order details for a user",
)
```

## Discovery Response Examples

### Phase 1: Initial Deployment (No Descriptions)
```json
{
  "serviceName": "users-service",
  "basePath": "users",
  "endpoints": [
    {
      "path": "getUser/:userId",
      "fullSubject": "users.getUser/:userId",
      "parameters": ["userId"]
    },
    {
      "path": "orders/*",
      "fullSubject": "users.orders/*",
      "wildcardType": "single"
    },
    {
      "path": "events/>",
      "fullSubject": "users.events/>",
      "wildcardType": "multi"
    }
  ]
}
```

### Phase 2: After Developers Add Descriptions
```json
{
  "serviceName": "users-service",
  "basePath": "users",
  "endpoints": [
    {
      "path": "getUser/:userId",
      "fullSubject": "users.getUser/:userId",
      "parameters": ["userId"],
      "description": "Retrieves user profile information including preferences and settings"
    },
    {
      "path": "orders/*",
      "fullSubject": "users.orders/*",
      "wildcardType": "single",
      "description": "Handles all order-related operations (list, create, update)"
    },
    {
      "path": "events/>",
      "fullSubject": "users.events/>",
      "wildcardType": "multi",
      "description": "Streams user activity events in real-time"
    }
  ]
}
```

## Rollout Strategy

### Phase 1: Library Update
1. Deploy updated nats-service library to all microservices
2. Services log warnings about missing discovery permissions
3. No functionality lost - services operate normally
4. Basic endpoint metadata automatically available (paths, parameters, wildcards)

### Phase 2: Grant Permissions
1. Grant `_discovery.all` subscription permissions to services gradually
2. Start with non-critical services
3. Verify discovery responses
4. Monitor logs for successful registration messages

### Phase 3: Add Descriptions
1. Developers update service code to use `AddEndpointWithDescription()`
2. Redeploy services with enhanced documentation
3. Discovery responses become progressively more detailed

### Phase 4: Tooling
1. Build centralized documentation tools that query `_discovery.all`
2. Generate API documentation from live service responses
3. Create service dependency maps
4. Build testing/monitoring tools

## Benefits

1. **Zero Configuration**: Works automatically on deployment
2. **Self-Documenting**: Live view of all available endpoints across infrastructure
3. **Gradual Enhancement**: Developers add descriptions over time
4. **Production Safe**: Fails gracefully if permissions not granted
5. **Extensible**: `_discovery.*` pattern allows future features
6. **No Central Registry**: Distributed discovery via NATS
7. **Real-Time**: Always reflects current deployed state

## Future Enhancements (Not in This Implementation)

- `_discovery.health`: Health check responses
- `_discovery.metrics`: Endpoint usage statistics
- `_discovery.version`: Service version information
- Request/response schema definitions
- Rate limiting information
- Authentication requirements per endpoint
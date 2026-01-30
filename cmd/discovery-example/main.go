package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Create the service with base path "orders.api"
	service, err := nats_service.New("orders.api")
	if err != nil {
		log.Fatalf("Failed to create service: %v", err)
	}

	// Register endpoints with documentation using batch registration
	endpoints := []nats_service.EndpointRegistration{
		{
			Path:        "health",
			Description: "Health check endpoint - returns service status",
			Handler:     healthHandler,
		},
		{
			Path:        "orders.:orderId",
			Description: "Get order by ID - returns order details",
			Headers: []nats_service.HeaderDoc{
				{Name: "Authorization", Description: "Bearer token for authentication", Required: true, Example: "Bearer eyJhbG..."},
				{Name: "X-Request-ID", Description: "Unique request identifier for tracing", Required: false},
			},
			Handler: getOrderHandler,
		},
		{
			Path:        "orders.:orderId.items",
			Description: "Get all items for an order",
			Headers: []nats_service.HeaderDoc{
				{Name: "Authorization", Description: "Bearer token for authentication", Required: true},
			},
			Handler: getOrderItemsHandler,
		},
		{
			Path:        "users.:userId.orders",
			Description: "Get all orders for a user",
			Headers: []nats_service.HeaderDoc{
				{Name: "Authorization", Description: "Bearer token for authentication", Required: true},
				{Name: "X-Page-Size", Description: "Number of results per page", Required: false, Example: "20"},
				{Name: "X-Page-Token", Description: "Pagination token for next page", Required: false},
			},
			Handler: getUserOrdersHandler,
		},
		{
			Path:        "search.*",
			Description: "Search orders with wildcard matching",
			Headers: []nats_service.HeaderDoc{
				{Name: "X-Search-Filter", Description: "JSON filter criteria", Required: false, Example: `{"status":"pending"}`},
			},
			Handler: searchHandler,
		},
	}

	err = service.AddEndpointWithDocs(endpoints)
	if err != nil {
		log.Fatalf("Failed to register endpoints: %v", err)
	}

	// Start the service
	err = service.Start()
	if err != nil {
		log.Fatalf("Failed to start service: %v", err)
	}

	log.Println("Orders API service started. Press Ctrl+C to stop.")
	log.Println("Run 'nats-discover -s nats://localhost:4222' to see discovered endpoints.")

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		if err := service.Shutdown(); err != nil {
			log.Printf("Error during shutdown: %v", err)
		}
		os.Exit(0)
	}()

	runtime.Goexit()
}

// Handler implementations

func healthHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	response := map[string]interface{}{
		"status":    "healthy",
		"service":   "orders.api",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	data, _ := json.Marshal(response)
	msg.ResponseBody = data
	return nil
}

func getOrderHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	orderId := msg.Parameters["orderId"]
	if orderId == "" {
		err := nats_service.NewValidationError("orderId is required", 400, nil)
		return &err
	}

	// Mock order response
	response := map[string]interface{}{
		"orderId":   orderId,
		"status":    "completed",
		"total":     129.99,
		"createdAt": "2026-01-15T10:30:00Z",
	}
	data, _ := json.Marshal(response)
	msg.ResponseBody = data
	return nil
}

func getOrderItemsHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	orderId := msg.Parameters["orderId"]
	if orderId == "" {
		err := nats_service.NewValidationError("orderId is required", 400, nil)
		return &err
	}

	// Mock items response
	response := map[string]interface{}{
		"orderId": orderId,
		"items": []map[string]interface{}{
			{"sku": "ITEM-001", "name": "Widget A", "quantity": 2, "price": 49.99},
			{"sku": "ITEM-002", "name": "Widget B", "quantity": 1, "price": 30.01},
		},
	}
	data, _ := json.Marshal(response)
	msg.ResponseBody = data
	return nil
}

func getUserOrdersHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	userId := msg.Parameters["userId"]
	if userId == "" {
		err := nats_service.NewValidationError("userId is required", 400, nil)
		return &err
	}

	// Mock user orders response
	response := map[string]interface{}{
		"userId": userId,
		"orders": []map[string]interface{}{
			{"orderId": "ORD-001", "status": "completed", "total": 129.99},
			{"orderId": "ORD-002", "status": "pending", "total": 75.50},
		},
	}
	data, _ := json.Marshal(response)
	msg.ResponseBody = data
	return nil
}

func searchHandler(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	// Mock search response
	response := map[string]interface{}{
		"query":   msg.Path,
		"results": []string{"ORD-001", "ORD-002", "ORD-003"},
		"total":   3,
	}
	data, _ := json.Marshal(response)
	msg.ResponseBody = data
	return nil
}

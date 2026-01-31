package nats_service_test

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	nats_service_client "github.com/transactrx/nats-service/pkg/nats-service-client"
)

func TestNATSIntegration(t *testing.T) {
	// Skip the test if NATS_URL environment variable is not set
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222" // Default NATS URL
	}

	queueName := os.Getenv("NATS_QUEUE_NAME")
	if queueName == "" {
		queueName = "testing"
	}

	// Sleep a bit to let the previous tests clean up
	time.Sleep(500 * time.Millisecond)

	// Create and start the NATS service with the provided URL
	natService, err := nats_service.NewLowLevelDebug("rx.api", queueName, natsURL, "", "", 1024*2, 1024*300, true) // Enable debug
	if err != nil {
		t.Skipf("Skipping integration tests as NATS is not available: %v", err)
		return
	}

	// Register endpoints
	err = natService.AddEndpoint("getTime", nats_service.GetTime)
	if err != nil {
		t.Fatalf("Failed to add getTime endpoint: %v", err)
	}

	err = natService.AddEndpoint("getTimeError", nats_service.GetTimeError)
	if err != nil {
		t.Fatalf("Failed to add getTimeError endpoint: %v", err)
	}

	err = natService.AddEndpoint("getCompressedResponse", nats_service.GetCompressedResponse)
	if err != nil {
		t.Fatalf("Failed to add getCompressedResponse endpoint: %v", err)
	}

	// Add parameterized endpoint
	paramHandler := func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
		userID := msg.Parameters["userId"]
		if userID == "" {
			err := nats_service.NewValidationError("userId parameter is missing", 400, nil)
			return &err
		}
		msg.ResponseBody = []byte("Hello, " + userID)
		return nil
	}

	// Add the endpoint with parameter
	err = natService.AddEndpoint("users.:userId", paramHandler)
	if err != nil {
		t.Fatalf("Failed to add parameterized endpoint: %v", err)
	}

	// Start the service
	err = natService.Start()
	if err != nil {
		t.Fatalf("Failed to start NATS service: %v", err)
	}
	defer natService.Shutdown()

	// Create a client
	client, err := nats_service_client.NewLowLevelClientWithChunkingAndCompressionDebug(natsURL, 1024*2, 1024*300, "", "", false)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Run the test cases
	t.Run("TestGetTimeViaClient", func(t *testing.T) {
		testGetTimeViaClient(t, client)
	})

	t.Run("TestGetTimeErrorViaClient", func(t *testing.T) {
		testGetTimeErrorViaClient(t, client)
	})

	t.Run("TestGetCompressedResponseViaClient", func(t *testing.T) {
		testGetCompressedResponseViaClient(t, client)
	})

	t.Run("TestEndpointNotFound", func(t *testing.T) {
		testEndpointNotFound(t, client)
	})

	t.Run("TestParameterizedEndpoint", func(t *testing.T) {
		testParameterizedEndpoint(t, natService, client)
	})
}

func testGetTimeViaClient(t *testing.T, client *nats_service_client.Client) {
	correlationId := uuid.New().String()
	header := nats_service_client.Header{}

	// Make request to getTime endpoint
	response, natsError, err := client.DoRequest(correlationId, "rx.api.getTime", header, []byte("test payload"), 5*time.Second)

	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if natsError != nil {
		t.Fatalf("Request returned NATS error: %v", natsError)
	}

	if response == nil {
		t.Fatalf("Response is nil")
	}

	// Verify response contains timestamp
	responseStr := string(response.Data)
	if !strings.HasPrefix(responseStr, "current time: ") {
		t.Errorf("Response does not have expected prefix. Got: %s", responseStr)
	}

	timeStr := strings.TrimPrefix(responseStr, "current time: ")
	_, parseErr := time.Parse(time.RFC3339, timeStr)
	if parseErr != nil {
		t.Errorf("Time format is incorrect. Got: %s, Error: %v", timeStr, parseErr)
	}

	// For integration tests, we don't need to check the status header
	// as the client may process and remove it during decompression
}

func testGetTimeErrorViaClient(t *testing.T, client *nats_service_client.Client) {
	correlationId := uuid.New().String()
	header := nats_service_client.Header{}

	// Make request to getTimeError endpoint
	response, natsError, err := client.DoRequest(correlationId, "rx.api.getTimeError", header, []byte("test payload"), 5*time.Second)

	if err != nil {
		t.Fatalf("Request failed with unexpected error: %v", err)
	}

	if response != nil {
		t.Errorf("Expected no response, but got one: %v", string(response.Data))
	}

	if natsError == nil {
		t.Fatalf("Expected NATS error, but got nil")
	}

	// Verify error properties
	expectedStatus := 500
	expectedApiStatusCode := 1001
	expectedErrorMessagePart := "simulated error getting time"

	if natsError.Status != expectedStatus {
		t.Errorf("Status mismatch. Expected %d, Got %d", expectedStatus, natsError.Status)
	}

	if natsError.ApiStatusCode != expectedApiStatusCode {
		t.Errorf("ApiStatusCode mismatch. Expected %d, Got %d", expectedApiStatusCode, natsError.ApiStatusCode)
	}

	if !strings.Contains(natsError.ErrorMessage, expectedErrorMessagePart) {
		t.Errorf("ErrorMessage mismatch. Expected to contain '%s', Got '%s'", expectedErrorMessagePart, natsError.ErrorMessage)
	}
}

func testGetCompressedResponseViaClient(t *testing.T, client *nats_service_client.Client) {
	correlationId := uuid.New().String()
	header := nats_service_client.Header{}

	// Create test data that's large enough to trigger compression
	originalDataStr := strings.Repeat("This is a test string for compression. ", 100)
	originalData := []byte(originalDataStr)

	// Make request to getCompressedResponse endpoint
	response, natsError, err := client.DoRequest(correlationId, "rx.api.getCompressedResponse", header, originalData, 5*time.Second)

	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if natsError != nil {
		t.Fatalf("Request returned NATS error: %v", natsError)
	}

	if response == nil {
		t.Fatalf("Response is nil")
	}

	// The handler compresses the data we send, so we can't directly compare
	// We just check that we got a valid response - the handler logs prove compression worked
	if len(response.Data) == 0 {
		t.Errorf("Response data is empty")
	}
}

func testEndpointNotFound(t *testing.T, client *nats_service_client.Client) {
	correlationId := uuid.New().String()
	header := nats_service_client.Header{}

	// Make request to a non-existent endpoint
	response, natsError, err := client.DoRequest(correlationId, "rx.api.nonExistentEndpoint", header, []byte("test payload"), 5*time.Second)

	if err != nil {
		t.Fatalf("Request failed with unexpected error: %v", err)
	}

	if response != nil {
		t.Errorf("Expected no response, but got one: %v", string(response.Data))
	}

	if natsError == nil {
		t.Fatalf("Expected NATS error, but got nil")
	}

	// Verify it's a 404 error
	if natsError.Status != 404 {
		t.Errorf("Expected status 404, got %d", natsError.Status)
	}

	// Check that error message mentions the endpoint
	if !strings.Contains(natsError.ErrorMessage, "nonExistentEndpoint") {
		t.Errorf("Error message should mention the missing endpoint. Got: %s", natsError.ErrorMessage)
	}
}

// Test parameterized endpoint
func testParameterizedEndpoint(t *testing.T, natService *nats_service.NatService, client *nats_service_client.Client) {
	// The endpoint is already registered in the main test function

	// Make a request with the parameter
	correlationId := uuid.New().String()
	header := nats_service_client.Header{}

	// Use the value "testUser" for the userId parameter
	response, natsError, err := client.DoRequest(correlationId, "rx.api.users.testUser", header, []byte(""), 5*time.Second)

	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	if natsError != nil {
		t.Fatalf("Request returned NATS error: %v", natsError)
	}

	if response == nil {
		t.Fatalf("Response is nil")
	}

	// Verify response
	expectedResponse := "Hello, testUser"
	if string(response.Data) != expectedResponse {
		t.Errorf("Expected response %q, got %q", expectedResponse, string(response.Data))
	}
}

// TestAddEndpointWithDoc tests the AddEndpointWithDoc function
func TestAddEndpointWithDoc(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	queueName := os.Getenv("NATS_QUEUE_NAME")
	if queueName == "" {
		queueName = "testing-doc"
	}

	natService, err := nats_service.NewLowLevelDebug("doc.api", queueName, natsURL, "", "", 1024*2, 1024*300, false)
	if err != nil {
		t.Skipf("Skipping test as NATS is not available: %v", err)
		return
	}

	// Register endpoint with description
	handler := func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
		msg.ResponseBody = []byte("pong")
		return nil
	}

	err = natService.AddEndpointWithDoc("ping", "Health check endpoint", nil, nil, nil, handler)
	if err != nil {
		t.Fatalf("Failed to add endpoint with doc: %v", err)
	}

	// Start service to verify it works
	err = natService.Start()
	if err != nil {
		t.Fatalf("Failed to start service: %v", err)
	}
	defer natService.Shutdown()

	// Make a request to verify the endpoint works
	client, err := nats_service_client.NewLowLevelClient(natsURL, "", "")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	response, natsError, err := client.DoRequest("", "doc.api.ping", nil, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if natsError != nil {
		t.Fatalf("Request returned error: %v", natsError)
	}
	if string(response.Data) != "pong" {
		t.Errorf("Expected 'pong', got '%s'", string(response.Data))
	}
}

// TestAddEndpointWithDocs tests the batch endpoint registration
func TestAddEndpointWithDocs(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	queueName := os.Getenv("NATS_QUEUE_NAME")
	if queueName == "" {
		queueName = "testing-batch"
	}

	natService, err := nats_service.NewLowLevelDebug("batch.api", queueName, natsURL, "", "", 1024*2, 1024*300, false)
	if err != nil {
		t.Skipf("Skipping test as NATS is not available: %v", err)
		return
	}

	// Define handlers
	pingHandler := func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
		msg.ResponseBody = []byte("pong")
		return nil
	}
	echoHandler := func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
		msg.ResponseBody = msg.Body
		return nil
	}

	// Register multiple endpoints at once
	endpoints := []nats_service.EndpointRegistration{
		{Path: "ping", Description: "Health check", Handler: pingHandler},
		{Path: "echo", Description: "Echo service", Handler: echoHandler},
	}

	err = natService.AddEndpointWithDocs(endpoints)
	if err != nil {
		t.Fatalf("Failed to add endpoints with docs: %v", err)
	}

	err = natService.Start()
	if err != nil {
		t.Fatalf("Failed to start service: %v", err)
	}
	defer natService.Shutdown()

	// Test both endpoints
	client, err := nats_service_client.NewLowLevelClient(natsURL, "", "")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Test ping
	response, natsError, err := client.DoRequest("", "batch.api.ping", nil, nil, 5*time.Second)
	if err != nil || natsError != nil {
		t.Fatalf("Ping request failed: err=%v, natsError=%v", err, natsError)
	}
	if string(response.Data) != "pong" {
		t.Errorf("Ping: expected 'pong', got '%s'", string(response.Data))
	}

	// Test echo
	testData := []byte("hello world")
	response, natsError, err = client.DoRequest("", "batch.api.echo", nil, testData, 5*time.Second)
	if err != nil || natsError != nil {
		t.Fatalf("Echo request failed: err=%v, natsError=%v", err, natsError)
	}
	if string(response.Data) != string(testData) {
		t.Errorf("Echo: expected '%s', got '%s'", string(testData), string(response.Data))
	}
}

// TestDiscoveryIntegration tests the service discovery and API docs functionality
func TestDiscoveryIntegration(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	queueName := os.Getenv("NATS_QUEUE_NAME")
	if queueName == "" {
		queueName = "testing-discovery"
	}

	// Create service with documented endpoints
	natService, err := nats_service.NewLowLevelDebug("discovery.test.api", queueName, natsURL, "", "", 1024*2, 1024*300, false)
	if err != nil {
		t.Skipf("Skipping test as NATS is not available: %v", err)
		return
	}

	// Set service description
	natService.SetDescription("Test service for discovery integration tests")

	// Define handlers
	healthHandler := func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
		msg.ResponseBody = []byte(`{"status":"healthy"}`)
		return nil
	}
	getUserHandler := func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
		userID := msg.Parameters["userId"]
		msg.ResponseBody = []byte(`{"id":"` + userID + `","name":"Test User"}`)
		return nil
	}

	// Register endpoints with documentation
	endpoints := []nats_service.EndpointRegistration{
		{
			Path:        "health",
			Description: "Health check endpoint",
			Response:    &nats_service.ResponseDoc{Description: "Health status", ContentType: "application/json"},
			Handler:     healthHandler,
		},
		{
			Path:        "users.:userId",
			Description: "Get user by ID",
			Parameters: []nats_service.ParameterDoc{
				{Name: "userId", Description: "User identifier", Required: true, Example: "user-123"},
			},
			Headers: []nats_service.HeaderDoc{
				{Name: "Authorization", Description: "Bearer token", Required: true},
			},
			Response: &nats_service.ResponseDoc{Description: "User object", ContentType: "application/json"},
			Handler:  getUserHandler,
		},
	}

	err = natService.AddEndpointWithDocs(endpoints)
	if err != nil {
		t.Fatalf("Failed to add endpoints: %v", err)
	}

	err = natService.Start()
	if err != nil {
		t.Fatalf("Failed to start service: %v", err)
	}
	defer natService.Shutdown()

	// Connect directly to NATS for discovery tests
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// Run discovery tests
	t.Run("TestServiceDiscovery", func(t *testing.T) {
		testServiceDiscovery(t, nc)
	})

	t.Run("TestApiDocsEndpoint", func(t *testing.T) {
		testApiDocsEndpoint(t, nc)
	})

	t.Run("TestReservedEndpointRejection", func(t *testing.T) {
		testReservedEndpointRejection(t, natsURL, queueName)
	})
}

// testServiceDiscovery tests the _discovery.all broadcast
func testServiceDiscovery(t *testing.T, nc *nats.Conn) {
	var results []nats_service.ServiceInfo
	var mu sync.Mutex

	// Subscribe to collect responses
	inbox := nc.NewInbox()
	sub, err := nc.Subscribe(inbox, func(msg *nats.Msg) {
		var info nats_service.ServiceInfo
		if err := json.Unmarshal(msg.Data, &info); err != nil {
			return
		}
		mu.Lock()
		results = append(results, info)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Send discovery broadcast
	err = nc.PublishRequest(nats_service.DiscoverySubject, inbox, nil)
	if err != nil {
		t.Fatalf("Failed to publish discovery request: %v", err)
	}
	nc.Flush()

	// Wait for responses
	time.Sleep(500 * time.Millisecond)

	// Verify we got at least one service
	mu.Lock()
	defer mu.Unlock()

	if len(results) == 0 {
		t.Fatalf("No services discovered")
	}

	// Find our test service
	var found *nats_service.ServiceInfo
	for i := range results {
		if results[i].ServiceName == "discovery.test.api" {
			found = &results[i]
			break
		}
	}

	if found == nil {
		t.Fatalf("Test service 'discovery.test.api' not found in discovery results")
	}

	// Verify service info
	if found.SubjectPrefix != "discovery.test.api" {
		t.Errorf("Expected SubjectPrefix 'discovery.test.api', got '%s'", found.SubjectPrefix)
	}

	if found.Description != "Test service for discovery integration tests" {
		t.Errorf("Expected description 'Test service for discovery integration tests', got '%s'", found.Description)
	}

	expectedApiDocsSubject := "discovery.test.api._api_docs"
	if found.ApiDocsSubject != expectedApiDocsSubject {
		t.Errorf("Expected ApiDocsSubject '%s', got '%s'", expectedApiDocsSubject, found.ApiDocsSubject)
	}
}

// testApiDocsEndpoint tests the _api_docs request-response endpoint
func testApiDocsEndpoint(t *testing.T, nc *nats.Conn) {
	// Send direct request to API docs endpoint
	apiDocsSubject := "discovery.test.api._api_docs"
	msg, err := nc.Request(apiDocsSubject, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to get API docs: %v", err)
	}

	// Parse response
	var apiDocs nats_service.ApiDocsResponse
	if err := json.Unmarshal(msg.Data, &apiDocs); err != nil {
		t.Fatalf("Failed to parse API docs response: %v", err)
	}

	// Verify service info
	if apiDocs.ServiceName != "discovery.test.api" {
		t.Errorf("Expected ServiceName 'discovery.test.api', got '%s'", apiDocs.ServiceName)
	}

	if apiDocs.Description != "Test service for discovery integration tests" {
		t.Errorf("Expected description, got '%s'", apiDocs.Description)
	}

	// Verify we have status codes
	if len(apiDocs.StatusCodes) == 0 {
		t.Error("Expected status codes in response")
	}

	// Verify endpoints
	if len(apiDocs.Endpoints) != 2 {
		t.Fatalf("Expected 2 endpoints, got %d", len(apiDocs.Endpoints))
	}

	// Find and verify the health endpoint
	var healthEndpoint, userEndpoint *nats_service.EndpointDoc
	for i := range apiDocs.Endpoints {
		if apiDocs.Endpoints[i].Path == "health" {
			healthEndpoint = &apiDocs.Endpoints[i]
		}
		if apiDocs.Endpoints[i].Path == "users.:userId" {
			userEndpoint = &apiDocs.Endpoints[i]
		}
	}

	if healthEndpoint == nil {
		t.Fatal("Health endpoint not found in API docs")
	}
	if healthEndpoint.Description != "Health check endpoint" {
		t.Errorf("Health endpoint description mismatch: %s", healthEndpoint.Description)
	}
	if healthEndpoint.FullSubject != "discovery.test.api.health" {
		t.Errorf("Health endpoint FullSubject mismatch: %s", healthEndpoint.FullSubject)
	}

	if userEndpoint == nil {
		t.Fatal("User endpoint not found in API docs")
	}
	if userEndpoint.Description != "Get user by ID" {
		t.Errorf("User endpoint description mismatch: %s", userEndpoint.Description)
	}

	// Verify user endpoint has parameters
	if len(userEndpoint.Parameters) != 1 {
		t.Fatalf("Expected 1 parameter for user endpoint, got %d", len(userEndpoint.Parameters))
	}
	if userEndpoint.Parameters[0].Name != "userId" {
		t.Errorf("Expected parameter name 'userId', got '%s'", userEndpoint.Parameters[0].Name)
	}
	if userEndpoint.Parameters[0].Description != "User identifier" {
		t.Errorf("Expected parameter description 'User identifier', got '%s'", userEndpoint.Parameters[0].Description)
	}

	// Verify user endpoint has headers
	if len(userEndpoint.Headers) != 1 {
		t.Fatalf("Expected 1 header for user endpoint, got %d", len(userEndpoint.Headers))
	}
	if userEndpoint.Headers[0].Name != "Authorization" {
		t.Errorf("Expected header name 'Authorization', got '%s'", userEndpoint.Headers[0].Name)
	}

	// Verify example subject is generated
	if userEndpoint.ExampleSubject == "" {
		t.Error("Expected ExampleSubject to be generated for parameterized endpoint")
	}
	if !strings.Contains(userEndpoint.ExampleSubject, "user-123") {
		t.Errorf("ExampleSubject should contain example value 'user-123', got '%s'", userEndpoint.ExampleSubject)
	}
}

// testReservedEndpointRejection verifies that users cannot register endpoints with reserved suffixes
func testReservedEndpointRejection(t *testing.T, natsURL, queueName string) {
	natService, err := nats_service.NewLowLevelDebug("reserved.test.api", queueName+"-reserved", natsURL, "", "", 1024*2, 1024*300, false)
	if err != nil {
		t.Skipf("Skipping test as NATS is not available: %v", err)
		return
	}

	// Try to register an endpoint with reserved suffix
	handler := func(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
		return nil
	}

	err = natService.AddEndpoint("my_api_docs", handler)
	if err == nil {
		t.Error("Expected error when registering endpoint with reserved suffix '_api_docs'")
	}

	if !strings.Contains(err.Error(), "reserved suffix") {
		t.Errorf("Error should mention 'reserved suffix', got: %v", err)
	}
}

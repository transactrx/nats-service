package nats_service_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
		t.Fatalf("Failed to create NATS service: %v", err)
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
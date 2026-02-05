package nats_service

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEndPointStats_BasicCounts(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	// Add some successes and failures
	stats.AddTransactionLatency(1000, true)  // 1ms success
	stats.AddTransactionLatency(2000, true)  // 2ms success
	stats.AddTransactionLatency(3000, false) // 3ms failure

	if stats.GetSuccessCount() != 2 {
		t.Errorf("expected 2 successes, got %d", stats.GetSuccessCount())
	}
	if stats.GetFailureCount() != 1 {
		t.Errorf("expected 1 failure, got %d", stats.GetFailureCount())
	}
	if stats.GetTotalCount() != 3 {
		t.Errorf("expected 3 total, got %d", stats.GetTotalCount())
	}
}

func TestEndPointStats_LatencyMinMax(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	// Add latencies: 1ms, 5ms, 10ms
	stats.AddTransactionLatency(1000, true)  // 1ms
	stats.AddTransactionLatency(5000, true)  // 5ms
	stats.AddTransactionLatency(10000, true) // 10ms

	latency := stats.GetLatencyStats()

	if latency.Min != 1.0 {
		t.Errorf("expected min 1.0ms, got %f", latency.Min)
	}
	if latency.Max != 10.0 {
		t.Errorf("expected max 10.0ms, got %f", latency.Max)
	}
}

func TestEndPointStats_LatencyAverage(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	// Add latencies: 1ms, 2ms, 3ms -> avg = 2ms
	stats.AddTransactionLatency(1000, true)
	stats.AddTransactionLatency(2000, true)
	stats.AddTransactionLatency(3000, true)

	latency := stats.GetLatencyStats()

	if latency.Avg != 2.0 {
		t.Errorf("expected avg 2.0ms, got %f", latency.Avg)
	}
}

func TestEndPointStats_Percentiles(t *testing.T) {
	stats := NewEndPointStatsWithBufferSize("test.endpoint", 100)

	// Add 100 samples: 1ms, 2ms, 3ms, ..., 100ms
	for i := 1; i <= 100; i++ {
		stats.AddTransactionLatency(int64(i*1000), true)
	}

	latency := stats.GetLatencyStats()

	// P50 should be around 50ms
	if latency.P50 < 49 || latency.P50 > 51 {
		t.Errorf("expected P50 around 50ms, got %f", latency.P50)
	}

	// P95 should be around 95ms
	if latency.P95 < 94 || latency.P95 > 96 {
		t.Errorf("expected P95 around 95ms, got %f", latency.P95)
	}
}

func TestEndPointStats_CircularBuffer(t *testing.T) {
	// Small buffer to test wrapping
	stats := NewEndPointStatsWithBufferSize("test.endpoint", 10)

	// Add 15 samples - buffer should wrap and only keep last 10
	for i := 1; i <= 15; i++ {
		stats.AddTransactionLatency(int64(i*1000), true)
	}

	latency := stats.GetLatencyStats()

	// Count should be 15 (total added)
	if latency.Count != 15 {
		t.Errorf("expected count 15, got %d", latency.Count)
	}

	// Samples in window should be 10 (buffer size)
	if latency.Samples != 10 {
		t.Errorf("expected 10 samples in window, got %d", latency.Samples)
	}

	// Min should still track the overall min (1ms)
	if latency.Min != 1.0 {
		t.Errorf("expected min 1.0ms, got %f", latency.Min)
	}

	// Max should still track the overall max (15ms)
	if latency.Max != 15.0 {
		t.Errorf("expected max 15.0ms, got %f", latency.Max)
	}
}

func TestEndPointStats_Duration(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	// Test the duration convenience method
	stats.AddTransactionLatencyDuration(5*time.Millisecond, true)

	latency := stats.GetLatencyStats()

	if latency.Min != 5.0 {
		t.Errorf("expected min 5.0ms, got %f", latency.Min)
	}
}

func TestEndPointStats_Reset(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	stats.AddTransactionLatency(1000, true)
	stats.AddTransactionLatency(2000, false)

	stats.Reset()

	if stats.GetSuccessCount() != 0 {
		t.Errorf("expected 0 successes after reset, got %d", stats.GetSuccessCount())
	}
	if stats.GetFailureCount() != 0 {
		t.Errorf("expected 0 failures after reset, got %d", stats.GetFailureCount())
	}

	latency := stats.GetLatencyStats()
	if latency.Count != 0 {
		t.Errorf("expected 0 count after reset, got %d", latency.Count)
	}
}

func TestEndPointStats_Snapshot(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	stats.AddTransactionLatency(5000, true)

	snapshot := stats.GetStats()

	if snapshot.Subject != "test.endpoint" {
		t.Errorf("expected subject 'test.endpoint', got '%s'", snapshot.Subject)
	}
	if snapshot.Success != 1 {
		t.Errorf("expected 1 success, got %d", snapshot.Success)
	}
	if snapshot.Latency.Count != 1 {
		t.Errorf("expected latency count 1, got %d", snapshot.Latency.Count)
	}
	if snapshot.Uptime == "" {
		t.Error("expected uptime to be set")
	}
}

func TestEndPointStats_EmptyStats(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	latency := stats.GetLatencyStats()

	if latency.Count != 0 {
		t.Errorf("expected count 0, got %d", latency.Count)
	}
	if latency.Min != 0 {
		t.Errorf("expected min 0, got %f", latency.Min)
	}
	if latency.Max != 0 {
		t.Errorf("expected max 0, got %f", latency.Max)
	}
	if latency.Avg != 0 {
		t.Errorf("expected avg 0, got %f", latency.Avg)
	}
}

func TestEndPointStats_StatusCodeTracking(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	// Add various status codes
	stats.AddTransactionLatencyWithStatus(1000, 200) // success
	stats.AddTransactionLatencyWithStatus(1000, 201) // success (created)
	stats.AddTransactionLatencyWithStatus(1000, 400) // 4xx client error
	stats.AddTransactionLatencyWithStatus(1000, 403) // 4xx authorization error
	stats.AddTransactionLatencyWithStatus(1000, 404) // 4xx not found
	stats.AddTransactionLatencyWithStatus(1000, 500) // 5xx server error
	stats.AddTransactionLatencyWithStatus(1000, 503) // 5xx service unavailable

	if stats.GetSuccessCount() != 2 {
		t.Errorf("expected 2 successes, got %d", stats.GetSuccessCount())
	}
	if stats.GetFailureCount() != 5 {
		t.Errorf("expected 5 total failures, got %d", stats.GetFailureCount())
	}
	if stats.GetFailure4xxCount() != 3 {
		t.Errorf("expected 3 4xx failures, got %d", stats.GetFailure4xxCount())
	}
	if stats.GetFailure5xxCount() != 2 {
		t.Errorf("expected 2 5xx failures, got %d", stats.GetFailure5xxCount())
	}
	if stats.GetTotalCount() != 7 {
		t.Errorf("expected 7 total, got %d", stats.GetTotalCount())
	}
}

func TestEndPointStats_StatusCodeSnapshot(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	stats.AddTransactionLatencyWithStatus(1000, 200) // success
	stats.AddTransactionLatencyWithStatus(1000, 400) // 4xx
	stats.AddTransactionLatencyWithStatus(1000, 404) // 4xx
	stats.AddTransactionLatencyWithStatus(1000, 500) // 5xx

	snapshot := stats.GetStats()

	if snapshot.Success != 1 {
		t.Errorf("expected 1 success, got %d", snapshot.Success)
	}
	if snapshot.Failures != 3 {
		t.Errorf("expected 3 failures, got %d", snapshot.Failures)
	}
	if snapshot.Failures4xx != 2 {
		t.Errorf("expected 2 4xx failures, got %d", snapshot.Failures4xx)
	}
	if snapshot.Failures5xx != 1 {
		t.Errorf("expected 1 5xx failure, got %d", snapshot.Failures5xx)
	}
}

func TestEndPointStats_DurationWithStatus(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	// Test the duration convenience method with status codes
	stats.AddTransactionLatencyDurationWithStatus(5*time.Millisecond, 200)
	stats.AddTransactionLatencyDurationWithStatus(3*time.Millisecond, 400)
	stats.AddTransactionLatencyDurationWithStatus(4*time.Millisecond, 500)

	if stats.GetSuccessCount() != 1 {
		t.Errorf("expected 1 success, got %d", stats.GetSuccessCount())
	}
	if stats.GetFailure4xxCount() != 1 {
		t.Errorf("expected 1 4xx failure, got %d", stats.GetFailure4xxCount())
	}
	if stats.GetFailure5xxCount() != 1 {
		t.Errorf("expected 1 5xx failure, got %d", stats.GetFailure5xxCount())
	}

	latency := stats.GetLatencyStats()
	if latency.Min != 3.0 {
		t.Errorf("expected min 3.0ms, got %f", latency.Min)
	}
	if latency.Max != 5.0 {
		t.Errorf("expected max 5.0ms, got %f", latency.Max)
	}
}

func TestEndPointStats_ResetWithStatusCodes(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	stats.AddTransactionLatencyWithStatus(1000, 200)
	stats.AddTransactionLatencyWithStatus(1000, 400)
	stats.AddTransactionLatencyWithStatus(1000, 500)

	stats.Reset()

	if stats.GetSuccessCount() != 0 {
		t.Errorf("expected 0 successes after reset, got %d", stats.GetSuccessCount())
	}
	if stats.GetFailureCount() != 0 {
		t.Errorf("expected 0 failures after reset, got %d", stats.GetFailureCount())
	}
	if stats.GetFailure4xxCount() != 0 {
		t.Errorf("expected 0 4xx failures after reset, got %d", stats.GetFailure4xxCount())
	}
	if stats.GetFailure5xxCount() != 0 {
		t.Errorf("expected 0 5xx failures after reset, got %d", stats.GetFailure5xxCount())
	}
}

func TestEndPointStats_EdgeStatusCodes(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")

	// Test edge cases for status code boundaries
	stats.AddTransactionLatencyWithStatus(1000, 199) // below 200 - treated as success
	stats.AddTransactionLatencyWithStatus(1000, 299) // top of 2xx - success
	stats.AddTransactionLatencyWithStatus(1000, 302) // 3xx redirect - treated as success
	stats.AddTransactionLatencyWithStatus(1000, 399) // below 400 - success
	stats.AddTransactionLatencyWithStatus(1000, 499) // top of 4xx - client error
	stats.AddTransactionLatencyWithStatus(1000, 599) // top of 5xx - server error

	if stats.GetSuccessCount() != 4 {
		t.Errorf("expected 4 successes, got %d", stats.GetSuccessCount())
	}
	if stats.GetFailure4xxCount() != 1 {
		t.Errorf("expected 1 4xx failure, got %d", stats.GetFailure4xxCount())
	}
	if stats.GetFailure5xxCount() != 1 {
		t.Errorf("expected 1 5xx failure, got %d", stats.GetFailure5xxCount())
	}
}

func TestEndPointStats_JSONBackwardsCompatibility(t *testing.T) {
	// Simulate an old client that doesn't know about the new fields
	type OldClientSnapshot struct {
		Subject  string `json:"subject"`
		Success  int64  `json:"success"`
		Failures int64  `json:"failures"`
	}

	stats := NewEndPointStats("test.endpoint")
	stats.AddTransactionLatencyWithStatus(1000, 200) // success
	stats.AddTransactionLatencyWithStatus(1000, 400) // 4xx
	stats.AddTransactionLatencyWithStatus(1000, 500) // 5xx

	snapshot := stats.GetStats()
	jsonBytes, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("failed to marshal snapshot: %v", err)
	}

	// Old client should be able to parse the JSON without errors
	var oldClient OldClientSnapshot
	err = json.Unmarshal(jsonBytes, &oldClient)
	if err != nil {
		t.Fatalf("old client failed to parse new JSON: %v", err)
	}

	// Old client should see the correct values for the fields it knows about
	if oldClient.Subject != "test.endpoint" {
		t.Errorf("expected subject 'test.endpoint', got '%s'", oldClient.Subject)
	}
	if oldClient.Success != 1 {
		t.Errorf("expected 1 success, got %d", oldClient.Success)
	}
	if oldClient.Failures != 2 {
		t.Errorf("expected 2 failures, got %d", oldClient.Failures)
	}
}

func TestEndPointStats_JSONOmitEmptyZeroValues(t *testing.T) {
	stats := NewEndPointStats("test.endpoint")
	stats.AddTransactionLatencyWithStatus(1000, 200) // success only, no failures

	snapshot := stats.GetStats()
	jsonBytes, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("failed to marshal snapshot: %v", err)
	}

	jsonStr := string(jsonBytes)

	// When failures4xx and failures5xx are 0, they should be omitted from JSON
	// due to omitempty tag
	var rawMap map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &rawMap); err != nil {
		t.Fatalf("failed to unmarshal to map: %v", err)
	}

	if _, exists := rawMap["failures4xx"]; exists {
		t.Errorf("failures4xx should be omitted when zero, but found in JSON: %s", jsonStr)
	}
	if _, exists := rawMap["failures5xx"]; exists {
		t.Errorf("failures5xx should be omitted when zero, but found in JSON: %s", jsonStr)
	}
}

func TestEndPointStats_NewCLIParsingOldServiceResponse(t *testing.T) {
	// Simulate JSON from an old service that doesn't have failures4xx/failures5xx fields
	oldServiceJSON := `{
		"subject": "old.service.endpoint",
		"success": 100,
		"failures": 10,
		"latency": {
			"count": 110,
			"minMs": 1.0,
			"maxMs": 50.0,
			"avgMs": 5.5,
			"p50Ms": 4.0,
			"p65Ms": 5.0,
			"p75Ms": 6.0,
			"p85Ms": 8.0,
			"p95Ms": 15.0,
			"samplesInWindow": 110
		},
		"startTime": "2024-01-01T00:00:00Z",
		"uptime": "1h0m0s"
	}`

	// New CLI uses the new struct with failures4xx/failures5xx fields
	var newSnapshot EndpointStatsSnapshot
	err := json.Unmarshal([]byte(oldServiceJSON), &newSnapshot)
	if err != nil {
		t.Fatalf("new CLI failed to parse old service JSON: %v", err)
	}

	// Verify existing fields are parsed correctly
	if newSnapshot.Subject != "old.service.endpoint" {
		t.Errorf("expected subject 'old.service.endpoint', got '%s'", newSnapshot.Subject)
	}
	if newSnapshot.Success != 100 {
		t.Errorf("expected 100 successes, got %d", newSnapshot.Success)
	}
	if newSnapshot.Failures != 10 {
		t.Errorf("expected 10 failures, got %d", newSnapshot.Failures)
	}

	// New fields should default to zero when missing from old service response
	if newSnapshot.Failures4xx != 0 {
		t.Errorf("expected failures4xx to be 0 (missing from old service), got %d", newSnapshot.Failures4xx)
	}
	if newSnapshot.Failures5xx != 0 {
		t.Errorf("expected failures5xx to be 0 (missing from old service), got %d", newSnapshot.Failures5xx)
	}
}

func BenchmarkEndPointStats_AddLatency(b *testing.B) {
	stats := NewEndPointStats("bench.endpoint")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stats.AddTransactionLatency(int64(i%10000), true)
	}
}

func BenchmarkEndPointStats_GetStats(b *testing.B) {
	stats := NewEndPointStats("bench.endpoint")

	// Pre-fill with samples
	for i := 0; i < 10000; i++ {
		stats.AddTransactionLatency(int64(i), true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = stats.GetStats()
	}
}

package nats_service

import (
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

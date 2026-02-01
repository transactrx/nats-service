package nats_service

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultLatencyBufferSize is the number of latency samples to keep for percentile calculations
	// This bounds memory usage while providing accurate recent percentiles
	DefaultLatencyBufferSize = 10000
)

// EndPointStats tracks performance metrics for a single endpoint
type EndPointStats struct {
	// Number of requests successfully processed
	success atomic.Int64
	// Number of requests that failed
	failures atomic.Int64

	// Latency tracking with bounded memory
	latencyMu     sync.RWMutex
	latencies     []int64 // circular buffer of latency samples (in microseconds)
	latencyIndex  int     // next write position in circular buffer
	latencyCount  int64   // total number of samples added (may exceed buffer size)
	latencySum    int64   // running sum for average calculation
	latencyMin    int64   // minimum latency observed
	latencyMax    int64   // maximum latency observed
	bufferSize    int     // size of the circular buffer
	bufferWrapped bool    // true if buffer has wrapped around at least once

	// Endpoint metadata
	subject       string
	startDateTime time.Time
}

// LatencyStats contains computed latency statistics
type LatencyStats struct {
	Count   int64   `json:"count"`             // Total number of requests
	Min     float64 `json:"minMs"`             // Minimum latency in milliseconds
	Max     float64 `json:"maxMs"`             // Maximum latency in milliseconds
	Avg     float64 `json:"avgMs"`             // Average latency in milliseconds
	P50     float64 `json:"p50Ms"`             // 50th percentile (median)
	P65     float64 `json:"p65Ms"`             // 65th percentile
	P75     float64 `json:"p75Ms"`             // 75th percentile
	P85     float64 `json:"p85Ms"`             // 85th percentile
	P95     float64 `json:"p95Ms"`             // 95th percentile
	Samples int     `json:"samplesInWindow"`   // Number of samples in current window
}

// EndpointStatsSnapshot contains a point-in-time snapshot of endpoint statistics
type EndpointStatsSnapshot struct {
	Subject   string       `json:"subject"`
	Success   int64        `json:"success"`
	Failures  int64        `json:"failures"`
	Latency   LatencyStats `json:"latency"`
	StartTime time.Time    `json:"startTime"`
	Uptime    string       `json:"uptime"`
}

// NewEndPointStats creates a new endpoint stats tracker with the default buffer size
func NewEndPointStats(subject string) *EndPointStats {
	return NewEndPointStatsWithBufferSize(subject, DefaultLatencyBufferSize)
}

// NewEndPointStatsWithBufferSize creates a new endpoint stats tracker with a custom buffer size
func NewEndPointStatsWithBufferSize(subject string, bufferSize int) *EndPointStats {
	if bufferSize <= 0 {
		bufferSize = DefaultLatencyBufferSize
	}
	return &EndPointStats{
		subject:       subject,
		startDateTime: time.Now(),
		latencies:     make([]int64, bufferSize),
		bufferSize:    bufferSize,
		latencyMin:    -1, // -1 indicates no samples yet
	}
}

// AddTransactionLatency records a transaction's latency and success/failure status
// latency should be provided in microseconds for precision
func (s *EndPointStats) AddTransactionLatency(latencyMicros int64, success bool) {
	if success {
		s.success.Add(1)
	} else {
		s.failures.Add(1)
	}

	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()

	// Add to circular buffer
	s.latencies[s.latencyIndex] = latencyMicros
	s.latencyIndex = (s.latencyIndex + 1) % s.bufferSize
	if s.latencyIndex == 0 && s.latencyCount >= int64(s.bufferSize) {
		s.bufferWrapped = true
	}
	s.latencyCount++
	s.latencySum += latencyMicros

	// Update min/max
	if s.latencyMin < 0 || latencyMicros < s.latencyMin {
		s.latencyMin = latencyMicros
	}
	if latencyMicros > s.latencyMax {
		s.latencyMax = latencyMicros
	}
}

// AddTransactionLatencyDuration is a convenience method that accepts a time.Duration
func (s *EndPointStats) AddTransactionLatencyDuration(latency time.Duration, success bool) {
	s.AddTransactionLatency(latency.Microseconds(), success)
}

// GetStats returns a snapshot of current endpoint statistics
func (s *EndPointStats) GetStats() EndpointStatsSnapshot {
	s.latencyMu.RLock()
	defer s.latencyMu.RUnlock()

	snapshot := EndpointStatsSnapshot{
		Subject:   s.subject,
		Success:   s.success.Load(),
		Failures:  s.failures.Load(),
		StartTime: s.startDateTime,
		Uptime:    time.Since(s.startDateTime).Round(time.Second).String(),
	}

	// Calculate latency stats
	snapshot.Latency = s.calculateLatencyStatsLocked()

	return snapshot
}

// GetLatencyStats returns only the latency statistics
func (s *EndPointStats) GetLatencyStats() LatencyStats {
	s.latencyMu.RLock()
	defer s.latencyMu.RUnlock()
	return s.calculateLatencyStatsLocked()
}

// calculateLatencyStatsLocked computes latency statistics from the buffer
// Must be called with latencyMu held (at least RLock)
func (s *EndPointStats) calculateLatencyStatsLocked() LatencyStats {
	stats := LatencyStats{
		Count: s.latencyCount,
	}

	if s.latencyCount == 0 {
		return stats
	}

	// Determine how many samples are in the buffer
	var sampleCount int
	if s.bufferWrapped || s.latencyCount >= int64(s.bufferSize) {
		sampleCount = s.bufferSize
	} else {
		sampleCount = int(s.latencyCount)
	}
	stats.Samples = sampleCount

	// Convert min/max to milliseconds
	stats.Min = float64(s.latencyMin) / 1000.0
	stats.Max = float64(s.latencyMax) / 1000.0

	// Calculate average from running sum
	stats.Avg = float64(s.latencySum) / float64(s.latencyCount) / 1000.0

	// Copy and sort samples for percentile calculation
	samples := make([]int64, sampleCount)
	if s.bufferWrapped || s.latencyCount >= int64(s.bufferSize) {
		copy(samples, s.latencies)
	} else {
		copy(samples, s.latencies[:sampleCount])
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	// Calculate percentiles
	stats.P50 = float64(s.percentile(samples, 50)) / 1000.0
	stats.P65 = float64(s.percentile(samples, 65)) / 1000.0
	stats.P75 = float64(s.percentile(samples, 75)) / 1000.0
	stats.P85 = float64(s.percentile(samples, 85)) / 1000.0
	stats.P95 = float64(s.percentile(samples, 95)) / 1000.0

	return stats
}

// percentile calculates the p-th percentile from a sorted slice
func (s *EndPointStats) percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}

	// Use nearest-rank method
	index := (p * len(sorted)) / 100
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// Reset clears all statistics
func (s *EndPointStats) Reset() {
	s.success.Store(0)
	s.failures.Store(0)

	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()

	s.latencyIndex = 0
	s.latencyCount = 0
	s.latencySum = 0
	s.latencyMin = -1
	s.latencyMax = 0
	s.bufferWrapped = false
	// Clear the buffer
	for i := range s.latencies {
		s.latencies[i] = 0
	}
	s.startDateTime = time.Now()
}

// GetSuccessCount returns the number of successful requests
func (s *EndPointStats) GetSuccessCount() int64 {
	return s.success.Load()
}

// GetFailureCount returns the number of failed requests
func (s *EndPointStats) GetFailureCount() int64 {
	return s.failures.Load()
}

// GetTotalCount returns the total number of requests (success + failures)
func (s *EndPointStats) GetTotalCount() int64 {
	return s.success.Load() + s.failures.Load()
}

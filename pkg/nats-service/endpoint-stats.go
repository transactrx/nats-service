package nats_service

import "sync/atomic"

type EndPointStats struct {
	// Number of requests successfully processed
	success atomic.Int64
	// Number of requests that failed
	failures atomic.Int64
	// Average Latency
	averageLatency int64
	//endpoint subject
	subject       string
	startDateTime int64
}

// add a successful request to the stats

func (s *EndPointStats) AddTransactionLatency(latency int64, success bool) {
	if success {
		s.success.Add(1)
	} else {
		s.failures.Add(1)
	}
	//	average latency
}

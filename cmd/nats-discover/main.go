package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	nats_service_client "github.com/transactrx/nats-service/pkg/nats-service-client"
)

const (
	defaultDiscoveryTimeout = 3 * time.Second  // For listing services (broadcast, wait for multiple responses)
	defaultRequestTimeout   = 10 * time.Second // For API docs request (single response)
)

var version = "dev" // Set via ldflags: -X main.version=...

type config struct {
	natsURL     string
	contextName string
	timeout     time.Duration
	format      string
	showVersion bool
	service     string // specific service to get API docs or stats for
	stats       bool   // get stats instead of API docs
	creds       string
	nkey        string
	jwt         string
	seed        string
}

func main() {
	cfg := parseFlags()

	if cfg.showVersion {
		fmt.Printf("nats-discover version %s\n", version)
		os.Exit(0)
	}

	// Resolve NATS connection options
	natsURL, opts, err := resolveConnection(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Connect to NATS
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	if cfg.stats {
		// Get stats for a service (requires service name)
		if cfg.service == "" {
			fmt.Fprintf(os.Stderr, "Error: --stats requires -S/--service to specify the service\n")
			os.Exit(1)
		}
		stats, err := getServiceStats(nc, cfg.service, cfg.timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting stats for service '%s': %v\n", cfg.service, err)
			os.Exit(1)
		}
		if err := outputStats(stats, cfg.format); err != nil {
			fmt.Fprintf(os.Stderr, "Error formatting output: %v\n", err)
			os.Exit(1)
		}
	} else if cfg.service != "" {
		// Get API docs for a specific service (uses defaultRequestTimeout)
		apiDocs, err := getServiceApiDocs(nc, cfg.service)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting API docs for service '%s': %v\n", cfg.service, err)
			os.Exit(1)
		}
		if err := outputApiDocs(apiDocs, cfg.format); err != nil {
			fmt.Fprintf(os.Stderr, "Error formatting output: %v\n", err)
			os.Exit(1)
		}
	} else {
		// List all services (basic info only)
		services, err := discoverServiceList(nc, cfg.timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error discovering services: %v\n", err)
			os.Exit(1)
		}
		if err := outputServiceList(services, cfg.format); err != nil {
			fmt.Fprintf(os.Stderr, "Error formatting output: %v\n", err)
			os.Exit(1)
		}
	}
}

func parseFlags() config {
	cfg := config{}

	flag.StringVar(&cfg.natsURL, "s", "", "NATS server URL (e.g., nats://localhost:4222)")
	flag.StringVar(&cfg.natsURL, "server", "", "NATS server URL (e.g., nats://localhost:4222)")
	flag.StringVar(&cfg.contextName, "context", "", "NATS context name (from nats CLI)")
	flag.DurationVar(&cfg.timeout, "timeout", defaultDiscoveryTimeout, "Timeout for discovery broadcast (waiting for multiple services to respond)")
	flag.StringVar(&cfg.format, "format", "table", "Output format: table, json, yaml")
	flag.BoolVar(&cfg.showVersion, "version", false, "Show version")
	flag.StringVar(&cfg.service, "service", "", "Service name to get API docs or stats for")
	flag.StringVar(&cfg.service, "S", "", "Service name to get API docs or stats for (shorthand)")
	flag.BoolVar(&cfg.stats, "stats", false, "Get stats for a service (requires -S/--service)")
	flag.StringVar(&cfg.creds, "creds", "", "Path to credentials file")
	flag.StringVar(&cfg.nkey, "nkey", "", "Path to NKey file")
	flag.StringVar(&cfg.jwt, "jwt", "", "JWT token for authentication")
	flag.StringVar(&cfg.seed, "seed", "", "NKey seed for authentication")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "nats-discover - Discover NATS services using the nats-service framework\n\n")
		fmt.Fprintf(os.Stderr, "Usage: nats-discover [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  nats-discover -s nats://localhost:4222                         # List all services\n")
		fmt.Fprintf(os.Stderr, "  nats-discover -s nats://localhost:4222 -S orders.api           # Show endpoints for orders.api\n")
		fmt.Fprintf(os.Stderr, "  nats-discover -s nats://localhost:4222 -S orders.api --stats   # Show stats for all instances\n")
		fmt.Fprintf(os.Stderr, "  nats-discover -s nats://localhost:4222 --service orders.api --format json\n")
	}

	flag.Parse()
	return cfg
}

// natsContext represents a NATS CLI context configuration
type natsContext struct {
	URL         string `json:"url"`
	User        string `json:"user,omitempty"`
	Password    string `json:"password,omitempty"`
	Token       string `json:"token,omitempty"`
	Creds       string `json:"creds,omitempty"`
	NKey        string `json:"nkey,omitempty"`
	Cert        string `json:"cert,omitempty"`
	Key         string `json:"key,omitempty"`
	CA          string `json:"ca,omitempty"`
	JWT         string `json:"jwt,omitempty"`
	Seed        string `json:"seed,omitempty"`
	Description string `json:"description,omitempty"`
}

func resolveConnection(cfg config) (string, []nats.Option, error) {
	var natsURL string
	var opts []nats.Option

	// Priority: explicit URL > context > environment variable
	if cfg.natsURL != "" {
		natsURL = cfg.natsURL
	} else if cfg.contextName != "" {
		ctx, err := loadNatsContext(cfg.contextName)
		if err != nil {
			return "", nil, fmt.Errorf("failed to load context '%s': %w", cfg.contextName, err)
		}
		natsURL = ctx.URL
		opts = append(opts, contextToOptions(ctx)...)
	} else if envURL := os.Getenv("NATS_URL"); envURL != "" {
		natsURL = envURL
	} else {
		// Try to load default context
		ctx, err := loadDefaultContext()
		if err == nil && ctx != nil {
			natsURL = ctx.URL
			opts = append(opts, contextToOptions(ctx)...)
		} else {
			return "", nil, fmt.Errorf("no NATS server specified. Use -s, --context, or set NATS_URL environment variable")
		}
	}

	// Add explicit credential options (override context)
	if cfg.creds != "" {
		opts = append(opts, nats.UserCredentials(cfg.creds))
	}
	if cfg.nkey != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.nkey)
		if err != nil {
			return "", nil, fmt.Errorf("failed to load NKey: %w", err)
		}
		opts = append(opts, opt)
	}
	if cfg.jwt != "" && cfg.seed != "" {
		opts = append(opts, nats.UserJWTAndSeed(cfg.jwt, cfg.seed))
	}

	// Add connection name
	opts = append(opts, nats.Name("nats-discover"))

	return natsURL, opts, nil
}

// getNatsConfigDirs returns possible NATS config directories in order of preference.
// NATS CLI uses ~/.config/nats/ on all platforms, but we also check os.UserConfigDir() as fallback.
func getNatsConfigDirs() []string {
	var dirs []string

	// First, check ~/.config/nats (where NATS CLI actually stores contexts)
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".config", "nats"))
	}

	// Fallback to os.UserConfigDir() (~/Library/Application Support on macOS)
	if configDir, err := os.UserConfigDir(); err == nil {
		dirs = append(dirs, filepath.Join(configDir, "nats"))
	}

	return dirs
}

func loadNatsContext(name string) (*natsContext, error) {
	// Try each config directory
	for _, dir := range getNatsConfigDirs() {
		contextPath := filepath.Join(dir, "context", name+".json")
		data, err := os.ReadFile(contextPath)
		if err != nil {
			continue // Try next directory
		}

		var ctx natsContext
		if err := json.Unmarshal(data, &ctx); err != nil {
			return nil, fmt.Errorf("invalid context file: %w", err)
		}

		return &ctx, nil
	}

	return nil, fmt.Errorf("context '%s' not found", name)
}

func loadDefaultContext() (*natsContext, error) {
	// Try each config directory for context.txt
	for _, dir := range getNatsConfigDirs() {
		defaultPath := filepath.Join(dir, "context.txt")
		data, err := os.ReadFile(defaultPath)
		if err != nil {
			continue // Try next directory
		}

		contextName := strings.TrimSpace(string(data))
		if contextName == "" {
			continue
		}

		return loadNatsContext(contextName)
	}

	return nil, fmt.Errorf("no default context found")
}

func contextToOptions(ctx *natsContext) []nats.Option {
	var opts []nats.Option

	if ctx.User != "" && ctx.Password != "" {
		opts = append(opts, nats.UserInfo(ctx.User, ctx.Password))
	}
	if ctx.Token != "" {
		opts = append(opts, nats.Token(ctx.Token))
	}
	if ctx.Creds != "" {
		opts = append(opts, nats.UserCredentials(ctx.Creds))
	}
	if ctx.NKey != "" {
		opt, err := nats.NkeyOptionFromSeed(ctx.NKey)
		if err == nil {
			opts = append(opts, opt)
		}
	}
	if ctx.JWT != "" && ctx.Seed != "" {
		opts = append(opts, nats.UserJWTAndSeed(ctx.JWT, ctx.Seed))
	}
	if ctx.Cert != "" && ctx.Key != "" {
		opts = append(opts, nats.ClientCert(ctx.Cert, ctx.Key))
	}
	if ctx.CA != "" {
		opts = append(opts, nats.RootCAs(ctx.CA))
	}

	return opts
}

// discoverServiceList discovers all services and returns basic info only
func discoverServiceList(nc *nats.Conn, timeout time.Duration) ([]nats_service.ServiceInfo, error) {
	results := make(map[string]nats_service.ServiceInfo)
	var mu sync.Mutex

	// Create an inbox for receiving responses
	inbox := nc.NewInbox()

	// Subscribe to the inbox
	sub, err := nc.Subscribe(inbox, func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		// Try to parse as new ServiceInfo format
		var serviceInfo nats_service.ServiceInfo
		if err := json.Unmarshal(msg.Data, &serviceInfo); err != nil {
			return // Skip malformed responses
		}

		// Check if this is a legacy response with endpoints (old format)
		var legacyResp nats_service.DiscoveryResponse
		if err := json.Unmarshal(msg.Data, &legacyResp); err == nil && len(legacyResp.Endpoints) > 0 {
			// Legacy response - extract basic info
			serviceName := legacyResp.ServiceName
			if _, exists := results[serviceName]; !exists {
				results[serviceName] = nats_service.ServiceInfo{
					ServiceName:   legacyResp.ServiceName,
					SubjectPrefix: legacyResp.BasePath,
				}
			}
			return
		}

		// New-style ServiceInfo response
		if serviceInfo.ServiceName != "" {
			if _, exists := results[serviceInfo.ServiceName]; !exists {
				results[serviceInfo.ServiceName] = serviceInfo
			}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to inbox: %w", err)
	}
	defer sub.Unsubscribe()

	// Publish discovery request
	if err := nc.PublishRequest(nats_service.DiscoverySubject, inbox, nil); err != nil {
		return nil, fmt.Errorf("failed to publish discovery request: %w", err)
	}

	// Flush to ensure the message is sent
	if err := nc.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	// Wait for responses
	time.Sleep(timeout)

	// Convert map to sorted slice
	services := make([]nats_service.ServiceInfo, 0, len(results))
	for _, svc := range results {
		services = append(services, svc)
	}
	sort.Slice(services, func(i, j int) bool {
		return services[i].ServiceName < services[j].ServiceName
	})

	return services, nil
}

// getServiceApiDocs fetches API documentation for a specific service.
// Uses the nats-service-client to properly handle compression and chunking.
// The serviceName is the base path of the service (e.g., "orders.api").
func getServiceApiDocs(nc *nats.Conn, serviceName string) (*nats_service.ApiDocsResponse, error) {
	// Construct the API docs subject directly: {serviceName}._api_docs
	apiDocsSubject := serviceName + "." + nats_service.ApiDocsSubjectSuffix

	// Create a client from the existing connection to handle compression/chunking
	client := nats_service_client.NewClientFromConnection(nc)

	// Use the client to make the request - handles decompression and chunk reassembly
	resp, svcErr, err := client.DoRequest("", apiDocsSubject, nil, nil, defaultRequestTimeout)
	if err != nil {
		if err == nats.ErrTimeout {
			return nil, fmt.Errorf("service '%s' not found or not responding (timeout after %v)", serviceName, defaultRequestTimeout)
		}
		return nil, fmt.Errorf("failed to get API docs: %w", err)
	}
	if svcErr != nil {
		return nil, fmt.Errorf("service error: %s (status: %d)", svcErr.ErrorMessage, svcErr.Status)
	}

	var apiDocs nats_service.ApiDocsResponse
	if err := json.Unmarshal(resp.Data, &apiDocs); err != nil {
		return nil, fmt.Errorf("failed to parse API docs response: %w", err)
	}
	return &apiDocs, nil
}

// AggregatedStats contains combined stats from all instances of a service
type AggregatedStats struct {
	ServiceName    string                              `json:"serviceName"`
	InstanceCount  int                                 `json:"instanceCount"`
	Instances      []nats_service.InstanceStatsResponse `json:"instances"`
	AggregatedEndpoints []AggregatedEndpointStats      `json:"aggregatedEndpoints"`
}

// AggregatedEndpointStats contains combined stats for a single endpoint across all instances
type AggregatedEndpointStats struct {
	Subject       string  `json:"subject"`
	TotalSuccess  int64   `json:"totalSuccess"`
	TotalFailures int64   `json:"totalFailures"`
	MinLatencyMs  float64 `json:"minLatencyMs"`
	MaxLatencyMs  float64 `json:"maxLatencyMs"`
	AvgLatencyMs  float64 `json:"avgLatencyMs"`
	// Per-instance percentiles (aggregating percentiles is statistically complex)
	P50Range string `json:"p50RangeMs"`
	P95Range string `json:"p95RangeMs"`
}

// getServiceStats collects stats from all instances of a service
func getServiceStats(nc *nats.Conn, serviceName string, timeout time.Duration) (*AggregatedStats, error) {
	results := make([]nats_service.InstanceStatsResponse, 0)
	var mu sync.Mutex

	// Create an inbox for receiving responses
	inbox := nc.NewInbox()

	// Subscribe to the inbox
	sub, err := nc.Subscribe(inbox, func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		var instanceStats nats_service.InstanceStatsResponse
		if err := json.Unmarshal(msg.Data, &instanceStats); err != nil {
			return // Skip malformed responses
		}
		results = append(results, instanceStats)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to inbox: %w", err)
	}
	defer sub.Unsubscribe()

	// Publish stats request (broadcast - all instances respond)
	statsSubject := serviceName + "." + nats_service.StatsSubjectSuffix
	if err := nc.PublishRequest(statsSubject, inbox, nil); err != nil {
		return nil, fmt.Errorf("failed to publish stats request: %w", err)
	}

	// Flush to ensure the message is sent
	if err := nc.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	// Wait for responses
	time.Sleep(timeout)

	if len(results) == 0 {
		return nil, fmt.Errorf("no instances responded for service '%s'", serviceName)
	}

	// Aggregate results
	aggregated := &AggregatedStats{
		ServiceName:   serviceName,
		InstanceCount: len(results),
		Instances:     results,
	}

	// Aggregate endpoint stats across instances
	endpointMap := make(map[string]*AggregatedEndpointStats)
	endpointP50s := make(map[string][]float64)
	endpointP95s := make(map[string][]float64)
	endpointAvgSums := make(map[string]float64)
	endpointCounts := make(map[string]int64)

	for _, instance := range results {
		for _, ep := range instance.Endpoints {
			if _, ok := endpointMap[ep.Subject]; !ok {
				endpointMap[ep.Subject] = &AggregatedEndpointStats{
					Subject:      ep.Subject,
					MinLatencyMs: ep.Latency.Min,
					MaxLatencyMs: ep.Latency.Max,
				}
				endpointP50s[ep.Subject] = make([]float64, 0)
				endpointP95s[ep.Subject] = make([]float64, 0)
			}

			agg := endpointMap[ep.Subject]
			agg.TotalSuccess += ep.Success
			agg.TotalFailures += ep.Failures

			if ep.Latency.Min < agg.MinLatencyMs || agg.MinLatencyMs == 0 {
				agg.MinLatencyMs = ep.Latency.Min
			}
			if ep.Latency.Max > agg.MaxLatencyMs {
				agg.MaxLatencyMs = ep.Latency.Max
			}

			// Track for weighted average
			endpointAvgSums[ep.Subject] += ep.Latency.Avg * float64(ep.Latency.Count)
			endpointCounts[ep.Subject] += ep.Latency.Count

			// Track percentiles for range display
			if ep.Latency.Count > 0 {
				endpointP50s[ep.Subject] = append(endpointP50s[ep.Subject], ep.Latency.P50)
				endpointP95s[ep.Subject] = append(endpointP95s[ep.Subject], ep.Latency.P95)
			}
		}
	}

	// Calculate weighted averages and percentile ranges
	for subject, agg := range endpointMap {
		if endpointCounts[subject] > 0 {
			agg.AvgLatencyMs = endpointAvgSums[subject] / float64(endpointCounts[subject])
		}

		p50s := endpointP50s[subject]
		p95s := endpointP95s[subject]

		if len(p50s) > 0 {
			sort.Float64s(p50s)
			sort.Float64s(p95s)
			agg.P50Range = fmt.Sprintf("%.2f-%.2f", p50s[0], p50s[len(p50s)-1])
			agg.P95Range = fmt.Sprintf("%.2f-%.2f", p95s[0], p95s[len(p95s)-1])
		}

		aggregated.AggregatedEndpoints = append(aggregated.AggregatedEndpoints, *agg)
	}

	// Sort by subject
	sort.Slice(aggregated.AggregatedEndpoints, func(i, j int) bool {
		return aggregated.AggregatedEndpoints[i].Subject < aggregated.AggregatedEndpoints[j].Subject
	})

	return aggregated, nil
}

// outputStats outputs the aggregated stats
func outputStats(stats *AggregatedStats, format string) error {
	switch strings.ToLower(format) {
	case "json":
		data, err := json.MarshalIndent(stats, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))

	case "yaml":
		fmt.Printf("serviceName: %s\n", stats.ServiceName)
		fmt.Printf("instanceCount: %d\n", stats.InstanceCount)
		fmt.Println("instances:")
		for _, inst := range stats.Instances {
			fmt.Printf("  - instanceId: %s\n", inst.InstanceId)
			fmt.Printf("    uptime: %s\n", inst.Uptime)
			if len(inst.Endpoints) > 0 {
				fmt.Println("    endpoints:")
				for _, ep := range inst.Endpoints {
					fmt.Printf("      - subject: %s\n", ep.Subject)
					fmt.Printf("        success: %d\n", ep.Success)
					fmt.Printf("        failures: %d\n", ep.Failures)
					fmt.Printf("        latency:\n")
					fmt.Printf("          avgMs: %.2f\n", ep.Latency.Avg)
					fmt.Printf("          p95Ms: %.2f\n", ep.Latency.P95)
				}
			}
		}
		fmt.Println("aggregatedEndpoints:")
		for _, ep := range stats.AggregatedEndpoints {
			fmt.Printf("  - subject: %s\n", ep.Subject)
			fmt.Printf("    totalSuccess: %d\n", ep.TotalSuccess)
			fmt.Printf("    totalFailures: %d\n", ep.TotalFailures)
			fmt.Printf("    avgLatencyMs: %.2f\n", ep.AvgLatencyMs)
			fmt.Printf("    p95RangeMs: %s\n", ep.P95Range)
		}

	case "table":
		fmt.Printf("Service: %s\n", stats.ServiceName)
		fmt.Printf("Instances: %d\n\n", stats.InstanceCount)

		// Instance summary
		fmt.Println("INSTANCES:")
		fmt.Printf("%-30s %-20s\n", "INSTANCE ID", "UPTIME")
		fmt.Printf("%-30s %-20s\n", strings.Repeat("-", 30), strings.Repeat("-", 20))
		for _, inst := range stats.Instances {
			fmt.Printf("%-30s %-20s\n", truncateString(inst.InstanceId, 30), inst.Uptime)
		}
		fmt.Println()

		// Aggregated endpoint stats
		fmt.Println("AGGREGATED ENDPOINT STATS:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "ENDPOINT\tSUCCESS\tFAILURES\tAVG(ms)\tMIN(ms)\tMAX(ms)\tP95 RANGE(ms)\n")
		fmt.Fprintf(w, "--------\t-------\t--------\t-------\t-------\t-------\t-------------\n")
		for _, ep := range stats.AggregatedEndpoints {
			fmt.Fprintf(w, "%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%s\n",
				ep.Subject, ep.TotalSuccess, ep.TotalFailures,
				ep.AvgLatencyMs, ep.MinLatencyMs, ep.MaxLatencyMs, ep.P95Range)
		}
		w.Flush()

	default:
		return fmt.Errorf("unknown format: %s (supported: table, json, yaml)", format)
	}
	return nil
}

// outputServiceList outputs the list of discovered services
func outputServiceList(services []nats_service.ServiceInfo, format string) error {
	if len(services) == 0 {
		fmt.Println("No services discovered")
		return nil
	}

	switch strings.ToLower(format) {
	case "json":
		data, err := json.MarshalIndent(services, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
	case "yaml":
		for i, svc := range services {
			if i > 0 {
				fmt.Println("---")
			}
			fmt.Printf("serviceName: %s\n", svc.ServiceName)
			fmt.Printf("subjectPrefix: %s\n", svc.SubjectPrefix)
			if svc.Description != "" {
				fmt.Printf("description: %s\n", svc.Description)
			}
			if svc.RepositoryURL != "" {
				fmt.Printf("repositoryUrl: %s\n", svc.RepositoryURL)
			}
			if svc.ApiDocsSubject != "" {
				fmt.Printf("apiDocsSubject: %s\n", svc.ApiDocsSubject)
			}
		}
	case "table":
		// Header
		fmt.Printf("%-50s %-50s %s\n", "SERVICE", "SUBJECT PREFIX", "REPOSITORY")
		fmt.Printf("%-50s %-50s %s\n", strings.Repeat("-", 50), strings.Repeat("-", 50), strings.Repeat("-", 100))

		for i, svc := range services {
			// Line 1: service, prefix, repo URL
			serviceName := truncateString(svc.ServiceName, 50)
			subjectPrefix := truncateString(svc.SubjectPrefix, 50)
			repoURL := truncateString(svc.RepositoryURL, 100)
			if repoURL == "" {
				repoURL = "-"
			}
			fmt.Printf("%-50s %-50s %s\n", serviceName, subjectPrefix, repoURL)

			// Line 2: description (wrapped)
			if svc.Description != "" {
				wrappedDesc := wrapText(svc.Description, 150)
				for _, line := range wrappedDesc {
					fmt.Printf("  %s\n", line)
				}
			}

			// Empty line between services (but not after the last one)
			if i < len(services)-1 {
				fmt.Println()
			}
		}
	default:
		return fmt.Errorf("unknown format: %s (supported: table, json, yaml)", format)
	}
	return nil
}

// outputApiDocs outputs the API documentation for a service
func outputApiDocs(apiDocs *nats_service.ApiDocsResponse, format string) error {
	if apiDocs == nil {
		fmt.Println("No API documentation available")
		return nil
	}

	switch strings.ToLower(format) {
	case "json":
		data, err := json.MarshalIndent(apiDocs, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
	case "yaml":
		fmt.Printf("serviceName: %s\n", apiDocs.ServiceName)
		fmt.Printf("subjectPrefix: %s\n", apiDocs.SubjectPrefix)
		if apiDocs.Description != "" {
			fmt.Printf("description: %s\n", apiDocs.Description)
		}
		if apiDocs.RepositoryURL != "" {
			fmt.Printf("repositoryUrl: %s\n", apiDocs.RepositoryURL)
		}
		if len(apiDocs.StatusCodes) > 0 {
			fmt.Println("statusCodes:")
			for _, sc := range apiDocs.StatusCodes {
				fmt.Printf("  - code: %d\n", sc.Code)
				fmt.Printf("    description: %s\n", sc.Description)
			}
		}
		if len(apiDocs.Endpoints) > 0 {
			fmt.Println("endpoints:")
			for _, ep := range apiDocs.Endpoints {
				fmt.Printf("  - path: %s\n", ep.Path)
				fmt.Printf("    fullSubject: %s\n", ep.FullSubject)
				if ep.ExampleSubject != "" {
					fmt.Printf("    exampleSubject: %s\n", ep.ExampleSubject)
				}
				if ep.Description != "" {
					fmt.Printf("    description: %s\n", ep.Description)
				}
				if len(ep.Parameters) > 0 {
					fmt.Println("    parameters:")
					for _, p := range ep.Parameters {
						fmt.Printf("      - name: %s\n", p.Name)
						if p.Description != "" {
							fmt.Printf("        description: %s\n", p.Description)
						}
						if p.Required {
							fmt.Printf("        required: true\n")
						}
						if p.Example != "" {
							fmt.Printf("        example: %s\n", p.Example)
						}
					}
				}
				if len(ep.Headers) > 0 {
					fmt.Println("    headers:")
					for _, h := range ep.Headers {
						fmt.Printf("      - name: %s\n", h.Name)
						if h.Description != "" {
							fmt.Printf("        description: %s\n", h.Description)
						}
						if h.Required {
							fmt.Printf("        required: true\n")
						}
						if h.Example != "" {
							fmt.Printf("        example: %s\n", h.Example)
						}
					}
				}
				if ep.Response != nil {
					fmt.Println("    response:")
					if ep.Response.Description != "" {
						fmt.Printf("      description: %s\n", ep.Response.Description)
					}
					if ep.Response.ContentType != "" {
						fmt.Printf("      contentType: %s\n", ep.Response.ContentType)
					}
					if ep.Response.Example != "" {
						fmt.Printf("      example: %s\n", ep.Response.Example)
					}
					if len(ep.Response.Headers) > 0 {
						fmt.Println("      headers:")
						for _, h := range ep.Response.Headers {
							fmt.Printf("        - name: %s\n", h.Name)
							if h.Description != "" {
								fmt.Printf("          description: %s\n", h.Description)
							}
							if h.Example != "" {
								fmt.Printf("          example: %s\n", h.Example)
							}
						}
					}
				}
				if ep.WildcardType != "" {
					fmt.Printf("    wildcardType: %s\n", ep.WildcardType)
				}
			}
		}
	case "table":
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Printf("Service: %s\n", apiDocs.ServiceName)
		if apiDocs.Description != "" {
			fmt.Printf("Description: %s\n", apiDocs.Description)
		}
		if apiDocs.RepositoryURL != "" {
			fmt.Printf("Repository: %s\n", apiDocs.RepositoryURL)
		}
		fmt.Println()
		fmt.Fprintf(w, "SUBJECT PATTERN\tEXAMPLE\tDESCRIPTION\n")
		fmt.Fprintf(w, "---------------\t-------\t-----------\n")

		for _, ep := range apiDocs.Endpoints {
			example := ep.ExampleSubject
			if example == "" {
				example = "-"
			}
			desc := ep.Description
			if len(desc) > 40 {
				desc = desc[:37] + "..."
			}
			fmt.Fprintf(w, "%s\t%s\t%s\n", ep.FullSubject, example, desc)

			// Show parameters and headers
			if len(ep.Parameters) > 0 {
				paramDetails := make([]string, 0, len(ep.Parameters))
				for _, p := range ep.Parameters {
					detail := p.Name
					if p.Required {
						detail += "*"
					}
					if p.Description != "" {
						detail += " (" + p.Description + ")"
					}
					paramDetails = append(paramDetails, detail)
				}
				fmt.Fprintf(w, "  Params: %s\t\t\n", strings.Join(paramDetails, ", "))
			}
			if len(ep.Headers) > 0 {
				headerDetails := make([]string, 0, len(ep.Headers))
				for _, h := range ep.Headers {
					detail := h.Name
					if h.Required {
						detail += "*"
					}
					headerDetails = append(headerDetails, detail)
				}
				fmt.Fprintf(w, "  Headers: %s\t\t\n", strings.Join(headerDetails, ", "))
			}
		}
		w.Flush()
	default:
		return fmt.Errorf("unknown format: %s (supported: table, json, yaml)", format)
	}
	return nil
}

// truncateString truncates a string to maxLen characters, adding "..." if truncated
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// wrapText wraps text to the specified width, breaking on word boundaries
func wrapText(text string, width int) []string {
	if len(text) <= width {
		return []string{text}
	}

	var lines []string
	words := strings.Fields(text)
	var currentLine string

	for _, word := range words {
		if currentLine == "" {
			currentLine = word
		} else if len(currentLine)+1+len(word) <= width {
			currentLine += " " + word
		} else {
			lines = append(lines, currentLine)
			currentLine = word
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}

	return lines
}

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
)

const (
	defaultTimeout = 2 * time.Second
	version        = "1.0.0"
)

type config struct {
	natsURL     string
	contextName string
	timeout     time.Duration
	format      string
	showVersion bool
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

	// Discover services
	responses, err := discoverServices(nc, cfg.timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error discovering services: %v\n", err)
		os.Exit(1)
	}

	// Output results
	if err := outputResults(responses, cfg.format); err != nil {
		fmt.Fprintf(os.Stderr, "Error formatting output: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	cfg := config{}

	flag.StringVar(&cfg.natsURL, "s", "", "NATS server URL (e.g., nats://localhost:4222)")
	flag.StringVar(&cfg.natsURL, "server", "", "NATS server URL (e.g., nats://localhost:4222)")
	flag.StringVar(&cfg.contextName, "context", "", "NATS context name (from nats CLI)")
	flag.DurationVar(&cfg.timeout, "timeout", defaultTimeout, "Timeout for discovery requests")
	flag.StringVar(&cfg.format, "format", "table", "Output format: table, json, yaml")
	flag.BoolVar(&cfg.showVersion, "version", false, "Show version")
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
		fmt.Fprintf(os.Stderr, "  nats-discover -s nats://localhost:4222\n")
		fmt.Fprintf(os.Stderr, "  nats-discover --context mycontext --format json\n")
		fmt.Fprintf(os.Stderr, "  nats-discover -s nats://localhost:4222 --timeout 5s --format yaml\n")
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

func loadNatsContext(name string) (*natsContext, error) {
	// NATS CLI contexts are stored in ~/.config/nats/context/<name>.json
	configDir, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}

	contextPath := filepath.Join(configDir, "nats", "context", name+".json")
	data, err := os.ReadFile(contextPath)
	if err != nil {
		return nil, fmt.Errorf("context file not found: %s", contextPath)
	}

	var ctx natsContext
	if err := json.Unmarshal(data, &ctx); err != nil {
		return nil, fmt.Errorf("invalid context file: %w", err)
	}

	return &ctx, nil
}

func loadDefaultContext() (*natsContext, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}

	// Check for default context file
	defaultPath := filepath.Join(configDir, "nats", "context.txt")
	data, err := os.ReadFile(defaultPath)
	if err != nil {
		return nil, err
	}

	contextName := strings.TrimSpace(string(data))
	if contextName == "" {
		return nil, fmt.Errorf("no default context set")
	}

	return loadNatsContext(contextName)
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

func discoverServices(nc *nats.Conn, timeout time.Duration) ([]nats_service.DiscoveryResponse, error) {
	// Use a map to deduplicate endpoints by full subject
	// Multiple instances of the same service will respond, but we only need one of each endpoint
	endpointMap := make(map[string]nats_service.EndpointDoc)
	serviceMap := make(map[string]string) // serviceName -> basePath
	var mu sync.Mutex

	// Create an inbox for receiving responses
	inbox := nc.NewInbox()

	// Subscribe to the inbox
	sub, err := nc.Subscribe(inbox, func(msg *nats.Msg) {
		var resp nats_service.DiscoveryResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			return // Skip malformed responses
		}
		mu.Lock()
		// Track service info
		serviceMap[resp.ServiceName] = resp.BasePath
		// Deduplicate endpoints by full subject
		for _, ep := range resp.Endpoints {
			if _, exists := endpointMap[ep.FullSubject]; !exists {
				endpointMap[ep.FullSubject] = ep
			}
		}
		mu.Unlock()
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

	// Group endpoints by service name (extracted from fullSubject prefix)
	serviceEndpoints := make(map[string][]nats_service.EndpointDoc)
	for _, ep := range endpointMap {
		// Extract service name from full subject (everything before the endpoint path)
		serviceName := extractServiceName(ep.FullSubject, ep.Path)
		serviceEndpoints[serviceName] = append(serviceEndpoints[serviceName], ep)
	}

	// Build response list
	responses := make([]nats_service.DiscoveryResponse, 0, len(serviceEndpoints))
	for serviceName, endpoints := range serviceEndpoints {
		// Sort endpoints within each service
		sort.Slice(endpoints, func(i, j int) bool {
			return endpoints[i].FullSubject < endpoints[j].FullSubject
		})

		basePath := serviceMap[serviceName]
		if basePath == "" {
			basePath = serviceName
		}

		responses = append(responses, nats_service.DiscoveryResponse{
			ServiceName: serviceName,
			BasePath:    basePath,
			Endpoints:   endpoints,
		})
	}

	// Sort responses by service name
	sort.Slice(responses, func(i, j int) bool {
		return responses[i].ServiceName < responses[j].ServiceName
	})

	return responses, nil
}

// extractServiceName extracts the service name from the full subject
func extractServiceName(fullSubject, path string) string {
	// fullSubject is like "orders.api.health" and path is "health"
	// service name would be "orders.api"
	if path == "" {
		return fullSubject
	}
	suffix := "." + path
	if strings.HasSuffix(fullSubject, suffix) {
		return strings.TrimSuffix(fullSubject, suffix)
	}
	return fullSubject
}

func outputResults(responses []nats_service.DiscoveryResponse, format string) error {
	if len(responses) == 0 {
		fmt.Println("No services discovered")
		return nil
	}

	switch strings.ToLower(format) {
	case "json":
		return outputJSON(responses)
	case "yaml":
		return outputYAML(responses)
	case "table":
		return outputTable(responses)
	default:
		return fmt.Errorf("unknown format: %s (supported: table, json, yaml)", format)
	}
}

func outputJSON(responses []nats_service.DiscoveryResponse) error {
	data, err := json.MarshalIndent(responses, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func outputYAML(responses []nats_service.DiscoveryResponse) error {
	// Simple YAML output without external dependency
	for i, resp := range responses {
		if i > 0 {
			fmt.Println("---")
		}
		fmt.Printf("serviceName: %s\n", resp.ServiceName)
		fmt.Printf("basePath: %s\n", resp.BasePath)
		fmt.Println("endpoints:")
		for _, ep := range resp.Endpoints {
			fmt.Printf("  - path: %s\n", ep.Path)
			fmt.Printf("    fullSubject: %s\n", ep.FullSubject)
			if ep.ExampleSubject != "" {
				fmt.Printf("    exampleSubject: %s\n", ep.ExampleSubject)
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
			if ep.Description != "" {
				fmt.Printf("    description: %s\n", ep.Description)
			}
			if ep.WildcardType != "" {
				fmt.Printf("    wildcardType: %s\n", ep.WildcardType)
			}
		}
	}
	return nil
}

func outputTable(responses []nats_service.DiscoveryResponse) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintf(w, "SERVICE\tSUBJECT PATTERN\tEXAMPLE\tDESCRIPTION\n")
	fmt.Fprintf(w, "-------\t---------------\t-------\t-----------\n")

	for _, resp := range responses {
		for _, ep := range resp.Endpoints {
			example := ep.ExampleSubject
			if example == "" {
				example = "-"
			}

			desc := ep.Description
			if len(desc) > 40 {
				desc = desc[:37] + "..."
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", resp.ServiceName, ep.FullSubject, example, desc)

			// Show parameters and headers on separate lines if present
			if len(ep.Parameters) > 0 || len(ep.Headers) > 0 {
				// Parameters
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
					fmt.Fprintf(w, "\t  Params: %s\t\t\n", strings.Join(paramDetails, ", "))
				}

				// Headers
				if len(ep.Headers) > 0 {
					headerDetails := make([]string, 0, len(ep.Headers))
					for _, h := range ep.Headers {
						detail := h.Name
						if h.Required {
							detail += "*"
						}
						headerDetails = append(headerDetails, detail)
					}
					fmt.Fprintf(w, "\t  Headers: %s\t\t\n", strings.Join(headerDetails, ", "))
				}
			}
		}
	}

	return w.Flush()
}

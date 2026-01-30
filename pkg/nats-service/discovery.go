package nats_service

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
)

// DiscoverySubject is the subject used for service discovery
const DiscoverySubject = "_discovery.all"

// DiscoveryResponse represents a service's response to a discovery request
type DiscoveryResponse struct {
	ServiceName string        `json:"serviceName"`
	BasePath    string        `json:"basePath"`
	Endpoints   []EndpointDoc `json:"endpoints"`
}

// HeaderDoc represents documentation for a single header
type HeaderDoc struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Example     string `json:"example,omitempty"`
}

// EndpointDoc represents documentation for a single endpoint
type EndpointDoc struct {
	Path         string      `json:"path"`
	FullSubject  string      `json:"fullSubject"`
	Parameters   []string    `json:"parameters,omitempty"`
	Headers      []HeaderDoc `json:"headers,omitempty"`
	Description  string      `json:"description,omitempty"`
	WildcardType string      `json:"wildcardType,omitempty"` // "single" (*) or "multi" (>)
}

// registerDiscoveryEndpoint subscribes to the discovery subject.
// If subscription fails (e.g., due to permissions), the service continues
// to operate normally with a warning logged.
func (ns *NatService) registerDiscoveryEndpoint() {
	sub, err := ns.nc.Subscribe(DiscoverySubject, ns.handleDiscoveryRequest)
	if err != nil {
		log.Printf("WARNING: Unable to subscribe to discovery subject '%s': %v. Service will operate normally.", DiscoverySubject, err)
		return
	}

	if !sub.IsValid() {
		log.Printf("WARNING: Discovery subscription to '%s' is invalid. Service will operate normally.", DiscoverySubject)
		return
	}

	log.Printf("Registered discovery endpoint on subject: %s", DiscoverySubject)
	ns.discoverySubscription = sub
}

// handleDiscoveryRequest responds to discovery requests with endpoint documentation
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

	if err := msg.Respond(jsonData); err != nil {
		log.Printf("Error responding to discovery request: %v", err)
	}
}

// buildEndpointDocs creates documentation for all registered endpoints
func (ns *NatService) buildEndpointDocs() []EndpointDoc {
	docs := make([]EndpointDoc, 0, len(ns.endPoints))

	for _, ep := range ns.endPoints {
		doc := EndpointDoc{
			Path:        ep.path,
			FullSubject: ns.basePath + "." + ep.path,
		}

		// Automatically extract parameter names from regex
		if ep.paramRegex != nil {
			groupNames := ep.paramRegex.GetGroupNames()
			if len(groupNames) > 1 {
				// Skip group 0 (full match)
				doc.Parameters = groupNames[1:]
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

		// Include headers if provided
		if len(ep.headers) > 0 {
			doc.Headers = ep.headers
		}

		docs = append(docs, doc)
	}

	return docs
}

// drainDiscoverySubscription drains the discovery subscription if it exists
func (ns *NatService) drainDiscoverySubscription() error {
	if ns.discoverySubscription != nil {
		if err := ns.discoverySubscription.Drain(); err != nil {
			log.Printf("Error draining discovery subscription: %v", err)
			return err
		}
	}
	return nil
}

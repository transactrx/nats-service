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

// ParameterDoc represents documentation for a single path parameter
type ParameterDoc struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Example     string `json:"example,omitempty"`
}

// ResponseDoc represents documentation for an endpoint's response
type ResponseDoc struct {
	Description string `json:"description,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	Example     string `json:"example,omitempty"`
}

// EndpointDoc represents documentation for a single endpoint
type EndpointDoc struct {
	Path           string         `json:"path"`
	FullSubject    string         `json:"fullSubject"`
	ExampleSubject string         `json:"exampleSubject,omitempty"`
	Parameters     []ParameterDoc `json:"parameters,omitempty"`
	Headers        []HeaderDoc    `json:"headers,omitempty"`
	Response       *ResponseDoc   `json:"response,omitempty"`
	Description    string         `json:"description,omitempty"`
	WildcardType   string         `json:"wildcardType,omitempty"` // "single" (*) or "multi" (>)
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

		// Get auto-discovered parameter names from regex
		var autoParams []string
		if ep.paramRegex != nil {
			groupNames := ep.paramRegex.GetGroupNames()
			if len(groupNames) > 1 {
				// Skip group 0 (full match)
				autoParams = groupNames[1:]
			}
		}

		// Merge auto-discovered params with user-provided documentation
		doc.Parameters = mergeParameterDocs(autoParams, ep.parameters)

		// Generate example subject by replacing :param with example values
		doc.ExampleSubject = buildExampleSubject(doc.FullSubject, doc.Parameters)

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

		// Include response documentation if provided
		if ep.response != nil {
			doc.Response = ep.response
		}

		docs = append(docs, doc)
	}

	return docs
}

// buildExampleSubject replaces :param placeholders with example values
func buildExampleSubject(fullSubject string, params []ParameterDoc) string {
	if len(params) == 0 {
		return ""
	}

	example := fullSubject
	hasExample := false

	for _, p := range params {
		placeholder := ":" + p.Name
		if strings.Contains(example, placeholder) {
			replacement := p.Example
			if replacement == "" {
				replacement = "{" + p.Name + "}"
			} else {
				hasExample = true
			}
			example = strings.Replace(example, placeholder, replacement, 1)
		}
	}

	// Only return example if at least one parameter had an example value
	// or if we have parameters (show placeholder format)
	if hasExample || len(params) > 0 {
		return example
	}
	return ""
}

// mergeParameterDocs combines auto-discovered parameter names with user-provided documentation.
// Auto-discovered names ensure all path parameters are included, while user docs provide
// descriptions, required flags, and examples.
func mergeParameterDocs(autoParams []string, userDocs []ParameterDoc) []ParameterDoc {
	if len(autoParams) == 0 {
		return nil
	}

	// Create map from user docs for quick lookup
	userDocMap := make(map[string]ParameterDoc)
	for _, pd := range userDocs {
		userDocMap[pd.Name] = pd
	}

	result := make([]ParameterDoc, 0, len(autoParams))
	for _, name := range autoParams {
		if ud, ok := userDocMap[name]; ok {
			// Use user-provided doc (ensures Name is set correctly)
			ud.Name = name
			result = append(result, ud)
		} else {
			// Create minimal doc from auto-discovered name
			result = append(result, ParameterDoc{Name: name})
		}
	}
	return result
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

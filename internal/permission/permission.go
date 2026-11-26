// Package permission implements the auto-mode classifier and the credential
// boundary. The agent never holds a real token; a proxy injects it outbound.
package permission

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// Decision is the result of an auto-mode review of one tool call.
type Decision string

const (
	Allow Decision = "allow"
	Block Decision = "block"
	Defer Decision = "defer"
)

// Classifier reviews a tool call before it executes. It stands in for the
// classifier that Claude Code's auto mode uses to stop high-risk actions.
type Classifier interface {
	Classify(toolName string, args map[string]any) Decision
}

// RulesClassifier is a deterministic, auditable classifier. Production
// deployments can replace it with a model-based classifier.
type RulesClassifier struct {
	BlockTools   map[string]bool
	DeferTools   map[string]bool
	BlockPhrases []string
}

// Classify applies tool-name rules first, then payload phrases.
func (c *RulesClassifier) Classify(toolName string, args map[string]any) Decision {
	if c.BlockTools[toolName] {
		return Block
	}
	if c.DeferTools[toolName] {
		return Defer
	}
	for _, v := range args {
		text, ok := v.(string)
		if !ok {
			continue
		}
		for _, phrase := range c.BlockPhrases {
			if strings.Contains(strings.ToLower(text), strings.ToLower(phrase)) {
				return Block
			}
		}
	}
	return Allow
}

// DefaultClassifier reviews the two privileged tools in this service.
func DefaultClassifier() *RulesClassifier {
	return &RulesClassifier{
		BlockTools:   map[string]bool{"aoa_drop_database": true},
		DeferTools:   map[string]bool{"aoa_rollback": true},
		BlockPhrases: []string{"drop table", "rm -rf /"},
	}
}

// Proxy forwards an outbound request and injects credentials. The agent
// builds a request without secrets; only the proxy can read the token.
type Proxy interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

// HeaderProxy injects an Authorization header from its own environment.
type HeaderProxy struct {
	Token  string
	Client *http.Client
}

// Do clones the request, adds the credential, and sends it.
func (p *HeaderProxy) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if p.Token == "" {
		return nil, fmt.Errorf("permission: proxy has no credential")
	}
	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}
	out := req.Clone(ctx)
	out.Header = req.Header.Clone()
	out.Header.Set("Authorization", "Bearer "+p.Token)
	return client.Do(out)
}

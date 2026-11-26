// Package agent assembles one short-lived run of the always-on agent on top
// of langchain_golang's create_react_agent, a durable checkpointer, and the
// human-in-the-loop interrupt that suspends a tool call.
package agent

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/projanvil/langchain-golang/core/language"
	coretools "github.com/projanvil/langchain-golang/core/tools"
	"github.com/projanvil/langchain-golang/langchain/agents"
	"github.com/projanvil/langchain-golang/langchain/agents/middleware"
	"github.com/projanvil/langchain-golang/langgraph/checkpoint"
	"github.com/projanvil/langchain-golang/langgraph/prebuilt"

	"github.com/projanvil/aoa/internal/memory"
	"github.com/projanvil/aoa/internal/permission"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

// runOnce enforces one outbound message per run. A retried event must not
// create a second copy in the outbox.
type runOnce struct {
	mu   sync.Mutex
	used bool
}

func (r *runOnce) take() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.used {
		return false
	}
	r.used = true
	return true
}

// Scope binds an agent run to one session. Tools close over the scope, so a
// run writes to the inbox owner's tables instead of holding state in memory.
type Scope struct {
	Store          *store.Store
	Memory         *memory.Memory
	SessionID      string
	TaskID         string
	Classifier     permission.Classifier
	Proxy          permission.Proxy
	Clock          func() time.Time
	Logger         *slog.Logger
	Metrics        *telemetry.Metrics
	TraceID        string
	RecursionLimit int
	Approved       bool
	sent           *runOnce
}

func (s *Scope) now() time.Time {
	if s.Clock != nil {
		return s.Clock()
	}
	return time.Now()
}

// Build compiles a fresh agent for one run. Compiling a graph is cheap, and
// it lets tool closures capture this run's scope without threading custom
// values through the graph runtime.
func Build(model language.ChatModel, saver checkpoint.Saver, scope Scope) (*agents.Agent, error) {
	if scope.Classifier == nil {
		scope.Classifier = permission.DefaultClassifier()
	}
	if scope.Proxy == nil {
		scope.Proxy = &permission.HeaderProxy{} // fails closed: no credential
	}
	if scope.Logger == nil {
		scope.Logger = slog.Default()
	}
	if scope.Metrics == nil {
		scope.Metrics = &telemetry.Metrics{}
	}
	if scope.sent == nil {
		scope.sent = &runOnce{}
	}
	if scope.RecursionLimit <= 0 {
		scope.RecursionLimit = 64
	}

	bundle, err := scope.Memory.ContextBundle()
	if err != nil {
		return nil, fmt.Errorf("agent: load memory bundle: %w", err)
	}
	system := bundle + fmt.Sprintf(`
## Run context
session_id: %s
task_id: %s
now: %s

A run has exactly three endings: call aoa_send to queue one message,
call nothing, or trigger a suspended tool call that waits for approval.
Do not stream intermediate reasoning to the user. Use aoa_set_timer to
schedule the next check; you do not hold a timer yourself.
Store only facts that cannot be queried elsewhere. Store pointers for the rest.
`, scope.SessionID, scope.TaskID, scope.now().UTC().Format(time.RFC3339))

	tools, err := buildTools(scope)
	if err != nil {
		return nil, err
	}

	hitl := middleware.NewInterruptHumanInTheLoopMiddleware(
		map[string]middleware.InterruptConfig{
			"aoa_rollback": {
				AllowedDecisions: []middleware.DecisionType{
					middleware.DecisionApprove,
					middleware.DecisionReject,
				},
				Description: "Production change requires human approval.",
			},
		},
	)

	return prebuilt.CreateReactAgent(model, tools,
		agents.WithAgentCheckpointer(saver),
		agents.WithAgentSystemPrompt(system),
		agents.WithAgentMiddleware(hitl),
		agents.WithAgentRecursionLimit(scope.RecursionLimit),
	)
}

// DescribeToolCall makes a stable text label for a pending approval card.
func DescribeToolCall(name string, args map[string]any) string {
	return fmt.Sprintf("%s %v", name, args)
}

var _ coretools.Tool = (*guardedTool)(nil)

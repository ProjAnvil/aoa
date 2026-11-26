package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/projanvil/langchain-golang/core/language"
	"github.com/projanvil/langchain-golang/core/messages"
	"github.com/projanvil/langchain-golang/core/runnables"
	"github.com/projanvil/langchain-golang/core/schema"
	coretools "github.com/projanvil/langchain-golang/core/tools"
)

// ScriptedModel is an offline ChatModel double. It keeps the service runnable
// and testable without an API key. BindTools returns the receiver so the
// response cursor survives the agent loop's per-iteration re-binds.
type ScriptedModel struct {
	mu        sync.Mutex
	Responses []messages.Message
	idx       int
	Invokes   int
	// Fallback is returned after the script is exhausted. The demo model
	// uses it to stay quiet on later runs instead of failing every retry.
	Fallback *messages.Message
}

var _ language.ChatModel = (*ScriptedModel)(nil)

// Invoke returns the next scripted response.
func (m *ScriptedModel) Invoke(_ context.Context, _ []messages.Message, _ ...runnables.Option) (messages.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Invokes++
	if m.idx >= len(m.Responses) {
		if m.Fallback != nil {
			return *m.Fallback, nil
		}
		return messages.Message{}, fmt.Errorf("scriptedModel: no response for call %d", m.idx+1)
	}
	resp := m.Responses[m.idx]
	m.idx++
	return resp, nil
}

// Batch invokes the model once per input.
func (m *ScriptedModel) Batch(ctx context.Context, inputs [][]messages.Message, opts ...runnables.Option) ([]messages.Message, error) {
	out := make([]messages.Message, len(inputs))
	for i, in := range inputs {
		resp, err := m.Invoke(ctx, in, opts...)
		if err != nil {
			return nil, err
		}
		out[i] = resp
	}
	return out, nil
}

// Stream wraps the next scripted response in a slice stream.
func (m *ScriptedModel) Stream(ctx context.Context, input []messages.Message, opts ...runnables.Option) (runnables.Stream[messages.Message], error) {
	resp, err := m.Invoke(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	return runnables.NewSliceStream([]messages.Message{resp}), nil
}

// InputSchema describes the chat input shape.
func (m *ScriptedModel) InputSchema() schema.Schema { return schema.Object(map[string]schema.Schema{}) }

// OutputSchema describes the chat output shape.
func (m *ScriptedModel) OutputSchema() schema.Schema {
	return schema.Object(map[string]schema.Schema{})
}

// BindTools keeps the same receiver so the script cursor advances.
func (m *ScriptedModel) BindTools(_ []coretools.Tool) (language.ChatModel, error) { return m, nil }

// Capabilities declares tool calling support.
func (m *ScriptedModel) Capabilities() language.ChatModelCapabilities {
	return language.ChatModelCapabilities{ToolCalling: true, Streaming: true}
}

// NewDemoModel returns a model that queues one message and then stops. It
// demonstrates that a run ends with exactly one outbox row.
func NewDemoModel() *ScriptedModel {
	quiet := messages.AI("No action needed.")
	return &ScriptedModel{
		Responses: []messages.Message{
			{
				Role: messages.RoleAI,
				ToolCalls: []messages.ToolCall{{
					ID:   "call_send_1",
					Name: "aoa_send",
					Args: map[string]any{"message": "Event received and handled."},
				}},
			},
			quiet,
		},
		Fallback: &quiet,
	}
}

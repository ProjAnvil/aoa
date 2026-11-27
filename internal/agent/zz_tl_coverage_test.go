package agent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/projanvil/langchain-golang/core/messages"
	coretools "github.com/projanvil/langchain-golang/core/tools"
	"github.com/projanvil/langchain-golang/langgraph/checkpoint"

	"github.com/projanvil/aoa/internal/memory"
	"github.com/projanvil/aoa/internal/permission"
	"github.com/projanvil/aoa/internal/store"
)

type tlProxy struct {
	calls int
	err   error
	code  int
}

func (p *tlProxy) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}
	return &http.Response{StatusCode: p.code, Body: io.NopCloser(strings.NewReader("ok"))}, nil
}

func tlScope(t *testing.T, approved bool) (Scope, *store.Store, *tlProxy) {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mem, err := memory.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	proxy := &tlProxy{code: 200}
	return Scope{
		Store:      st,
		Memory:     mem,
		SessionID:  "s1",
		TaskID:     "t1",
		Classifier: permission.DefaultClassifier(),
		Proxy:      proxy,
		Approved:   approved,
		sent:       &runOnce{},
	}, st, proxy
}

func tlTool(t *testing.T, tools []coretools.Tool, name string) coretools.Tool {
	t.Helper()
	for _, tool := range tools {
		if tool.Name() == name {
			return tool
		}
	}
	t.Fatalf("tool %q missing", name)
	return nil
}

func TestTLScriptedModelAllMethods(t *testing.T) {
	ctx := context.Background()
	first := messages.AI("first")
	second := messages.AI("second")
	m := &ScriptedModel{Responses: []messages.Message{first, second}}
	if got, err := m.Invoke(ctx, nil); err != nil || messages.Text(got) != "first" {
		t.Fatalf("Invoke = %v, %v", got, err)
	}
	if got, err := m.Invoke(ctx, nil); err != nil || messages.Text(got) != "second" {
		t.Fatalf("second Invoke = %v, %v", got, err)
	}
	if _, err := m.Invoke(ctx, nil); err == nil {
		t.Fatal("exhausted script must fail")
	}
	batchModel := &ScriptedModel{Responses: []messages.Message{first, second}}
	if got, err := batchModel.Batch(ctx, [][]messages.Message{{}, {}}); err != nil || len(got) != 2 ||
		messages.Text(got[0]) != "first" {
		t.Fatalf("Batch = %v, %v", got, err)
	}
	if _, err := (&ScriptedModel{}).Batch(ctx, [][]messages.Message{{}}); err == nil {
		t.Fatal("Batch with an exhausted script must fail")
	}
	if _, err := m.Stream(ctx, nil); err == nil {
		t.Fatal("Stream after exhaustion must fail")
	}

	fallback := messages.AI("fallback")
	fm := &ScriptedModel{Fallback: &fallback}
	if got, err := fm.Invoke(ctx, nil); err != nil || messages.Text(got) != "fallback" {
		t.Fatalf("fallback Invoke = %v, %v", got, err)
	}
	if _, err := fm.Stream(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := fm.Batch(ctx, [][]messages.Message{{}, {}}); err != nil {
		t.Fatal(err)
	}
	_ = fm.InputSchema()
	_ = fm.OutputSchema()
	bound, err := fm.BindTools(nil)
	if err != nil || bound != fm {
		t.Fatal("BindTools must return the same receiver")
	}
	if !fm.Capabilities().ToolCalling || !fm.Capabilities().Streaming {
		t.Fatal("Capabilities must declare tool calling and streaming")
	}
}

func TestTLRunOnceDescribeAndStringArg(t *testing.T) {
	r := &runOnce{}
	if !r.take() || r.take() {
		t.Fatal("runOnce.take failed")
	}
	if got := DescribeToolCall("aoa_send", map[string]any{"message": "x"}); !strings.Contains(got, "aoa_send") {
		t.Fatalf("DescribeToolCall = %q", got)
	}
	if stringArg(nil, "missing") != "" {
		t.Fatal("missing key must return an empty string")
	}
	if stringArg(map[string]any{"n": nil}, "n") != "" {
		t.Fatal("nil value must return an empty string")
	}
	if got := stringArg(map[string]any{"n": 42}, "n"); got != "42" {
		t.Fatalf("non-string value = %q", got)
	}
	// A value that cannot be marshaled falls back to fmt.Sprint.
	if got := stringArg(map[string]any{"n": func() {}}, "n"); got == "" {
		t.Fatal("marshal failure must fall back to a printable value")
	}
}

func TestTLAllToolClosures(t *testing.T) {
	ctx := context.Background()
	scope, st, proxy := tlScope(t, true)
	tools, err := buildTools(scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 8 {
		t.Fatalf("tools = %d, want 8", len(tools))
	}
	g := tools[0].(*guardedTool)
	if g.Name() == "" || g.Description() == "" {
		t.Fatal("guardedTool accessors failed")
	}
	_ = g.ArgsSchema()

	send := tlTool(t, tools, "aoa_send")
	if _, err := send.Invoke(ctx, map[string]any{"message": ""}); err == nil {
		t.Fatal("empty message must fail")
	}
	if _, err := g.inner.Invoke(ctx, map[string]any{
		"message": strings.Repeat("x", maxToolInput+1),
	}); err == nil {
		t.Fatal("oversized message must fail")
	}
	if _, err := send.Invoke(ctx, map[string]any{
		"message": strings.Repeat("x", maxToolInput+1),
	}); err == nil {
		t.Fatal("guarded oversized input must fail")
	}
	if res, err := send.Invoke(ctx, map[string]any{"message": "hello"}); err != nil || res.Content != "queued" {
		t.Fatalf("send = %v, %v", res, err)
	}
	if res, err := send.Invoke(ctx, map[string]any{"message": "again"}); err != nil || !strings.Contains(res.Content, "already") {
		t.Fatalf("second send = %v, %v", res, err)
	}
	outbox, err := st.PendingOutbox(ctx, 10)
	if err != nil || len(outbox) != 1 {
		t.Fatalf("outbox = %v, %v", outbox, err)
	}

	timer := tlTool(t, tools, "aoa_set_timer")
	for _, in := range []map[string]any{
		{"delay_seconds": "abc", "payload": "x"},
		{"delay_seconds": "60", "payload": ""},
		{"delay_seconds": "60", "payload": "x", "periodic": "true", "interval_seconds": "bad"},
	} {
		if _, err := timer.Invoke(ctx, in); err == nil {
			t.Fatalf("invalid timer input %v must fail", in)
		}
	}
	if _, err := timer.Invoke(ctx, map[string]any{
		"delay_seconds": "60", "payload": "check", "periodic": "true",
	}); err != nil {
		t.Fatalf("periodic timer with default interval: %v", err)
	}
	if res, err := timer.Invoke(ctx, map[string]any{"delay_seconds": "60", "payload": "once"}); err != nil ||
		!strings.Contains(res.Content, "timer") {
		t.Fatalf("follow-up timer = %v, %v", res, err)
	}

	read := tlTool(t, tools, "aoa_memory_read")
	if _, err := read.Invoke(ctx, map[string]any{"name": "../bad"}); err == nil {
		t.Fatal("unsafe memory read must fail")
	}
	if res, err := read.Invoke(ctx, map[string]any{"name": memory.IndexFile}); err != nil || res.Content == "" {
		t.Fatalf("memory read = %v, %v", res, err)
	}
	write := tlTool(t, tools, "aoa_memory_write")
	if _, err := write.Invoke(ctx, map[string]any{
		"name": memory.InstructionsFile, "content": "x",
	}); err == nil {
		t.Fatal("instructions must be immutable")
	}
	if _, err := write.Invoke(ctx, map[string]any{"name": "../bad", "content": "x"}); err == nil {
		t.Fatal("unsafe memory write must fail")
	}
	if _, err := write.Invoke(ctx, map[string]any{"name": "progress/a.md", "content": "note"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tlTool(t, tools, "aoa_progress").Invoke(ctx, map[string]any{"content": "step 1"}); err != nil {
		t.Fatal(err)
	}

	external := tlTool(t, tools, "aoa_external_request")
	if _, err := external.Invoke(ctx, map[string]any{"url": "https://x", "method": "DELETE"}); err == nil {
		t.Fatal("DELETE must fail")
	}
	if _, err := external.Invoke(ctx, map[string]any{"url": "://bad"}); err == nil {
		t.Fatal("invalid URL must fail")
	}
	proxy.err = errors.New("proxy refused")
	if _, err := external.Invoke(ctx, map[string]any{"url": "https://api.example.com/x"}); err == nil {
		t.Fatal("proxy error must surface")
	}
	proxy.err = nil
	if res, err := external.Invoke(ctx, map[string]any{
		"url": "https://api.example.com/x", "method": "head",
	}); err != nil || res.Content != "status 200" {
		t.Fatalf("external request = %v, %v", res, err)
	}

	rollback := tlTool(t, tools, "aoa_rollback")
	if res, err := rollback.Invoke(ctx, map[string]any{"target": "cfg-1"}); err != nil ||
		!strings.Contains(res.Content, "rollback status 200") {
		t.Fatalf("rollback = %v, %v", res, err)
	}
	proxy.err = errors.New("blocked")
	if _, err := rollback.Invoke(ctx, map[string]any{"target": "cfg-2"}); err == nil {
		t.Fatal("rollback proxy error must surface")
	}
	proxy.err = nil

	if res, err := tlTool(t, tools, "aoa_drop_database").Invoke(ctx, map[string]any{"name": "x"}); err != nil ||
		!strings.Contains(res.Content, "blocked") {
		t.Fatalf("drop database = %v, %v", res, err)
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	closedScope, closedStore, _ := tlScope(t, true)
	if err := closedStore.Close(); err != nil {
		t.Fatal(err)
	}
	closedTools, err := buildTools(closedScope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tlTool(t, closedTools, "aoa_send").Invoke(ctx, map[string]any{"message": "x"}); err == nil {
		t.Fatal("send on closed store must fail")
	}
	if _, err := tlTool(t, closedTools, "aoa_set_timer").Invoke(ctx, map[string]any{
		"delay_seconds": "60", "payload": "x",
	}); err == nil {
		t.Fatal("timer on closed store must fail")
	}
}

func TestTLBuildWithBrokenMemoryBundle(t *testing.T) {
	root := t.TempDir()
	mem, err := memory.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, memory.IndexFile)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, memory.IndexFile), 0o755); err != nil {
		t.Fatal(err)
	}
	scope, _, _ := tlScope(t, false)
	scope.Memory = mem
	if _, err := Build(NewDemoModel(), checkpoint.NewMemorySaver(), scope); err == nil {
		t.Fatal("Build with a broken memory bundle must fail")
	}
	if got := scope.now(); got.IsZero() {
		t.Fatal("Scope.now must return a real time")
	}
}

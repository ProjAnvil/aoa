package agent

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/projanvil/langchain-golang/core/messages"
	"github.com/projanvil/langchain-golang/core/schema"
	"github.com/projanvil/langchain-golang/langgraph/checkpoint"

	"github.com/projanvil/aoa/internal/memory"
	"github.com/projanvil/aoa/internal/permission"
)

// errProxy always refuses an outbound request, so the external-request and
// rollback tools can exercise their proxy-failure branches.
type errProxy struct{}

func (errProxy) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, errors.New("proxy unavailable")
}

// brokenMemory returns a Memory whose root has been replaced by a regular
// file. Every read, write, append, and ContextBundle call then fails, which
// lets tests reach the tool and Build error branches without a database.
func brokenMemory(t *testing.T) *memory.Memory {
	t.Helper()
	root := t.TempDir()
	mem, err := memory.Open(root)
	if err != nil {
		t.Fatalf("memory.Open: %v", err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("remove memory root: %v", err)
	}
	if err := os.WriteFile(root, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("replace memory root: %v", err)
	}
	return mem
}

func TestScriptedModelInvokeAndFallback(t *testing.T) {
	ctx := context.Background()
	quiet := messages.AI("quiet")
	m := &ScriptedModel{
		Responses: []messages.Message{messages.AI("first"), messages.AI("second")},
		Fallback:  &quiet,
	}
	first, err := m.Invoke(ctx, nil)
	if err != nil || first.Content != "first" {
		t.Fatalf("first invoke = %+v, %v", first, err)
	}
	second, err := m.Invoke(ctx, nil)
	if err != nil || second.Content != "second" {
		t.Fatalf("second invoke = %+v, %v", second, err)
	}
	third, err := m.Invoke(ctx, nil)
	if err != nil || third.Content != "quiet" {
		t.Fatalf("fallback invoke = %+v, %v", third, err)
	}
	if m.Invokes != 3 {
		t.Fatalf("Invokes = %d, want 3", m.Invokes)
	}

	empty := &ScriptedModel{}
	if _, err := empty.Invoke(ctx, nil); err == nil {
		t.Fatal("Invoke without responses or fallback must fail")
	}
}

func TestScriptedModelBatchAndStream(t *testing.T) {
	ctx := context.Background()
	m := &ScriptedModel{Responses: []messages.Message{
		messages.AI("a"), messages.AI("b"), messages.AI("c"),
	}}
	out, err := m.Batch(ctx, [][]messages.Message{nil, nil})
	if err != nil || len(out) != 2 || out[0].Content != "a" || out[1].Content != "b" {
		t.Fatalf("Batch = %+v, %v", out, err)
	}

	empty := &ScriptedModel{}
	if _, err := empty.Batch(ctx, [][]messages.Message{nil}); err == nil {
		t.Fatal("Batch must propagate the Invoke error")
	}

	stream, err := m.Stream(ctx, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	chunk, ok, err := stream.Next(ctx)
	if err != nil || !ok || chunk.Content != "c" {
		t.Fatalf("Stream chunk = %+v, %v, %v", chunk, ok, err)
	}
	if _, ok, err := stream.Next(ctx); err != nil || ok {
		t.Fatalf("exhausted Stream = %v, %v", ok, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Stream.Close: %v", err)
	}

	if _, err := empty.Stream(ctx, nil); err == nil {
		t.Fatal("Stream must propagate the Invoke error")
	}
}

func TestScriptedModelMetadata(t *testing.T) {
	m := &ScriptedModel{}
	if got := m.InputSchema(); got["type"] != "object" {
		t.Fatalf("InputSchema = %v", got)
	}
	if got := m.OutputSchema(); got["type"] != "object" {
		t.Fatalf("OutputSchema = %v", got)
	}
	var _ schema.Schema = m.InputSchema()
	bound, err := m.BindTools(nil)
	if err != nil || bound != m {
		t.Fatalf("BindTools = %v, %v; want the same receiver", bound, err)
	}
	caps := m.Capabilities()
	if !caps.ToolCalling || !caps.Streaming {
		t.Fatalf("Capabilities = %+v; want tool calling and streaming", caps)
	}
}

func TestDescribeToolCall(t *testing.T) {
	got := DescribeToolCall("aoa_send", map[string]any{"message": "hi"})
	if !strings.HasPrefix(got, "aoa_send ") || !strings.Contains(got, "hi") {
		t.Fatalf("DescribeToolCall = %q", got)
	}
}

func TestBuildAppliesDefaults(t *testing.T) {
	scope, _, _ := testScope(t, false)
	scope.Classifier = nil
	scope.Proxy = nil
	scope.sent = nil
	scope.RecursionLimit = 0
	a, err := Build(NewDemoModel(), checkpoint.NewMemorySaver(), scope)
	if err != nil {
		t.Fatalf("Build with defaulted scope: %v", err)
	}
	if a == nil || a.Graph == nil {
		t.Fatal("Build returned no compiled graph")
	}
	if scope.Classifier != nil || scope.Proxy != nil || scope.sent != nil || scope.RecursionLimit != 0 {
		t.Fatal("Build must not mutate the caller's Scope value")
	}
}

func TestBuildFailsOnBadMemoryBundle(t *testing.T) {
	scope, _, _ := testScope(t, false)
	scope.Memory = brokenMemory(t)
	if _, err := Build(NewDemoModel(), checkpoint.NewMemorySaver(), scope); err == nil ||
		!strings.Contains(err.Error(), "load memory bundle") {
		t.Fatalf("Build error = %v; want a memory-bundle failure", err)
	}
}

func TestGuardedToolMetadataAndOversizedInput(t *testing.T) {
	scope, _, _ := testScope(t, false)
	tools, err := buildTools(scope)
	if err != nil {
		t.Fatal(err)
	}
	send := findTool(t, tools, "aoa_send")
	if send.Name() != "aoa_send" || send.Description() == "" {
		t.Fatalf("guarded tool metadata: name=%q desc=%q", send.Name(), send.Description())
	}
	if send.ArgsSchema()["type"] != "object" {
		t.Fatalf("ArgsSchema = %v", send.ArgsSchema())
	}

	memRead := findTool(t, tools, "aoa_memory_read")
	if _, err := memRead.Invoke(context.Background(), map[string]any{
		"name": strings.Repeat("x", maxToolInput+1),
	}); err == nil {
		t.Fatal("oversized tool input must be rejected by the guard")
	}
}

func TestStringArgNonStringValues(t *testing.T) {
	if got := stringArg(map[string]any{"k": 42}, "k"); got != "42" {
		t.Fatalf("stringArg(number) = %q, want 42", got)
	}
	if got := stringArg(map[string]any{"k": map[string]any{"a": true}}, "k"); got != `{"a":true}` {
		t.Fatalf("stringArg(map) = %q", got)
	}
	if got := stringArg(map[string]any{"k": make(chan int)}, "k"); got == "" {
		t.Fatalf("stringArg(unmarshalable) = %q, want a fmt.Sprint fallback", got)
	}
	if got := stringArg(map[string]any{}, "missing"); got != "" {
		t.Fatalf("stringArg(missing) = %q, want empty", got)
	}
}

func TestSendValidationAndStoreErrors(t *testing.T) {
	scope, _, _ := testScope(t, false)
	tools, err := buildTools(scope)
	if err != nil {
		t.Fatal(err)
	}
	send := findTool(t, tools, "aoa_send")
	if _, err := send.Invoke(context.Background(), map[string]any{"message": ""}); err == nil {
		t.Fatal("empty message must be rejected")
	}
	if _, err := send.Invoke(context.Background(), map[string]any{
		"message": strings.Repeat("x", maxToolInput+1),
	}); err == nil {
		t.Fatal("oversized message must be rejected")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := send.Invoke(cancelled, map[string]any{"message": "hello"}); err == nil {
		t.Fatal("EnqueueOutbox failure must be surfaced")
	}

	broken, _, _ := testScope(t, false)
	broken.Memory = brokenMemory(t)
	brokenTools, err := buildTools(broken)
	if err != nil {
		t.Fatal(err)
	}
	brokenSend := findTool(t, brokenTools, "aoa_send")
	if _, err := brokenSend.Invoke(context.Background(), map[string]any{"message": "hello"}); err == nil {
		t.Fatal("Memory.Append failure must be surfaced")
	}
}

func TestSetTimerPeriodicBranches(t *testing.T) {
	scope, _, _ := testScope(t, false)
	tools, err := buildTools(scope)
	if err != nil {
		t.Fatal(err)
	}
	timer := findTool(t, tools, "aoa_set_timer")

	res, err := timer.Invoke(context.Background(), map[string]any{
		"delay_seconds": "60", "payload": "check", "periodic": "true", "interval_seconds": "30",
	})
	if err != nil || !strings.Contains(res.Content, "periodic") {
		t.Fatalf("explicit periodic interval = %+v, %v", res, err)
	}
	if _, err := timer.Invoke(context.Background(), map[string]any{
		"delay_seconds": "60", "payload": "check2", "periodic": "true",
	}); err != nil {
		t.Fatalf("periodic interval defaulting to delay: %v", err)
	}
	if _, err := timer.Invoke(context.Background(), map[string]any{
		"delay_seconds": "60", "payload": "check3", "periodic": "true", "interval_seconds": "abc",
	}); err == nil {
		t.Fatal("invalid interval_seconds must be rejected")
	}
	if _, err := timer.Invoke(context.Background(), map[string]any{
		"delay_seconds": "60", "payload": "check4", "periodic": "true", "interval_seconds": "-1",
	}); err == nil {
		t.Fatal("non-positive interval_seconds must be rejected")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := timer.Invoke(cancelled, map[string]any{
		"delay_seconds": "60", "payload": "check5",
	}); err == nil {
		t.Fatal("AddTimer failure must be surfaced")
	}
}

func TestMemoryReadWriteAndProgressTools(t *testing.T) {
	scope, _, _ := testScope(t, false)
	tools, err := buildTools(scope)
	if err != nil {
		t.Fatal(err)
	}
	read := findTool(t, tools, "aoa_memory_read")
	write := findTool(t, tools, "aoa_memory_write")
	progress := findTool(t, tools, "aoa_progress")

	if _, err := write.Invoke(context.Background(), map[string]any{
		"name": "notes.md", "content": "a fact",
	}); err != nil {
		t.Fatalf("memory write: %v", err)
	}
	body, err := read.Invoke(context.Background(), map[string]any{"name": "notes.md"})
	if err != nil || body.Content != "a fact" {
		t.Fatalf("memory read = %+v, %v", body, err)
	}
	res, err := progress.Invoke(context.Background(), map[string]any{"content": "step one"})
	if err != nil || res.Content == "" {
		t.Fatalf("progress = %+v, %v", res, err)
	}

	broken, _, _ := testScope(t, false)
	broken.Memory = brokenMemory(t)
	brokenTools, err := buildTools(broken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := findTool(t, brokenTools, "aoa_memory_read").Invoke(context.Background(),
		map[string]any{"name": "notes.md"}); err == nil {
		t.Fatal("Memory.Read failure must be surfaced")
	}
	if _, err := findTool(t, brokenTools, "aoa_memory_write").Invoke(context.Background(),
		map[string]any{"name": "notes.md", "content": "x"}); err == nil {
		t.Fatal("Memory.Write failure must be surfaced")
	}
	if _, err := findTool(t, brokenTools, "aoa_progress").Invoke(context.Background(),
		map[string]any{"content": "x"}); err == nil {
		t.Fatal("Memory.Append failure must be surfaced")
	}
}

func TestExternalRequestAndRollbackTools(t *testing.T) {
	scope, _, _ := testScope(t, false)
	tools, err := buildTools(scope)
	if err != nil {
		t.Fatal(err)
	}
	request := findTool(t, tools, "aoa_external_request")

	res, err := request.Invoke(context.Background(), map[string]any{"url": "https://example.com"})
	if err != nil || res.Content != "status 200" {
		t.Fatalf("default GET request = %+v, %v", res, err)
	}
	if _, err := request.Invoke(context.Background(), map[string]any{"url": "://bad"}); err == nil {
		t.Fatal("invalid URL must be rejected")
	}
	if _, err := request.Invoke(context.Background(), map[string]any{
		"url": "https://example.com", "method": "PUT",
	}); err == nil {
		t.Fatal("disallowed method must be rejected")
	}

	failing, _, _ := testScope(t, false)
	failing.Proxy = errProxy{}
	failingTools, err := buildTools(failing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := findTool(t, failingTools, "aoa_external_request").Invoke(context.Background(),
		map[string]any{"url": "https://example.com"}); err == nil {
		t.Fatal("proxy failure must be surfaced by external request")
	}

	approved, _, _ := testScope(t, true)
	approved.Proxy = errProxy{}
	approvedTools, err := buildTools(approved)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := findTool(t, approvedTools, "aoa_rollback").Invoke(context.Background(),
		map[string]any{"target": "cfg"}); err == nil {
		t.Fatal("proxy failure must be surfaced by rollback")
	}
}

func TestDropDatabaseRunsOnlyWhenClassifierAllows(t *testing.T) {
	scope, _, _ := testScope(t, false)
	scope.Classifier = &permission.RulesClassifier{}
	tools, err := buildTools(scope)
	if err != nil {
		t.Fatal(err)
	}
	res, err := findTool(t, tools, "aoa_drop_database").Invoke(context.Background(),
		map[string]any{"name": "prod"})
	if err != nil || res.Content != "should never run" {
		t.Fatalf("drop tool = %+v, %v", res, err)
	}
}

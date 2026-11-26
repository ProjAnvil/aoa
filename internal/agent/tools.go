package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/projanvil/langchain-golang/core/schema"
	coretools "github.com/projanvil/langchain-golang/core/tools"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/memory"
	"github.com/projanvil/aoa/internal/permission"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

const maxToolInput = 16 << 10

// guardedTool applies the auto-mode classifier before a tool runs. Block
// returns a refusal message so the model can choose a safer path. Defer is
// handled by the HITL middleware before the tool node is reached, so an
// approved call must execute without a second refusal here.
type guardedTool struct {
	inner        coretools.Tool
	classifier   permission.Classifier
	deferAllowed bool
}

func (g *guardedTool) Name() string              { return g.inner.Name() }
func (g *guardedTool) Description() string       { return g.inner.Description() }
func (g *guardedTool) ArgsSchema() schema.Schema { return g.inner.ArgsSchema() }

func (g *guardedTool) Invoke(ctx context.Context, input map[string]any) (coretools.Result, error) {
	switch g.classifier.Classify(g.inner.Name(), input) {
	case permission.Block:
		return coretools.Result{Content: "auto mode blocked this call; choose a safer path"}, nil
	case permission.Defer:
		if !g.deferAllowed {
			return coretools.Result{}, fmt.Errorf("tool %q requires an approved suspension", g.inner.Name())
		}
	}
	for _, v := range input {
		if s, ok := v.(string); ok && len(s) > maxToolInput {
			return coretools.Result{}, fmt.Errorf("tool input exceeds %d bytes", maxToolInput)
		}
	}
	return g.inner.Invoke(ctx, input)
}

func stringArg(input map[string]any, key string) string {
	v, ok := input[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// buildTools returns the agent's entire action surface. Every side effect is
// idempotent when keyed on its arguments, because an event can replay after
// a crash between the effect and the inbox Ack.
func buildTools(scope Scope) ([]coretools.Tool, error) {
	if scope.Logger == nil {
		scope.Logger = slog.Default()
	}
	var tools []coretools.Tool

	send, err := coretools.NewFunc(
		"aoa_send",
		"Queue one message for the user. Call at most once per run. Call nothing when there is nothing worth reporting.",
		schema.Object(map[string]schema.Schema{
			"message": schema.String("the message text"),
		}, "message"),
		func(ctx context.Context, input map[string]any) (coretools.Result, error) {
			msg := stringArg(input, "message")
			if msg == "" || len(msg) > maxToolInput {
				return coretools.Result{}, fmt.Errorf("aoa_send: message must be 1..%d bytes", maxToolInput)
			}
			if !scope.sent.take() {
				return coretools.Result{Content: "this run already queued a message"}, nil
			}
			if err := scope.Store.EnqueueOutbox(ctx, scope.SessionID, aoa.OutboxMessage, msg); err != nil {
				return coretools.Result{}, err
			}
			if err := scope.Memory.Append(memory.ReportedFile, "- "+msg); err != nil {
				return coretools.Result{}, err
			}
			scope.Logger.Info("outbox queued",
				"session_id", scope.SessionID,
				"trace_id", scope.TraceID,
				"bytes", len(msg),
			)
			return coretools.Result{Content: "queued"}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	tools = append(tools, send)

	setTimer, err := coretools.NewFunc(
		"aoa_set_timer",
		"Schedule the next wake-up. Use follow-up for a one-shot check and periodic for a recurring job. The scheduler, not the model, fires it.",
		schema.Object(map[string]schema.Schema{
			"delay_seconds":    schema.String("seconds from now as a number"),
			"payload":          schema.String("what to check when the timer fires"),
			"periodic":         schema.String("true or false"),
			"interval_seconds": schema.String("repeat interval for periodic timers"),
		}, "delay_seconds", "payload"),
		func(ctx context.Context, input map[string]any) (coretools.Result, error) {
			delay, err := strconv.ParseFloat(strings.TrimSpace(stringArg(input, "delay_seconds")), 64)
			if err != nil || delay <= 0 || delay > 30*24*3600 {
				return coretools.Result{}, fmt.Errorf("aoa_set_timer: delay_seconds must be in (0, 2592000]")
			}
			payload := stringArg(input, "payload")
			if payload == "" || len(payload) > 4096 {
				return coretools.Result{}, fmt.Errorf("aoa_set_timer: payload must be 1..4096 bytes")
			}
			kind := aoa.TimerFollowUp
			interval := 0
			if strings.EqualFold(stringArg(input, "periodic"), "true") {
				kind = aoa.TimerPeriodic
				raw := strings.TrimSpace(stringArg(input, "interval_seconds"))
				if raw == "" {
					raw = stringArg(input, "delay_seconds")
				}
				parsed, err := strconv.ParseFloat(raw, 64)
				if err != nil || parsed <= 0 {
					return coretools.Result{}, fmt.Errorf("aoa_set_timer: invalid interval_seconds")
				}
				interval = int(parsed)
			}
			timer := store.Timer{
				ID:          fmt.Sprintf("%s:%s:%s", scope.SessionID, scope.TaskID, stringArg(input, "payload")),
				SessionID:   scope.SessionID,
				Kind:        kind,
				DueAt:       scope.now().Add(time.Duration(delay * float64(time.Second))),
				IntervalSec: interval,
				Payload:     stringArg(input, "payload"),
			}
			if err := scope.Store.AddTimer(ctx, timer); err != nil {
				return coretools.Result{}, err
			}
			return coretools.Result{Content: fmt.Sprintf("timer %s at %s", kind, timer.DueAt.UTC().Format(time.RFC3339))}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	tools = append(tools, setTimer)

	memRead, err := coretools.NewFunc(
		"aoa_memory_read",
		"Read one memory file by name, for example instructions.md or progress/alert-4821.md.",
		schema.Object(map[string]schema.Schema{
			"name": schema.String("memory file name"),
		}, "name"),
		func(_ context.Context, input map[string]any) (coretools.Result, error) {
			body, err := scope.Memory.Read(stringArg(input, "name"))
			if err != nil {
				return coretools.Result{}, err
			}
			return coretools.Result{Content: body}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	tools = append(tools, memRead)

	memWrite, err := coretools.NewFunc(
		"aoa_memory_write",
		"Write one memory file. Store only facts that cannot be looked up elsewhere; keep pointers for the rest.",
		schema.Object(map[string]schema.Schema{
			"name":    schema.String("memory file name"),
			"content": schema.String("file content"),
		}, "name", "content"),
		func(_ context.Context, input map[string]any) (coretools.Result, error) {
			name := stringArg(input, "name")
			if name == memory.InstructionsFile {
				return coretools.Result{}, fmt.Errorf("aoa_memory_write: instructions are operator-owned")
			}
			if err := scope.Memory.Write(name, stringArg(input, "content")); err != nil {
				return coretools.Result{}, err
			}
			return coretools.Result{Content: "saved"}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	tools = append(tools, memWrite)

	progress, err := coretools.NewFunc(
		"aoa_progress",
		"Record task progress in a per-task memory file so the next run can continue.",
		schema.Object(map[string]schema.Schema{
			"content": schema.String("progress note"),
		}, "content"),
		func(_ context.Context, input map[string]any) (coretools.Result, error) {
			path := memory.ProgressPath(scope.TaskID)
			if err := scope.Memory.Append(path, stringArg(input, "content")); err != nil {
				return coretools.Result{}, err
			}
			return coretools.Result{Content: path}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	tools = append(tools, progress)

	request, err := coretools.NewFunc(
		"aoa_external_request",
		"Send an outbound HTTP request through the credential proxy. The agent never sees the token.",
		schema.Object(map[string]schema.Schema{
			"url":    schema.String("absolute target URL"),
			"method": schema.String("HTTP method, defaults to GET"),
		}, "url"),
		func(ctx context.Context, input map[string]any) (coretools.Result, error) {
			rawURL := stringArg(input, "url")
			method := strings.ToUpper(stringArg(input, "method"))
			if method == "" {
				method = http.MethodGet
			}
			if method != http.MethodGet && method != http.MethodPost && method != http.MethodHead {
				return coretools.Result{}, fmt.Errorf("aoa_external_request: method %q is not allowed", method)
			}
			req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
			if err != nil {
				return coretools.Result{}, err
			}
			scope.Logger.Info("outbound tool request",
				"host", req.URL.Host,
				"method", method,
				"session_id", scope.SessionID,
				"trace_id", scope.TraceID,
			)
			resp, err := scope.Proxy.Do(ctx, req)
			if err != nil {
				scope.Logger.Warn("outbound tool request blocked",
					"host", req.URL.Host,
					"error", telemetry.Redact(err.Error(), 256),
					"trace_id", scope.TraceID,
				)
				return coretools.Result{}, err
			}
			defer resp.Body.Close()
			return coretools.Result{Content: fmt.Sprintf("status %d", resp.StatusCode)}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	tools = append(tools, request)

	rollback, err := coretools.NewFunc(
		"aoa_rollback",
		"Roll back a production change through the credential proxy. This call is suspended until a human approves it.",
		schema.Object(map[string]schema.Schema{
			"target": schema.String("change identifier"),
		}, "target"),
		func(ctx context.Context, input map[string]any) (coretools.Result, error) {
			target := "https://gateway.internal/rollback?target=" + url.QueryEscape(stringArg(input, "target"))
			req, err := http.NewRequestWithContext(ctx, "POST", target, nil)
			if err != nil {
				return coretools.Result{}, err
			}
			resp, err := scope.Proxy.Do(ctx, req)
			if err != nil {
				return coretools.Result{}, err
			}
			defer resp.Body.Close()
			return coretools.Result{Content: fmt.Sprintf("rollback status %d", resp.StatusCode)}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	tools = append(tools, rollback)

	drop, err := coretools.NewFunc(
		"aoa_drop_database",
		"Destructive operation blocked by the auto-mode classifier.",
		schema.Object(map[string]schema.Schema{
			"name": schema.String("database name"),
		}, "name"),
		func(_ context.Context, _ map[string]any) (coretools.Result, error) {
			return coretools.Result{Content: "should never run"}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	tools = append(tools, drop)

	guarded := make([]coretools.Tool, 0, len(tools))
	for _, t := range tools {
		guarded = append(guarded, &guardedTool{
			inner: t, classifier: scope.Classifier, deferAllowed: scope.Approved,
		})
	}
	return guarded, nil
}

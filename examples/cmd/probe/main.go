// Command aoa-probe drives the compose examples. It calls the langchain-golang
// Ollama client directly, emits events in each protocol, and receives outbox
// deliveries. All binaries in examples/*/bin come from this package.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/projanvil/langchain-golang/core/messages"
	"github.com/projanvil/langchain-golang/langchain/agents"
	"github.com/projanvil/langchain-golang/langchain/chatmodels"
	"github.com/projanvil/langchain-golang/langgraph/checkpoint"
	graphpkg "github.com/projanvil/langchain-golang/langgraph/graph"
	"github.com/projanvil/langchain-golang/langgraph/prebuilt"
	_ "github.com/projanvil/langchain-golang/partners/ollama" // registers "ollama"

	"github.com/projanvil/aoa/internal/aoa"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "hello":
		err = hello(os.Args[2:])
	case "agent-hello":
		err = agentHello(os.Args[2:])
	case "emit":
		err = emit(os.Args[2:])
	case "publish-nats":
		err = publishNATS(os.Args[2:])
	case "publish-kafka":
		err = publishKafka(os.Args[2:])
	case "publish-redis":
		err = publishRedis(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	case "scenario":
		err = scenario(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatalf("aoa-probe %s: %v", os.Args[1], err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: aoa-probe <command> [flags]
  hello                         call the langchain-golang Ollama client
  agent-hello                   run a compiled LangGraph create_react_agent
  emit --target URL             send a CloudEvent (or --dapr/--webhook/--admission)
  publish-nats --url --subject   publish a JetStream message
  publish-kafka --brokers --topic
  publish-redis --addr --stream
  serve --addr                  receive outbox deliveries
  scenario <name> --target URL   dedup | memory | timer | approval`)
}

// hello proves the langchain-golang Ollama client works against a local model.
func hello(args []string) error {
	fs := flag.NewFlagSet("hello", flag.ExitOnError)
	model := fs.String("model", env("OLLAMA_MODEL", "qwen2.5:0.5b"), "Ollama model name")
	fs.Parse(args)

	// OLLAMA_HOST is read by partners/ollama's provider factory at init().
	if v := os.Getenv("OLLAMA_HOST"); v == "" {
		_ = os.Setenv("OLLAMA_HOST", "http://localhost:11434")
	}
	chatModel, err := chatmodels.Resolve(chatmodels.ChatModelSpec{
		Provider: "ollama", Model: *model,
	})
	if err != nil {
		return fmt.Errorf("resolve ollama model: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	reply, err := chatModel.Invoke(ctx, []messages.Message{
		messages.Human("Reply with exactly these two words and nothing else: Hello, world!"),
	})
	if err != nil {
		return fmt.Errorf("ollama invoke: %w", err)
	}
	text := strings.TrimSpace(messages.Text(reply))
	fmt.Printf("HELLO WORLD from langchain-golang ollama client\nmodel=%s\nreply=%s\n", *model, text)
	if text == "" {
		return fmt.Errorf("ollama returned an empty reply")
	}
	return nil
}

// agentHello runs the full LangGraph Go runtime: create_react_agent +
// checkpointer + a threaded Invoke. This is the "real langchain golang"
// path, not a direct chat-model call.
func agentHello(args []string) error {
	fs := flag.NewFlagSet("agent-hello", flag.ExitOnError)
	model := fs.String("model", env("OLLAMA_MODEL", "qwen2.5:0.5b"), "Ollama model name")
	fs.Parse(args)

	if v := os.Getenv("OLLAMA_HOST"); v == "" {
		_ = os.Setenv("OLLAMA_HOST", "http://localhost:11434")
	}
	chatModel, err := chatmodels.Resolve(chatmodels.ChatModelSpec{
		Provider: "ollama", Model: *model,
	})
	if err != nil {
		return fmt.Errorf("resolve ollama model: %w", err)
	}
	compiled, err := prebuilt.CreateReactAgent(chatModel, nil,
		agents.WithAgentCheckpointer(checkpoint.NewMemorySaver()))
	if err != nil {
		return fmt.Errorf("create react agent: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	state, interrupts, err := compiled.InvokeWithStateOptions(ctx,
		[]messages.Message{messages.Human("Reply with exactly these two words and nothing else: Hello, world!")},
		graphpkg.Options{ThreadID: "example-agent-hello"})
	if err != nil {
		return fmt.Errorf("agent invoke: %w", err)
	}
	if len(interrupts) > 0 {
		return fmt.Errorf("agent paused with %d interrupt(s)", len(interrupts))
	}
	history, _ := state["messages"].([]messages.Message)
	reply := ""
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == messages.RoleAI {
			reply = strings.TrimSpace(messages.Text(history[i]))
			break
		}
	}
	fmt.Printf("HELLO WORLD from langchain-golang create_react_agent\nmodel=%s\nreply=%s\n",
		*model, reply)
	if reply == "" {
		return fmt.Errorf("agent returned an empty reply")
	}
	return nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type emitFlags struct {
	target, ceType, sourceID, session, payload, mode string
}

func parseEmit(name string, args []string) (*emitFlags, error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	out := &emitFlags{}
	fs.StringVar(&out.target, "target", "http://aoad:8080/events", "aoa endpoint")
	fs.StringVar(&out.ceType, "type", aoa.CETypeWebhook, "CloudEvent type")
	fs.StringVar(&out.sourceID, "source-id", "", "dedup id; empty generates one")
	fs.StringVar(&out.session, "session", "s1", "session id")
	fs.StringVar(&out.payload, "payload", "P95 > 800ms", "event payload")
	fs.StringVar(&out.mode, "mode", "cloudevents", "cloudevents|dapr|webhook|admission")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if out.sourceID == "" {
		out.sourceID = fmt.Sprintf("probe-%d", time.Now().UnixNano())
	}
	return out, nil
}

func emitBody(f *emitFlags) ([]byte, error) {
	switch f.mode {
	case "cloudevents":
		e := cloudevents.NewEvent()
		e.SetID(f.sourceID)
		e.SetSource("aoa-probe")
		e.SetType(f.ceType)
		if err := e.SetData(cloudevents.ApplicationJSON, map[string]any{
			"session_id": f.session, "payload": f.payload,
		}); err != nil {
			return nil, err
		}
		return json.Marshal(e)
	case "dapr":
		return json.Marshal(map[string]any{
			"id": f.sourceID, "source": "aoa-probe", "type": f.ceType,
			"topic": "aoa-events", "pubsubname": "aoa",
			"datacontenttype": "application/json",
			"data":            map[string]any{"session_id": f.session, "payload": f.payload},
		})
	case "webhook":
		return json.Marshal(map[string]any{
			"source_id": f.sourceID, "session_id": f.session, "payload": f.payload,
		})
	case "admission":
		return json.Marshal(map[string]any{
			"apiVersion": "admission.k8s.io/v1",
			"kind":       "AdmissionReview",
			"request": map[string]any{
				"uid": f.sourceID, "namespace": f.session, "operation": "CREATE",
				"object": map[string]any{"kind": "Pod", "metadata": map[string]any{"name": f.payload}},
			},
		})
	default:
		return nil, fmt.Errorf("unknown mode %q", f.mode)
	}
}

func emit(args []string) error {
	f, err := parseEmit("emit", args)
	if err != nil {
		return err
	}
	return postOnce(f)
}

func postOnce(f *emitFlags) error {
	body, err := emitBody(f)
	if err != nil {
		return err
	}
	contentType := "application/json"
	if f.mode == "cloudevents" {
		contentType = cloudevents.ApplicationCloudEventsJSON
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	reply, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	fmt.Printf("emit mode=%s source=%s status=%d body=%s\n",
		f.mode, f.sourceID, resp.StatusCode, strings.TrimSpace(string(reply)))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("aoa returned %d", resp.StatusCode)
	}
	return nil
}

// scenario groups extra example ideas that do not need a large model.
func scenario(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("scenario name is required: dedup|memory|timer|approval")
	}
	name := args[0]
	f, err := parseEmit("scenario", args[1:])
	if err != nil {
		return err
	}
	switch name {
	case "dedup":
		f.sourceID = "probe-dedup-fixed"
		if err := postOnce(f); err != nil {
			return err
		}
		return postOnce(f) // second POST must return 200
	case "memory":
		f.session = "session-memory-demo"
		f.payload = "Remember: the deployment id is cfg-4821."
		if err := postOnce(f); err != nil {
			return err
		}
		f.sourceID = "probe-memory-2"
		f.payload = "What was the deployment id?"
		return postOnce(f)
	case "timer":
		f.payload = "Check the P95 latency again in 30 seconds."
	case "approval":
		f.payload = "Roll back production config cfg-4821."
	}
	return postOnce(f)
}

func publishNATS(args []string) error {
	fs := flag.NewFlagSet("publish-nats", flag.ExitOnError)
	url := fs.String("url", "nats://nats:4222", "NATS URL")
	subject := fs.String("subject", "aoa.events", "subject")
	payload := fs.String("payload", "P95 > 800ms", "message body")
	fs.Parse(args)
	nc, err := nats.Connect(*url)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	// The aoa listener uses a durable JetStream subscription, so the stream
	// must exist before a message is published.
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "AOA", Subjects: []string{*subject}, Storage: nats.FileStorage,
	}, nats.MaxWait(15*time.Second))
	if err != nil && !strings.Contains(err.Error(), "stream name already in use") {
		return err
	}
	if _, err := js.Publish(*subject, []byte(*payload)); err != nil {
		return err
	}
	fmt.Printf("published JetStream subject=%s payload=%s\n", *subject, *payload)
	return nil
}

func publishKafka(args []string) error {
	fs := flag.NewFlagSet("publish-kafka", flag.ExitOnError)
	brokers := fs.String("brokers", "redpanda:9092", "Kafka brokers")
	topic := fs.String("topic", "aoa-events", "topic")
	payload := fs.String("payload", "P95 > 800ms", "message body")
	fs.Parse(args)
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(*brokers, ","), Topic: *topic,
	})
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, kafka.Message{Value: []byte(*payload)}); err != nil {
		return err
	}
	fmt.Printf("published Kafka topic=%s payload=%s\n", *topic, *payload)
	return nil
}

func publishRedis(args []string) error {
	fs := flag.NewFlagSet("publish-redis", flag.ExitOnError)
	addr := fs.String("addr", "redis:6379", "Redis address")
	stream := fs.String("stream", "aoa-events", "stream key")
	payload := fs.String("payload", "P95 > 800ms", "message body")
	fs.Parse(args)
	client := redis.NewClient(&redis.Options{Addr: *addr})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: *stream,
		Values: map[string]any{"payload": *payload, "session_id": "s1"},
	}).Result()
	if err != nil {
		return err
	}
	fmt.Printf("published Redis stream=%s id=%s payload=%s\n", *stream, id, *payload)
	return nil
}

// serve is the outbox webhook receiver. It prints every delivered message,
// which is how the examples show the agent's final answer.
func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":9000", "listen address")
	fs.Parse(args)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		fmt.Printf("OUTBOX DELIVERY %s\n", strings.TrimSpace(string(body)))
		w.WriteHeader(http.StatusOK)
	})
	fmt.Printf("outbox receiver listening on %s\n", *addr)
	return http.ListenAndServe(*addr, mux)
}
